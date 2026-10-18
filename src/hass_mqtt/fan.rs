use crate::hass_mqtt::base::{Device, EntityConfig, Origin};
use crate::hass_mqtt::instance::{publish_entity_config, EntityInstance};
use crate::hass_mqtt::work_mode::{ParsedWorkMode, WorkMode};
use crate::service::device::Device as ServiceDevice;
use crate::service::hass::{availability_topic, topic_safe_id, HassClient, IdParameter};
use crate::service::state::StateHandle;
use anyhow::anyhow;
use async_trait::async_trait;
use mosquitto_rs::router::{Params, Payload, State};
use serde::Serialize;
use serde_json::Value as JsonValue;

/// hass resets the percentage or preset mode when it receives this payload.
const PAYLOAD_RESET: &str = "None";

/// <https://www.home-assistant.io/integrations/fan.mqtt/>
#[derive(Serialize, Clone, Debug)]
pub struct FanConfig {
    #[serde(flatten)]
    pub base: EntityConfig,

    pub command_topic: String,
    pub state_topic: String,

    pub percentage_command_topic: String,
    pub percentage_state_topic: String,
    /// hass scales its percentage onto this range before publishing to
    /// `percentage_command_topic`, so we send and receive the device's own
    /// speed values.
    pub speed_range_min: i64,
    pub speed_range_max: i64,

    pub preset_mode_command_topic: String,
    pub preset_mode_state_topic: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub preset_modes: Vec<String>,

    pub optimistic: bool,
}

/// An air purifier, presented the way hass expects one: a fan whose speed
/// slider drives the work mode that has selectable speeds (the H7129's
/// gearMode: Low, Medium, High), and whose presets are the remaining work
/// modes (Auto, Sleep, Turbo).
#[derive(Clone)]
pub struct AirPurifierFan {
    fan: FanConfig,
    state: StateHandle,
    device_id: String,
}

/// The work mode whose values are fan speeds.
fn speed_mode(work_modes: &ParsedWorkMode) -> Option<&WorkMode> {
    work_modes.modes.values().find(|m| !m.values.is_empty())
}

fn speed_range(mode: &WorkMode) -> Option<(i64, i64)> {
    let speeds = mode.values.iter().filter_map(|v| v.value.as_i64());
    Some((speeds.clone().min()?, speeds.max()?))
}

impl AirPurifierFan {
    pub fn new(device: &ServiceDevice, state: &StateHandle) -> anyhow::Result<Self> {
        let work_modes = ParsedWorkMode::with_device(device)?;
        let speed = speed_mode(&work_modes)
            .ok_or_else(|| anyhow!("{device} has no work mode with selectable speeds"))?;
        let (speed_range_min, speed_range_max) = speed_range(speed)
            .ok_or_else(|| anyhow!("{device} work mode {} has no speeds", speed.name))?;

        let preset_modes = work_modes
            .modes
            .values()
            .filter(|m| m.values.is_empty())
            .map(|m| m.name.to_string())
            .collect();

        let id = topic_safe_id(device);

        Ok(Self {
            fan: FanConfig {
                base: EntityConfig {
                    availability_topic: availability_topic(),
                    // Take the device name, as this entity is the device.
                    name: None,
                    device_class: None,
                    origin: Origin::default(),
                    device: Device::for_device(device),
                    unique_id: format!("gv2mqtt-{id}-fan"),
                    entity_category: None,
                    icon: device.resolve_quirk().map(|q| q.icon.to_string()),
                },
                // Power is routed to the general power switch handler
                command_topic: format!("gv2mqtt/switch/{id}/command/powerSwitch"),
                state_topic: format!("gv2mqtt/fan/{id}/state"),
                percentage_command_topic: format!("gv2mqtt/fan/{id}/set-speed"),
                percentage_state_topic: format!("gv2mqtt/fan/{id}/speed"),
                speed_range_min,
                speed_range_max,
                preset_mode_command_topic: format!("gv2mqtt/{id}/set-work-mode"),
                preset_mode_state_topic: format!("gv2mqtt/fan/{id}/preset"),
                preset_modes,
                optimistic: false,
            },
            state: state.clone(),
            device_id: device.id.to_string(),
        })
    }
}

/// The active work mode and its parameter, preferring the IoT reports,
/// which are pushed as soon as anything changes, over the polled platform
/// state.
fn current_work_mode(device: &ServiceDevice) -> Option<(JsonValue, Option<i64>)> {
    if let Some(mode) = device.humidifier_work_mode {
        let param = device
            .humidifier_param_by_mode
            .get(&mode)
            .map(|&p| p.into());
        return Some((mode.into(), param));
    }

    let cap = device.get_state_capability_by_instance("workMode")?;
    let mode = cap.state.pointer("/value/workMode")?.clone();
    let param = cap
        .state
        .pointer("/value/modeValue")
        .and_then(|v| v.as_i64());
    Some((mode, param))
}

#[async_trait]
impl EntityInstance for AirPurifierFan {
    async fn publish_config(&self, state: &StateHandle, client: &HassClient) -> anyhow::Result<()> {
        publish_entity_config("fan", state, client, &self.fan.base, &self.fan).await
    }

    async fn notify_state(&self, client: &HassClient) -> anyhow::Result<()> {
        let device = self
            .state
            .device_by_id(&self.device_id)
            .await
            .expect("device to exist");

        let is_on = device.device_state().map(|s| s.on).unwrap_or(false);
        client
            .publish(&self.fan.state_topic, if is_on { "ON" } else { "OFF" })
            .await?;

        let work_modes = ParsedWorkMode::with_device(&device)?;
        let Some((mode_value, param)) = current_work_mode(&device) else {
            return Ok(());
        };
        let Some(mode) = work_modes.mode_for_value(&mode_value) else {
            log::warn!("{device} reported unknown work mode {mode_value}");
            return Ok(());
        };

        // A device is either running at a chosen speed or in a preset; hass
        // shows whichever one we don't reset.
        let speed = speed_mode(&work_modes)
            .filter(|speed| speed.name == mode.name)
            .and(param)
            .filter(|p| (self.fan.speed_range_min..=self.fan.speed_range_max).contains(p));

        match speed {
            Some(speed) => {
                client
                    .publish(&self.fan.percentage_state_topic, speed.to_string())
                    .await?;
                client
                    .publish(&self.fan.preset_mode_state_topic, PAYLOAD_RESET)
                    .await
            }
            None => {
                client
                    .publish(&self.fan.percentage_state_topic, PAYLOAD_RESET)
                    .await?;
                client
                    .publish(&self.fan.preset_mode_state_topic, mode.name.to_string())
                    .await
            }
        }
    }
}

/// hass moved the speed slider. The value is already scaled onto
/// `speed_range_min..=speed_range_max`; 0 means off.
pub async fn mqtt_fan_set_speed(
    Payload(speed): Payload<i64>,
    Params(IdParameter { id }): Params<IdParameter>,
    State(state): State<StateHandle>,
) -> anyhow::Result<()> {
    log::info!("mqtt_fan_set_speed: {id}: {speed}");
    let device = state.resolve_device_for_control(&id).await?;

    if speed == 0 {
        return state.device_power_on(&device, false).await;
    }

    let work_modes = ParsedWorkMode::with_device(&device)?;
    let mode = speed_mode(&work_modes)
        .ok_or_else(|| anyhow!("{device} has no work mode with selectable speeds"))?;
    let mode_num = mode
        .value
        .as_i64()
        .ok_or_else(|| anyhow!("expected workMode to be a number"))?;

    state
        .humidifier_set_parameter(&device, mode_num, speed)
        .await
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::platform_api::{from_json, HttpDeviceState};
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct StateFixture {
        payload: HttpDeviceState,
    }

    fn h7129() -> ServiceDevice {
        let mut device = ServiceDevice::new("H7129", "1B:42:D4:13:68:2B:C1:A4");
        device.http_device_info =
            Some(from_json(include_str!("../../test-data/h7129_platform_metadata.json")).unwrap());
        device
    }

    #[test]
    fn h7129_speeds_and_presets() {
        let work_modes = ParsedWorkMode::with_device(&h7129()).unwrap();
        let speed = speed_mode(&work_modes).unwrap();
        assert_eq!(speed.name, "gearMode");
        assert_eq!(speed_range(speed), Some((1, 3)));

        let mut presets: Vec<_> = work_modes
            .modes
            .values()
            .filter(|m| m.values.is_empty())
            .map(|m| m.name.as_str())
            .collect();
        presets.sort();
        assert_eq!(presets, vec!["Auto", "Sleep", "Turbo"]);
    }

    #[test]
    fn iot_work_mode_wins_over_platform_state() {
        let mut device = h7129();
        let fixture: StateFixture =
            from_json(include_str!("../../test-data/h7129_platform_state.json")).unwrap();
        device.set_http_device_state(fixture.payload);
        // The platform fixture is gearMode, Low.
        assert_eq!(current_work_mode(&device), Some((1.into(), Some(1))));

        // IoT then reports Turbo.
        device.set_work_mode(7);
        assert_eq!(current_work_mode(&device).map(|(m, _)| m), Some(7.into()));
    }
}
