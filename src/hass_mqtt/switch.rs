use crate::hass_mqtt::base::{Device, EntityConfig, Origin};
use crate::hass_mqtt::instance::{publish_entity_config, EntityInstance};
use crate::platform_api::DeviceCapability;
use crate::service::device::Device as ServiceDevice;
use crate::service::hass::{
    availability_topic, camel_case_to_space_separated, switch_instance_state_topic, topic_safe_id,
    HassClient,
};
use crate::service::state::StateHandle;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::json;

#[derive(Serialize, Clone, Debug)]
pub struct SwitchConfig {
    #[serde(flatten)]
    pub base: EntityConfig,
    pub command_topic: String,
    pub state_topic: String,
}

impl SwitchConfig {
    pub async fn for_device(
        device: &ServiceDevice,
        instance: &DeviceCapability,
    ) -> anyhow::Result<Self> {
        let command_topic = format!(
            "gv2mqtt/switch/{id}/command/{inst}",
            id = topic_safe_id(device),
            inst = instance.instance
        );
        let state_topic = switch_instance_state_topic(device, &instance.instance);
        let availability_topic = availability_topic();
        let unique_id = format!(
            "gv2mqtt-{id}-{inst}",
            id = topic_safe_id(device),
            inst = instance.instance
        );

        Ok(Self {
            base: EntityConfig {
                availability_topic,
                name: Some(camel_case_to_space_separated(&instance.instance)),
                device_class: None,
                origin: Origin::default(),
                device: Device::for_device(device),
                unique_id,
                entity_category: None,
                icon: None,
            },
            command_topic,
            state_topic,
        })
    }

    pub async fn publish(&self, state: &StateHandle, client: &HassClient) -> anyhow::Result<()> {
        publish_entity_config("switch", state, client, &self.base, self).await
    }
}

pub struct CapabilitySwitch {
    switch: SwitchConfig,
    device_id: String,
    state: StateHandle,
    instance_name: String,
}

impl CapabilitySwitch {
    pub async fn new(
        device: &ServiceDevice,
        state: &StateHandle,
        instance: &DeviceCapability,
    ) -> anyhow::Result<Self> {
        let switch = SwitchConfig::for_device(device, instance).await?;
        Ok(Self {
            switch,
            device_id: device.id.to_string(),
            state: state.clone(),
            instance_name: instance.instance.to_string(),
        })
    }
}

/// H1310/H1370 ceiling fans report empty platform state for most toggles
/// (`fanToggle`, `mainLightToggle`, `reverseAirflowToggle`, ...) rather than
/// omitting the capability or returning a numeric value. Other device
/// families are not known to exhibit this, so the "default to OFF instead of
/// leaving the entity unknown" fallbacks below are intentionally scoped to
/// this family to avoid changing behavior for devices that legitimately mean
/// "unknown" when they return an empty string.
fn is_empty_state_quirk_device(device: &ServiceDevice) -> bool {
    matches!(device.sku.as_str(), "H1310" | "H1370")
}

/// Resolve ON/OFF for platform toggle capabilities (not powerSwitch).
/// Priority: numeric platform value → decoded IoT state → optimistic cache →
/// H1310/H1370 inference → (H1310/H1370 only) empty-string OFF default.
pub fn resolve_capability_toggle_state(device: &ServiceDevice, instance: &str) -> Option<bool> {
    if let Some(cap) = device.get_state_capability_by_instance(instance) {
        if let Some(n) = cap.state.pointer("/value").and_then(|v| v.as_i64()) {
            return Some(n != 0);
        }
    }

    // Decoded IoT notifications are the only source that observes changes
    // made outside of Home Assistant, so they outrank the optimistic cache
    // (which only ever reflects commands that we ourselves sent).
    if let Some(on) = device.iot_toggle_state(instance) {
        return Some(on);
    }

    if let Some(on) = device.get_toggle_capability_state(instance) {
        return Some(on);
    }

    if let Some(on) = inferred_toggle_state(device, instance) {
        return Some(on);
    }

    if !is_empty_state_quirk_device(device) {
        return None;
    }

    if let Some(cap) = device.get_state_capability_by_instance(instance) {
        if cap.state.pointer("/value") == Some(&json!("")) {
            return Some(false);
        }
        log::warn!("CapabilitySwitch: unhandled platform state for {instance}: {cap:#?}");
        return Some(false);
    }

    // No platform state at all yet. This is the situation at initial
    // registration, before the first poll has landed. Returning None here
    // would publish nothing, leaving the HASS entity `unknown` and rendering
    // it as force-off/force-on bolt buttons instead of a toggle. These
    // devices have no meaningful state to report until a poll arrives, so
    // seed them OFF; the first poll corrects it if it is actually on.
    Some(false)
}

/// Infer toggle state for H1310/H1370 when Govee returns empty platform values.
pub fn inferred_toggle_state(device: &ServiceDevice, instance: &str) -> Option<bool> {
    if !is_empty_state_quirk_device(device) || !device.needs_platform_poll() {
        return None;
    }

    match instance {
        "mainLightToggle" => device
            .device_state()
            .map(|state| state.brightness > 0 || state.on),
        "fanToggle" => {
            if device.get_mode_capability_label("fanSpeedMode").is_some() {
                return Some(true);
            }
            device
                .get_state_capability_by_instance("fanSpeedMode")
                .and_then(|cap| cap.state.pointer("/value"))
                .and_then(|v| v.as_i64())
                .map(|n| n > 0)
        }
        // Govee reports no usable state for these, and there is nothing else
        // in the payload to infer them from. They fall through to the
        // empty-string OFF default so that HASS gets a definite state rather
        // than rendering the switch as force-off/force-on bolt buttons.
        "backgroundLightToggle" | "reverseAirflowToggle" => None,
        _ => None,
    }
}

fn toggle_mqtt_payload(on: bool) -> &'static str {
    if on {
        "ON"
    } else {
        "OFF"
    }
}

#[async_trait]
impl EntityInstance for CapabilitySwitch {
    async fn publish_config(&self, state: &StateHandle, client: &HassClient) -> anyhow::Result<()> {
        self.switch.publish(state, client).await
    }

    async fn notify_state(&self, client: &HassClient) -> anyhow::Result<()> {
        let device = self
            .state
            .device_by_id(&self.device_id)
            .await
            .expect("device to exist");

        if self.instance_name == "powerSwitch" {
            if let Some(state) = device.device_state() {
                client
                    .publish(&self.switch.state_topic, toggle_mqtt_payload(state.on))
                    .await?;
            }
            return Ok(());
        }

        if let Some(on) = resolve_capability_toggle_state(&device, &self.instance_name) {
            return client
                .publish(&self.switch.state_topic, toggle_mqtt_payload(on))
                .await;
        }

        log::trace!(
            "CapabilitySwitch::notify_state: no state for {device} {instance}",
            instance = self.instance_name
        );
        Ok(())
    }
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

    fn h1310_with_platform_state() -> ServiceDevice {
        let mut device = ServiceDevice::new("H1310", "47:64:F8:9C:BD:BC:DF:4A");
        let fixture: StateFixture =
            from_json(include_str!("../../test-data/h1310_platform_state.json")).unwrap();
        device.set_http_device_state(fixture.payload);
        device
    }

    #[test]
    fn empty_platform_toggle_defaults_off() {
        let device = h1310_with_platform_state();
        assert_eq!(
            resolve_capability_toggle_state(&device, "fanToggle"),
            Some(false)
        );
        assert_eq!(
            resolve_capability_toggle_state(&device, "reverseAirflowToggle"),
            Some(false)
        );
    }

    #[test]
    fn optimistic_toggle_state() {
        let mut device = h1310_with_platform_state();
        device.set_toggle_capability_state("fanToggle", true);
        assert_eq!(
            resolve_capability_toggle_state(&device, "fanToggle"),
            Some(true)
        );
    }

    #[test]
    fn inferred_main_light_from_brightness() {
        let device = h1310_with_platform_state();
        assert_eq!(
            inferred_toggle_state(&device, "mainLightToggle"),
            Some(true)
        );
    }

    #[test]
    fn inferred_fan_from_mode_label() {
        let mut device = h1310_with_platform_state();
        device.set_mode_capability_label("fanSpeedMode", "Speed 3".to_string());
        assert_eq!(inferred_toggle_state(&device, "fanToggle"), Some(true));
        assert_eq!(
            resolve_capability_toggle_state(&device, "fanToggle"),
            Some(true)
        );
    }

    /// Non-H1310/H1370 devices must keep the pre-existing behavior: an
    /// empty-string platform value (Govee's "no meaningful data yet" state)
    /// leaves the entity state unresolved (`None`, reported as "unknown" in
    /// HA) instead of being defaulted to OFF. Only the H1310/H1370 family
    /// has the empty-state quirk that justifies the OFF fallback.
    #[test]
    fn non_h1310_empty_platform_toggle_stays_unknown() {
        let mut device = ServiceDevice::new("H7131", "some-other-device-id");
        device.set_http_device_state(HttpDeviceState {
            sku: "H7131".to_string(),
            device: "some-other-device-id".to_string(),
            capabilities: vec![crate::platform_api::DeviceCapabilityState {
                kind: crate::platform_api::DeviceCapabilityKind::Toggle,
                instance: "gradientToggle".to_string(),
                state: json!({ "value": "" }),
            }],
        });

        assert_eq!(
            resolve_capability_toggle_state(&device, "gradientToggle"),
            None
        );
    }

    /// At initial registration no platform state has been fetched yet. The
    /// quirk devices must still resolve to a definite OFF so that HASS gets a
    /// state on the topic; otherwise the switch renders as force-off/force-on
    /// bolt buttons rather than a toggle.
    #[test]
    fn quirk_device_without_platform_state_defaults_off() {
        let device = ServiceDevice::new("H1310", "47:64:F8:9C:BD:BC:DF:4A");
        assert!(device.http_device_state.is_none());

        for instance in [
            "fanToggle",
            "mainLightToggle",
            "backgroundLightToggle",
            "reverseAirflowToggle",
        ] {
            assert_eq!(
                resolve_capability_toggle_state(&device, instance),
                Some(false),
                "{instance} should seed OFF before the first poll"
            );
        }
    }

    /// The empty-string platform value must resolve to OFF for every H1310
    /// toggle, including backgroundLightToggle which has no inference rule.
    #[test]
    fn background_light_toggle_defaults_off() {
        let device = h1310_with_platform_state();
        assert_eq!(
            resolve_capability_toggle_state(&device, "backgroundLightToggle"),
            Some(false)
        );
    }

    #[test]
    fn numeric_platform_toggle() {
        let mut device = h1310_with_platform_state();
        let cap = device
            .http_device_state
            .as_mut()
            .unwrap()
            .capabilities
            .iter_mut()
            .find(|c| c.instance == "fanToggle")
            .unwrap();
        cap.state = json!({ "value": 1 });
        assert_eq!(
            resolve_capability_toggle_state(&device, "fanToggle"),
            Some(true)
        );
    }
}
