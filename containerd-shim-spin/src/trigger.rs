use std::{collections::HashSet, path::Path};

use anyhow::Result;
use futures::{future::BoxFuture, FutureExt};
use log::{debug, info};
use spin_app::{locked::LockedApp, locked::LockedTrigger, App};
use spin_runtime_factors::{FactorsBuilder, TriggerAppArgs, TriggerFactors};
use spin_trigger::{
    cli::{FactorsConfig, TriggerAppBuilder, UserProvidedPath},
    loader::ComponentLoader,
    Trigger,
};
use spin_trigger_http::HttpTrigger;
use spin_trigger_redis::RedisTrigger;
use trigger_command::CommandTrigger;
use trigger_mqtt::MqttTrigger;
use trigger_sqs::SqsTrigger;

use crate::constants::{RUNTIME_CONFIG_PATH, SPIN_TRIGGER_WORKING_DIR};

pub(crate) const HTTP_TRIGGER_TYPE: &str = <HttpTrigger as Trigger<TriggerFactors>>::TYPE;
pub(crate) const REDIS_TRIGGER_TYPE: &str = <RedisTrigger as Trigger<TriggerFactors>>::TYPE;
pub(crate) const SQS_TRIGGER_TYPE: &str = <SqsTrigger as Trigger<TriggerFactors>>::TYPE;
pub(crate) const MQTT_TRIGGER_TYPE: &str = <MqttTrigger as Trigger<TriggerFactors>>::TYPE;
pub(crate) const COMMAND_TRIGGER_TYPE: &str = <CommandTrigger as Trigger<TriggerFactors>>::TYPE;

/// Run the trigger with the given CLI args, [`App`] and [`ComponentLoader`].
pub(crate) async fn run<T>(
    cli_args: T::CliArgs,
    app: App,
    loader: &'static ComponentLoader,
) -> Result<BoxFuture<'static, Result<()>>>
where
    T: Trigger<TriggerFactors> + 'static,
{
    info!(" >>> running {} trigger", T::TYPE);
    let trigger = T::new(cli_args, &app)?;
    let builder: TriggerAppBuilder<_, FactorsBuilder> = TriggerAppBuilder::new(trigger);
    let builder_args = match std::env::var("SPIN_MAX_INSTANCE_MEMORY") {
        Ok(limit) => {
            debug!("Setting instance max memory to {limit} bytes");
            let mut args = TriggerAppArgs::default();
            args.max_instance_memory = limit.parse().ok();
            args
        }
        Err(_) => Default::default(),
    };
    let future = builder
        .run(app, factors_config(), builder_args, loader)
        .await?;
    Ok(future.boxed())
}

/// Configuration for the factors.
fn factors_config() -> FactorsConfig {
    // Load in runtime config if one exists at expected location
    let runtime_config_file = Path::new(RUNTIME_CONFIG_PATH)
        .exists()
        .then(|| RUNTIME_CONFIG_PATH.into());
    // Configure the application state directory path. This is used in the default
    // locations for logs, key value stores, etc.
    FactorsConfig {
        working_dir: SPIN_TRIGGER_WORKING_DIR.into(),
        runtime_config_file,
        // This is the default base for the state_dir (.spin) unless it is
        // explicitly configured via the runtime config.
        local_app_dir: Some(SPIN_TRIGGER_WORKING_DIR.to_string()),
        // Explicitly do not set log dir in order to force logs to be displayed to stdout.
        // Otherwise, would default to the state directory.
        log_dir: UserProvidedPath::Unset,
        ..Default::default()
    }
}

/// get the supported trigger types from the `LockedApp`.
///
/// this function filters the trigger types to only return the ones that are currently supported.
/// If an unsupported trigger type is found, it returns an error indicating which trigger type is unsupported.
///
/// supported trigger types include:
/// - redis
/// - http
/// - sqs
/// - mqtt
/// - command
///
/// Note: this function returns a `HashSet` of supported trigger types. Duplicates are removed.
pub(crate) fn get_supported_triggers(locked_app: &LockedApp) -> anyhow::Result<HashSet<String>> {
    let supported_triggers: HashSet<&str> = HashSet::from([
        HTTP_TRIGGER_TYPE,
        REDIS_TRIGGER_TYPE,
        SQS_TRIGGER_TYPE,
        COMMAND_TRIGGER_TYPE,
        MQTT_TRIGGER_TYPE,
    ]);

    locked_app.triggers.iter()
        .map(|trigger| {
            let trigger_type = &trigger.trigger_type;
            if !supported_triggers.contains(trigger_type.as_str()) {
                Err(anyhow::anyhow!(
                    "Only Http, Redis, MQTT, SQS, and Command triggers are currently supported. Found unsupported trigger: {trigger_type:?}"
                ))
            } else {
                Ok(trigger_type.clone())
            }
        })
        .collect::<anyhow::Result<HashSet<_>>>()
}

/// Log non-HTTP trigger configuration to stdout at startup.
///
/// The HTTP trigger already prints its listen address and routes; this prints
/// summary lines for the other supported trigger types in the same style.
pub(crate) fn log_triggers_at_startup(locked_app: &LockedApp) {
    for line in format_non_http_trigger_lines(locked_app) {
        println!("{line}");
    }
}

fn format_non_http_trigger_lines(locked_app: &LockedApp) -> Vec<String> {
    let non_http_triggers: Vec<_> = locked_app
        .triggers
        .iter()
        .filter(|trigger| trigger.trigger_type.as_str() != HTTP_TRIGGER_TYPE)
        .collect();

    if non_http_triggers.is_empty() {
        return Vec::new();
    }

    let mut lines = vec!["Available Triggers:".to_string()];
    for trigger in non_http_triggers {
        lines.push(format_trigger_line(trigger));
    }
    lines
}

fn format_trigger_line(trigger: &LockedTrigger) -> String {
    let config = &trigger.trigger_config;
    if let Some(component) = config.get("component").and_then(|value| value.as_str()) {
        return match trigger.trigger_type.as_str() {
            MQTT_TRIGGER_TYPE => {
                let topic = config_field(config, "topic");
                format!("  {component}: mqtt topic {topic}")
            }
            REDIS_TRIGGER_TYPE => {
                let channel = config_field(config, "channel");
                let address = config
                    .get("address")
                    .and_then(|value| value.as_str())
                    .unwrap_or("(from application trigger config)");
                format!("  {component}: redis channel {channel} @ {address}")
            }
            SQS_TRIGGER_TYPE => {
                let queue_url = config_field(config, "queue_url");
                format!("  {component}: sqs queue {queue_url}")
            }
            COMMAND_TRIGGER_TYPE => format!("  {component}: command trigger"),
            other => format!("  {component}: {other} trigger ({})", trigger.id),
        };
    }

    match trigger.trigger_type.as_str() {
        MQTT_TRIGGER_TYPE if config.get("address").is_some() => {
            let address = config_field(config, "address");
            format!("  mqtt broker: {address}")
        }
        other => format!("  {} ({}): {}", trigger.id, other, config),
    }
}

fn config_field<'a>(config: &'a serde_json::Value, field: &str) -> &'a str {
    config
        .get(field)
        .and_then(|value| value.as_str())
        .unwrap_or("-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_supported_triggers_rejects_unknown_type() {
        let app_json = r#"{
            "spin_lock_version": 1,
            "triggers": [{ "id": "x", "trigger_type": "cron", "trigger_config": {} }],
            "components": []
        }"#;
        let locked_app = LockedApp::from_json(app_json.as_bytes()).unwrap();
        assert!(get_supported_triggers(&locked_app).is_err());
    }

    #[test]
    fn log_triggers_at_startup_includes_non_http_triggers_only() {
        let app_json = r#"{
            "spin_lock_version": 1,
            "triggers": [
                {
                    "id": "http",
                    "trigger_type": "http",
                    "trigger_config": { "component": "web", "route": "/api" }
                },
                {
                    "id": "mqtt-1",
                    "trigger_type": "mqtt",
                    "trigger_config": { "component": "logger", "topic": "events", "qos": "1" }
                }
            ],
            "components": []
        }"#;
        let locked_app = LockedApp::from_json(app_json.as_bytes()).unwrap();

        let lines = format_non_http_trigger_lines(&locked_app);

        assert_eq!(lines[0], "Available Triggers:");
        assert!(lines
            .iter()
            .any(|line| line.contains("logger: mqtt topic events")));
        assert!(!lines.iter().any(|line| line.contains("web")));
        assert!(!lines.iter().any(|line| line.contains("/api")));
    }
}
