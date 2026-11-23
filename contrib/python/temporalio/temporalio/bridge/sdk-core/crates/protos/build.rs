use std::{env, path::PathBuf};
use tonic_prost_build::Config;

static ALWAYS_SERDE: &str = "#[cfg_attr(not(feature = \"serde_serialize\"), \
                               derive(::serde::Serialize, ::serde::Deserialize))]";

static SERDE_ATTR: &str =
    "#[cfg_attr(feature = \"serde_serialize\", derive(::serde::Serialize, ::serde::Deserialize))]";

/// Package prefixes that get conditional serde derive via type_attribute.
/// Packages under `temporal.api` are listed individually to exclude the pbjson packages
/// (temporal.api.common, temporal.api.enums, temporal.api.failure), which get their
/// Serialize/Deserialize impls from generated .serde.rs files.
///
/// If you add a new proto package, add its prefix here unless it is also
/// added to the pbjson_build list below.
const SERDE_DERIVE_PREFIXES: &[&str] = &[
    ".coresdk",
    ".grpc",
    ".temporal.api.activity",
    ".temporal.api.batch",
    ".temporal.api.callback",
    ".temporal.api.cloud",
    ".temporal.api.command",
    ".temporal.api.compute",
    ".temporal.api.deployment",
    ".temporal.api.filter",
    ".temporal.api.history",
    ".temporal.api.namespace",
    ".temporal.api.nexus",
    ".temporal.api.nexusservices",
    ".temporal.api.operatorservice",
    ".temporal.api.protocol",
    ".temporal.api.query",
    ".temporal.api.replication",
    ".temporal.api.rules",
    ".temporal.api.schedule",
    ".temporal.api.sdk",
    ".temporal.api.taskqueue",
    ".temporal.api.testservice",
    ".temporal.api.update",
    ".temporal.api.version",
    ".temporal.api.worker",
    ".temporal.api.workflow",
    ".temporal.api.workflowservice",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=./protos");
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_file = out.join("descriptors.bin");
    println!("cargo:descriptor_path={}", descriptor_file.display());
    let mut builder = tonic_prost_build::configure()
        // Workflow guests need message structs, while the native common crate enables this
        // feature to preserve the generated clients it re-exports today.
        .build_server(false)
        .build_client(env::var_os("CARGO_FEATURE_GRPC_CLIENTS").is_some())
        // Make conversions easier for some types
        .type_attribute(
            "temporal.api.history.v1.HistoryEvent.attributes",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.history.v1.History",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.command.v1.Command.attributes",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.common.v1.WorkflowType",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.common.v1.Header",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.common.v1.Memo",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "temporal.api.enums.v1.SignalExternalWorkflowExecutionFailedCause",
            "#[derive(::derive_more::Display)]",
        )
        .type_attribute(
            "temporal.api.enums.v1.CancelExternalWorkflowExecutionFailedCause",
            "#[derive(::derive_more::Display)]",
        )
        .type_attribute(
            "coresdk.workflow_commands.WorkflowCommand.variant",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.workflow_commands.QueryResult.variant",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.workflow_activation.workflow_activation_job",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.workflow_activation.WorkflowActivationJob.variant",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.workflow_completion.WorkflowActivationCompletion.status",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.activity_result.ActivityExecutionResult.status",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.activity_result.ActivityResolution.status",
            "#[derive(::derive_more::From)]",
        )
        .type_attribute(
            "coresdk.activity_task.ActivityCancelReason",
            "#[derive(::derive_more::Display)]",
        )
        .type_attribute("coresdk.Task.variant", "#[derive(::derive_more::From)]")
        .type_attribute(
            "coresdk.workflow_commands.WorkflowCommand.variant",
            "#[derive(::derive_more::Display)]",
        )
        // All external data is useful to be able to JSON serialize, so it can render in web UI
        .type_attribute(".coresdk.external_data", ALWAYS_SERDE)
        .field_attribute(
            "coresdk.external_data.LocalActivityMarkerData.complete_time",
            "#[serde(with = \"sdk_helpers::opt_timestamp\")]",
        )
        .field_attribute(
            "coresdk.external_data.LocalActivityMarkerData.original_schedule_time",
            "#[serde(with = \"sdk_helpers::opt_timestamp\")]",
        )
        .field_attribute(
            "coresdk.external_data.LocalActivityMarkerData.backoff",
            "#[serde(with = \"sdk_helpers::opt_duration\")]",
        )
        .skip_debug(["temporal.api.common.v1.Payload"]);

    for prefix in SERDE_DERIVE_PREFIXES {
        builder = builder.type_attribute(*prefix, SERDE_ATTR);
    }

    builder
        .file_descriptor_set_path(&descriptor_file)
        .compile_with_config(
            {
                let mut c = Config::new();
                c.enable_type_names();
                c
            },
            &[
                "./protos/local/temporal/sdk/core/core_interface.proto",
                "./protos/api_upstream/temporal/api/sdk/v1/workflow_metadata.proto",
                "./protos/api_upstream/temporal/api/workflowservice/v1/service.proto",
                "./protos/api_upstream/temporal/api/nexusservices/workerservice/v1/request_response.proto",
                "./protos/api_upstream/temporal/api/operatorservice/v1/service.proto",
                "./protos/api_upstream/temporal/api/errordetails/v1/message.proto",
                "./protos/api_cloud_upstream/temporal/api/cloud/cloudservice/v1/service.proto",
                "./protos/testsrv_upstream/temporal/api/testservice/v1/service.proto",
                "./protos/grpc/health/v1/health.proto",
                "./protos/google/rpc/status.proto",
            ],
            &[
                "./protos/api_upstream",
                "./protos/api_cloud_upstream",
                "./protos/local",
                "./protos/testsrv_upstream",
                "./protos/grpc",
                "./protos",
            ],
        )?;

    // TODO [rust-sdk-branch]: support normal JSON and proto JSON serialization
    let descriptors = std::fs::read(&descriptor_file)?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptors)?
        .build(&[
            ".temporal.api.failure",
            ".temporal.api.common",
            ".temporal.api.enums",
        ])?;

    Ok(())
}
