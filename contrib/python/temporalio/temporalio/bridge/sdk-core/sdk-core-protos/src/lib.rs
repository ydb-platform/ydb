//! Contains the protobuf definitions used as arguments to and return values from interactions with
//! the Temporal Core SDK. Language SDK authors can generate structs using the proto definitions
//! that will match the generated structs in this module.

pub mod constants;
pub mod utilities;

#[cfg(feature = "history_builders")]
mod history_builder;
#[cfg(feature = "history_builders")]
mod history_info;
mod task_token;

#[cfg(feature = "history_builders")]
pub use history_builder::{
    DEFAULT_ACTIVITY_TYPE, DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, default_act_sched,
    default_wes_attribs,
};
#[cfg(feature = "history_builders")]
pub use history_info::HistoryInfo;
pub use task_token::TaskToken;

pub static ENCODING_PAYLOAD_KEY: &str = "encoding";
pub static JSON_ENCODING_VAL: &str = "json/plain";
pub static PATCHED_MARKER_DETAILS_KEY: &str = "patch-data";

#[allow(
    clippy::large_enum_variant,
    clippy::derive_partial_eq_without_eq,
    clippy::reserve_after_initialization
)]
// I'd prefer not to do this, but there are some generated things that just don't need it.
#[allow(missing_docs)]
pub mod coresdk {
    //! Contains all protobufs relating to communication between core and lang-specific SDKs

    tonic::include_proto!("coresdk");

    use crate::{
        ENCODING_PAYLOAD_KEY, JSON_ENCODING_VAL,
        temporal::api::{
            common::v1::{Payload, Payloads, WorkflowExecution},
            enums::v1::{
                ApplicationErrorCategory, TimeoutType, VersioningBehavior, WorkflowTaskFailedCause,
            },
            failure::v1::{
                ActivityFailureInfo, ApplicationFailureInfo, Failure, TimeoutFailureInfo,
                failure::FailureInfo,
            },
            workflowservice::v1::PollActivityTaskQueueResponse,
        },
    };
    use activity_task::ActivityTask;
    use serde::{Deserialize, Serialize};
    use std::{
        collections::HashMap,
        convert::TryFrom,
        fmt::{Display, Formatter},
        iter::FromIterator,
    };
    use workflow_activation::{WorkflowActivationJob, workflow_activation_job};
    use workflow_commands::{WorkflowCommand, workflow_command, workflow_command::Variant};
    use workflow_completion::{WorkflowActivationCompletion, workflow_activation_completion};

    #[allow(clippy::module_inception)]
    pub mod activity_task {
        use crate::{coresdk::ActivityTaskCompletion, task_token::fmt_tt};
        use std::fmt::{Display, Formatter};
        tonic::include_proto!("coresdk.activity_task");

        impl ActivityTask {
            pub fn cancel_from_ids(
                task_token: Vec<u8>,
                reason: ActivityCancelReason,
                details: ActivityCancellationDetails,
            ) -> Self {
                Self {
                    task_token,
                    variant: Some(activity_task::Variant::Cancel(Cancel {
                        reason: reason as i32,
                        details: Some(details),
                    })),
                }
            }

            // Checks if both the primary reason or details have a timeout cancellation.
            pub fn is_timeout(&self) -> bool {
                match &self.variant {
                    Some(activity_task::Variant::Cancel(Cancel { reason, details })) => {
                        *reason == ActivityCancelReason::TimedOut as i32
                            || details.as_ref().is_some_and(|d| d.is_timed_out)
                    }
                    _ => false,
                }
            }

            pub fn primary_reason_to_cancellation_details(
                reason: ActivityCancelReason,
            ) -> ActivityCancellationDetails {
                ActivityCancellationDetails {
                    is_not_found: reason == ActivityCancelReason::NotFound,
                    is_cancelled: reason == ActivityCancelReason::Cancelled,
                    is_paused: reason == ActivityCancelReason::Paused,
                    is_timed_out: reason == ActivityCancelReason::TimedOut,
                    is_worker_shutdown: reason == ActivityCancelReason::WorkerShutdown,
                    is_reset: reason == ActivityCancelReason::Reset,
                }
            }
        }

        impl Display for ActivityTaskCompletion {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "ActivityTaskCompletion(token: {}",
                    fmt_tt(&self.task_token),
                )?;
                if let Some(r) = self.result.as_ref().and_then(|r| r.status.as_ref()) {
                    write!(f, ", {r}")?;
                } else {
                    write!(f, ", missing result")?;
                }
                write!(f, ")")
            }
        }
    }
    #[allow(clippy::module_inception)]
    pub mod activity_result {
        tonic::include_proto!("coresdk.activity_result");
        use super::super::temporal::api::{
            common::v1::Payload,
            failure::v1::{CanceledFailureInfo, Failure as APIFailure, failure},
        };
        use crate::{
            coresdk::activity_result::activity_resolution::Status,
            temporal::api::enums::v1::TimeoutType,
        };
        use activity_execution_result as aer;
        use anyhow::anyhow;
        use std::fmt::{Display, Formatter};

        impl ActivityExecutionResult {
            pub const fn ok(result: Payload) -> Self {
                Self {
                    status: Some(aer::Status::Completed(Success {
                        result: Some(result),
                    })),
                }
            }

            pub fn fail(fail: APIFailure) -> Self {
                Self {
                    status: Some(aer::Status::Failed(Failure {
                        failure: Some(fail),
                    })),
                }
            }

            pub fn cancel_from_details(payload: Option<Payload>) -> Self {
                Self {
                    status: Some(aer::Status::Cancelled(Cancellation::from_details(payload))),
                }
            }

            pub const fn will_complete_async() -> Self {
                Self {
                    status: Some(aer::Status::WillCompleteAsync(WillCompleteAsync {})),
                }
            }

            pub fn is_cancelled(&self) -> bool {
                matches!(self.status, Some(aer::Status::Cancelled(_)))
            }
        }

        impl Display for aer::Status {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "ActivityExecutionResult(")?;
                match self {
                    aer::Status::Completed(v) => {
                        write!(f, "{v})")
                    }
                    aer::Status::Failed(v) => {
                        write!(f, "{v})")
                    }
                    aer::Status::Cancelled(v) => {
                        write!(f, "{v})")
                    }
                    aer::Status::WillCompleteAsync(_) => {
                        write!(f, "Will complete async)")
                    }
                }
            }
        }

        impl Display for Success {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "Success(")?;
                if let Some(ref v) = self.result {
                    write!(f, "{v}")?;
                }
                write!(f, ")")
            }
        }

        impl Display for Failure {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "Failure(")?;
                if let Some(ref v) = self.failure {
                    write!(f, "{v}")?;
                }
                write!(f, ")")
            }
        }

        impl Display for Cancellation {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "Cancellation(")?;
                if let Some(ref v) = self.failure {
                    write!(f, "{v}")?;
                }
                write!(f, ")")
            }
        }

        impl From<Result<Payload, APIFailure>> for ActivityExecutionResult {
            fn from(r: Result<Payload, APIFailure>) -> Self {
                Self {
                    status: match r {
                        Ok(p) => Some(aer::Status::Completed(Success { result: Some(p) })),
                        Err(f) => Some(aer::Status::Failed(Failure { failure: Some(f) })),
                    },
                }
            }
        }

        impl ActivityResolution {
            /// Extract an activity's payload if it completed successfully, or return an error for all
            /// other outcomes.
            pub fn success_payload_or_error(self) -> Result<Option<Payload>, anyhow::Error> {
                let Some(status) = self.status else {
                    return Err(anyhow!("Activity completed without a status"));
                };

                match status {
                    activity_resolution::Status::Completed(success) => Ok(success.result),
                    e => Err(anyhow!("Activity was not successful: {e:?}")),
                }
            }

            pub fn unwrap_ok_payload(self) -> Payload {
                self.success_payload_or_error().unwrap().unwrap()
            }

            pub fn completed_ok(&self) -> bool {
                matches!(self.status, Some(activity_resolution::Status::Completed(_)))
            }

            pub fn failed(&self) -> bool {
                matches!(self.status, Some(activity_resolution::Status::Failed(_)))
            }

            pub fn timed_out(&self) -> Option<TimeoutType> {
                match self.status {
                    Some(activity_resolution::Status::Failed(Failure {
                        failure: Some(ref f),
                    })) => f
                        .is_timeout()
                        .or_else(|| f.cause.as_ref().and_then(|c| c.is_timeout())),
                    _ => None,
                }
            }

            pub fn cancelled(&self) -> bool {
                matches!(self.status, Some(activity_resolution::Status::Cancelled(_)))
            }

            /// If this resolution is any kind of failure, return the inner failure details. Panics
            /// if the activity succeeded, is in backoff, or this resolution is malformed.
            pub fn unwrap_failure(self) -> APIFailure {
                match self.status.unwrap() {
                    Status::Failed(f) => f.failure.unwrap(),
                    Status::Cancelled(c) => c.failure.unwrap(),
                    _ => panic!("Actvity did not fail"),
                }
            }
        }

        impl Cancellation {
            /// Create a cancellation result from some payload. This is to be used when telling Core
            /// that an activity completed as cancelled.
            pub fn from_details(details: Option<Payload>) -> Self {
                Cancellation {
                    failure: Some(APIFailure {
                        message: "Activity cancelled".to_string(),
                        failure_info: Some(failure::FailureInfo::CanceledFailureInfo(
                            CanceledFailureInfo {
                                details: details.map(Into::into),
                            },
                        )),
                        ..Default::default()
                    }),
                }
            }
        }
    }

    pub mod common {
        tonic::include_proto!("coresdk.common");
        use super::external_data::LocalActivityMarkerData;
        use crate::{
            PATCHED_MARKER_DETAILS_KEY,
            coresdk::{
                AsJsonPayloadExt, FromJsonPayloadExt, IntoPayloadsExt,
                external_data::PatchedMarkerData,
            },
            temporal::api::common::v1::{Payload, Payloads},
        };
        use std::collections::HashMap;

        pub fn build_has_change_marker_details(
            patch_id: impl Into<String>,
            deprecated: bool,
        ) -> anyhow::Result<HashMap<String, Payloads>> {
            let mut hm = HashMap::new();
            let encoded = PatchedMarkerData {
                id: patch_id.into(),
                deprecated,
            }
            .as_json_payload()?;
            hm.insert(PATCHED_MARKER_DETAILS_KEY.to_string(), encoded.into());
            Ok(hm)
        }

        pub fn decode_change_marker_details(
            details: &HashMap<String, Payloads>,
        ) -> Option<(String, bool)> {
            // We used to write change markers with plain bytes, so try to decode if they are
            // json first, then fall back to that.
            if let Some(cd) = details.get(PATCHED_MARKER_DETAILS_KEY) {
                let decoded = PatchedMarkerData::from_json_payload(cd.payloads.first()?).ok()?;
                return Some((decoded.id, decoded.deprecated));
            }

            let id_entry = details.get("patch_id")?.payloads.first()?;
            let deprecated_entry = details.get("deprecated")?.payloads.first()?;
            let name = std::str::from_utf8(&id_entry.data).ok()?;
            let deprecated = *deprecated_entry.data.first()? != 0;
            Some((name.to_string(), deprecated))
        }

        pub fn build_local_activity_marker_details(
            metadata: LocalActivityMarkerData,
            result: Option<Payload>,
        ) -> HashMap<String, Payloads> {
            let mut hm = HashMap::new();
            // It would be more efficient for this to be proto binary, but then it shows up as
            // meaningless in the Temporal UI...
            if let Some(jsonified) = metadata.as_json_payload().into_payloads() {
                hm.insert("data".to_string(), jsonified);
            }
            if let Some(res) = result {
                hm.insert("result".to_string(), res.into());
            }
            hm
        }

        /// Given a marker detail map, returns just the local activity info, but not the payload.
        /// This is fairly inexpensive. Deserializing the whole payload may not be.
        pub fn extract_local_activity_marker_data(
            details: &HashMap<String, Payloads>,
        ) -> Option<LocalActivityMarkerData> {
            details
                .get("data")
                .and_then(|p| p.payloads.first())
                .and_then(|p| std::str::from_utf8(&p.data).ok())
                .and_then(|s| serde_json::from_str(s).ok())
        }

        /// Given a marker detail map, returns the local activity info and the result payload
        /// if they are found and the marker data is well-formed. This removes the data from the
        /// map.
        pub fn extract_local_activity_marker_details(
            details: &mut HashMap<String, Payloads>,
        ) -> (Option<LocalActivityMarkerData>, Option<Payload>) {
            let data = extract_local_activity_marker_data(details);
            let result = details.remove("result").and_then(|mut p| p.payloads.pop());
            (data, result)
        }
    }

    pub mod external_data {
        use prost_wkt_types::{Duration, Timestamp};
        use serde::{Deserialize, Deserializer, Serialize, Serializer};
        tonic::include_proto!("coresdk.external_data");

        // Buncha hullaballoo because prost types aren't serde compat.
        // See https://github.com/tokio-rs/prost/issues/75 which hilariously Chad opened ages ago

        #[derive(Serialize, Deserialize)]
        #[serde(remote = "Timestamp")]
        struct TimestampDef {
            seconds: i64,
            nanos: i32,
        }
        mod opt_timestamp {
            use super::*;

            pub(super) fn serialize<S>(
                value: &Option<Timestamp>,
                serializer: S,
            ) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                #[derive(Serialize)]
                struct Helper<'a>(#[serde(with = "TimestampDef")] &'a Timestamp);

                value.as_ref().map(Helper).serialize(serializer)
            }

            pub(super) fn deserialize<'de, D>(
                deserializer: D,
            ) -> Result<Option<Timestamp>, D::Error>
            where
                D: Deserializer<'de>,
            {
                #[derive(Deserialize)]
                struct Helper(#[serde(with = "TimestampDef")] Timestamp);

                let helper = Option::deserialize(deserializer)?;
                Ok(helper.map(|Helper(external)| external))
            }
        }

        // Luckily Duration is also stored the exact same way
        #[derive(Serialize, Deserialize)]
        #[serde(remote = "Duration")]
        struct DurationDef {
            seconds: i64,
            nanos: i32,
        }
        mod opt_duration {
            use super::*;

            pub(super) fn serialize<S>(
                value: &Option<Duration>,
                serializer: S,
            ) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                #[derive(Serialize)]
                struct Helper<'a>(#[serde(with = "DurationDef")] &'a Duration);

                value.as_ref().map(Helper).serialize(serializer)
            }

            pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
            where
                D: Deserializer<'de>,
            {
                #[derive(Deserialize)]
                struct Helper(#[serde(with = "DurationDef")] Duration);

                let helper = Option::deserialize(deserializer)?;
                Ok(helper.map(|Helper(external)| external))
            }
        }
    }

    pub mod workflow_activation {
        use crate::{
            coresdk::{
                FromPayloadsExt,
                activity_result::{ActivityResolution, activity_resolution},
                common::NamespacedWorkflowExecution,
                workflow_activation::remove_from_cache::EvictionReason,
            },
            temporal::api::{
                enums::v1::WorkflowTaskFailedCause,
                history::v1::{
                    WorkflowExecutionCancelRequestedEventAttributes,
                    WorkflowExecutionSignaledEventAttributes,
                    WorkflowExecutionStartedEventAttributes,
                },
                query::v1::WorkflowQuery,
            },
        };
        use prost_wkt_types::Timestamp;
        use std::fmt::{Display, Formatter};

        tonic::include_proto!("coresdk.workflow_activation");

        pub fn create_evict_activation(
            run_id: String,
            message: String,
            reason: EvictionReason,
        ) -> WorkflowActivation {
            WorkflowActivation {
                timestamp: None,
                run_id,
                is_replaying: false,
                history_length: 0,
                jobs: vec![WorkflowActivationJob::from(
                    workflow_activation_job::Variant::RemoveFromCache(RemoveFromCache {
                        message,
                        reason: reason as i32,
                    }),
                )],
                available_internal_flags: vec![],
                history_size_bytes: 0,
                continue_as_new_suggested: false,
                deployment_version_for_current_task: None,
            }
        }

        pub fn query_to_job(id: String, q: WorkflowQuery) -> QueryWorkflow {
            QueryWorkflow {
                query_id: id,
                query_type: q.query_type,
                arguments: Vec::from_payloads(q.query_args),
                headers: q.header.map(|h| h.into()).unwrap_or_default(),
            }
        }

        impl WorkflowActivation {
            /// Returns true if the only job in the activation is eviction
            pub fn is_only_eviction(&self) -> bool {
                matches!(
                    self.jobs.as_slice(),
                    [WorkflowActivationJob {
                        variant: Some(workflow_activation_job::Variant::RemoveFromCache(_))
                    }]
                )
            }

            /// Returns eviction reason if this activation is an eviction
            pub fn eviction_reason(&self) -> Option<EvictionReason> {
                self.jobs.iter().find_map(|j| {
                    if let Some(workflow_activation_job::Variant::RemoveFromCache(ref rj)) =
                        j.variant
                    {
                        EvictionReason::try_from(rj.reason).ok()
                    } else {
                        None
                    }
                })
            }
        }

        impl workflow_activation_job::Variant {
            pub fn is_local_activity_resolution(&self) -> bool {
                matches!(self, workflow_activation_job::Variant::ResolveActivity(ra) if ra.is_local)
            }
        }

        impl Display for EvictionReason {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{self:?}")
            }
        }

        impl From<EvictionReason> for WorkflowTaskFailedCause {
            fn from(value: EvictionReason) -> Self {
                match value {
                    EvictionReason::Nondeterminism => {
                        WorkflowTaskFailedCause::NonDeterministicError
                    }
                    _ => WorkflowTaskFailedCause::Unspecified,
                }
            }
        }

        impl Display for WorkflowActivation {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "WorkflowActivation(")?;
                write!(f, "run_id: {}, ", self.run_id)?;
                write!(f, "is_replaying: {}, ", self.is_replaying)?;
                write!(
                    f,
                    "jobs: {})",
                    self.jobs
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .as_slice()
                        .join(", ")
                )
            }
        }

        impl Display for WorkflowActivationJob {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match &self.variant {
                    None => write!(f, "empty"),
                    Some(v) => write!(f, "{v}"),
                }
            }
        }

        impl Display for workflow_activation_job::Variant {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match self {
                    workflow_activation_job::Variant::InitializeWorkflow(_) => {
                        write!(f, "InitializeWorkflow")
                    }
                    workflow_activation_job::Variant::FireTimer(t) => {
                        write!(f, "FireTimer({})", t.seq)
                    }
                    workflow_activation_job::Variant::UpdateRandomSeed(_) => {
                        write!(f, "UpdateRandomSeed")
                    }
                    workflow_activation_job::Variant::QueryWorkflow(_) => {
                        write!(f, "QueryWorkflow")
                    }
                    workflow_activation_job::Variant::CancelWorkflow(_) => {
                        write!(f, "CancelWorkflow")
                    }
                    workflow_activation_job::Variant::SignalWorkflow(_) => {
                        write!(f, "SignalWorkflow")
                    }
                    workflow_activation_job::Variant::ResolveActivity(r) => {
                        write!(
                            f,
                            "ResolveActivity({}, {})",
                            r.seq,
                            r.result
                                .as_ref()
                                .unwrap_or(&ActivityResolution { status: None })
                        )
                    }
                    workflow_activation_job::Variant::NotifyHasPatch(_) => {
                        write!(f, "NotifyHasPatch")
                    }
                    workflow_activation_job::Variant::ResolveChildWorkflowExecutionStart(_) => {
                        write!(f, "ResolveChildWorkflowExecutionStart")
                    }
                    workflow_activation_job::Variant::ResolveChildWorkflowExecution(_) => {
                        write!(f, "ResolveChildWorkflowExecution")
                    }
                    workflow_activation_job::Variant::ResolveSignalExternalWorkflow(_) => {
                        write!(f, "ResolveSignalExternalWorkflow")
                    }
                    workflow_activation_job::Variant::RemoveFromCache(_) => {
                        write!(f, "RemoveFromCache")
                    }
                    workflow_activation_job::Variant::ResolveRequestCancelExternalWorkflow(_) => {
                        write!(f, "ResolveRequestCancelExternalWorkflow")
                    }
                    workflow_activation_job::Variant::DoUpdate(u) => {
                        write!(f, "DoUpdate({})", u.id)
                    }
                    workflow_activation_job::Variant::ResolveNexusOperationStart(_) => {
                        write!(f, "ResolveNexusOperationStart")
                    }
                    workflow_activation_job::Variant::ResolveNexusOperation(_) => {
                        write!(f, "ResolveNexusOperation")
                    }
                }
            }
        }

        impl Display for ActivityResolution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match self.status {
                    None => {
                        write!(f, "None")
                    }
                    Some(activity_resolution::Status::Failed(_)) => {
                        write!(f, "Failed")
                    }
                    Some(activity_resolution::Status::Completed(_)) => {
                        write!(f, "Completed")
                    }
                    Some(activity_resolution::Status::Cancelled(_)) => {
                        write!(f, "Cancelled")
                    }
                    Some(activity_resolution::Status::Backoff(_)) => {
                        write!(f, "Backoff")
                    }
                }
            }
        }

        impl Display for QueryWorkflow {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "QueryWorkflow(id: {}, type: {})",
                    self.query_id, self.query_type
                )
            }
        }

        impl From<WorkflowExecutionSignaledEventAttributes> for SignalWorkflow {
            fn from(a: WorkflowExecutionSignaledEventAttributes) -> Self {
                Self {
                    signal_name: a.signal_name,
                    input: Vec::from_payloads(a.input),
                    identity: a.identity,
                    headers: a.header.map(Into::into).unwrap_or_default(),
                }
            }
        }

        impl From<WorkflowExecutionCancelRequestedEventAttributes> for CancelWorkflow {
            fn from(a: WorkflowExecutionCancelRequestedEventAttributes) -> Self {
                Self { reason: a.cause }
            }
        }

        /// Create a [InitializeWorkflow] job from corresponding event attributes
        pub fn start_workflow_from_attribs(
            attrs: WorkflowExecutionStartedEventAttributes,
            workflow_id: String,
            randomness_seed: u64,
            start_time: Timestamp,
        ) -> InitializeWorkflow {
            InitializeWorkflow {
                workflow_type: attrs.workflow_type.map(|wt| wt.name).unwrap_or_default(),
                workflow_id,
                arguments: Vec::from_payloads(attrs.input),
                randomness_seed,
                headers: attrs.header.unwrap_or_default().fields,
                identity: attrs.identity,
                parent_workflow_info: attrs.parent_workflow_execution.map(|pe| {
                    NamespacedWorkflowExecution {
                        namespace: attrs.parent_workflow_namespace,
                        run_id: pe.run_id,
                        workflow_id: pe.workflow_id,
                    }
                }),
                workflow_execution_timeout: attrs.workflow_execution_timeout,
                workflow_run_timeout: attrs.workflow_run_timeout,
                workflow_task_timeout: attrs.workflow_task_timeout,
                continued_from_execution_run_id: attrs.continued_execution_run_id,
                continued_initiator: attrs.initiator,
                continued_failure: attrs.continued_failure,
                last_completion_result: attrs.last_completion_result,
                first_execution_run_id: attrs.first_execution_run_id,
                retry_policy: attrs.retry_policy,
                attempt: attrs.attempt,
                cron_schedule: attrs.cron_schedule,
                workflow_execution_expiration_time: attrs.workflow_execution_expiration_time,
                cron_schedule_to_schedule_interval: attrs.first_workflow_task_backoff,
                memo: attrs.memo,
                search_attributes: attrs.search_attributes,
                start_time: Some(start_time),
                root_workflow: attrs.root_workflow_execution,
                priority: attrs.priority,
            }
        }
    }

    pub mod workflow_completion {
        use crate::temporal::api::{enums::v1::WorkflowTaskFailedCause, failure};
        tonic::include_proto!("coresdk.workflow_completion");

        impl workflow_activation_completion::Status {
            pub const fn is_success(&self) -> bool {
                match &self {
                    Self::Successful(_) => true,
                    Self::Failed(_) => false,
                }
            }
        }

        impl From<failure::v1::Failure> for Failure {
            fn from(f: failure::v1::Failure) -> Self {
                Failure {
                    failure: Some(f),
                    force_cause: WorkflowTaskFailedCause::Unspecified as i32,
                }
            }
        }
    }

    pub mod child_workflow {
        tonic::include_proto!("coresdk.child_workflow");
    }

    pub mod nexus {
        use crate::temporal::api::workflowservice::v1::PollNexusTaskQueueResponse;
        use std::fmt::{Display, Formatter};

        tonic::include_proto!("coresdk.nexus");

        impl NexusTask {
            /// Unwrap the inner server-delivered nexus task if that's what this is, else panic.
            pub fn unwrap_task(self) -> PollNexusTaskQueueResponse {
                if let Some(nexus_task::Variant::Task(t)) = self.variant {
                    return t;
                }
                panic!("Nexus task did not contain a server task");
            }

            /// Get the task token
            pub fn task_token(&self) -> &[u8] {
                match &self.variant {
                    Some(nexus_task::Variant::Task(t)) => t.task_token.as_slice(),
                    Some(nexus_task::Variant::CancelTask(c)) => c.task_token.as_slice(),
                    None => panic!("Nexus task did not contain a task token"),
                }
            }
        }

        impl Display for nexus_task_completion::Status {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "NexusTaskCompletion(")?;
                match self {
                    nexus_task_completion::Status::Completed(c) => {
                        write!(f, "{c}")
                    }
                    nexus_task_completion::Status::Error(e) => {
                        write!(f, "{e}")
                    }
                    nexus_task_completion::Status::AckCancel(_) => {
                        write!(f, "AckCancel")
                    }
                }?;
                write!(f, ")")
            }
        }
    }

    pub mod workflow_commands {
        tonic::include_proto!("coresdk.workflow_commands");

        use crate::temporal::api::{common::v1::Payloads, enums::v1::QueryResultType};
        use std::fmt::{Display, Formatter};

        impl Display for WorkflowCommand {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                match &self.variant {
                    None => write!(f, "Empty"),
                    Some(v) => write!(f, "{v}"),
                }
            }
        }

        impl Display for StartTimer {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "StartTimer({})", self.seq)
            }
        }

        impl Display for ScheduleActivity {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "ScheduleActivity({}, {})", self.seq, self.activity_type)
            }
        }

        impl Display for ScheduleLocalActivity {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "ScheduleLocalActivity({}, {})",
                    self.seq, self.activity_type
                )
            }
        }

        impl Display for QueryResult {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "RespondToQuery({})", self.query_id)
            }
        }

        impl Display for RequestCancelActivity {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "RequestCancelActivity({})", self.seq)
            }
        }

        impl Display for RequestCancelLocalActivity {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "RequestCancelLocalActivity({})", self.seq)
            }
        }

        impl Display for CancelTimer {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "CancelTimer({})", self.seq)
            }
        }

        impl Display for CompleteWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "CompleteWorkflowExecution")
            }
        }

        impl Display for FailWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "FailWorkflowExecution")
            }
        }

        impl Display for ContinueAsNewWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "ContinueAsNewWorkflowExecution")
            }
        }

        impl Display for CancelWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "CancelWorkflowExecution")
            }
        }

        impl Display for SetPatchMarker {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "SetPatchMarker({})", self.patch_id)
            }
        }

        impl Display for StartChildWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "StartChildWorkflowExecution({}, {})",
                    self.seq, self.workflow_type
                )
            }
        }

        impl Display for RequestCancelExternalWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "RequestCancelExternalWorkflowExecution({})", self.seq)
            }
        }

        impl Display for UpsertWorkflowSearchAttributes {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "UpsertWorkflowSearchAttributes({:?})",
                    self.search_attributes.keys()
                )
            }
        }

        impl Display for SignalExternalWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "SignalExternalWorkflowExecution({})", self.seq)
            }
        }

        impl Display for CancelSignalWorkflow {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "CancelSignalWorkflow({})", self.seq)
            }
        }

        impl Display for CancelChildWorkflowExecution {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "CancelChildWorkflowExecution({})",
                    self.child_workflow_seq
                )
            }
        }

        impl Display for ModifyWorkflowProperties {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "ModifyWorkflowProperties(upserted memo keys: {:?})",
                    self.upserted_memo.as_ref().map(|m| m.fields.keys())
                )
            }
        }

        impl Display for UpdateResponse {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "UpdateResponse(protocol_instance_id: {}, response: {:?})",
                    self.protocol_instance_id, self.response
                )
            }
        }

        impl Display for ScheduleNexusOperation {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "ScheduleNexusOperation({})", self.seq)
            }
        }

        impl Display for RequestCancelNexusOperation {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "RequestCancelNexusOperation({})", self.seq)
            }
        }

        impl QueryResult {
            /// Helper to construct the Temporal API query result types.
            pub fn into_components(self) -> (String, QueryResultType, Option<Payloads>, String) {
                match self {
                    QueryResult {
                        variant: Some(query_result::Variant::Succeeded(qs)),
                        query_id,
                    } => (
                        query_id,
                        QueryResultType::Answered,
                        qs.response.map(Into::into),
                        "".to_string(),
                    ),
                    QueryResult {
                        variant: Some(query_result::Variant::Failed(err)),
                        query_id,
                    } => (query_id, QueryResultType::Failed, None, err.message),
                    QueryResult {
                        variant: None,
                        query_id,
                    } => (
                        query_id,
                        QueryResultType::Failed,
                        None,
                        "Query response was empty".to_string(),
                    ),
                }
            }
        }
    }

    pub type HistoryEventId = i64;

    impl From<workflow_activation_job::Variant> for WorkflowActivationJob {
        fn from(a: workflow_activation_job::Variant) -> Self {
            Self { variant: Some(a) }
        }
    }

    impl From<Vec<WorkflowCommand>> for workflow_completion::Success {
        fn from(v: Vec<WorkflowCommand>) -> Self {
            Self {
                commands: v,
                used_internal_flags: vec![],
                versioning_behavior: VersioningBehavior::Unspecified.into(),
            }
        }
    }

    impl From<workflow_command::Variant> for WorkflowCommand {
        fn from(v: workflow_command::Variant) -> Self {
            Self {
                variant: Some(v),
                user_metadata: None,
            }
        }
    }

    impl workflow_completion::Success {
        pub fn from_variants(cmds: Vec<Variant>) -> Self {
            let cmds: Vec<_> = cmds.into_iter().map(|c| c.into()).collect();
            cmds.into()
        }
    }

    impl WorkflowActivationCompletion {
        /// Create a successful activation with no commands in it
        pub fn empty(run_id: impl Into<String>) -> Self {
            let success = workflow_completion::Success::from_variants(vec![]);
            Self {
                run_id: run_id.into(),
                status: Some(workflow_activation_completion::Status::Successful(success)),
            }
        }

        /// Create a successful activation from a list of command variants
        pub fn from_cmds(run_id: impl Into<String>, cmds: Vec<workflow_command::Variant>) -> Self {
            let success = workflow_completion::Success::from_variants(cmds);
            Self {
                run_id: run_id.into(),
                status: Some(workflow_activation_completion::Status::Successful(success)),
            }
        }

        /// Create a successful activation from just one command variant
        pub fn from_cmd(run_id: impl Into<String>, cmd: workflow_command::Variant) -> Self {
            let success = workflow_completion::Success::from_variants(vec![cmd]);
            Self {
                run_id: run_id.into(),
                status: Some(workflow_activation_completion::Status::Successful(success)),
            }
        }

        pub fn fail(
            run_id: impl Into<String>,
            failure: Failure,
            cause: Option<WorkflowTaskFailedCause>,
        ) -> Self {
            Self {
                run_id: run_id.into(),
                status: Some(workflow_activation_completion::Status::Failed(
                    workflow_completion::Failure {
                        failure: Some(failure),
                        force_cause: cause.unwrap_or(WorkflowTaskFailedCause::Unspecified) as i32,
                    },
                )),
            }
        }

        /// Returns true if the activation has either a fail, continue, cancel, or complete workflow
        /// execution command in it.
        pub fn has_execution_ending(&self) -> bool {
            self.has_complete_workflow_execution()
                || self.has_fail_execution()
                || self.has_continue_as_new()
                || self.has_cancel_workflow_execution()
        }

        /// Returns true if the activation contains a fail workflow execution command
        pub fn has_fail_execution(&self) -> bool {
            if let Some(workflow_activation_completion::Status::Successful(s)) = &self.status {
                return s.commands.iter().any(|wfc| {
                    matches!(
                        wfc,
                        WorkflowCommand {
                            variant: Some(workflow_command::Variant::FailWorkflowExecution(_)),
                            ..
                        }
                    )
                });
            }
            false
        }

        /// Returns true if the activation contains a cancel workflow execution command
        pub fn has_cancel_workflow_execution(&self) -> bool {
            if let Some(workflow_activation_completion::Status::Successful(s)) = &self.status {
                return s.commands.iter().any(|wfc| {
                    matches!(
                        wfc,
                        WorkflowCommand {
                            variant: Some(workflow_command::Variant::CancelWorkflowExecution(_)),
                            ..
                        }
                    )
                });
            }
            false
        }

        /// Returns true if the activation contains a continue as new workflow execution command
        pub fn has_continue_as_new(&self) -> bool {
            if let Some(workflow_activation_completion::Status::Successful(s)) = &self.status {
                return s.commands.iter().any(|wfc| {
                    matches!(
                        wfc,
                        WorkflowCommand {
                            variant: Some(
                                workflow_command::Variant::ContinueAsNewWorkflowExecution(_)
                            ),
                            ..
                        }
                    )
                });
            }
            false
        }

        /// Returns true if the activation contains a complete workflow execution command
        pub fn has_complete_workflow_execution(&self) -> bool {
            self.complete_workflow_execution_value().is_some()
        }

        /// Returns the completed execution result value, if any
        pub fn complete_workflow_execution_value(&self) -> Option<&Payload> {
            if let Some(workflow_activation_completion::Status::Successful(s)) = &self.status {
                s.commands.iter().find_map(|wfc| match wfc {
                    WorkflowCommand {
                        variant: Some(workflow_command::Variant::CompleteWorkflowExecution(v)),
                        ..
                    } => v.result.as_ref(),
                    _ => None,
                })
            } else {
                None
            }
        }

        /// Returns true if the activation completion is a success with no commands
        pub fn is_empty(&self) -> bool {
            if let Some(workflow_activation_completion::Status::Successful(s)) = &self.status {
                return s.commands.is_empty();
            }
            false
        }

        pub fn add_internal_flags(&mut self, patch: u32) {
            if let Some(workflow_activation_completion::Status::Successful(s)) = &mut self.status {
                s.used_internal_flags.push(patch);
            }
        }
    }

    /// Makes converting outgoing lang commands into [WorkflowActivationCompletion]s easier
    pub trait IntoCompletion {
        /// The conversion function
        fn into_completion(self, run_id: String) -> WorkflowActivationCompletion;
    }

    impl IntoCompletion for workflow_command::Variant {
        fn into_completion(self, run_id: String) -> WorkflowActivationCompletion {
            WorkflowActivationCompletion::from_cmd(run_id, self)
        }
    }

    impl<I, V> IntoCompletion for I
    where
        I: IntoIterator<Item = V>,
        V: Into<WorkflowCommand>,
    {
        fn into_completion(self, run_id: String) -> WorkflowActivationCompletion {
            let success = self.into_iter().map(Into::into).collect::<Vec<_>>().into();
            WorkflowActivationCompletion {
                run_id,
                status: Some(workflow_activation_completion::Status::Successful(success)),
            }
        }
    }

    impl Display for WorkflowActivationCompletion {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "WorkflowActivationCompletion(run_id: {}, status: ",
                &self.run_id
            )?;
            match &self.status {
                None => write!(f, "empty")?,
                Some(s) => write!(f, "{s}")?,
            };
            write!(f, ")")
        }
    }

    impl Display for workflow_activation_completion::Status {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                workflow_activation_completion::Status::Successful(
                    workflow_completion::Success { commands, .. },
                ) => {
                    write!(f, "Success(")?;
                    let mut written = 0;
                    for c in commands {
                        write!(f, "{c} ")?;
                        written += 1;
                        if written >= 10 && written < commands.len() {
                            write!(f, "... {} more", commands.len() - written)?;
                            break;
                        }
                    }
                    write!(f, ")")
                }
                workflow_activation_completion::Status::Failed(_) => {
                    write!(f, "Failed")
                }
            }
        }
    }

    impl ActivityTask {
        pub fn start_from_poll_resp(r: PollActivityTaskQueueResponse) -> Self {
            let (workflow_id, run_id) = r
                .workflow_execution
                .map(|we| (we.workflow_id, we.run_id))
                .unwrap_or_default();
            Self {
                task_token: r.task_token,
                variant: Some(activity_task::activity_task::Variant::Start(
                    activity_task::Start {
                        workflow_namespace: r.workflow_namespace,
                        workflow_type: r.workflow_type.map_or_else(|| "".to_string(), |wt| wt.name),
                        workflow_execution: Some(WorkflowExecution {
                            workflow_id,
                            run_id,
                        }),
                        activity_id: r.activity_id,
                        activity_type: r.activity_type.map_or_else(|| "".to_string(), |at| at.name),
                        header_fields: r.header.map(Into::into).unwrap_or_default(),
                        input: Vec::from_payloads(r.input),
                        heartbeat_details: Vec::from_payloads(r.heartbeat_details),
                        scheduled_time: r.scheduled_time,
                        current_attempt_scheduled_time: r.current_attempt_scheduled_time,
                        started_time: r.started_time,
                        attempt: r.attempt as u32,
                        schedule_to_close_timeout: r.schedule_to_close_timeout,
                        start_to_close_timeout: r.start_to_close_timeout,
                        heartbeat_timeout: r.heartbeat_timeout,
                        retry_policy: r.retry_policy,
                        priority: r.priority,
                        is_local: false,
                    },
                )),
            }
        }
    }

    impl Failure {
        pub fn is_timeout(&self) -> Option<crate::temporal::api::enums::v1::TimeoutType> {
            match &self.failure_info {
                Some(FailureInfo::TimeoutFailureInfo(ti)) => Some(ti.timeout_type()),
                _ => None,
            }
        }

        pub fn application_failure(message: String, non_retryable: bool) -> Self {
            Self {
                message,
                failure_info: Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        non_retryable,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }
        }

        pub fn application_failure_from_error(ae: anyhow::Error, non_retryable: bool) -> Self {
            Self {
                failure_info: Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        non_retryable,
                        ..Default::default()
                    },
                )),
                ..ae.chain()
                    .rfold(None, |cause, e| {
                        Some(Self {
                            message: e.to_string(),
                            cause: cause.map(Box::new),
                            ..Default::default()
                        })
                    })
                    .unwrap_or_default()
            }
        }

        pub fn timeout(timeout_type: TimeoutType) -> Self {
            Self {
                message: "Activity timed out".to_string(),
                cause: Some(Box::new(Failure {
                    message: "Activity timed out".to_string(),
                    failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                        timeout_type: timeout_type.into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
                failure_info: Some(FailureInfo::ActivityFailureInfo(
                    ActivityFailureInfo::default(),
                )),
                ..Default::default()
            }
        }

        /// Extracts an ApplicationFailureInfo from a Failure instance if it exists
        pub fn maybe_application_failure(&self) -> Option<&ApplicationFailureInfo> {
            if let Failure {
                failure_info: Some(FailureInfo::ApplicationFailureInfo(f)),
                ..
            } = self
            {
                Some(f)
            } else {
                None
            }
        }

        // Checks if a failure is an ApplicationFailure with Benign category.
        pub fn is_benign_application_failure(&self) -> bool {
            self.maybe_application_failure()
                .is_some_and(|app_info| app_info.category() == ApplicationErrorCategory::Benign)
        }
    }

    impl Display for Failure {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "Failure({}, ", self.message)?;
            match self.failure_info.as_ref() {
                None => write!(f, "missing info")?,
                Some(FailureInfo::TimeoutFailureInfo(v)) => {
                    write!(f, "Timeout: {:?}", v.timeout_type())?;
                }
                Some(FailureInfo::ApplicationFailureInfo(v)) => {
                    write!(f, "Application Failure: {}", v.r#type)?;
                }
                Some(FailureInfo::CanceledFailureInfo(_)) => {
                    write!(f, "Cancelled")?;
                }
                Some(FailureInfo::TerminatedFailureInfo(_)) => {
                    write!(f, "Terminated")?;
                }
                Some(FailureInfo::ServerFailureInfo(_)) => {
                    write!(f, "Server Failure")?;
                }
                Some(FailureInfo::ResetWorkflowFailureInfo(_)) => {
                    write!(f, "Reset Workflow")?;
                }
                Some(FailureInfo::ActivityFailureInfo(v)) => {
                    write!(
                        f,
                        "Activity Failure: scheduled_event_id: {}",
                        v.scheduled_event_id
                    )?;
                }
                Some(FailureInfo::ChildWorkflowExecutionFailureInfo(v)) => {
                    write!(
                        f,
                        "Child Workflow: started_event_id: {}",
                        v.started_event_id
                    )?;
                }
                Some(FailureInfo::NexusOperationExecutionFailureInfo(v)) => {
                    write!(
                        f,
                        "Nexus Operation Failure: scheduled_event_id: {}",
                        v.scheduled_event_id
                    )?;
                }
                Some(FailureInfo::NexusHandlerFailureInfo(v)) => {
                    write!(f, "Nexus Handler Failure: {}", v.r#type)?;
                }
            }
            write!(f, ")")
        }
    }

    impl From<&str> for Failure {
        fn from(v: &str) -> Self {
            Failure::application_failure(v.to_string(), false)
        }
    }

    impl From<String> for Failure {
        fn from(v: String) -> Self {
            Failure::application_failure(v, false)
        }
    }

    impl From<anyhow::Error> for Failure {
        fn from(ae: anyhow::Error) -> Self {
            Failure::application_failure_from_error(ae, false)
        }
    }

    pub trait FromPayloadsExt {
        fn from_payloads(p: Option<Payloads>) -> Self;
    }
    impl<T> FromPayloadsExt for T
    where
        T: FromIterator<Payload>,
    {
        fn from_payloads(p: Option<Payloads>) -> Self {
            match p {
                None => std::iter::empty().collect(),
                Some(p) => p.payloads.into_iter().collect(),
            }
        }
    }

    pub trait IntoPayloadsExt {
        fn into_payloads(self) -> Option<Payloads>;
    }
    impl<T> IntoPayloadsExt for T
    where
        T: IntoIterator<Item = Payload>,
    {
        fn into_payloads(self) -> Option<Payloads> {
            let mut iterd = self.into_iter().peekable();
            if iterd.peek().is_none() {
                None
            } else {
                Some(Payloads {
                    payloads: iterd.collect(),
                })
            }
        }
    }

    impl From<Payload> for Payloads {
        fn from(p: Payload) -> Self {
            Self { payloads: vec![p] }
        }
    }

    impl<T> From<T> for Payloads
    where
        T: AsRef<[u8]>,
    {
        fn from(v: T) -> Self {
            Self {
                payloads: vec![v.into()],
            }
        }
    }

    #[derive(thiserror::Error, Debug)]
    pub enum PayloadDeserializeErr {
        /// This deserializer does not handle this type of payload. Allows composing multiple
        /// deserializers.
        #[error("This deserializer does not understand this payload")]
        DeserializerDoesNotHandle,
        #[error("Error during deserialization: {0}")]
        DeserializeErr(#[from] anyhow::Error),
    }

    // TODO: Once the prototype SDK is un-prototyped this serialization will need to be compat with
    //   other SDKs (given they might execute an activity).
    pub trait AsJsonPayloadExt {
        fn as_json_payload(&self) -> anyhow::Result<Payload>;
    }
    impl<T> AsJsonPayloadExt for T
    where
        T: Serialize,
    {
        fn as_json_payload(&self) -> anyhow::Result<Payload> {
            let as_json = serde_json::to_string(self)?;
            let mut metadata = HashMap::new();
            metadata.insert(
                ENCODING_PAYLOAD_KEY.to_string(),
                JSON_ENCODING_VAL.as_bytes().to_vec(),
            );
            Ok(Payload {
                metadata,
                data: as_json.into_bytes(),
            })
        }
    }

    pub trait FromJsonPayloadExt: Sized {
        fn from_json_payload(payload: &Payload) -> Result<Self, PayloadDeserializeErr>;
    }
    impl<T> FromJsonPayloadExt for T
    where
        T: for<'de> Deserialize<'de>,
    {
        fn from_json_payload(payload: &Payload) -> Result<Self, PayloadDeserializeErr> {
            if !payload.is_json_payload() {
                return Err(PayloadDeserializeErr::DeserializerDoesNotHandle);
            }
            let payload_str = std::str::from_utf8(&payload.data).map_err(anyhow::Error::from)?;
            Ok(serde_json::from_str(payload_str).map_err(anyhow::Error::from)?)
        }
    }

    /// Errors when converting from a [Payloads] api proto to our internal [Payload]
    #[derive(derive_more::Display, Debug)]
    pub enum PayloadsToPayloadError {
        MoreThanOnePayload,
        NoPayload,
    }
    impl TryFrom<Payloads> for Payload {
        type Error = PayloadsToPayloadError;

        fn try_from(mut v: Payloads) -> Result<Self, Self::Error> {
            match v.payloads.pop() {
                None => Err(PayloadsToPayloadError::NoPayload),
                Some(p) => {
                    if v.payloads.is_empty() {
                        Ok(p)
                    } else {
                        Err(PayloadsToPayloadError::MoreThanOnePayload)
                    }
                }
            }
        }
    }
}

// No need to lint these
#[allow(
    clippy::all,
    missing_docs,
    rustdoc::broken_intra_doc_links,
    rustdoc::bare_urls
)]
// This is disgusting, but unclear to me how to avoid it. TODO: Discuss w/ prost maintainer
pub mod temporal {
    pub mod api {
        pub mod activity {
            pub mod v1 {
                tonic::include_proto!("temporal.api.activity.v1");
            }
        }
        pub mod batch {
            pub mod v1 {
                tonic::include_proto!("temporal.api.batch.v1");
            }
        }
        pub mod command {
            pub mod v1 {
                tonic::include_proto!("temporal.api.command.v1");

                use crate::{
                    coresdk::{IntoPayloadsExt, workflow_commands},
                    temporal::api::{
                        common::v1::{ActivityType, WorkflowType},
                        enums::v1::CommandType,
                    },
                };
                use command::Attributes;
                use std::fmt::{Display, Formatter};

                impl From<command::Attributes> for Command {
                    fn from(c: command::Attributes) -> Self {
                        match c {
                            a @ Attributes::StartTimerCommandAttributes(_) => Self {
                                command_type: CommandType::StartTimer as i32,
                                attributes: Some(a),
                                user_metadata: Default::default(),
                            },
                            a @ Attributes::CancelTimerCommandAttributes(_) => Self {
                                command_type: CommandType::CancelTimer as i32,
                                attributes: Some(a),
                                user_metadata: Default::default(),
                            },
                            a @ Attributes::CompleteWorkflowExecutionCommandAttributes(_) => Self {
                                command_type: CommandType::CompleteWorkflowExecution as i32,
                                attributes: Some(a),
                                user_metadata: Default::default(),
                            },
                            a @ Attributes::FailWorkflowExecutionCommandAttributes(_) => Self {
                                command_type: CommandType::FailWorkflowExecution as i32,
                                attributes: Some(a),
                                user_metadata: Default::default(),
                            },
                            a @ Attributes::ScheduleActivityTaskCommandAttributes(_) => Self {
                                command_type: CommandType::ScheduleActivityTask as i32,
                                attributes: Some(a),
                                user_metadata: Default::default(),
                            },
                            a @ Attributes::RequestCancelActivityTaskCommandAttributes(_) => Self {
                                command_type: CommandType::RequestCancelActivityTask as i32,
                                attributes: Some(a),
                                user_metadata: Default::default(),
                            },
                            a @ Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(_) => {
                                Self {
                                    command_type: CommandType::ContinueAsNewWorkflowExecution
                                        as i32,
                                    attributes: Some(a),
                                    user_metadata: Default::default(),
                                }
                            }
                            a @ Attributes::CancelWorkflowExecutionCommandAttributes(_) => Self {
                                command_type: CommandType::CancelWorkflowExecution as i32,
                                attributes: Some(a),
                                user_metadata: Default::default(),
                            },
                            a @ Attributes::RecordMarkerCommandAttributes(_) => Self {
                                command_type: CommandType::RecordMarker as i32,
                                attributes: Some(a),
                                user_metadata: Default::default(),
                            },
                            a @ Attributes::ProtocolMessageCommandAttributes(_) => Self {
                                command_type: CommandType::ProtocolMessage as i32,
                                attributes: Some(a),
                                user_metadata: Default::default(),
                            },
                            a @ Attributes::RequestCancelNexusOperationCommandAttributes(_) => {
                                Self {
                                    command_type: CommandType::RequestCancelNexusOperation as i32,
                                    attributes: Some(a),
                                    user_metadata: Default::default(),
                                }
                            }
                            _ => unimplemented!(),
                        }
                    }
                }

                impl Display for Command {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        let ct = CommandType::try_from(self.command_type)
                            .unwrap_or(CommandType::Unspecified);
                        write!(f, "{:?}", ct)
                    }
                }

                pub trait CommandAttributesExt {
                    fn as_type(&self) -> CommandType;
                }

                impl CommandAttributesExt for command::Attributes {
                    fn as_type(&self) -> CommandType {
                        match self {
                            Attributes::ScheduleActivityTaskCommandAttributes(_) => {
                                CommandType::ScheduleActivityTask
                            }
                            Attributes::StartTimerCommandAttributes(_) => CommandType::StartTimer,
                            Attributes::CompleteWorkflowExecutionCommandAttributes(_) => {
                                CommandType::CompleteWorkflowExecution
                            }
                            Attributes::FailWorkflowExecutionCommandAttributes(_) => {
                                CommandType::FailWorkflowExecution
                            }
                            Attributes::RequestCancelActivityTaskCommandAttributes(_) => {
                                CommandType::RequestCancelActivityTask
                            }
                            Attributes::CancelTimerCommandAttributes(_) => CommandType::CancelTimer,
                            Attributes::CancelWorkflowExecutionCommandAttributes(_) => {
                                CommandType::CancelWorkflowExecution
                            }
                            Attributes::RequestCancelExternalWorkflowExecutionCommandAttributes(
                                _,
                            ) => CommandType::RequestCancelExternalWorkflowExecution,
                            Attributes::RecordMarkerCommandAttributes(_) => {
                                CommandType::RecordMarker
                            }
                            Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(_) => {
                                CommandType::ContinueAsNewWorkflowExecution
                            }
                            Attributes::StartChildWorkflowExecutionCommandAttributes(_) => {
                                CommandType::StartChildWorkflowExecution
                            }
                            Attributes::SignalExternalWorkflowExecutionCommandAttributes(_) => {
                                CommandType::SignalExternalWorkflowExecution
                            }
                            Attributes::UpsertWorkflowSearchAttributesCommandAttributes(_) => {
                                CommandType::UpsertWorkflowSearchAttributes
                            }
                            Attributes::ProtocolMessageCommandAttributes(_) => {
                                CommandType::ProtocolMessage
                            }
                            Attributes::ModifyWorkflowPropertiesCommandAttributes(_) => {
                                CommandType::ModifyWorkflowProperties
                            }
                            Attributes::ScheduleNexusOperationCommandAttributes(_) => {
                                CommandType::ScheduleNexusOperation
                            }
                            Attributes::RequestCancelNexusOperationCommandAttributes(_) => {
                                CommandType::RequestCancelNexusOperation
                            }
                        }
                    }
                }

                impl Display for command::Attributes {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        write!(f, "{:?}", self.as_type())
                    }
                }

                impl From<workflow_commands::StartTimer> for command::Attributes {
                    fn from(s: workflow_commands::StartTimer) -> Self {
                        Self::StartTimerCommandAttributes(StartTimerCommandAttributes {
                            timer_id: s.seq.to_string(),
                            start_to_fire_timeout: s.start_to_fire_timeout,
                        })
                    }
                }

                impl From<workflow_commands::UpsertWorkflowSearchAttributes> for command::Attributes {
                    fn from(s: workflow_commands::UpsertWorkflowSearchAttributes) -> Self {
                        Self::UpsertWorkflowSearchAttributesCommandAttributes(
                            UpsertWorkflowSearchAttributesCommandAttributes {
                                search_attributes: Some(s.search_attributes.into()),
                            },
                        )
                    }
                }

                impl From<workflow_commands::ModifyWorkflowProperties> for command::Attributes {
                    fn from(s: workflow_commands::ModifyWorkflowProperties) -> Self {
                        Self::ModifyWorkflowPropertiesCommandAttributes(
                            ModifyWorkflowPropertiesCommandAttributes {
                                upserted_memo: s.upserted_memo.map(Into::into),
                            },
                        )
                    }
                }

                impl From<workflow_commands::CancelTimer> for command::Attributes {
                    fn from(s: workflow_commands::CancelTimer) -> Self {
                        Self::CancelTimerCommandAttributes(CancelTimerCommandAttributes {
                            timer_id: s.seq.to_string(),
                        })
                    }
                }

                pub fn schedule_activity_cmd_to_api(
                    s: workflow_commands::ScheduleActivity,
                    use_workflow_build_id: bool,
                ) -> command::Attributes {
                    command::Attributes::ScheduleActivityTaskCommandAttributes(
                        ScheduleActivityTaskCommandAttributes {
                            activity_id: s.activity_id,
                            activity_type: Some(ActivityType {
                                name: s.activity_type,
                            }),
                            task_queue: Some(s.task_queue.into()),
                            header: Some(s.headers.into()),
                            input: s.arguments.into_payloads(),
                            schedule_to_close_timeout: s.schedule_to_close_timeout,
                            schedule_to_start_timeout: s.schedule_to_start_timeout,
                            start_to_close_timeout: s.start_to_close_timeout,
                            heartbeat_timeout: s.heartbeat_timeout,
                            retry_policy: s.retry_policy.map(Into::into),
                            request_eager_execution: !s.do_not_eagerly_execute,
                            use_workflow_build_id,
                            priority: s.priority,
                        },
                    )
                }

                #[allow(deprecated)]
                pub fn start_child_workflow_cmd_to_api(
                    s: workflow_commands::StartChildWorkflowExecution,
                    inherit_build_id: bool,
                ) -> command::Attributes {
                    command::Attributes::StartChildWorkflowExecutionCommandAttributes(
                        StartChildWorkflowExecutionCommandAttributes {
                            workflow_id: s.workflow_id,
                            workflow_type: Some(WorkflowType {
                                name: s.workflow_type,
                            }),
                            control: "".into(),
                            namespace: s.namespace,
                            task_queue: Some(s.task_queue.into()),
                            header: Some(s.headers.into()),
                            memo: Some(s.memo.into()),
                            search_attributes: Some(s.search_attributes.into()),
                            input: s.input.into_payloads(),
                            workflow_id_reuse_policy: s.workflow_id_reuse_policy,
                            workflow_execution_timeout: s.workflow_execution_timeout,
                            workflow_run_timeout: s.workflow_run_timeout,
                            workflow_task_timeout: s.workflow_task_timeout,
                            retry_policy: s.retry_policy.map(Into::into),
                            cron_schedule: s.cron_schedule.clone(),
                            parent_close_policy: s.parent_close_policy,
                            inherit_build_id,
                            priority: s.priority,
                        },
                    )
                }

                impl From<workflow_commands::CompleteWorkflowExecution> for command::Attributes {
                    fn from(c: workflow_commands::CompleteWorkflowExecution) -> Self {
                        Self::CompleteWorkflowExecutionCommandAttributes(
                            CompleteWorkflowExecutionCommandAttributes {
                                result: c.result.map(Into::into),
                            },
                        )
                    }
                }

                impl From<workflow_commands::FailWorkflowExecution> for command::Attributes {
                    fn from(c: workflow_commands::FailWorkflowExecution) -> Self {
                        Self::FailWorkflowExecutionCommandAttributes(
                            FailWorkflowExecutionCommandAttributes {
                                failure: c.failure.map(Into::into),
                            },
                        )
                    }
                }

                #[allow(deprecated)]
                pub fn continue_as_new_cmd_to_api(
                    c: workflow_commands::ContinueAsNewWorkflowExecution,
                    inherit_build_id: bool,
                ) -> command::Attributes {
                    command::Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(
                        ContinueAsNewWorkflowExecutionCommandAttributes {
                            workflow_type: Some(c.workflow_type.into()),
                            task_queue: Some(c.task_queue.into()),
                            input: c.arguments.into_payloads(),
                            workflow_run_timeout: c.workflow_run_timeout,
                            workflow_task_timeout: c.workflow_task_timeout,
                            memo: if c.memo.is_empty() {
                                None
                            } else {
                                Some(c.memo.into())
                            },
                            header: if c.headers.is_empty() {
                                None
                            } else {
                                Some(c.headers.into())
                            },
                            retry_policy: c.retry_policy,
                            search_attributes: if c.search_attributes.is_empty() {
                                None
                            } else {
                                Some(c.search_attributes.into())
                            },
                            inherit_build_id,
                            ..Default::default()
                        },
                    )
                }

                impl From<workflow_commands::CancelWorkflowExecution> for command::Attributes {
                    fn from(_c: workflow_commands::CancelWorkflowExecution) -> Self {
                        Self::CancelWorkflowExecutionCommandAttributes(
                            CancelWorkflowExecutionCommandAttributes { details: None },
                        )
                    }
                }

                impl From<workflow_commands::ScheduleNexusOperation> for command::Attributes {
                    fn from(c: workflow_commands::ScheduleNexusOperation) -> Self {
                        Self::ScheduleNexusOperationCommandAttributes(
                            ScheduleNexusOperationCommandAttributes {
                                endpoint: c.endpoint,
                                service: c.service,
                                operation: c.operation,
                                input: c.input,
                                schedule_to_close_timeout: c.schedule_to_close_timeout,
                                nexus_header: c.nexus_header,
                            },
                        )
                    }
                }
            }
        }
        pub mod cloud {
            pub mod account {
                pub mod v1 {
                    tonic::include_proto!("temporal.api.cloud.account.v1");
                }
            }
            pub mod cloudservice {
                pub mod v1 {
                    tonic::include_proto!("temporal.api.cloud.cloudservice.v1");
                }
            }
            pub mod connectivityrule {
                pub mod v1 {
                    tonic::include_proto!("temporal.api.cloud.connectivityrule.v1");
                }
            }
            pub mod identity {
                pub mod v1 {
                    tonic::include_proto!("temporal.api.cloud.identity.v1");
                }
            }
            pub mod namespace {
                pub mod v1 {
                    tonic::include_proto!("temporal.api.cloud.namespace.v1");
                }
            }
            pub mod nexus {
                pub mod v1 {
                    tonic::include_proto!("temporal.api.cloud.nexus.v1");
                }
            }
            pub mod operation {
                pub mod v1 {
                    tonic::include_proto!("temporal.api.cloud.operation.v1");
                }
            }
            pub mod region {
                pub mod v1 {
                    tonic::include_proto!("temporal.api.cloud.region.v1");
                }
            }
            pub mod resource {
                pub mod v1 {
                    tonic::include_proto!("temporal.api.cloud.resource.v1");
                }
            }
            pub mod sink {
                pub mod v1 {
                    tonic::include_proto!("temporal.api.cloud.sink.v1");
                }
            }
            pub mod usage {
                pub mod v1 {
                    tonic::include_proto!("temporal.api.cloud.usage.v1");
                }
            }
        }
        pub mod common {
            pub mod v1 {
                use crate::{ENCODING_PAYLOAD_KEY, JSON_ENCODING_VAL};
                use base64::{Engine, prelude::BASE64_STANDARD};
                use std::{
                    collections::HashMap,
                    fmt::{Display, Formatter},
                };
                tonic::include_proto!("temporal.api.common.v1");

                impl<T> From<T> for Payload
                where
                    T: AsRef<[u8]>,
                {
                    fn from(v: T) -> Self {
                        // TODO: Set better encodings, whole data converter deal. Setting anything
                        //  for now at least makes it show up in the web UI.
                        let mut metadata = HashMap::new();
                        metadata.insert(ENCODING_PAYLOAD_KEY.to_string(), b"binary/plain".to_vec());
                        Self {
                            metadata,
                            data: v.as_ref().to_vec(),
                        }
                    }
                }

                impl Payload {
                    // Is its own function b/c asref causes implementation conflicts
                    pub fn as_slice(&self) -> &[u8] {
                        self.data.as_slice()
                    }

                    pub fn is_json_payload(&self) -> bool {
                        self.metadata
                            .get(ENCODING_PAYLOAD_KEY)
                            .map(|v| v.as_slice() == JSON_ENCODING_VAL.as_bytes())
                            .unwrap_or_default()
                    }
                }

                impl std::fmt::Debug for Payload {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        if std::env::var("TEMPORAL_PRINT_FULL_PAYLOADS").is_err()
                            && self.data.len() > 64
                        {
                            let mut windows = self.data.as_slice().windows(32);
                            write!(
                                f,
                                "[{}..{}]",
                                BASE64_STANDARD.encode(windows.next().unwrap_or_default()),
                                BASE64_STANDARD.encode(windows.next_back().unwrap_or_default())
                            )
                        } else {
                            write!(f, "[{}]", BASE64_STANDARD.encode(&self.data))
                        }
                    }
                }

                impl Display for Payload {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        write!(f, "{:?}", self)
                    }
                }

                impl Display for Header {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        write!(f, "Header(")?;
                        for kv in &self.fields {
                            write!(f, "{}: ", kv.0)?;
                            write!(f, "{}, ", kv.1)?;
                        }
                        write!(f, ")")
                    }
                }

                impl From<Header> for HashMap<String, Payload> {
                    fn from(h: Header) -> Self {
                        h.fields.into_iter().map(|(k, v)| (k, v.into())).collect()
                    }
                }

                impl From<Memo> for HashMap<String, Payload> {
                    fn from(h: Memo) -> Self {
                        h.fields.into_iter().map(|(k, v)| (k, v.into())).collect()
                    }
                }

                impl From<SearchAttributes> for HashMap<String, Payload> {
                    fn from(h: SearchAttributes) -> Self {
                        h.indexed_fields
                            .into_iter()
                            .map(|(k, v)| (k, v.into()))
                            .collect()
                    }
                }

                impl From<HashMap<String, Payload>> for SearchAttributes {
                    fn from(h: HashMap<String, Payload>) -> Self {
                        Self {
                            indexed_fields: h.into_iter().map(|(k, v)| (k, v.into())).collect(),
                        }
                    }
                }

                impl From<String> for ActivityType {
                    fn from(name: String) -> Self {
                        Self { name }
                    }
                }

                impl From<&str> for ActivityType {
                    fn from(name: &str) -> Self {
                        Self {
                            name: name.to_string(),
                        }
                    }
                }

                impl From<ActivityType> for String {
                    fn from(at: ActivityType) -> Self {
                        at.name
                    }
                }

                impl From<&str> for WorkflowType {
                    fn from(v: &str) -> Self {
                        Self {
                            name: v.to_string(),
                        }
                    }
                }
            }
        }
        pub mod deployment {
            pub mod v1 {
                tonic::include_proto!("temporal.api.deployment.v1");
            }
        }
        pub mod enums {
            pub mod v1 {
                tonic::include_proto!("temporal.api.enums.v1");
            }
        }
        pub mod failure {
            pub mod v1 {
                tonic::include_proto!("temporal.api.failure.v1");
            }
        }
        pub mod filter {
            pub mod v1 {
                tonic::include_proto!("temporal.api.filter.v1");
            }
        }
        pub mod history {
            pub mod v1 {
                use crate::temporal::api::{
                    enums::v1::EventType, history::v1::history_event::Attributes,
                };
                use anyhow::bail;
                use prost::alloc::fmt::Formatter;
                use std::fmt::Display;

                tonic::include_proto!("temporal.api.history.v1");

                impl History {
                    pub fn extract_run_id_from_start(&self) -> Result<&str, anyhow::Error> {
                        extract_original_run_id_from_events(&self.events)
                    }

                    /// Returns the event id of the final event in the history. Will return 0 if
                    /// there are no events.
                    pub fn last_event_id(&self) -> i64 {
                        self.events.last().map(|e| e.event_id).unwrap_or_default()
                    }
                }

                pub fn extract_original_run_id_from_events(
                    events: &[HistoryEvent],
                ) -> Result<&str, anyhow::Error> {
                    if let Some(Attributes::WorkflowExecutionStartedEventAttributes(wes)) =
                        events.get(0).and_then(|x| x.attributes.as_ref())
                    {
                        Ok(&wes.original_execution_run_id)
                    } else {
                        bail!("First event is not WorkflowExecutionStarted?!?")
                    }
                }

                impl HistoryEvent {
                    /// Returns true if this is an event created to mirror a command
                    pub fn is_command_event(&self) -> bool {
                        EventType::try_from(self.event_type).map_or(false, |et| match et {
                            EventType::ActivityTaskScheduled
                            | EventType::ActivityTaskCancelRequested
                            | EventType::MarkerRecorded
                            | EventType::RequestCancelExternalWorkflowExecutionInitiated
                            | EventType::SignalExternalWorkflowExecutionInitiated
                            | EventType::StartChildWorkflowExecutionInitiated
                            | EventType::TimerCanceled
                            | EventType::TimerStarted
                            | EventType::UpsertWorkflowSearchAttributes
                            | EventType::WorkflowPropertiesModified
                            | EventType::NexusOperationScheduled
                            | EventType::NexusOperationCancelRequested
                            | EventType::WorkflowExecutionCanceled
                            | EventType::WorkflowExecutionCompleted
                            | EventType::WorkflowExecutionContinuedAsNew
                            | EventType::WorkflowExecutionFailed
                            | EventType::WorkflowExecutionUpdateAccepted
                            | EventType::WorkflowExecutionUpdateRejected
                            | EventType::WorkflowExecutionUpdateCompleted => true,
                            _ => false,
                        })
                    }

                    /// Returns the command's initiating event id, if present. This is the id of the
                    /// event which "started" the command. Usually, the "scheduled" event for the
                    /// command.
                    pub fn get_initial_command_event_id(&self) -> Option<i64> {
                        self.attributes.as_ref().and_then(|a| {
                            // Fun! Not really any way to make this better w/o incompatibly changing
                            // protos.
                            match a {
                                Attributes::ActivityTaskStartedEventAttributes(a) =>
                                    Some(a.scheduled_event_id),
                                Attributes::ActivityTaskCompletedEventAttributes(a) =>
                                    Some(a.scheduled_event_id),
                                Attributes::ActivityTaskFailedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::ActivityTaskTimedOutEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::ActivityTaskCancelRequestedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::ActivityTaskCanceledEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::TimerFiredEventAttributes(a) => Some(a.started_event_id),
                                Attributes::TimerCanceledEventAttributes(a) => Some(a.started_event_id),
                                Attributes::RequestCancelExternalWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ExternalWorkflowExecutionCancelRequestedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::StartChildWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionStartedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionCompletedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionCanceledEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionTimedOutEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ChildWorkflowExecutionTerminatedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::SignalExternalWorkflowExecutionFailedEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::ExternalWorkflowExecutionSignaledEventAttributes(a) => Some(a.initiated_event_id),
                                Attributes::WorkflowTaskStartedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::WorkflowTaskCompletedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::WorkflowTaskTimedOutEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::WorkflowTaskFailedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::NexusOperationStartedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::NexusOperationCompletedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::NexusOperationFailedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::NexusOperationTimedOutEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::NexusOperationCanceledEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::NexusOperationCancelRequestedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::NexusOperationCancelRequestCompletedEventAttributes(a) => Some(a.scheduled_event_id),
                                Attributes::NexusOperationCancelRequestFailedEventAttributes(a) => Some(a.scheduled_event_id),
                                _ => None
                            }
                        })
                    }

                    /// Return the event's associated protocol instance, if one exists.
                    pub fn get_protocol_instance_id(&self) -> Option<&str> {
                        self.attributes.as_ref().and_then(|a| match a {
                            Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(a) => {
                                Some(a.protocol_instance_id.as_str())
                            }
                            _ => None,
                        })
                    }

                    /// Returns true if the event is one which would end a workflow
                    pub fn is_final_wf_execution_event(&self) -> bool {
                        match self.event_type() {
                            EventType::WorkflowExecutionCompleted => true,
                            EventType::WorkflowExecutionCanceled => true,
                            EventType::WorkflowExecutionFailed => true,
                            EventType::WorkflowExecutionTimedOut => true,
                            EventType::WorkflowExecutionContinuedAsNew => true,
                            EventType::WorkflowExecutionTerminated => true,
                            _ => false,
                        }
                    }

                    pub fn is_wft_closed_event(&self) -> bool {
                        match self.event_type() {
                            EventType::WorkflowTaskCompleted => true,
                            EventType::WorkflowTaskFailed => true,
                            EventType::WorkflowTaskTimedOut => true,
                            _ => false,
                        }
                    }

                    pub fn is_ignorable(&self) -> bool {
                        if !self.worker_may_ignore {
                            return false;
                        }
                        // Never add a catch-all case to this match statement. We need to explicitly
                        // mark any new event types as ignorable or not.
                        if let Some(a) = self.attributes.as_ref() {
                            match a {
                                Attributes::WorkflowExecutionStartedEventAttributes(_) => false,
                                Attributes::WorkflowExecutionCompletedEventAttributes(_) => false,
                                Attributes::WorkflowExecutionFailedEventAttributes(_) => false,
                                Attributes::WorkflowExecutionTimedOutEventAttributes(_) => false,
                                Attributes::WorkflowTaskScheduledEventAttributes(_) => false,
                                Attributes::WorkflowTaskStartedEventAttributes(_) => false,
                                Attributes::WorkflowTaskCompletedEventAttributes(_) => false,
                                Attributes::WorkflowTaskTimedOutEventAttributes(_) => false,
                                Attributes::WorkflowTaskFailedEventAttributes(_) => false,
                                Attributes::ActivityTaskScheduledEventAttributes(_) => false,
                                Attributes::ActivityTaskStartedEventAttributes(_) => false,
                                Attributes::ActivityTaskCompletedEventAttributes(_) => false,
                                Attributes::ActivityTaskFailedEventAttributes(_) => false,
                                Attributes::ActivityTaskTimedOutEventAttributes(_) => false,
                                Attributes::TimerStartedEventAttributes(_) => false,
                                Attributes::TimerFiredEventAttributes(_) => false,
                                Attributes::ActivityTaskCancelRequestedEventAttributes(_) => false,
                                Attributes::ActivityTaskCanceledEventAttributes(_) => false,
                                Attributes::TimerCanceledEventAttributes(_) => false,
                                Attributes::MarkerRecordedEventAttributes(_) => false,
                                Attributes::WorkflowExecutionSignaledEventAttributes(_) => false,
                                Attributes::WorkflowExecutionTerminatedEventAttributes(_) => false,
                                Attributes::WorkflowExecutionCancelRequestedEventAttributes(_) => false,
                                Attributes::WorkflowExecutionCanceledEventAttributes(_) => false,
                                Attributes::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(_) => false,
                                Attributes::RequestCancelExternalWorkflowExecutionFailedEventAttributes(_) => false,
                                Attributes::ExternalWorkflowExecutionCancelRequestedEventAttributes(_) => false,
                                Attributes::WorkflowExecutionContinuedAsNewEventAttributes(_) => false,
                                Attributes::StartChildWorkflowExecutionInitiatedEventAttributes(_) => false,
                                Attributes::StartChildWorkflowExecutionFailedEventAttributes(_) => false,
                                Attributes::ChildWorkflowExecutionStartedEventAttributes(_) => false,
                                Attributes::ChildWorkflowExecutionCompletedEventAttributes(_) => false,
                                Attributes::ChildWorkflowExecutionFailedEventAttributes(_) => false,
                                Attributes::ChildWorkflowExecutionCanceledEventAttributes(_) => false,
                                Attributes::ChildWorkflowExecutionTimedOutEventAttributes(_) => false,
                                Attributes::ChildWorkflowExecutionTerminatedEventAttributes(_) => false,
                                Attributes::SignalExternalWorkflowExecutionInitiatedEventAttributes(_) => false,
                                Attributes::SignalExternalWorkflowExecutionFailedEventAttributes(_) => false,
                                Attributes::ExternalWorkflowExecutionSignaledEventAttributes(_) => false,
                                Attributes::UpsertWorkflowSearchAttributesEventAttributes(_) => false,
                                Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(_) => false,
                                Attributes::WorkflowExecutionUpdateRejectedEventAttributes(_) => false,
                                Attributes::WorkflowExecutionUpdateCompletedEventAttributes(_) => false,
                                Attributes::WorkflowPropertiesModifiedExternallyEventAttributes(_) => false,
                                Attributes::ActivityPropertiesModifiedExternallyEventAttributes(_) => false,
                                Attributes::WorkflowPropertiesModifiedEventAttributes(_) => false,
                                Attributes::WorkflowExecutionUpdateAdmittedEventAttributes(_) => false,
                                Attributes::NexusOperationScheduledEventAttributes(_) => false,
                                Attributes::NexusOperationStartedEventAttributes(_) => false,
                                Attributes::NexusOperationCompletedEventAttributes(_) => false,
                                Attributes::NexusOperationFailedEventAttributes(_) => false,
                                Attributes::NexusOperationCanceledEventAttributes(_) => false,
                                Attributes::NexusOperationTimedOutEventAttributes(_) => false,
                                Attributes::NexusOperationCancelRequestedEventAttributes(_) => false,
                                // !! Ignorable !!
                                Attributes::WorkflowExecutionOptionsUpdatedEventAttributes(_) => true,
                                Attributes::NexusOperationCancelRequestCompletedEventAttributes(_) => false,
                                Attributes::NexusOperationCancelRequestFailedEventAttributes(_) => false,
                            }
                        } else {
                            false
                        }
                    }
                }

                impl Display for HistoryEvent {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        write!(
                            f,
                            "HistoryEvent(id: {}, {:?})",
                            self.event_id,
                            EventType::try_from(self.event_type).unwrap_or_default()
                        )
                    }
                }

                impl Attributes {
                    pub fn event_type(&self) -> EventType {
                        // I just absolutely _love_ this
                        match self {
                            Attributes::WorkflowExecutionStartedEventAttributes(_) => { EventType::WorkflowExecutionStarted }
                            Attributes::WorkflowExecutionCompletedEventAttributes(_) => { EventType::WorkflowExecutionCompleted }
                            Attributes::WorkflowExecutionFailedEventAttributes(_) => { EventType::WorkflowExecutionFailed }
                            Attributes::WorkflowExecutionTimedOutEventAttributes(_) => { EventType::WorkflowExecutionTimedOut }
                            Attributes::WorkflowTaskScheduledEventAttributes(_) => { EventType::WorkflowTaskScheduled }
                            Attributes::WorkflowTaskStartedEventAttributes(_) => { EventType::WorkflowTaskStarted }
                            Attributes::WorkflowTaskCompletedEventAttributes(_) => { EventType::WorkflowTaskCompleted }
                            Attributes::WorkflowTaskTimedOutEventAttributes(_) => { EventType::WorkflowTaskTimedOut }
                            Attributes::WorkflowTaskFailedEventAttributes(_) => { EventType::WorkflowTaskFailed }
                            Attributes::ActivityTaskScheduledEventAttributes(_) => { EventType::ActivityTaskScheduled }
                            Attributes::ActivityTaskStartedEventAttributes(_) => { EventType::ActivityTaskStarted }
                            Attributes::ActivityTaskCompletedEventAttributes(_) => { EventType::ActivityTaskCompleted }
                            Attributes::ActivityTaskFailedEventAttributes(_) => { EventType::ActivityTaskFailed }
                            Attributes::ActivityTaskTimedOutEventAttributes(_) => { EventType::ActivityTaskTimedOut }
                            Attributes::TimerStartedEventAttributes(_) => { EventType::TimerStarted }
                            Attributes::TimerFiredEventAttributes(_) => { EventType::TimerFired }
                            Attributes::ActivityTaskCancelRequestedEventAttributes(_) => { EventType::ActivityTaskCancelRequested }
                            Attributes::ActivityTaskCanceledEventAttributes(_) => { EventType::ActivityTaskCanceled }
                            Attributes::TimerCanceledEventAttributes(_) => { EventType::TimerCanceled }
                            Attributes::MarkerRecordedEventAttributes(_) => { EventType::MarkerRecorded }
                            Attributes::WorkflowExecutionSignaledEventAttributes(_) => { EventType::WorkflowExecutionSignaled }
                            Attributes::WorkflowExecutionTerminatedEventAttributes(_) => { EventType::WorkflowExecutionTerminated }
                            Attributes::WorkflowExecutionCancelRequestedEventAttributes(_) => { EventType::WorkflowExecutionCancelRequested }
                            Attributes::WorkflowExecutionCanceledEventAttributes(_) => { EventType::WorkflowExecutionCanceled }
                            Attributes::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(_) => { EventType::RequestCancelExternalWorkflowExecutionInitiated }
                            Attributes::RequestCancelExternalWorkflowExecutionFailedEventAttributes(_) => { EventType::RequestCancelExternalWorkflowExecutionFailed }
                            Attributes::ExternalWorkflowExecutionCancelRequestedEventAttributes(_) => { EventType::ExternalWorkflowExecutionCancelRequested }
                            Attributes::WorkflowExecutionContinuedAsNewEventAttributes(_) => { EventType::WorkflowExecutionContinuedAsNew }
                            Attributes::StartChildWorkflowExecutionInitiatedEventAttributes(_) => { EventType::StartChildWorkflowExecutionInitiated }
                            Attributes::StartChildWorkflowExecutionFailedEventAttributes(_) => { EventType::StartChildWorkflowExecutionFailed }
                            Attributes::ChildWorkflowExecutionStartedEventAttributes(_) => { EventType::ChildWorkflowExecutionStarted }
                            Attributes::ChildWorkflowExecutionCompletedEventAttributes(_) => { EventType::ChildWorkflowExecutionCompleted }
                            Attributes::ChildWorkflowExecutionFailedEventAttributes(_) => { EventType::ChildWorkflowExecutionFailed }
                            Attributes::ChildWorkflowExecutionCanceledEventAttributes(_) => { EventType::ChildWorkflowExecutionCanceled }
                            Attributes::ChildWorkflowExecutionTimedOutEventAttributes(_) => { EventType::ChildWorkflowExecutionTimedOut }
                            Attributes::ChildWorkflowExecutionTerminatedEventAttributes(_) => { EventType::ChildWorkflowExecutionTerminated }
                            Attributes::SignalExternalWorkflowExecutionInitiatedEventAttributes(_) => { EventType::SignalExternalWorkflowExecutionInitiated }
                            Attributes::SignalExternalWorkflowExecutionFailedEventAttributes(_) => { EventType::SignalExternalWorkflowExecutionFailed }
                            Attributes::ExternalWorkflowExecutionSignaledEventAttributes(_) => { EventType::ExternalWorkflowExecutionSignaled }
                            Attributes::UpsertWorkflowSearchAttributesEventAttributes(_) => { EventType::UpsertWorkflowSearchAttributes }
                            Attributes::WorkflowExecutionUpdateAdmittedEventAttributes(_) => { EventType::WorkflowExecutionUpdateAdmitted }
                            Attributes::WorkflowExecutionUpdateRejectedEventAttributes(_) => { EventType::WorkflowExecutionUpdateRejected }
                            Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(_) => { EventType::WorkflowExecutionUpdateAccepted }
                            Attributes::WorkflowExecutionUpdateCompletedEventAttributes(_) => { EventType::WorkflowExecutionUpdateCompleted }
                            Attributes::WorkflowPropertiesModifiedExternallyEventAttributes(_) => { EventType::WorkflowPropertiesModifiedExternally }
                            Attributes::ActivityPropertiesModifiedExternallyEventAttributes(_) => { EventType::ActivityPropertiesModifiedExternally }
                            Attributes::WorkflowPropertiesModifiedEventAttributes(_) => { EventType::WorkflowPropertiesModified }
                            Attributes::NexusOperationScheduledEventAttributes(_) => { EventType::NexusOperationScheduled }
                            Attributes::NexusOperationStartedEventAttributes(_) => { EventType::NexusOperationStarted }
                            Attributes::NexusOperationCompletedEventAttributes(_) => { EventType::NexusOperationCompleted }
                            Attributes::NexusOperationFailedEventAttributes(_) => { EventType::NexusOperationFailed }
                            Attributes::NexusOperationCanceledEventAttributes(_) => { EventType::NexusOperationCanceled }
                            Attributes::NexusOperationTimedOutEventAttributes(_) => { EventType::NexusOperationTimedOut }
                            Attributes::NexusOperationCancelRequestedEventAttributes(_) => { EventType::NexusOperationCancelRequested }
                            Attributes::WorkflowExecutionOptionsUpdatedEventAttributes(_) => { EventType::WorkflowExecutionOptionsUpdated }
                            Attributes::NexusOperationCancelRequestCompletedEventAttributes(_) => { EventType::NexusOperationCancelRequestCompleted }
                            Attributes::NexusOperationCancelRequestFailedEventAttributes(_) => { EventType::NexusOperationCancelRequestFailed }
                        }
                    }
                }
            }
        }
        pub mod namespace {
            pub mod v1 {
                tonic::include_proto!("temporal.api.namespace.v1");
            }
        }
        pub mod operatorservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.operatorservice.v1");
            }
        }
        pub mod protocol {
            pub mod v1 {
                use std::fmt::{Display, Formatter};
                tonic::include_proto!("temporal.api.protocol.v1");

                impl Display for Message {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        write!(f, "ProtocolMessage({})", self.id)
                    }
                }
            }
        }
        pub mod query {
            pub mod v1 {
                tonic::include_proto!("temporal.api.query.v1");
            }
        }
        pub mod replication {
            pub mod v1 {
                tonic::include_proto!("temporal.api.replication.v1");
            }
        }
        pub mod rules {
            pub mod v1 {
                tonic::include_proto!("temporal.api.rules.v1");
            }
        }
        pub mod schedule {
            #[allow(rustdoc::invalid_html_tags)]
            pub mod v1 {
                tonic::include_proto!("temporal.api.schedule.v1");
            }
        }
        pub mod sdk {
            pub mod v1 {
                tonic::include_proto!("temporal.api.sdk.v1");
            }
        }
        pub mod taskqueue {
            pub mod v1 {
                use crate::temporal::api::enums::v1::TaskQueueKind;
                tonic::include_proto!("temporal.api.taskqueue.v1");

                impl From<String> for TaskQueue {
                    fn from(name: String) -> Self {
                        Self {
                            name,
                            kind: TaskQueueKind::Normal as i32,
                            normal_name: "".to_string(),
                        }
                    }
                }
            }
        }
        pub mod testservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.testservice.v1");
            }
        }
        pub mod update {
            pub mod v1 {
                use crate::temporal::api::update::v1::outcome::Value;
                tonic::include_proto!("temporal.api.update.v1");

                impl Outcome {
                    pub fn is_success(&self) -> bool {
                        match self.value {
                            Some(Value::Success(_)) => true,
                            _ => false,
                        }
                    }
                }
            }
        }
        pub mod version {
            pub mod v1 {
                tonic::include_proto!("temporal.api.version.v1");
            }
        }
        pub mod worker {
            pub mod v1 {
                tonic::include_proto!("temporal.api.worker.v1");
            }
        }
        pub mod workflow {
            pub mod v1 {
                tonic::include_proto!("temporal.api.workflow.v1");
            }
        }
        pub mod nexus {
            pub mod v1 {
                use crate::{
                    camel_case_to_screaming_snake,
                    temporal::api::{
                        common,
                        common::v1::link::{WorkflowEvent, workflow_event},
                        enums::v1::EventType,
                    },
                };
                use anyhow::{anyhow, bail};
                use std::fmt::{Display, Formatter};
                use tonic::transport::Uri;

                tonic::include_proto!("temporal.api.nexus.v1");

                impl Display for Response {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        write!(f, "NexusResponse(",)?;
                        match &self.variant {
                            None => {}
                            Some(v) => {
                                write!(f, "{v}")?;
                            }
                        }
                        write!(f, ")")
                    }
                }

                impl Display for response::Variant {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        match self {
                            response::Variant::StartOperation(_) => {
                                write!(f, "StartOperation")
                            }
                            response::Variant::CancelOperation(_) => {
                                write!(f, "CancelOperation")
                            }
                        }
                    }
                }

                impl Display for HandlerError {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        write!(f, "HandlerError")
                    }
                }

                static SCHEME_PREFIX: &str = "temporal://";

                /// Attempt to parse a nexus lint into a workflow event link
                pub fn workflow_event_link_from_nexus(
                    l: &Link,
                ) -> Result<common::v1::Link, anyhow::Error> {
                    if !l.url.starts_with(SCHEME_PREFIX) {
                        bail!("Invalid scheme for nexus link: {:?}", l.url);
                    }
                    // We strip the scheme/authority portion because of
                    // https://github.com/hyperium/http/issues/696
                    let no_authority_url = l.url.strip_prefix(SCHEME_PREFIX).unwrap();
                    let uri = Uri::try_from(no_authority_url)?;
                    let parts = uri.into_parts();
                    let path = parts.path_and_query.ok_or_else(|| {
                        anyhow!("Failed to parse nexus link, invalid path: {:?}", l)
                    })?;
                    let path_parts = path.path().split('/').collect::<Vec<_>>();
                    if path_parts.get(1) != Some(&"namespaces") {
                        bail!("Invalid path for nexus link: {:?}", l);
                    }
                    let namespace = path_parts.get(2).ok_or_else(|| {
                        anyhow!("Failed to parse nexus link, no namespace: {:?}", l)
                    })?;
                    if path_parts.get(3) != Some(&"workflows") {
                        bail!("Invalid path for nexus link, no workflows segment: {:?}", l);
                    }
                    let workflow_id = path_parts.get(4).ok_or_else(|| {
                        anyhow!("Failed to parse nexus link, no workflow id: {:?}", l)
                    })?;
                    let run_id = path_parts
                        .get(5)
                        .ok_or_else(|| anyhow!("Failed to parse nexus link, no run id: {:?}", l))?;
                    if path_parts.get(6) != Some(&"history") {
                        bail!("Invalid path for nexus link, no history segment: {:?}", l);
                    }
                    let reference = if let Some(query) = path.query() {
                        let mut eventref = workflow_event::EventReference::default();
                        let query_parts = query.split('&').collect::<Vec<_>>();
                        for qp in query_parts {
                            let mut kv = qp.split('=');
                            let key = kv.next().ok_or_else(|| {
                                anyhow!("Failed to parse nexus link query parameter: {:?}", l)
                            })?;
                            let val = kv.next().ok_or_else(|| {
                                anyhow!("Failed to parse nexus link query parameter: {:?}", l)
                            })?;
                            match key {
                                "eventID" => {
                                    eventref.event_id = val.parse().map_err(|_| {
                                        anyhow!("Failed to parse nexus link event id: {:?}", l)
                                    })?;
                                }
                                "eventType" => {
                                    eventref.event_type = EventType::from_str_name(val)
                                        .unwrap_or_else(|| {
                                            EventType::from_str_name(
                                                &("EVENT_TYPE_".to_string()
                                                    + &camel_case_to_screaming_snake(val)),
                                            )
                                            .unwrap_or_default()
                                        })
                                        .into()
                                }
                                _ => continue,
                            }
                        }
                        Some(workflow_event::Reference::EventRef(eventref))
                    } else {
                        None
                    };

                    Ok(common::v1::Link {
                        variant: Some(common::v1::link::Variant::WorkflowEvent(WorkflowEvent {
                            namespace: namespace.to_string(),
                            workflow_id: workflow_id.to_string(),
                            run_id: run_id.to_string(),
                            reference,
                        })),
                    })
                }
            }
        }
        pub mod workflowservice {
            pub mod v1 {
                use std::{
                    convert::TryInto,
                    fmt::{Display, Formatter},
                    time::{Duration, SystemTime},
                };

                tonic::include_proto!("temporal.api.workflowservice.v1");

                macro_rules! sched_to_start_impl {
                    ($sched_field:ident) => {
                        /// Return the duration of the task schedule time (current attempt) to its
                        /// start time if both are set and time went forward.
                        pub fn sched_to_start(&self) -> Option<Duration> {
                            if let Some((sch, st)) =
                                self.$sched_field.clone().zip(self.started_time.clone())
                            {
                                if let Some(value) = elapsed_between_prost_times(sch, st) {
                                    return value;
                                }
                            }
                            None
                        }
                    };
                }

                fn elapsed_between_prost_times(
                    from: prost_wkt_types::Timestamp,
                    to: prost_wkt_types::Timestamp,
                ) -> Option<Option<Duration>> {
                    let from: Result<SystemTime, _> = from.try_into();
                    let to: Result<SystemTime, _> = to.try_into();
                    if let (Ok(from), Ok(to)) = (from, to) {
                        return Some(to.duration_since(from).ok());
                    }
                    None
                }

                impl PollWorkflowTaskQueueResponse {
                    sched_to_start_impl!(scheduled_time);
                }

                impl Display for PollWorkflowTaskQueueResponse {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        let last_event = self
                            .history
                            .as_ref()
                            .and_then(|h| h.events.last().map(|he| he.event_id))
                            .unwrap_or(0);
                        write!(
                            f,
                            "PollWFTQResp(run_id: {}, attempt: {}, last_event: {})",
                            self.workflow_execution
                                .as_ref()
                                .map_or("", |we| we.run_id.as_str()),
                            self.attempt,
                            last_event
                        )
                    }
                }

                /// Can be used while debugging to avoid filling up a whole screen with poll resps
                pub struct CompactHist<'a>(pub &'a PollWorkflowTaskQueueResponse);
                impl Display for CompactHist<'_> {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        writeln!(
                            f,
                            "PollWorkflowTaskQueueResponse (prev_started: {}, started: {})",
                            self.0.previous_started_event_id, self.0.started_event_id
                        )?;
                        if let Some(h) = self.0.history.as_ref() {
                            for event in &h.events {
                                writeln!(f, "{}", event)?;
                            }
                        }
                        writeln!(f, "query: {:#?}", self.0.query)?;
                        writeln!(f, "queries: {:#?}", self.0.queries)
                    }
                }

                impl PollActivityTaskQueueResponse {
                    sched_to_start_impl!(current_attempt_scheduled_time);
                }

                impl PollNexusTaskQueueResponse {
                    pub fn sched_to_start(&self) -> Option<Duration> {
                        if let Some((sch, st)) = self
                            .request
                            .as_ref()
                            .and_then(|r| r.scheduled_time)
                            .clone()
                            .zip(SystemTime::now().try_into().ok())
                        {
                            if let Some(value) = elapsed_between_prost_times(sch, st) {
                                return value;
                            }
                        }
                        None
                    }
                }

                impl QueryWorkflowResponse {
                    /// Unwrap a successful response as vec of payloads
                    pub fn unwrap(self) -> Vec<crate::temporal::api::common::v1::Payload> {
                        self.query_result.unwrap().payloads
                    }
                }
            }
        }
    }
}

#[allow(
    clippy::all,
    missing_docs,
    rustdoc::broken_intra_doc_links,
    rustdoc::bare_urls
)]
pub mod grpc {
    pub mod health {
        pub mod v1 {
            tonic::include_proto!("grpc.health.v1");
        }
    }
}

/// Case conversion, used for json -> proto enum string conversion
pub fn camel_case_to_screaming_snake(val: &str) -> String {
    let mut out = String::new();
    let mut last_was_upper = true;
    for c in val.chars() {
        if c.is_uppercase() {
            if !last_was_upper {
                out.push('_');
            }
            out.push(c.to_ascii_uppercase());
            last_was_upper = true;
        } else {
            out.push(c.to_ascii_uppercase());
            last_was_upper = false;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use crate::temporal::api::failure::v1::Failure;
    use anyhow::anyhow;

    #[test]
    fn anyhow_to_failure_conversion() {
        let no_causes: Failure = anyhow!("no causes").into();
        assert_eq!(no_causes.cause, None);
        assert_eq!(no_causes.message, "no causes");
        let orig = anyhow!("fail 1");
        let mid = orig.context("fail 2");
        let top = mid.context("fail 3");
        let as_fail: Failure = top.into();
        assert_eq!(as_fail.message, "fail 3");
        assert_eq!(as_fail.cause.as_ref().unwrap().message, "fail 2");
        assert_eq!(as_fail.cause.unwrap().cause.unwrap().message, "fail 1");
    }
}
