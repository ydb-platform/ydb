//! Functionality related to defining and interacting with activities
//!
//!
//! An example of defining an activity:
//! ```
//! use std::sync::{
//!     Arc,
//!     atomic::{AtomicUsize, Ordering},
//! };
//! use temporalio_macros::{activities, activity_definitions};
//! use temporalio_sdk::activities::{ActivityContext, ActivityError};
//!
//! struct MyActivities {
//!     counter: AtomicUsize,
//! }
//!
//! #[activities]
//! impl MyActivities {
//!     #[activity]
//!     async fn echo(_ctx: ActivityContext, e: String) -> Result<String, ActivityError> {
//!         Ok(e)
//!     }
//!
//!     #[activity]
//!     async fn uses_self(self: Arc<Self>, _ctx: ActivityContext) -> Result<(), ActivityError> {
//!         self.counter.fetch_add(1, Ordering::Relaxed);
//!         Ok(())
//!     }
//! }
//!
//! // If you need to refer to an activity that is defined externally, in a different codebase or
//! // possibly a different language, use `#[activity_definitions]`. Methods must omit the
//! // `ActivityContext` parameter and have a body of `unimplemented!()`. Workflows can then call
//! // these definitions just like real activities.
//!
//! struct ExternalActivities;
//! #[activity_definitions]
//! impl ExternalActivities {
//!     #[activity(name = "foo")]
//!     fn foo(_: String) -> Result<String, ActivityError> {
//!         unimplemented!()
//!     }
//! }
//! ```
//!
//! This will allows you to call the activity from workflow code still, but the actual function
//! will never be invoked, since you won't have registered it with the worker.

#[doc(inline)]
pub use temporalio_macros::activities;

use crate::{
    OutgoingActivityError, OutgoingError,
    interceptors::{
        ActivityExecutionValue, ActivityInboundInterceptor, ExecuteActivityInput,
        ExecuteActivityOutput, Next,
    },
    panic_formatter,
};
use futures_util::{
    FutureExt,
    future::{BoxFuture, ready},
};
use prost_types::{Duration, Timestamp};
use std::{
    collections::HashMap,
    fmt::Debug,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration as StdDuration, SystemTime},
};
use temporalio_client::{Client, ClientOptions, Priority, WorkflowExecutionInfo, WorkflowHandle};
pub use temporalio_common::ActivityError;
use temporalio_common::{
    ActivityDefinition, HasWorkflowDefinition, RetryPolicy,
    data_converters::{
        DataConverter, DecodablePayloads, GenericPayloadConverter, PayloadConversionError,
        PayloadConverter, RawValue, SerializationContext, SerializationContextData,
        TemporalDeserializable, TemporalSerializable,
    },
    error::ApplicationFailure,
    protos::{
        coresdk::{ActivityHeartbeat, activity_result::ActivityExecutionResult, activity_task},
        temporal::api::common::v1::Payload,
        utilities::TryIntoOrNone,
    },
};
use temporalio_sdk_core::Worker as CoreWorker;
use tokio_util::sync::CancellationToken;

/// Used within activities to get info, heartbeat management etc.
#[derive(Clone)]
pub struct ActivityContext {
    worker: Arc<CoreWorker>,
    client_options: ClientOptions,
    cancellation_token: CancellationToken,
    heartbeat_details: ActivityHeartbeatDetails,
    header_fields: HashMap<String, Payload>,
    info: ActivityInfo,
}

impl ActivityContext {
    pub(crate) fn new(
        worker: Arc<CoreWorker>,
        client_options: ClientOptions,
        cancellation_token: CancellationToken,
        task_queue: String,
        task_token: Vec<u8>,
        task: activity_task::Start,
    ) -> (Self, Vec<Payload>) {
        let activity_task::Start {
            workflow_namespace,
            workflow_type,
            workflow_execution,
            activity_id,
            activity_type,
            header_fields,
            input,
            heartbeat_details,
            scheduled_time,
            current_attempt_scheduled_time,
            started_time,
            attempt,
            schedule_to_close_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            retry_policy,
            is_local,
            priority,
            run_id,
        } = task;
        let deadline = calculate_deadline(
            scheduled_time.as_ref(),
            started_time.as_ref(),
            start_to_close_timeout.as_ref(),
            schedule_to_close_timeout.as_ref(),
        );
        let heartbeat_details = ActivityHeartbeatDetails::new(
            heartbeat_details,
            client_options.data_converter.payload_converter().clone(),
        );
        let (workflow_id, workflow_run_id) = workflow_execution
            .map(|we| (we.workflow_id, we.run_id))
            .unzip();
        let activity_run_id = (workflow_id.is_none() && !run_id.is_empty()).then_some(run_id);

        (
            ActivityContext {
                worker,
                client_options,
                cancellation_token,
                heartbeat_details,
                header_fields,
                info: ActivityInfo {
                    task_token,
                    task_queue,
                    workflow_type: (!workflow_type.is_empty()).then_some(workflow_type),
                    namespace: workflow_namespace,
                    workflow_id,
                    workflow_run_id,
                    activity_id,
                    activity_type,
                    heartbeat_timeout: heartbeat_timeout.try_into_or_none(),
                    scheduled_time: scheduled_time.try_into_or_none(),
                    started_time: started_time.try_into_or_none(),
                    deadline,
                    attempt,
                    current_attempt_scheduled_time: current_attempt_scheduled_time
                        .try_into_or_none(),
                    retry_policy: retry_policy.map(Into::into),
                    is_local,
                    priority: priority.map(Into::into).unwrap_or_default(),
                    activity_run_id,
                },
            },
            input,
        )
    }

    /// Returns a future the completes if and when the activity this was called inside has been
    /// cancelled
    pub async fn cancelled(&self) {
        self.cancellation_token.clone().cancelled().await
    }

    /// Returns true if this activity has already been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Extract heartbeat details from last failed attempt. This is used in combination with retry
    /// policy.
    pub fn heartbeat_details(&self) -> &ActivityHeartbeatDetails {
        &self.heartbeat_details
    }

    /// Record a heartbeat with typed progress details for the currently executing activity.
    pub async fn record_heartbeat<T>(&self, details: T) -> Result<(), PayloadConversionError>
    where
        T: TemporalSerializable + 'static,
    {
        if !self.info.is_local {
            let details = self
                .client_options
                .data_converter
                .to_payloads(&SerializationContextData::Activity, &details)
                .await?;
            self.worker.record_activity_heartbeat(ActivityHeartbeat {
                task_token: self.info.task_token.clone(),
                details,
            })
        }
        Ok(())
    }

    /// Returns activity info of the executing activity
    pub fn info(&self) -> &ActivityInfo {
        &self.info
    }

    /// Return a client targeting the same Temporal service and namespace as this activity's worker.
    pub fn client(&self) -> Client {
        let connection = self.worker.get_client_connection().expect(
            "activity context client is unavailable because the worker was not created from a \
             Temporal client",
        );
        Client::new(connection, self.client_options.clone())
            .expect("client construction from a worker connection should be infallible")
    }

    /// Return a workflow handle for the workflow execution that started this activity, if any.
    pub fn workflow_handle<W: HasWorkflowDefinition>(&self) -> Option<WorkflowHandle<Client, W>> {
        let workflow_id = self.info.workflow_id.clone()?;
        let run_id = self.info.workflow_run_id.clone();
        let first_execution_run_id = run_id.clone();

        Some(WorkflowHandle::new(
            self.client(),
            WorkflowExecutionInfo {
                namespace: self.client_options.namespace.clone(),
                workflow_id,
                run_id,
                first_execution_run_id,
            },
        ))
    }

    /// Get headers attached to this activity
    pub fn headers(&self) -> &HashMap<String, Payload> {
        &self.header_fields
    }

    pub(crate) fn headers_mut(&mut self) -> &mut HashMap<String, Payload> {
        &mut self.header_fields
    }
}

/// Heartbeat details supplied by the previous activity attempt.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ActivityHeartbeatDetails {
    payloads: DecodablePayloads,
}

impl ActivityHeartbeatDetails {
    fn new(payloads: Vec<Payload>, payload_converter: PayloadConverter) -> Self {
        Self {
            payloads: DecodablePayloads::new(
                payloads,
                payload_converter,
                SerializationContextData::Activity,
            ),
        }
    }

    /// Deserialize the previous heartbeat details, or return `None` when there are none.
    pub fn deserialize<T: TemporalDeserializable + 'static>(
        &self,
    ) -> Result<Option<T>, PayloadConversionError> {
        if self.payloads.raw().is_empty() {
            Ok(None)
        } else {
            self.payloads.deserialize().map(Some)
        }
    }

    /// Returns the codec-decoded raw heartbeat payloads.
    pub fn raw(&self) -> &[Payload] {
        self.payloads.raw()
    }

    /// Consume these details and return their codec-decoded payloads.
    pub fn into_raw(self) -> RawValue {
        self.payloads.into_raw()
    }
}

/// Various information about a specific activity attempt.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ActivityInfo {
    /// An opaque token representing a specific Activity task.
    pub task_token: Vec<u8>,
    /// The type of the workflow that invoked this activity. None for standalone activities.
    pub workflow_type: Option<String>,
    /// The namespace of this activity.
    pub namespace: String,
    /// ID of the workflow that invoked this activity. None for standalone activities.
    pub workflow_id: Option<String>,
    /// Run ID of the workflow that invoked this activity. None for standalone activities.
    pub workflow_run_id: Option<String>,
    /// The ID of this activity.
    pub activity_id: String,
    /// The type of this activity.
    pub activity_type: String,
    /// The task queue of this activity.
    pub task_queue: String,
    /// The interval within which this activity must heartbeat or be timed out.
    pub heartbeat_timeout: Option<StdDuration>,
    /// Time activity was scheduled by a workflow.
    pub scheduled_time: Option<SystemTime>,
    /// Time of activity start.
    pub started_time: Option<SystemTime>,
    /// Time of activity timeout.
    pub deadline: Option<SystemTime>,
    /// Attempt starts from 1, and increase by 1 for every retry, if retry policy is specified.
    pub attempt: u32,
    /// Time this attempt at the activity was scheduled.
    pub current_attempt_scheduled_time: Option<SystemTime>,
    /// The retry policy for this activity.
    pub retry_policy: Option<RetryPolicy>,
    /// Whether or not this is a local activity.
    pub is_local: bool,
    /// Priority of this activity. If unset uses [Priority::default].
    pub priority: Priority,
    /// Run ID of this activity execution. Only set for standalone activities.
    pub activity_run_id: Option<String>,
}

/// Deadline calculation.  This is a port of
/// https://github.com/temporalio/sdk-go/blob/8651550973088f27f678118f997839fb1bb9e62f/internal/activity.go#L225
fn calculate_deadline(
    scheduled_time: Option<&Timestamp>,
    started_time: Option<&Timestamp>,
    start_to_close_timeout: Option<&Duration>,
    schedule_to_close_timeout: Option<&Duration>,
) -> Option<SystemTime> {
    match (
        scheduled_time,
        started_time,
        start_to_close_timeout,
        schedule_to_close_timeout,
    ) {
        (
            Some(scheduled),
            Some(started),
            Some(start_to_close_timeout),
            Some(schedule_to_close_timeout),
        ) => {
            let scheduled: SystemTime = maybe_convert_timestamp(scheduled)?;
            let started: SystemTime = maybe_convert_timestamp(started)?;
            let start_to_close_timeout: StdDuration = (*start_to_close_timeout).try_into().ok()?;
            let schedule_to_close_timeout: StdDuration =
                (*schedule_to_close_timeout).try_into().ok()?;

            let start_to_close_deadline: SystemTime =
                started.checked_add(start_to_close_timeout)?;
            if schedule_to_close_timeout > StdDuration::ZERO {
                let schedule_to_close_deadline =
                    scheduled.checked_add(schedule_to_close_timeout)?;
                // Minimum of the two deadlines.
                if schedule_to_close_deadline < start_to_close_deadline {
                    Some(schedule_to_close_deadline)
                } else {
                    Some(start_to_close_deadline)
                }
            } else {
                Some(start_to_close_deadline)
            }
        }
        _ => None,
    }
}

/// Helper function lifted from prost_types::Timestamp implementation to prevent double cloning in
/// error construction
fn maybe_convert_timestamp(timestamp: &Timestamp) -> Option<SystemTime> {
    let mut timestamp = *timestamp;
    timestamp.normalize();

    let system_time = if timestamp.seconds >= 0 {
        std::time::UNIX_EPOCH.checked_add(StdDuration::from_secs(timestamp.seconds as u64))
    } else {
        std::time::UNIX_EPOCH.checked_sub(StdDuration::from_secs((-timestamp.seconds) as u64))
    };

    system_time.and_then(|system_time| {
        system_time.checked_add(StdDuration::from_nanos(timestamp.nanos as u64))
    })
}

pub(crate) type ActivityInvocation = Arc<
    dyn Fn(
            Vec<Payload>,
            DataConverter,
            ActivityContext,
            Vec<Arc<dyn ActivityInboundInterceptor>>,
        ) -> ExecuteActivityOutput<'static>
        + Send
        + Sync,
>;

fn call_execute_activity<'a>(
    interceptors: &'a [Arc<dyn ActivityInboundInterceptor>],
    input: ExecuteActivityInput,
    next: Next<'a, ExecuteActivityInput, ExecuteActivityOutput<'a>>,
) -> ExecuteActivityOutput<'a> {
    if let Some((first, rest)) = interceptors.split_first() {
        first.execute_activity(
            input,
            Next::new(move |input| call_execute_activity(rest, input, next)),
        )
    } else {
        next.run(input)
    }
}

#[doc(hidden)]
pub trait ActivityImplementer {
    fn register_all(self: Arc<Self>, defs: &mut ActivityDefinitions);
}

#[doc(hidden)]
pub trait ExecutableActivity: ActivityDefinition + Sized {
    type Implementer: ActivityImplementer + Send + Sync + 'static;
    fn definition() -> Self;
    fn execute(
        receiver: Option<Arc<Self::Implementer>>,
        ctx: ActivityContext,
        input: Self::Input,
    ) -> BoxFuture<'static, Result<Self::Output, ActivityError>>;
}

#[doc(hidden)]
pub trait HasOnlyStaticMethods {}

/// Contains activity registrations in a form ready for execution by workers.
#[derive(Default, Clone)]
pub struct ActivityDefinitions {
    activities: HashMap<String, ActivityInvocation>,
}

impl ActivityDefinitions {
    pub(crate) fn extend(&mut self, other: &Self) {
        self.activities.extend(other.activities.clone());
    }

    /// Registers all activities on an activity implementer.
    pub fn register_activities<AI: ActivityImplementer>(&mut self, instance: AI) -> &mut Self {
        let arcd = Arc::new(instance);
        AI::register_all(arcd, self);
        self
    }
    /// Registers a specific activitiy.
    pub fn register_activity<AD>(&mut self, instance: Arc<AD::Implementer>) -> &mut Self
    where
        AD: ActivityDefinition + ExecutableActivity,
        AD::Input: Send + Sync,
        AD::Output: Send + Sync,
    {
        self.activities.insert(
            AD::definition().name().to_string(),
            Arc::new(move |payloads, dc, c, activity_inbound_interceptors| {
                let instance = instance.clone();
                async move {
                    // Codec application happens at the SDK/Core boundary, so activity
                    // implementations work with the payload converter directly.
                    let pc = dc.payload_converter();
                    let ctx = SerializationContext {
                        data: &SerializationContextData::Activity,
                        converter: pc,
                    };
                    let input: AD::Input = pc.from_payloads(&ctx, payloads)?;
                    let input = ExecuteActivityInput::new(c, Box::new(input));
                    let leaf = activity_inbound_base::<AD>(instance);
                    let activity_execution =
                        call_execute_activity(&activity_inbound_interceptors, input, leaf);
                    match AssertUnwindSafe(activity_execution).catch_unwind().await {
                        Ok(output) => output,
                        Err(panic) => Err(ApplicationFailure::new(anyhow::anyhow!(
                            "Activity function panicked: {}",
                            panic_formatter(panic)
                        ))
                        .into()),
                    }
                }
                .boxed()
            }),
        );
        self
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.activities.is_empty()
    }

    pub(crate) fn get(&self, act_type: &str) -> Option<ActivityInvocation> {
        self.activities.get(act_type).cloned()
    }

    pub(crate) fn names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.activities.keys().cloned().collect();
        names.sort_unstable();
        names
    }
}

fn activity_inbound_base<'a, AD>(
    instance: Arc<AD::Implementer>,
) -> Next<'a, ExecuteActivityInput, ExecuteActivityOutput<'a>>
where
    AD: ActivityDefinition + ExecutableActivity,
    AD::Input: Send + Sync,
    AD::Output: Send + Sync,
{
    Next::new(
        move |input: ExecuteActivityInput| -> ExecuteActivityOutput<'a> {
            let (activity_context, args) = input.into_parts();
            let args = match args.downcast::<AD::Input>() {
                Ok(args) => args,
                Err(_) => {
                    return ready(Err(ApplicationFailure::new(anyhow::anyhow!(
                    "Activity inbound interceptor returned arguments with wrong concrete type for activity {}",
                    AD::definition().name()
                ))
                .into()))
                .boxed();
                }
            };

            async move {
                match AssertUnwindSafe(AD::execute(Some(instance), activity_context, *args))
                    .catch_unwind()
                    .await
                {
                    Ok(result) => {
                        result.map(|output| Box::new(output) as Box<dyn ActivityExecutionValue>)
                    }
                    Err(panic) => Err(ApplicationFailure::new(anyhow::anyhow!(
                        "Activity function panicked: {}",
                        panic_formatter(panic)
                    ))
                    .into()),
                }
            }
            .boxed()
        },
    )
}

pub(crate) fn activity_error_to_core_result(
    dc: &DataConverter,
    err: ActivityError,
) -> ActivityExecutionResult {
    match err {
        ActivityError::Application(app) => ActivityExecutionResult::fail(dc.to_failure(
            &SerializationContextData::Activity,
            OutgoingError::Activity(OutgoingActivityError::Application(app)),
        )),
        ActivityError::Cancelled { details } => ActivityExecutionResult::cancel(dc.to_failure(
            &SerializationContextData::Activity,
            OutgoingError::Activity(OutgoingActivityError::Cancelled { details }),
        )),
        ActivityError::WillCompleteAsync => ActivityExecutionResult::will_complete_async(),
    }
}

impl Debug for ActivityDefinitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActivityDefinitions")
            .field("activities", &self.activities.keys())
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;
    use temporalio_common::error::{ApplicationErrorCategory, ApplicationFailure};

    #[test]
    fn activity_heartbeat_details_support_typed_decoding() {
        let payload_converter = PayloadConverter::default();
        let payload = payload_converter
            .to_payload(
                &SerializationContext {
                    data: &SerializationContextData::Activity,
                    converter: &payload_converter,
                },
                &"progress".to_owned(),
            )
            .unwrap();
        let details = ActivityHeartbeatDetails::new(vec![payload.clone()], payload_converter);

        assert_eq!(details.raw(), &[payload]);
        assert_eq!(
            details.deserialize::<String>().unwrap(),
            Some("progress".to_owned())
        );
    }

    #[test]
    fn empty_activity_heartbeat_details_decode_to_none() {
        let details = ActivityHeartbeatDetails::new(Vec::new(), PayloadConverter::default());

        assert_eq!(details.deserialize::<String>().unwrap(), None);
        assert!(details.into_raw().payloads.is_empty());
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    fn activity_error_conversion_is_not_lossy(#[case] non_retryable: bool) {
        let original = ApplicationFailure::builder(anyhow::anyhow!("big boom"))
            .type_name("BigBoom".to_owned())
            .non_retryable(non_retryable)
            .next_retry_delay(StdDuration::from_secs(3))
            .category(ApplicationErrorCategory::Benign)
            .details("details")
            .build();
        let err = ActivityError::from(original);
        let ActivityError::Application(actual) = err else {
            panic!("application failure should become app failure")
        };
        assert_eq!(actual.type_name(), Some("BigBoom"));
        assert_eq!(actual.is_non_retryable(), non_retryable);
        assert_eq!(actual.next_retry_delay(), Some(StdDuration::from_secs(3)));
        assert_eq!(actual.category(), ApplicationErrorCategory::Benign);
        assert_eq!(actual.to_string(), "big boom");
    }

    #[test]
    fn activity_error_from_special_err_becomes_application() {
        #[derive(Debug, PartialEq)]
        struct MyError;

        impl std::error::Error for MyError {}
        impl std::fmt::Display for MyError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("MyError")
            }
        }

        let err = ActivityError::from(MyError);
        let ActivityError::Application(actual) = err else {
            panic!("expected application failure, got {err:?}")
        };
        assert_eq!(actual.to_string(), "MyError");
    }
}
