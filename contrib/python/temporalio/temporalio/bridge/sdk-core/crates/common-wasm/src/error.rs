//! Shared error types used across Temporal SDK crates.

use crate::{
    WorkflowExecution,
    data_converters::{
        DecodablePayloads, GenericPayloadConverter, PayloadConversionError, PayloadConverter,
        RawValue, SerializationContext, SerializationContextData, TemporalDeserializable,
        TemporalSerializable,
    },
    protos::{
        coresdk::child_workflow::StartChildWorkflowExecutionFailedCause,
        temporal::api::{
            common::v1::{Payload, Payloads},
            enums::v1::{
                ApplicationErrorCategory as ProtoApplicationErrorCategory,
                RetryState as ProtoRetryState, TimeoutType as ProtoTimeoutType,
            },
            failure::v1::Failure,
        },
    },
};
use std::time::Duration;

/// Describes why a retry did or did not occur.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum RetryState {
    /// No retry state was specified.
    Unspecified,
    /// Another retry is in progress.
    InProgress,
    /// The failure is not retryable.
    NonRetryableFailure,
    /// The retry timed out.
    Timeout,
    /// The retry policy's maximum attempts were reached.
    MaximumAttemptsReached,
    /// No retry policy was configured.
    RetryPolicyNotSet,
    /// An internal server error prevented retrying.
    InternalServerError,
    /// Cancellation was requested.
    CancelRequested,
    /// A state introduced by a newer server or API version.
    Unknown,
}

impl RetryState {
    fn from_raw(value: i32) -> Self {
        match ProtoRetryState::try_from(value) {
            Ok(ProtoRetryState::Unspecified) => Self::Unspecified,
            Ok(ProtoRetryState::InProgress) => Self::InProgress,
            Ok(ProtoRetryState::NonRetryableFailure) => Self::NonRetryableFailure,
            Ok(ProtoRetryState::Timeout) => Self::Timeout,
            Ok(ProtoRetryState::MaximumAttemptsReached) => Self::MaximumAttemptsReached,
            Ok(ProtoRetryState::RetryPolicyNotSet) => Self::RetryPolicyNotSet,
            Ok(ProtoRetryState::InternalServerError) => Self::InternalServerError,
            Ok(ProtoRetryState::CancelRequested) => Self::CancelRequested,
            Err(_) => Self::Unknown,
        }
    }
}

impl From<ProtoRetryState> for RetryState {
    fn from(value: ProtoRetryState) -> Self {
        Self::from_raw(value as i32)
    }
}

/// Identifies which timeout expired.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum TimeoutType {
    /// No timeout type was specified.
    Unspecified,
    /// The execution exceeded its start-to-close timeout.
    StartToClose,
    /// The task exceeded its schedule-to-start timeout.
    ScheduleToStart,
    /// The execution exceeded its schedule-to-close timeout.
    ScheduleToClose,
    /// An activity heartbeat was not received in time.
    Heartbeat,
    /// A timeout introduced by a newer server or API version.
    Unknown,
}

impl TimeoutType {
    fn from_raw(value: i32) -> Self {
        match ProtoTimeoutType::try_from(value) {
            Ok(ProtoTimeoutType::Unspecified) => Self::Unspecified,
            Ok(ProtoTimeoutType::StartToClose) => Self::StartToClose,
            Ok(ProtoTimeoutType::ScheduleToStart) => Self::ScheduleToStart,
            Ok(ProtoTimeoutType::ScheduleToClose) => Self::ScheduleToClose,
            Ok(ProtoTimeoutType::Heartbeat) => Self::Heartbeat,
            Err(_) => Self::Unknown,
        }
    }
}

impl From<ProtoTimeoutType> for TimeoutType {
    fn from(value: ProtoTimeoutType) -> Self {
        Self::from_raw(value as i32)
    }
}

// We cannot store `Box<dyn TemporalSerializable>` directly here because erased values still need
// to be driven back through the active `PayloadConverter` to reach serde-based implementations.
trait SerializableFailurePayload: Send + Sync {
    fn to_payloads(
        &self,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<Vec<Payload>, PayloadConversionError>;
}

impl<T> SerializableFailurePayload for T
where
    T: TemporalSerializable + Send + Sync + 'static,
{
    fn to_payloads(
        &self,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        payload_converter.to_payloads(
            &SerializationContext {
                data: context,
                converter: payload_converter,
            },
            self,
        )
    }
}

/// Payloads attached to a failure, either as a deferred outbound value or decoded inbound payloads.
#[derive(derive_more::Debug)]
pub struct FailurePayloads {
    repr: FailurePayloadsRepr,
}

#[derive(derive_more::Debug)]
enum FailurePayloadsRepr {
    #[debug("Serializable(...)")]
    Serializable(#[debug(skip)] Box<dyn SerializableFailurePayload>),
    Decoded(DecodablePayloads),
}

impl FailurePayloads {
    pub(crate) fn encode(
        &self,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<Payloads, PayloadConversionError> {
        let payloads = match &self.repr {
            FailurePayloadsRepr::Serializable(value) => {
                value.to_payloads(payload_converter, context)?
            }
            FailurePayloadsRepr::Decoded(value) => value.raw().to_vec(),
        };
        Ok(Payloads { payloads })
    }

    /// Deserialize the decoded payloads into a typed value.
    pub fn deserialize<T: TemporalDeserializable + 'static>(
        &self,
    ) -> Result<T, PayloadConversionError> {
        match &self.repr {
            FailurePayloadsRepr::Decoded(value) => value.deserialize(),
            FailurePayloadsRepr::Serializable(_) => Err(PayloadConversionError::WrongEncoding),
        }
    }

    /// Returns the decoded raw payloads, if present.
    pub fn raw(&self) -> Option<&[Payload]> {
        match &self.repr {
            FailurePayloadsRepr::Decoded(value) => Some(value.raw()),
            FailurePayloadsRepr::Serializable(_) => None,
        }
    }

    /// Consume this value and return the decoded payloads as a [`RawValue`], if present.
    pub fn into_raw(self) -> Option<RawValue> {
        match self.repr {
            FailurePayloadsRepr::Decoded(value) => Some(value.into_raw()),
            FailurePayloadsRepr::Serializable(_) => None,
        }
    }
}

impl From<DecodablePayloads> for FailurePayloads {
    fn from(value: DecodablePayloads) -> Self {
        Self {
            repr: FailurePayloadsRepr::Decoded(value),
        }
    }
}

impl<T> From<T> for FailurePayloads
where
    T: TemporalSerializable + Send + Sync + 'static,
{
    fn from(value: T) -> Self {
        Self {
            repr: FailurePayloadsRepr::Serializable(Box::new(value)),
        }
    }
}

/// Categorizes an [`ApplicationFailure`] to hint at how the server and tooling should treat it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum ApplicationErrorCategory {
    /// No category was specified.
    #[default]
    Unspecified,
    /// An expected error with little or no severity. Benign errors are logged at a reduced level
    /// and excluded from error metrics by the server.
    Benign,
}

impl From<ApplicationErrorCategory> for ProtoApplicationErrorCategory {
    fn from(value: ApplicationErrorCategory) -> Self {
        match value {
            ApplicationErrorCategory::Unspecified => ProtoApplicationErrorCategory::Unspecified,
            ApplicationErrorCategory::Benign => ProtoApplicationErrorCategory::Benign,
        }
    }
}

impl From<ProtoApplicationErrorCategory> for ApplicationErrorCategory {
    fn from(value: ProtoApplicationErrorCategory) -> Self {
        match value {
            ProtoApplicationErrorCategory::Unspecified => ApplicationErrorCategory::Unspecified,
            ProtoApplicationErrorCategory::Benign => ApplicationErrorCategory::Benign,
        }
    }
}

/// User-authored application failure metadata that can be converted into a Temporal failure.
#[derive(Debug, bon::Builder)]
#[builder(start_fn = builder, state_mod(vis = "pub"))]
pub struct ApplicationFailure {
    #[builder(start_fn, into)]
    source: Box<dyn std::error::Error + Send + Sync>,
    type_name: Option<String>,
    #[builder(default)]
    non_retryable: bool,
    next_retry_delay: Option<Duration>,
    #[builder(default = ApplicationErrorCategory::Unspecified)]
    category: ApplicationErrorCategory,
    #[builder(into)]
    details: Option<FailurePayloads>,
    failure: Option<Failure>,
    cause: Option<Box<IncomingError>>,
}

impl ApplicationFailure {
    /// Construct a retryable application failure with no extra metadata.
    pub fn new(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self {
            source: source.into(),
            type_name: None,
            non_retryable: false,
            next_retry_delay: None,
            category: ApplicationErrorCategory::Unspecified,
            details: None,
            failure: None,
            cause: None,
        }
    }

    /// Construct a non-retryable application failure with no extra metadata.
    pub fn non_retryable(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self {
            non_retryable: true,
            ..Self::new(source)
        }
    }

    /// Returns the wrapped source error.
    pub fn source_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        &*self.source as &(dyn std::error::Error + Send + Sync + 'static)
    }

    /// Returns the configured application failure type name, if any.
    pub fn type_name(&self) -> Option<&str> {
        self.type_name.as_deref()
    }

    /// Returns true if this failure should be treated as non-retryable.
    pub fn is_non_retryable(&self) -> bool {
        self.non_retryable
    }

    /// Returns the explicitly configured next retry delay, if any.
    pub fn next_retry_delay(&self) -> Option<Duration> {
        self.next_retry_delay
    }

    /// Returns the application error category.
    pub fn category(&self) -> ApplicationErrorCategory {
        self.category
    }

    /// Returns the decoded details deserialized as the requested type, if any.
    pub fn details<T: TemporalDeserializable + 'static>(
        &self,
    ) -> Result<Option<T>, PayloadConversionError> {
        self.details
            .as_ref()
            .map(FailurePayloads::deserialize)
            .transpose()
    }

    /// Returns the raw decoded details payloads, if any.
    pub fn raw_details(&self) -> Option<&[Payload]> {
        self.details.as_ref().and_then(FailurePayloads::raw)
    }

    pub(crate) fn failure_payloads(&self) -> Option<&FailurePayloads> {
        self.details.as_ref()
    }

    /// Returns the original failure proto when this application failure was decoded from one.
    pub fn failure(&self) -> Option<&Failure> {
        self.failure.as_ref()
    }

    /// Consumes this application failure and returns the retained proto failure, if one exists.
    pub fn into_failure(self) -> Option<Failure> {
        self.failure
    }

    /// Returns the normalized cause, if any.
    pub fn cause(&self) -> Option<&IncomingError> {
        self.cause.as_deref()
    }

    /// If this [`ApplicationFailure`] was caused by a timeout, returns the associated
    /// [`TimeoutError`].
    pub fn as_timeout(&self) -> Option<&TimeoutError> {
        self.cause().and_then(IncomingError::as_timeout)
    }

    /// If this [`ApplicationFailure`] was caused by a cancellation, returns the associated
    /// [`CancelledError`].
    pub fn as_cancelled(&self) -> Option<&CancelledError> {
        self.cause().and_then(IncomingError::as_cancelled)
    }

    pub(crate) fn from_failure(
        failure: Failure,
        cause: Option<IncomingError>,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Self {
        let app_info = failure
            .maybe_application_failure()
            .cloned()
            .unwrap_or_default();
        let type_name = (!app_info.r#type.is_empty()).then_some(app_info.r#type.clone());
        Self {
            source: failure.message.clone().into(),
            type_name,
            non_retryable: app_info.non_retryable,
            next_retry_delay: app_info.next_retry_delay.and_then(|d| d.try_into().ok()),
            category: app_info.category().into(),
            details: app_info.details.map(|details| {
                FailurePayloads::from(DecodablePayloads::new(
                    details.payloads,
                    payload_converter.clone(),
                    *context,
                ))
            }),
            failure: Some(failure),
            cause: cause.map(Box::new),
        }
    }
}

impl std::fmt::Display for ApplicationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.source)
    }
}

impl std::error::Error for ApplicationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_deref()
            .map(|cause| cause as &(dyn std::error::Error + 'static))
            .or_else(|| Some(self.source.as_ref()))
    }
}

impl From<anyhow::Error> for ApplicationFailure {
    fn from(value: anyhow::Error) -> Self {
        Self::new(value)
    }
}

impl From<PayloadConversionError> for ApplicationFailure {
    fn from(value: PayloadConversionError) -> Self {
        Self::new(value)
    }
}

/// A typed outbound error surface used before encoding to a Temporal failure proto.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OutgoingError {
    /// An error produced while completing an activity.
    #[error(transparent)]
    Activity(#[from] OutgoingActivityError),
    /// An error produced from a workflow.
    #[error(transparent)]
    Workflow(#[from] OutgoingWorkflowError),
}

/// A typed outbound activity error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OutgoingActivityError {
    /// An activity application failure.
    #[error(transparent)]
    Application(#[from] Box<ApplicationFailure>),
    /// An activity cancellation with optional details.
    #[error("Activity cancelled")]
    Cancelled {
        /// Optional cancellation details.
        details: Option<FailurePayloads>,
    },
}

/// A typed outbound workflow failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OutgoingWorkflowError {
    /// A workflow application failure.
    #[error(transparent)]
    Application(#[from] Box<ApplicationFailure>),
    /// A workflow payload conversion failure.
    #[error(transparent)]
    PayloadConversion(#[from] PayloadConversionError),
    /// A workflow failure sourced from an activity execution.
    #[error(transparent)]
    ActivityExecution(#[from] Box<ActivityExecutionError>),
    /// A workflow failure sourced from a child-workflow execution.
    #[error(transparent)]
    ChildWorkflowExecution(#[from] Box<ChildWorkflowExecutionError>),
    /// A workflow failure sourced from child-workflow start.
    #[error(transparent)]
    ChildWorkflowStart(#[from] Box<ChildWorkflowStartError>),
    /// A workflow failure sourced from signaling a workflow.
    #[error(transparent)]
    WorkflowSignal(#[from] Box<WorkflowSignalError>),
}

impl OutgoingWorkflowError {
    /// If this workflow error was caused by cancellation, returns the associated
    /// [`CancelledError`].
    pub fn as_cancelled(&self) -> Option<&CancelledError> {
        match self {
            Self::Application(err) => err.as_cancelled(),
            Self::PayloadConversion(_) => None,
            Self::ActivityExecution(err) => err.as_cancelled(),
            Self::ChildWorkflowExecution(err) => err.as_cancelled(),
            Self::ChildWorkflowStart(err) => err.as_cancelled(),
            Self::WorkflowSignal(err) => err.as_cancelled(),
        }
    }
}

impl From<anyhow::Error> for OutgoingWorkflowError {
    fn from(value: anyhow::Error) -> Self {
        Self::Application(Box::new(ApplicationFailure::new(value)))
    }
}

impl From<ApplicationFailure> for OutgoingWorkflowError {
    fn from(value: ApplicationFailure) -> Self {
        Self::Application(Box::new(value))
    }
}

impl From<ActivityExecutionError> for OutgoingWorkflowError {
    fn from(value: ActivityExecutionError) -> Self {
        match value {
            ActivityExecutionError::Serialization(err) => Self::PayloadConversion(err),
            other => Self::ActivityExecution(Box::new(other)),
        }
    }
}

impl From<ChildWorkflowExecutionError> for OutgoingWorkflowError {
    fn from(value: ChildWorkflowExecutionError) -> Self {
        match value {
            ChildWorkflowExecutionError::Serialization(err) => Self::PayloadConversion(err),
            other => Self::ChildWorkflowExecution(Box::new(other)),
        }
    }
}

impl From<ChildWorkflowStartError> for OutgoingWorkflowError {
    fn from(value: ChildWorkflowStartError) -> Self {
        match value {
            ChildWorkflowStartError::Serialization(err) => Self::PayloadConversion(err),
            other => Self::ChildWorkflowStart(Box::new(other)),
        }
    }
}

impl From<WorkflowSignalError> for OutgoingWorkflowError {
    fn from(value: WorkflowSignalError) -> Self {
        match value {
            WorkflowSignalError::Serialization(err) => Self::PayloadConversion(err),
            other => Self::WorkflowSignal(Box::new(other)),
        }
    }
}

/// A normalized incoming Temporal failure decoded from a protobuf [`Failure`].
#[derive(Debug)]
pub enum IncomingError {
    /// A decoded application failure.
    Application(ApplicationFailure),
    /// A decoded timeout failure.
    Timeout(TimeoutError),
    /// A decoded cancellation failure.
    Cancelled(CancelledError),
    /// A decoded terminated failure.
    Terminated(TerminatedError),
    /// A decoded server failure.
    Server(ServerError),
    /// A decoded reset-workflow failure.
    ResetWorkflow(ResetWorkflowError),
    /// A decoded activity failure wrapper.
    Activity(ActivityFailureError),
    /// A decoded child-workflow failure wrapper.
    ChildWorkflowExecution(ChildWorkflowFailureError),
    /// A decoded nexus operation failure wrapper.
    NexusOperationExecution(IncomingNexusOperationExecutionError),
    /// A decoded nexus handler failure wrapper.
    NexusHandler(IncomingNexusHandlerError),
}

impl IncomingError {
    /// Returns the original failure proto for this normalized error.
    pub fn failure(&self) -> &Failure {
        match self {
            IncomingError::Application(err) => err
                .failure()
                .expect("decoded application failures retain their original proto"),
            IncomingError::Timeout(err) => err.failure(),
            IncomingError::Cancelled(err) => err.failure(),
            IncomingError::Terminated(err) => err.failure(),
            IncomingError::Server(err) => err.failure(),
            IncomingError::ResetWorkflow(err) => err.failure(),
            IncomingError::Activity(err) => err.failure(),
            IncomingError::ChildWorkflowExecution(err) => err.failure(),
            IncomingError::NexusOperationExecution(err) => err.failure(),
            IncomingError::NexusHandler(err) => err.failure(),
        }
    }

    /// Returns the normalized cause, if any.
    pub fn cause(&self) -> Option<&IncomingError> {
        match self {
            IncomingError::Application(err) => err.cause(),
            IncomingError::Timeout(err) => err.cause(),
            IncomingError::Cancelled(err) => err.cause(),
            IncomingError::Terminated(err) => err.cause(),
            IncomingError::Server(err) => err.cause(),
            IncomingError::ResetWorkflow(err) => err.cause(),
            IncomingError::Activity(err) => err.cause(),
            IncomingError::ChildWorkflowExecution(err) => err.cause(),
            IncomingError::NexusOperationExecution(err) => err.cause(),
            IncomingError::NexusHandler(err) => err.cause(),
        }
    }

    /// Consumes this normalized error and returns the retained proto failure.
    pub fn into_failure(self) -> Failure {
        match self {
            IncomingError::Application(err) => err
                .into_failure()
                .expect("decoded application failures retain their original proto"),
            IncomingError::Timeout(err) => err.into_failure(),
            IncomingError::Cancelled(err) => err.into_failure(),
            IncomingError::Terminated(err) => err.into_failure(),
            IncomingError::Server(err) => err.into_failure(),
            IncomingError::ResetWorkflow(err) => err.into_failure(),
            IncomingError::Activity(err) => err.into_failure(),
            IncomingError::ChildWorkflowExecution(err) => err.into_failure(),
            IncomingError::NexusOperationExecution(err) => err.into_failure(),
            IncomingError::NexusHandler(err) => err.into_failure(),
        }
    }

    /// If the [`IncomingError`] is a timeout, returns the associated [`TimeoutError`].
    pub fn as_timeout(&self) -> Option<&TimeoutError> {
        match self {
            IncomingError::Timeout(err) => Some(err),
            _ => None,
        }
    }

    /// If the [`IncomingError`] is a cancellation, returns the associated [`CancelledError`].
    pub fn as_cancelled(&self) -> Option<&CancelledError> {
        match self {
            IncomingError::Cancelled(err) => Some(err),
            _ => None,
        }
    }
}

impl std::fmt::Display for IncomingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncomingError::Application(err) => err.fmt(f),
            IncomingError::Timeout(err) => err.fmt(f),
            IncomingError::Cancelled(err) => err.fmt(f),
            IncomingError::Terminated(err) => err.fmt(f),
            IncomingError::Server(err) => err.fmt(f),
            IncomingError::ResetWorkflow(err) => err.fmt(f),
            IncomingError::Activity(err) => err.fmt(f),
            IncomingError::ChildWorkflowExecution(err) => err.fmt(f),
            IncomingError::NexusOperationExecution(err) => err.fmt(f),
            IncomingError::NexusHandler(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for IncomingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            IncomingError::Application(err) => Some(err),
            IncomingError::Timeout(err) => Some(err),
            IncomingError::Cancelled(err) => Some(err),
            IncomingError::Terminated(err) => Some(err),
            IncomingError::Server(err) => Some(err),
            IncomingError::ResetWorkflow(err) => Some(err),
            IncomingError::Activity(err) => Some(err),
            IncomingError::ChildWorkflowExecution(err) => Some(err),
            IncomingError::NexusOperationExecution(err) => Some(err),
            IncomingError::NexusHandler(err) => Some(err),
        }
    }
}

macro_rules! impl_incoming_failure_wrapper {
    ($name:ident) => {
        impl $name {
            /// Returns the original failure proto.
            pub fn failure(&self) -> &Failure {
                &self.failure
            }

            /// Returns the normalized cause, if any.
            pub fn cause(&self) -> Option<&IncomingError> {
                self.cause.as_deref()
            }

            /// Consumes this wrapper and returns the retained proto failure.
            pub fn into_failure(self) -> Failure {
                self.failure
            }

            /// Consumes this wrapper and returns the retained proto failure and normalized cause.
            pub fn into_parts(self) -> (Failure, Option<IncomingError>) {
                (self.failure, self.cause.map(|cause| *cause))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.failure.fmt(f)
            }
        }

        impl std::error::Error for $name {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.cause
                    .as_deref()
                    .map(|cause| cause as &(dyn std::error::Error + 'static))
            }
        }
    };
}

macro_rules! incoming_failure_wrapper {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug)]
        pub struct $name {
            failure: Failure,
            cause: Option<Box<IncomingError>>,
        }

        impl $name {
            /// Creates a new normalized incoming error wrapper.
            pub(crate) fn new(failure: Failure, cause: Option<IncomingError>) -> Self {
                Self {
                    failure,
                    cause: cause.map(Box::new),
                }
            }
        }

        impl_incoming_failure_wrapper!($name);
    };
}

/// A normalized timeout failure.
#[derive(Debug)]
pub struct TimeoutError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
    timeout_type: TimeoutType,
    last_heartbeat_details: Option<DecodablePayloads>,
}

impl TimeoutError {
    /// Creates a new normalized timeout error wrapper.
    pub(crate) fn new(
        failure: Failure,
        failure_info: crate::protos::temporal::api::failure::v1::TimeoutFailureInfo,
        cause: Option<IncomingError>,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Self {
        Self {
            failure,
            cause: cause.map(Box::new),
            timeout_type: TimeoutType::from_raw(failure_info.timeout_type),
            last_heartbeat_details: failure_info.last_heartbeat_details.map(|details| {
                DecodablePayloads::new(details.payloads, payload_converter.clone(), *context)
            }),
        }
    }

    /// Returns the timeout kind described by the failure.
    pub fn timeout_type(&self) -> TimeoutType {
        self.timeout_type
    }

    /// Returns the last heartbeat details carried by the timeout, if any.
    pub fn last_heartbeat_details<T: TemporalDeserializable + 'static>(
        &self,
    ) -> Result<Option<T>, PayloadConversionError> {
        self.last_heartbeat_details
            .as_ref()
            .map(DecodablePayloads::deserialize)
            .transpose()
    }

    /// Returns the raw decoded heartbeat details carried by the timeout, if any.
    pub fn raw_last_heartbeat_details(&self) -> Option<&[Payload]> {
        self.last_heartbeat_details
            .as_ref()
            .map(DecodablePayloads::raw)
    }
}

impl_incoming_failure_wrapper!(TimeoutError);

/// A normalized cancellation failure.
#[derive(Debug)]
pub struct CancelledError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
    details: Option<DecodablePayloads>,
}

impl CancelledError {
    /// Creates a new normalized cancellation error wrapper.
    pub(crate) fn new(
        failure: Failure,
        failure_info: crate::protos::temporal::api::failure::v1::CanceledFailureInfo,
        cause: Option<IncomingError>,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Self {
        Self {
            failure,
            cause: cause.map(Box::new),
            details: failure_info.details.map(|details| {
                DecodablePayloads::new(details.payloads, payload_converter.clone(), *context)
            }),
        }
    }

    /// Returns the cancellation details carried by the failure, deserialized as the requested
    /// type, if any.
    pub fn details<T: TemporalDeserializable + 'static>(
        &self,
    ) -> Result<Option<T>, PayloadConversionError> {
        self.details
            .as_ref()
            .map(DecodablePayloads::deserialize)
            .transpose()
    }

    /// Returns the raw decoded cancellation details carried by the failure, if any.
    pub fn raw_details(&self) -> Option<&[Payload]> {
        self.details.as_ref().map(DecodablePayloads::raw)
    }
}

impl_incoming_failure_wrapper!(CancelledError);
incoming_failure_wrapper!(TerminatedError, "A normalized terminated failure.");
incoming_failure_wrapper!(ServerError, "A normalized server failure.");
incoming_failure_wrapper!(ResetWorkflowError, "A normalized reset-workflow failure.");

/// A normalized activity failure wrapper.
#[derive(Debug)]
pub struct ActivityFailureError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
    activity_id: String,
    activity_type: Option<String>,
    scheduled_event_id: i64,
    started_event_id: i64,
    identity: String,
    retry_state: RetryState,
}

impl ActivityFailureError {
    /// Creates a new normalized activity failure wrapper.
    pub(crate) fn new(
        failure: Failure,
        failure_info: crate::protos::temporal::api::failure::v1::ActivityFailureInfo,
        cause: Option<IncomingError>,
    ) -> Self {
        let retry_state = RetryState::from_raw(failure_info.retry_state);
        Self {
            failure,
            cause: cause.map(Box::new),
            activity_id: failure_info.activity_id,
            activity_type: failure_info
                .activity_type
                .map(|activity_type| activity_type.name),
            scheduled_event_id: failure_info.scheduled_event_id,
            started_event_id: failure_info.started_event_id,
            identity: failure_info.identity,
            retry_state,
        }
    }

    /// Returns the activity id reported by the failure.
    pub fn activity_id(&self) -> &str {
        &self.activity_id
    }

    /// Returns the activity type, if present.
    pub fn activity_type(&self) -> Option<&str> {
        self.activity_type.as_deref()
    }

    /// Returns the scheduled event id.
    pub fn scheduled_event_id(&self) -> i64 {
        self.scheduled_event_id
    }

    /// Returns the started event id.
    pub fn started_event_id(&self) -> i64 {
        self.started_event_id
    }

    /// Returns the worker identity captured on the failure.
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Returns the retry state reported by core.
    pub fn retry_state(&self) -> RetryState {
        self.retry_state
    }

    /// If this [`ActivityFailureError`] was caused by a timeout, returns the associated
    /// [`TimeoutError`].
    pub fn as_timeout(&self) -> Option<&TimeoutError> {
        self.cause().and_then(IncomingError::as_timeout)
    }

    /// If this [`ActivityFailureError`] was caused by a cancellation, returns the associated
    /// [`CancelledError`].
    pub fn as_cancelled(&self) -> Option<&CancelledError> {
        self.cause().and_then(IncomingError::as_cancelled)
    }
}

impl_incoming_failure_wrapper!(ActivityFailureError);
/// A normalized child-workflow execution failure wrapper.
#[derive(Debug)]
pub struct ChildWorkflowFailureError {
    failure: Failure,
    cause: Option<Box<IncomingError>>,
    namespace: String,
    workflow_execution: Option<WorkflowExecution>,
    workflow_type: Option<String>,
    initiated_event_id: i64,
    started_event_id: i64,
    retry_state: RetryState,
}

impl ChildWorkflowFailureError {
    /// Creates a new normalized child-workflow execution failure wrapper.
    pub(crate) fn new(
        failure: Failure,
        failure_info: crate::protos::temporal::api::failure::v1::ChildWorkflowExecutionFailureInfo,
        cause: Option<IncomingError>,
    ) -> Self {
        let retry_state = RetryState::from_raw(failure_info.retry_state);
        Self {
            failure,
            cause: cause.map(Box::new),
            namespace: failure_info.namespace,
            workflow_execution: failure_info.workflow_execution.map(Into::into),
            workflow_type: failure_info
                .workflow_type
                .map(|workflow_type| workflow_type.name),
            initiated_event_id: failure_info.initiated_event_id,
            started_event_id: failure_info.started_event_id,
            retry_state,
        }
    }

    /// Returns the namespace of the child workflow.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the child workflow execution, if present.
    pub fn workflow_execution(&self) -> Option<&WorkflowExecution> {
        self.workflow_execution.as_ref()
    }

    /// Returns the child workflow type, if present.
    pub fn workflow_type(&self) -> Option<&str> {
        self.workflow_type.as_deref()
    }

    /// Returns the initiated event id.
    pub fn initiated_event_id(&self) -> i64 {
        self.initiated_event_id
    }

    /// Returns the started event id.
    pub fn started_event_id(&self) -> i64 {
        self.started_event_id
    }

    /// Returns the retry state reported by core.
    pub fn retry_state(&self) -> RetryState {
        self.retry_state
    }

    /// If this [`ChildWorkflowFailureError`] was caused by a timeout, returns the associated
    /// [`TimeoutError`].
    pub fn as_timeout(&self) -> Option<&TimeoutError> {
        self.cause().and_then(IncomingError::as_timeout)
    }

    /// If this [`ChildWorkflowFailureError`] was caused by a cancellation, returns the associated
    /// [`CancelledError`].
    pub fn as_cancelled(&self) -> Option<&CancelledError> {
        self.cause().and_then(IncomingError::as_cancelled)
    }
}

impl_incoming_failure_wrapper!(ChildWorkflowFailureError);
incoming_failure_wrapper!(
    IncomingNexusOperationExecutionError,
    "A normalized nexus operation failure wrapper."
);
incoming_failure_wrapper!(
    IncomingNexusHandlerError,
    "A normalized nexus handler failure wrapper."
);

/// Error type for activity execution outcomes.
#[derive(Debug, thiserror::Error)]
pub enum ActivityExecutionError {
    /// The activity failed with the given failure details.
    #[error("Activity failed: {}", .0.failure().message)]
    Failed(#[source] ActivityFailureError),
    /// The activity was cancelled.
    #[error("Activity cancelled: {}", .0.failure().message)]
    Cancelled(#[source] CancelledError),
    /// Failed to serialize input or deserialize result payload.
    #[error("Payload conversion failed: {0}")]
    Serialization(#[from] PayloadConversionError),
}

impl ActivityExecutionError {
    /// Returns the retained top-level activity failure proto, if one exists.
    pub fn failure(&self) -> Option<&Failure> {
        match self {
            ActivityExecutionError::Failed(err) => Some(err.failure()),
            ActivityExecutionError::Cancelled(err) => Some(err.failure()),
            ActivityExecutionError::Serialization(_) => None,
        }
    }

    /// Returns the normalized cause of the top-level activity failure, if any.
    pub fn cause(&self) -> Option<&IncomingError> {
        match self {
            ActivityExecutionError::Failed(err) => err.cause(),
            ActivityExecutionError::Cancelled(err) => err.cause(),
            ActivityExecutionError::Serialization(_) => None,
        }
    }

    /// Returns the underlying failure reason for wrapper-shaped activity failures.
    pub fn reason(&self) -> Option<&IncomingError> {
        match self {
            ActivityExecutionError::Failed(err) => err.cause(),
            ActivityExecutionError::Cancelled(_) | ActivityExecutionError::Serialization(_) => None,
        }
    }

    /// If this [`ActivityExecutionError`] was caused by a timeout, returns the associated
    /// [`TimeoutError`].
    pub fn as_timeout(&self) -> Option<&TimeoutError> {
        match self {
            ActivityExecutionError::Failed(err) => err.as_timeout(),
            ActivityExecutionError::Serialization(_) | ActivityExecutionError::Cancelled(_) => None,
        }
    }

    /// If this [`ActivityExecutionError`] was caused by a cancellation, returns the associated
    /// [`CancelledError`].
    pub fn as_cancelled(&self) -> Option<&CancelledError> {
        match self {
            ActivityExecutionError::Failed(err) => err.as_cancelled(),
            ActivityExecutionError::Cancelled(err) => Some(err),
            ActivityExecutionError::Serialization(_) => None,
        }
    }
}

/// Error returned when starting a child workflow fails.
#[derive(Debug, thiserror::Error)]
pub enum ChildWorkflowStartError {
    /// The child workflow start was cancelled before the normal execution wrapper path existed.
    #[error("Child workflow start cancelled: {}", .0.failure().message)]
    Cancelled(#[source] Box<CancelledError>),
    /// The child workflow failed to start (e.g., workflow ID already exists).
    #[error(
        "Child workflow start failed: workflow_id={workflow_id}, workflow_type={workflow_type}, cause={cause:?}"
    )]
    StartFailed {
        /// The workflow ID that was requested.
        workflow_id: String,
        /// The workflow type that was requested.
        workflow_type: String,
        /// The cause of the start failure.
        cause: StartChildWorkflowExecutionFailedCause,
    },
    /// Failed to serialize child workflow input payloads.
    #[error("Payload conversion failed: {0}")]
    Serialization(#[from] PayloadConversionError),
}

impl ChildWorkflowStartError {
    /// Returns the retained top-level failure proto, if one exists.
    pub fn failure(&self) -> Option<&Failure> {
        match self {
            ChildWorkflowStartError::Cancelled(err) => Some(err.failure()),
            ChildWorkflowStartError::StartFailed { .. }
            | ChildWorkflowStartError::Serialization(_) => None,
        }
    }

    /// Returns the normalized cause of the retained failure proto, if any.
    pub fn cause(&self) -> Option<&IncomingError> {
        match self {
            ChildWorkflowStartError::Cancelled(err) => err.cause(),
            ChildWorkflowStartError::StartFailed { .. }
            | ChildWorkflowStartError::Serialization(_) => None,
        }
    }

    /// If this [`ChildWorkflowStartError`] was caused by cancellation, returns the associated
    /// [`CancelledError`].
    pub fn as_cancelled(&self) -> Option<&CancelledError> {
        match self {
            ChildWorkflowStartError::Cancelled(err) => Some(err),
            ChildWorkflowStartError::StartFailed { .. }
            | ChildWorkflowStartError::Serialization(_) => None,
        }
    }
}

/// Error returned when a child workflow execution fails.
#[derive(Debug, thiserror::Error)]
pub enum ChildWorkflowExecutionError {
    /// The child workflow failed.
    #[error("Child workflow failed: {}", .0.failure().message)]
    Failed(#[source] Box<ChildWorkflowFailureError>),
    /// Failed to serialize input or deserialize the child workflow result payload.
    #[error("Payload conversion failed: {0}")]
    Serialization(#[from] PayloadConversionError),
}

impl ChildWorkflowExecutionError {
    /// Returns the retained top-level child-workflow failure proto, if one exists.
    pub fn failure(&self) -> Option<&Failure> {
        match self {
            ChildWorkflowExecutionError::Failed(err) => Some(err.failure()),
            ChildWorkflowExecutionError::Serialization(_) => None,
        }
    }

    /// Returns the normalized cause of the top-level child-workflow failure, if any.
    pub fn cause(&self) -> Option<&IncomingError> {
        match self {
            ChildWorkflowExecutionError::Failed(err) => err.cause(),
            ChildWorkflowExecutionError::Serialization(_) => None,
        }
    }

    /// Returns the underlying failure reason for wrapper-shaped child-workflow failures.
    pub fn reason(&self) -> Option<&IncomingError> {
        match self {
            ChildWorkflowExecutionError::Failed(err) => err.cause(),
            ChildWorkflowExecutionError::Serialization(_) => None,
        }
    }

    /// If this [`ChildWorkflowExecutionError`] was caused by a timeout, returns the associated
    /// [`TimeoutError`].
    pub fn as_timeout(&self) -> Option<&TimeoutError> {
        match self {
            ChildWorkflowExecutionError::Failed(err) => err.as_timeout(),
            ChildWorkflowExecutionError::Serialization(_) => None,
        }
    }

    /// If this [`ChildWorkflowExecutionError`] was caused by a cancellation, returns the associated
    /// [`CancelledError`].
    pub fn as_cancelled(&self) -> Option<&CancelledError> {
        match self {
            ChildWorkflowExecutionError::Failed(err) => err.as_cancelled(),
            ChildWorkflowExecutionError::Serialization(_) => None,
        }
    }
}

/// Error returned when signaling a workflow fails.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowSignalError {
    /// The signal delivery failed.
    #[error("Child workflow signal failed: {}", .0.failure().message)]
    Failed(#[source] Box<WorkflowSignalFailureError>),
    /// Failed to serialize the signal input payload.
    #[error("Signal payload conversion failed: {0}")]
    Serialization(#[from] PayloadConversionError),
}

impl WorkflowSignalError {
    /// Returns the retained top-level workflow signal failure proto, if one exists.
    pub fn failure(&self) -> Option<&Failure> {
        match self {
            WorkflowSignalError::Failed(err) => Some(err.failure()),
            WorkflowSignalError::Serialization(_) => None,
        }
    }

    /// Returns the normalized cause of the workflow signal failure, if any.
    pub fn cause(&self) -> Option<&IncomingError> {
        match self {
            WorkflowSignalError::Failed(err) => err.cause(),
            WorkflowSignalError::Serialization(_) => None,
        }
    }

    /// Returns the underlying failure reason for wrapper-shaped signal failures.
    pub fn reason(&self) -> Option<&IncomingError> {
        match self {
            WorkflowSignalError::Failed(err) => Some(err.error()),
            WorkflowSignalError::Serialization(_) => None,
        }
    }

    /// If this [`WorkflowSignalError`] was caused by cancellation, returns the associated
    /// [`CancelledError`].
    pub fn as_cancelled(&self) -> Option<&CancelledError> {
        self.reason()?.as_cancelled()
    }
}

/// A normalized workflow signal failure wrapper.
#[derive(Debug)]
pub struct WorkflowSignalFailureError {
    failure: Failure,
    error: Box<IncomingError>,
}

impl WorkflowSignalFailureError {
    /// Creates a workflow signal failure wrapper.
    pub(crate) fn new(failure: Failure, error: IncomingError) -> Self {
        Self {
            failure,
            error: Box::new(error),
        }
    }

    /// Returns the retained top-level proto failure.
    pub fn failure(&self) -> &Failure {
        &self.failure
    }

    /// Returns the normalized direct cause of the workflow signal failure, if any.
    pub fn cause(&self) -> Option<&IncomingError> {
        self.error.cause()
    }

    /// Returns the direct decoded incoming error represented by the top-level proto failure.
    pub fn error(&self) -> &IncomingError {
        &self.error
    }
}

impl std::fmt::Display for WorkflowSignalFailureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.failure.fmt(f)
    }
}

impl std::error::Error for WorkflowSignalFailureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause()
            .map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_converters::{
            DefaultFailureConverter, FailureConverter, GenericPayloadConverter, PayloadConverter,
            SerializationContext, SerializationContextData,
        },
        protos::temporal::api::{
            common::v1::Payload,
            failure::v1::{ActivityFailureInfo, TimeoutFailureInfo, failure::FailureInfo},
        },
    };

    struct AlwaysFailsSerialize;

    impl serde::Serialize for AlwaysFailsSerialize {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("serialize boom"))
        }
    }

    #[test]
    fn decoded_failures_hide_unknown_state_values() {
        let failure = Failure {
            cause: Some(Box::new(Failure {
                failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                    timeout_type: 654_321,
                    ..Default::default()
                })),
                ..Default::default()
            })),
            failure_info: Some(FailureInfo::ActivityFailureInfo(ActivityFailureInfo {
                retry_state: 123_456,
                ..Default::default()
            })),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure,
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();
        let IncomingError::Activity(activity) = decoded else {
            panic!("expected activity failure");
        };
        assert_eq!(activity.retry_state(), RetryState::Unknown);
        let Some(FailureInfo::ActivityFailureInfo(raw_activity)) =
            activity.failure().failure_info.as_ref()
        else {
            panic!("expected raw activity failure info");
        };
        assert_eq!(raw_activity.retry_state, 123_456);
        let Some(IncomingError::Timeout(timeout)) = activity.cause() else {
            panic!("expected timeout cause");
        };
        assert_eq!(timeout.timeout_type(), TimeoutType::Unknown);
        let Some(FailureInfo::TimeoutFailureInfo(raw_timeout)) =
            timeout.failure().failure_info.as_ref()
        else {
            panic!("expected raw timeout failure info");
        };
        assert_eq!(raw_timeout.timeout_type, 654_321);
    }

    #[test]
    fn constructors_set_retryability_defaults() {
        assert!(!ApplicationFailure::new(anyhow::anyhow!("retryable")).is_non_retryable());
        assert!(
            ApplicationFailure::non_retryable(anyhow::anyhow!("non-retryable")).is_non_retryable()
        );
    }

    #[test]
    fn conversion_preserves_application_metadata() {
        let payloads = Payloads {
            payloads: vec![Payload {
                data: b"details".to_vec(),
                ..Default::default()
            }],
        };
        let failure = DefaultFailureConverter.to_failure(
            OutgoingError::Workflow(OutgoingWorkflowError::Application(Box::new(
                ApplicationFailure::builder(anyhow::anyhow!("oops"))
                    .type_name("MyType".to_owned())
                    .non_retryable(true)
                    .next_retry_delay(Duration::from_secs(3))
                    .category(ApplicationErrorCategory::Benign)
                    .details(RawValue::new(payloads.payloads.clone()))
                    .build(),
            ))),
            &PayloadConverter::default(),
            &SerializationContextData::None,
        );
        let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
            panic!("expected application failure info");
        };
        assert_eq!(failure.message, "oops");
        assert_eq!(info.r#type, "MyType");
        assert!(info.non_retryable);
        assert_eq!(info.details, Some(payloads));
        assert_eq!(info.category(), ProtoApplicationErrorCategory::Benign);
        assert_eq!(info.next_retry_delay.unwrap().seconds, 3);
    }

    #[test]
    fn builder_accepts_raw_payload_details() {
        let payload = Payload {
            data: b"details".to_vec(),
            ..Default::default()
        };
        let failure = DefaultFailureConverter.to_failure(
            OutgoingError::Workflow(OutgoingWorkflowError::Application(Box::new(
                ApplicationFailure::builder(anyhow::anyhow!("oops"))
                    .details(RawValue::new(vec![payload.clone()]))
                    .build(),
            ))),
            &PayloadConverter::default(),
            &SerializationContextData::None,
        );

        let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
            panic!("expected application failure info");
        };
        assert_eq!(info.details.unwrap().payloads, vec![payload]);
    }

    #[test]
    fn builder_accepts_serializable_details() {
        let failure = DefaultFailureConverter.to_failure(
            OutgoingError::Workflow(OutgoingWorkflowError::Application(Box::new(
                ApplicationFailure::builder(anyhow::anyhow!("oops"))
                    .details("details".to_string())
                    .build(),
            ))),
            &PayloadConverter::default(),
            &SerializationContextData::None,
        );

        let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
            panic!("expected application failure info");
        };
        let payloads = info.details.expect("expected details").payloads;
        let converter = PayloadConverter::default();
        let details: String = converter
            .from_payloads(
                &SerializationContext {
                    data: &SerializationContextData::None,
                    converter: &converter,
                },
                payloads,
            )
            .unwrap();
        assert_eq!(details, "details");
    }

    #[test]
    fn application_failure_encoding_surfaces_detail_encoding_errors() {
        let failure = DefaultFailureConverter.to_failure(
            OutgoingError::Workflow(OutgoingWorkflowError::Application(Box::new(
                ApplicationFailure::builder(anyhow::anyhow!("oops"))
                    .details(AlwaysFailsSerialize)
                    .build(),
            ))),
            &PayloadConverter::default(),
            &SerializationContextData::None,
        );

        assert_eq!(
            failure.message,
            "Failed converting error to failure: Encoding error: serialize boom, original error message: oops"
        );
        assert!(matches!(
            failure.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
    }

    #[test]
    fn anyhow_workflow_errors_default_to_application_outgoing_errors() {
        let outgoing: OutgoingWorkflowError = anyhow::anyhow!("workflow boom").into();

        let OutgoingWorkflowError::Application(app) = outgoing else {
            panic!("plain workflow errors should default to application failures");
        };
        assert_eq!(app.to_string(), "workflow boom");
    }

    #[test]
    fn payload_conversion_errors_use_dedicated_outgoing_variant() {
        let outgoing: OutgoingWorkflowError =
            PayloadConversionError::EncodingError(anyhow::anyhow!("encode boom").into()).into();

        let OutgoingWorkflowError::PayloadConversion(err) = outgoing else {
            panic!("expected a payload conversion failure");
        };
        assert_eq!(err.to_string(), "Encoding error: encode boom");
    }
}
