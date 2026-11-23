//! Failure conversion sits at the normalized boundary between Rust-side error surfaces and
//! Temporal's proto [`Failure`] transport object.
//!
//! - [`FailureConverter`] owns translation between proto [`Failure`] and the SDK's shared
//!   normalized error model.
//! - encode-side call sites adapt caller-facing errors into [`OutgoingError`] before reaching this
//!   module.
//! - decode-side call sites first normalize proto failures into [`IncomingError`], then
//!   [`FailureDecodeHint`] implementations adapt that normalized value into the caller-facing error
//!   type they expect.

use super::{PayloadConversionError, PayloadConverter, SerializationContextData};
use crate::{
    error::{
        ActivityExecutionError, ActivityFailureError, ApplicationFailure, CancelledError,
        ChildWorkflowExecutionError, ChildWorkflowFailureError, ChildWorkflowStartError,
        IncomingError, IncomingNexusHandlerError, IncomingNexusOperationExecutionError,
        OutgoingActivityError, OutgoingError, OutgoingWorkflowError, ResetWorkflowError,
        ServerError, TerminatedError, TimeoutError, WorkflowSignalError,
        WorkflowSignalFailureError,
    },
    protos::temporal::api::{
        enums::v1::ApplicationErrorCategory as ProtoApplicationErrorCategory,
        failure::v1::{
            ActivityFailureInfo, ApplicationFailureInfo, CanceledFailureInfo,
            ChildWorkflowExecutionFailureInfo, Failure, failure::FailureInfo,
        },
    },
};

/// Converts between Rust errors and Temporal [`Failure`] protobufs.
pub trait FailureConverter {
    /// Convert an error into a Temporal failure protobuf.
    fn to_failure(
        &self,
        error: OutgoingError,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Failure;

    /// Convert a Temporal failure protobuf back into a Rust error.
    fn to_error(
        &self,
        failure: Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<IncomingError, PayloadConversionError>;
}

/// Default failure converter.
pub struct DefaultFailureConverter;

/// Adapts a normalized incoming failure into a caller-facing error surface.
pub trait FailureDecodeHint {
    /// The caller-facing error type produced by this hint.
    type Output;

    /// Adapt a normalized incoming error to the caller-facing output.
    fn adapt(self, normalized: IncomingError) -> Self::Output;
}

/// No-op decode hint; returns the error unchanged.
#[derive(Debug, Clone, Copy)]
pub struct NoopDecodeHint;

impl FailureDecodeHint for NoopDecodeHint {
    type Output = IncomingError;

    fn adapt(self, normalized: IncomingError) -> Self::Output {
        normalized
    }
}

/// Decode hint for activity execution results.
#[derive(Debug, Clone, Copy)]
pub struct ActivityExecutionDecodeHint {
    /// Whether the workflow-side resolution was cancelled rather than failed.
    pub cancelled: bool,
}

impl FailureDecodeHint for ActivityExecutionDecodeHint {
    type Output = ActivityExecutionError;

    fn adapt(self, normalized: IncomingError) -> Self::Output {
        match normalized {
            IncomingError::Activity(activity) => {
                if self.cancelled && matches!(activity.cause(), Some(IncomingError::Cancelled(_))) {
                    // We collapse to the inner cancellation error so callers do not see a cancel
                    // caused by another cancel.
                    let (_, cause) = activity.into_parts();
                    let Some(IncomingError::Cancelled(cancelled)) = cause else {
                        unreachable!("checked above");
                    };
                    ActivityExecutionError::Cancelled(cancelled)
                } else {
                    ActivityExecutionError::Failed(activity)
                }
            }
            other => match other {
                IncomingError::Cancelled(cancelled) if self.cancelled => {
                    ActivityExecutionError::Cancelled(cancelled)
                }
                other => {
                    let activity = ActivityFailureError::new(
                        other.into_failure(),
                        ActivityFailureInfo::default(),
                        None,
                    );
                    ActivityExecutionError::Failed(activity)
                }
            },
        }
    }
}

/// Decode hint for child-workflow start results.
#[derive(Debug, Clone, Copy)]
pub struct ChildWorkflowStartDecodeHint;

impl FailureDecodeHint for ChildWorkflowStartDecodeHint {
    type Output = ChildWorkflowStartError;

    fn adapt(self, normalized: IncomingError) -> Self::Output {
        match normalized {
            IncomingError::Cancelled(cancelled) => {
                ChildWorkflowStartError::Cancelled(Box::new(cancelled))
            }
            other => {
                let payload_converter = PayloadConverter::default();
                ChildWorkflowStartError::Cancelled(Box::new(CancelledError::new(
                    other.into_failure(),
                    CanceledFailureInfo::default(),
                    None,
                    &payload_converter,
                    &SerializationContextData::None,
                )))
            }
        }
    }
}

/// Decode hint for child-workflow execution results.
#[derive(Debug, Clone, Copy)]
pub struct ChildWorkflowExecutionDecodeHint;

impl FailureDecodeHint for ChildWorkflowExecutionDecodeHint {
    type Output = ChildWorkflowExecutionError;

    fn adapt(self, normalized: IncomingError) -> Self::Output {
        match normalized {
            IncomingError::ChildWorkflowExecution(child) => {
                ChildWorkflowExecutionError::Failed(Box::new(child))
            }
            other => ChildWorkflowExecutionError::Failed(Box::new(ChildWorkflowFailureError::new(
                other.into_failure(),
                ChildWorkflowExecutionFailureInfo::default(),
                None,
            ))),
        }
    }
}

/// Decode hint for workflow signal failures.
#[derive(Debug, Clone, Copy)]
pub struct WorkflowSignalDecodeHint;

impl FailureDecodeHint for WorkflowSignalDecodeHint {
    type Output = WorkflowSignalError;

    fn adapt(self, normalized: IncomingError) -> Self::Output {
        let failure = normalized.failure().clone();
        WorkflowSignalError::Failed(Box::new(WorkflowSignalFailureError::new(
            failure, normalized,
        )))
    }
}

impl FailureConverter for DefaultFailureConverter {
    fn to_failure(
        &self,
        error: OutgoingError,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Failure {
        let original_error = error.to_string();
        let encoded = match error {
            OutgoingError::Activity(activity) => {
                encode_outgoing_activity_error(activity, payload_converter, context)
            }
            OutgoingError::Workflow(OutgoingWorkflowError::Application(app)) => {
                app.encode_failure(payload_converter, context)
            }
            OutgoingError::Workflow(OutgoingWorkflowError::PayloadConversion(err)) => {
                Ok(encode_generic_application_failure(&err))
            }
            OutgoingError::Workflow(OutgoingWorkflowError::ActivityExecution(activity)) => {
                activity.encode_failure(payload_converter, context)
            }
            OutgoingError::Workflow(OutgoingWorkflowError::ChildWorkflowExecution(child)) => {
                child.encode_failure(payload_converter, context)
            }
            OutgoingError::Workflow(OutgoingWorkflowError::ChildWorkflowStart(child)) => {
                child.encode_failure(payload_converter, context)
            }
            OutgoingError::Workflow(OutgoingWorkflowError::WorkflowSignal(signal)) => {
                signal.encode_failure(payload_converter, context)
            }
        };
        encoded.unwrap_or_else(|converter_error| {
            Failure::application_failure(
                failed_error_conversion_message(&original_error, &converter_error),
                false,
            )
        })
    }

    fn to_error(
        &self,
        failure: Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<IncomingError, PayloadConversionError> {
        Ok(decode_failure(failure, payload_converter, context))
    }
}

/// Trait for expressing that a type has a known conversion to a Failure proto
trait EncodeFailure {
    fn encode_failure(
        &self,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError>;
}

enum ClassifiedFailure<'a> {
    Application(&'a ApplicationFailure),
    ActivityExecution(&'a ActivityExecutionError),
    ChildWorkflowExecution(&'a ChildWorkflowExecutionError),
    ChildWorkflowStart(&'a ChildWorkflowStartError),
    WorkflowSignal(&'a WorkflowSignalError),
    Generic(&'a (dyn std::error::Error + 'static)),
}

fn failed_error_conversion_message(
    original_error: impl std::fmt::Display,
    converter_error: &PayloadConversionError,
) -> String {
    format!(
        "Failed converting error to failure: {converter_error}, original error message: \
         {original_error}"
    )
}

impl<'a> ClassifiedFailure<'a> {
    fn from_error(err: &'a (dyn std::error::Error + 'static)) -> Self {
        if let Some(app) = err.downcast_ref::<ApplicationFailure>() {
            Self::Application(app)
        } else if let Some(activity) = err.downcast_ref::<ActivityExecutionError>() {
            Self::ActivityExecution(activity)
        } else if let Some(child) = err.downcast_ref::<ChildWorkflowExecutionError>() {
            Self::ChildWorkflowExecution(child)
        } else if let Some(child) = err.downcast_ref::<ChildWorkflowStartError>() {
            Self::ChildWorkflowStart(child)
        } else if let Some(child_signal) = err.downcast_ref::<WorkflowSignalError>() {
            Self::WorkflowSignal(child_signal)
        } else {
            Self::Generic(err)
        }
    }

    fn encode(self) -> Failure {
        match self {
            Self::Application(app) => app
                .encode_failure(
                    &PayloadConverter::default(),
                    &SerializationContextData::None,
                )
                .unwrap_or_else(|converter_error| {
                    encode_failed_error_conversion(app, converter_error)
                }),
            Self::ActivityExecution(activity) => activity
                .encode_failure(
                    &PayloadConverter::default(),
                    &SerializationContextData::None,
                )
                .unwrap_or_else(|converter_error| {
                    encode_failed_error_conversion(activity, converter_error)
                }),
            Self::ChildWorkflowExecution(child) => child
                .encode_failure(
                    &PayloadConverter::default(),
                    &SerializationContextData::None,
                )
                .unwrap_or_else(|converter_error| {
                    encode_failed_error_conversion(child, converter_error)
                }),
            Self::ChildWorkflowStart(child) => child
                .encode_failure(
                    &PayloadConverter::default(),
                    &SerializationContextData::None,
                )
                .unwrap_or_else(|converter_error| {
                    encode_failed_error_conversion(child, converter_error)
                }),
            Self::WorkflowSignal(signal) => signal
                .encode_failure(
                    &PayloadConverter::default(),
                    &SerializationContextData::None,
                )
                .unwrap_or_else(|converter_error| {
                    encode_failed_error_conversion(signal, converter_error)
                }),
            Self::Generic(err) => encode_generic_application_failure(err),
        }
    }
}

impl EncodeFailure for ApplicationFailure {
    fn encode_failure(
        &self,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError> {
        let details = self
            .failure_payloads()
            .map(|details| details.encode(payload_converter, context))
            .transpose()?;
        Ok(Failure {
            message: self.to_string(),
            cause: self
                .cause()
                .map(|cause| Box::new(cause.failure().clone()))
                .or_else(|| encode_application_failure_cause(self.source_error())),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    r#type: self.type_name().unwrap_or_default().to_owned(),
                    non_retryable: self.is_non_retryable(),
                    details,
                    next_retry_delay: self.next_retry_delay().and_then(|d| d.try_into().ok()),
                    category: ProtoApplicationErrorCategory::from(self.category()) as i32,
                },
            )),
            ..Default::default()
        })
    }
}

fn encode_application_failure_cause(
    source: &(dyn std::error::Error + 'static),
) -> Option<Box<Failure>> {
    if matches!(
        ClassifiedFailure::from_error(source),
        ClassifiedFailure::Application(_) | ClassifiedFailure::Generic(_)
    ) {
        source.source().map(encode_error_as_failure).map(Box::new)
    } else {
        Some(Box::new(encode_error_as_failure(source)))
    }
}

fn encode_error_as_failure(err: &(dyn std::error::Error + 'static)) -> Failure {
    ClassifiedFailure::from_error(err).encode()
}

impl EncodeFailure for ActivityExecutionError {
    fn encode_failure(
        &self,
        _: &PayloadConverter,
        _: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError> {
        Ok(match self {
            Self::Failed(failure) => failure.failure().clone(),
            Self::Cancelled(failure) => failure.failure().clone(),
            Self::Serialization(err) => encode_generic_application_failure(err),
        })
    }
}

impl EncodeFailure for ChildWorkflowExecutionError {
    fn encode_failure(
        &self,
        _: &PayloadConverter,
        _: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError> {
        Ok(match self {
            Self::Failed(failure) => failure.failure().clone(),
            Self::Serialization(_) => encode_generic_application_failure(self),
        })
    }
}

impl EncodeFailure for ChildWorkflowStartError {
    fn encode_failure(
        &self,
        _: &PayloadConverter,
        _: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError> {
        Ok(match self {
            Self::Cancelled(failure) => failure.failure().clone(),
            Self::StartFailed { .. } | Self::Serialization(_) => {
                encode_generic_application_failure(self)
            }
        })
    }
}

impl EncodeFailure for WorkflowSignalError {
    fn encode_failure(
        &self,
        _: &PayloadConverter,
        _: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError> {
        Ok(match self {
            Self::Failed(failure) => failure.failure().clone(),
            Self::Serialization(err) => encode_generic_application_failure(err),
        })
    }
}

fn encode_outgoing_activity_error(
    err: OutgoingActivityError,
    payload_converter: &PayloadConverter,
    context: &SerializationContextData,
) -> Result<Failure, PayloadConversionError> {
    Ok(match err {
        OutgoingActivityError::Application(app) => {
            app.encode_failure(payload_converter, context)?
        }
        OutgoingActivityError::Cancelled { details } => Failure {
            message: "Activity cancelled".to_string(),
            failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                details: details
                    .map(|details| details.encode(payload_converter, context))
                    .transpose()?,
                identity: Default::default(),
            })),
            ..Default::default()
        },
    })
}

fn encode_generic_application_failure(err: &(dyn std::error::Error + 'static)) -> Failure {
    Failure {
        message: err.to_string(),
        cause: err.source().map(encode_error_as_failure).map(Box::new),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo::default(),
        )),
        ..Default::default()
    }
}

fn encode_failed_error_conversion(
    err: &(dyn std::error::Error + 'static),
    converter_error: PayloadConversionError,
) -> Failure {
    Failure {
        message: failed_error_conversion_message(err, &converter_error),
        cause: err.source().map(encode_error_as_failure).map(Box::new),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo::default(),
        )),
        ..Default::default()
    }
}

fn decode_failure(
    failure: Failure,
    payload_converter: &PayloadConverter,
    context: &SerializationContextData,
) -> IncomingError {
    let cause = failure
        .cause
        .clone()
        .map(|cause| decode_failure(*cause, payload_converter, context));
    match failure.failure_info.clone() {
        Some(FailureInfo::ApplicationFailureInfo(_)) | None => IncomingError::Application(
            ApplicationFailure::from_failure(failure, cause, payload_converter, context),
        ),
        Some(FailureInfo::TimeoutFailureInfo(failure_info)) => IncomingError::Timeout(
            TimeoutError::new(failure, failure_info, cause, payload_converter, context),
        ),
        Some(FailureInfo::CanceledFailureInfo(failure_info)) => IncomingError::Cancelled(
            CancelledError::new(failure, failure_info, cause, payload_converter, context),
        ),
        Some(FailureInfo::TerminatedFailureInfo(_)) => {
            IncomingError::Terminated(TerminatedError::new(failure, cause))
        }
        Some(FailureInfo::ServerFailureInfo(_)) => {
            IncomingError::Server(ServerError::new(failure, cause))
        }
        Some(FailureInfo::ResetWorkflowFailureInfo(_)) => {
            IncomingError::ResetWorkflow(ResetWorkflowError::new(failure, cause))
        }
        Some(FailureInfo::ActivityFailureInfo(failure_info)) => {
            IncomingError::Activity(ActivityFailureError::new(failure, failure_info, cause))
        }
        Some(FailureInfo::ChildWorkflowExecutionFailureInfo(failure_info)) => {
            IncomingError::ChildWorkflowExecution(ChildWorkflowFailureError::new(
                failure,
                failure_info,
                cause,
            ))
        }
        Some(FailureInfo::NexusOperationExecutionFailureInfo(_)) => {
            IncomingError::NexusOperationExecution(IncomingNexusOperationExecutionError::new(
                failure, cause,
            ))
        }
        Some(FailureInfo::NexusHandlerFailureInfo(_)) => {
            IncomingError::NexusHandler(IncomingNexusHandlerError::new(failure, cause))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_converters::{GenericPayloadConverter, SerializationContext},
        error::ApplicationErrorCategory,
        protos::temporal::api::{
            common::v1::{Payload, Payloads},
            failure::v1::{
                ActivityFailureInfo, ChildWorkflowExecutionFailureInfo, NexusHandlerFailureInfo,
                NexusOperationFailureInfo, ResetWorkflowFailureInfo, ServerFailureInfo,
                TerminatedFailureInfo, TimeoutFailureInfo, failure::FailureInfo,
            },
        },
    };
    use rstest::rstest;
    use std::fmt;

    #[derive(Debug, Clone, Copy)]
    enum IncomingKind {
        Application,
        Timeout,
        Cancelled,
        Terminated,
        Server,
        ResetWorkflow,
        Activity,
        ChildWorkflowExecution,
        NexusOperationExecution,
        NexusHandler,
    }

    #[derive(Debug, Clone, Copy)]
    enum ActivityExecutionKind {
        Failed,
        Cancelled,
    }

    #[derive(Debug)]
    struct TestError {
        message: &'static str,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    }

    impl TestError {
        fn new(
            message: &'static str,
            source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
        ) -> Self {
            Self { message, source }
        }
    }

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.message)
        }
    }

    impl std::error::Error for TestError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.source
                .as_deref()
                .map(|source| source as &(dyn std::error::Error + 'static))
        }
    }

    struct AlwaysFailsSerialize;

    impl serde::Serialize for AlwaysFailsSerialize {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("serialize boom"))
        }
    }

    fn assert_incoming_kind(decoded: &IncomingError, expected: IncomingKind) {
        match expected {
            IncomingKind::Application => assert!(matches!(decoded, IncomingError::Application(_))),
            IncomingKind::Timeout => assert!(matches!(decoded, IncomingError::Timeout(_))),
            IncomingKind::Cancelled => assert!(matches!(decoded, IncomingError::Cancelled(_))),
            IncomingKind::Terminated => assert!(matches!(decoded, IncomingError::Terminated(_))),
            IncomingKind::Server => assert!(matches!(decoded, IncomingError::Server(_))),
            IncomingKind::ResetWorkflow => {
                assert!(matches!(decoded, IncomingError::ResetWorkflow(_)))
            }
            IncomingKind::Activity => assert!(matches!(decoded, IncomingError::Activity(_))),
            IncomingKind::ChildWorkflowExecution => {
                assert!(matches!(decoded, IncomingError::ChildWorkflowExecution(_)))
            }
            IncomingKind::NexusOperationExecution => {
                assert!(matches!(decoded, IncomingError::NexusOperationExecution(_)))
            }
            IncomingKind::NexusHandler => {
                assert!(matches!(decoded, IncomingError::NexusHandler(_)))
            }
        }
    }

    fn convert(err: OutgoingWorkflowError) -> Failure {
        DefaultFailureConverter.to_failure(
            OutgoingError::Workflow(err),
            &PayloadConverter::default(),
            &SerializationContextData::Workflow,
        )
    }

    fn data_converter() -> crate::data_converters::DataConverter {
        crate::data_converters::DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            crate::data_converters::DefaultPayloadCodec,
        )
    }

    fn cancelled_failure(message: &str) -> Failure {
        Failure {
            message: message.to_owned(),
            failure_info: Some(FailureInfo::CanceledFailureInfo(
                CanceledFailureInfo::default(),
            )),
            ..Default::default()
        }
    }

    fn timeout_failure(message: &str) -> Failure {
        Failure {
            message: message.to_owned(),
            failure_info: Some(FailureInfo::TimeoutFailureInfo(
                TimeoutFailureInfo::default(),
            )),
            ..Default::default()
        }
    }

    #[test]
    fn application_failures_preserve_metadata() {
        let failure = convert(OutgoingWorkflowError::Application(Box::new(
            ApplicationFailure::builder(anyhow::anyhow!("app boom"))
                .type_name("MyType".to_owned())
                .non_retryable(true)
                .category(ApplicationErrorCategory::Benign)
                .details(crate::data_converters::RawValue::new(vec![Payload {
                    data: b"details".to_vec(),
                    ..Default::default()
                }]))
                .build(),
        )));
        let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
            panic!("expected application failure info");
        };
        assert_eq!(failure.message, "app boom");
        assert_eq!(info.r#type, "MyType");
        assert!(info.non_retryable);
        assert_eq!(info.category(), ProtoApplicationErrorCategory::Benign);
        assert_eq!(info.details.unwrap().payloads[0].data, b"details".to_vec());
    }

    #[test]
    fn application_failures_encode_serializable_details_with_payload_converter() {
        let failure = convert(OutgoingWorkflowError::Application(Box::new(
            ApplicationFailure::builder(anyhow::anyhow!("app boom"))
                .details("detail")
                .build(),
        )));
        let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
            panic!("expected application failure info");
        };
        let payloads = info.details.expect("details should be present").payloads;
        let converter = PayloadConverter::default();
        let details: String = converter
            .from_payloads(
                &SerializationContext {
                    data: &SerializationContextData::Workflow,
                    converter: &converter,
                },
                payloads,
            )
            .unwrap();
        assert_eq!(details, "detail");
    }

    #[test]
    fn application_failures_surface_detail_encoding_errors_with_original_message() {
        let failure = DefaultFailureConverter.to_failure(
            OutgoingError::Workflow(OutgoingWorkflowError::Application(Box::new(
                ApplicationFailure::builder(anyhow::anyhow!("app boom"))
                    .details(AlwaysFailsSerialize)
                    .build(),
            ))),
            &PayloadConverter::default(),
            &SerializationContextData::Workflow,
        );

        assert_eq!(
            failure.message,
            "Failed converting error to failure: Encoding error: serialize boom, original error message: app boom"
        );
    }

    #[test]
    fn application_failures_decode_details_through_payload_converter() {
        let converter = PayloadConverter::default();
        let payloads = converter
            .to_payloads(
                &SerializationContext {
                    data: &SerializationContextData::Workflow,
                    converter: &converter,
                },
                &"detail",
            )
            .unwrap();
        let failure = Failure {
            message: "app boom".to_owned(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    details: Some(Payloads { payloads }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(failure, &converter, &SerializationContextData::Workflow)
            .unwrap();

        let IncomingError::Application(app) = decoded else {
            panic!("expected application error");
        };
        assert_eq!(app.details::<String>().unwrap(), Some("detail".to_string()));
    }

    #[test]
    fn nested_application_failures_surface_detail_encoding_errors_in_fallback_failure() {
        let app = ApplicationFailure::new(anyhow::Error::new(TestError::new(
            "outer wrapper",
            Some(Box::new(
                ApplicationFailure::builder(anyhow::anyhow!("inner boom"))
                    .details(AlwaysFailsSerialize)
                    .build(),
            )),
        )));

        let converted = convert(OutgoingWorkflowError::Application(Box::new(app)));
        let cause = converted.cause.expect("expected nested fallback failure");
        assert_eq!(
            cause.message,
            "Failed converting error to failure: Encoding error: serialize boom, original error message: inner boom"
        );
        assert!(matches!(
            cause.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
    }

    #[test]
    fn application_failures_do_not_duplicate_their_source_as_cause() {
        let failure = convert(OutgoingWorkflowError::Application(Box::new(
            ApplicationFailure::new(anyhow::anyhow!("app boom")),
        )));

        assert_eq!(failure.message, "app boom");
        assert!(failure.cause.is_none());
    }

    #[test]
    fn application_failures_keep_special_causes_nested() {
        let activity_failure = Failure {
            message: "activity failed".to_owned(),
            failure_info: Some(FailureInfo::ActivityFailureInfo(
                ActivityFailureInfo::default(),
            )),
            ..Default::default()
        };
        let app =
            ApplicationFailure::new(ActivityExecutionError::Failed(ActivityFailureError::new(
                activity_failure.clone(),
                ActivityFailureInfo::default(),
                None,
            )));
        let converted = convert(OutgoingWorkflowError::Application(Box::new(app)));
        assert!(matches!(
            converted.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
        assert_eq!(converted.cause.unwrap().as_ref(), &activity_failure);
    }

    #[test]
    fn application_failures_fall_back_to_source_error() {
        let activity_failure = Failure {
            message: "activity failed".to_owned(),
            failure_info: Some(FailureInfo::ActivityFailureInfo(
                ActivityFailureInfo::default(),
            )),
            ..Default::default()
        };
        let app =
            ApplicationFailure::new(ActivityExecutionError::Failed(ActivityFailureError::new(
                activity_failure.clone(),
                ActivityFailureInfo::default(),
                None,
            )));

        assert!(app.cause().is_none());

        let converted = convert(OutgoingWorkflowError::Application(Box::new(app)));

        assert_eq!(converted.cause.unwrap().as_ref(), &activity_failure);
    }

    #[test]
    fn application_failures_skip_generic_wrappers_around_known_causes() {
        let activity_failure = Failure {
            message: "activity failed".to_owned(),
            failure_info: Some(FailureInfo::ActivityFailureInfo(
                ActivityFailureInfo::default(),
            )),
            ..Default::default()
        };
        let app = ApplicationFailure::new(anyhow::Error::new(TestError::new(
            "outer wrapper",
            Some(Box::new(ActivityExecutionError::Failed(
                ActivityFailureError::new(
                    activity_failure.clone(),
                    ActivityFailureInfo::default(),
                    None,
                ),
            ))),
        )));

        let converted = convert(OutgoingWorkflowError::Application(Box::new(app)));

        assert!(matches!(
            converted.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
        assert_eq!(converted.message, "outer wrapper");
        assert_eq!(converted.cause.unwrap().as_ref(), &activity_failure);
    }

    #[test]
    fn application_failures_serialize_unknown_nested_causes_as_application_failures() {
        let app = ApplicationFailure::new(anyhow::Error::new(TestError::new(
            "outer wrapper",
            Some(Box::new(TestError::new("generic inner cause", None))),
        )));

        let converted = convert(OutgoingWorkflowError::Application(Box::new(app)));

        assert!(matches!(
            converted.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
        assert_eq!(converted.message, "outer wrapper",);
        let cause = converted
            .cause
            .clone()
            .expect("expected nested generic cause");
        assert_eq!(cause.message, "generic inner cause");
        assert!(matches!(
            cause.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
        assert!(cause.cause.is_none());

        let decoded = DefaultFailureConverter
            .to_error(
                converted.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        let IncomingError::Application(decoded_app) = decoded else {
            panic!("expected application error");
        };
        assert_eq!(decoded_app.failure(), Some(&converted));
        let Some(IncomingError::Application(wrapper)) = decoded_app.cause() else {
            panic!("expected application cause");
        };
        assert_eq!(
            wrapper.failure().map(|failure| failure.message.as_str()),
            Some("generic inner cause")
        );
        assert!(wrapper.cause().is_none());
    }

    #[test]
    fn start_failed_child_workflow_errors_fall_back_to_application_failures() {
        let failure = convert(OutgoingWorkflowError::ChildWorkflowStart(Box::new(
            ChildWorkflowStartError::StartFailed {
                workflow_id: "wf-id".to_owned(),
                workflow_type: "wf-type".to_owned(),
                cause: crate::protos::coresdk::child_workflow::StartChildWorkflowExecutionFailedCause::WorkflowAlreadyExists,
            },
        )));
        assert!(matches!(
            failure.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
        assert!(failure.message.contains("Child workflow start failed"));
    }

    #[test]
    fn application_failures_decode_with_metadata_and_proto() {
        let failure = Failure {
            message: "app boom".to_owned(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    r#type: "MyType".to_owned(),
                    non_retryable: true,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        let IncomingError::Application(app) = decoded else {
            panic!("expected application error");
        };
        assert_eq!(app.type_name(), Some("MyType"));
        assert!(app.is_non_retryable());
        assert_eq!(app.failure(), Some(&failure));
    }

    #[test]
    fn application_failures_decode_with_normalized_cause() {
        let failure = Failure {
            message: "app boom".to_owned(),
            cause: Some(Box::new(Failure {
                message: "timed out".to_owned(),
                failure_info: Some(FailureInfo::TimeoutFailureInfo(
                    TimeoutFailureInfo::default(),
                )),
                ..Default::default()
            })),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        let IncomingError::Application(app) = decoded else {
            panic!("expected application error");
        };
        assert_eq!(app.failure(), Some(&failure));
        assert!(matches!(app.cause(), Some(IncomingError::Timeout(_))));
    }

    #[test]
    fn decoded_application_failures_preserve_cause() {
        let failure = Failure {
            message: "app boom".to_owned(),
            cause: Some(Box::new(timeout_failure("timed out"))),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        let IncomingError::Application(app) = decoded else {
            panic!("expected application error");
        };
        assert!(app.as_timeout().is_some());

        let reencoded = convert(OutgoingWorkflowError::Application(Box::new(app)));

        assert_eq!(reencoded.message, failure.message);
        assert_eq!(reencoded.cause.as_deref(), failure.cause.as_deref());

        let decoded_reencoded = DefaultFailureConverter
            .to_error(
                reencoded,
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();
        let IncomingError::Application(roundtripped) = decoded_reencoded else {
            panic!("expected application error");
        };
        assert!(roundtripped.as_timeout().is_some());
    }

    #[test]
    fn application_failures_decode_wrapped_known_causes_without_collapsing_wrapper() {
        let failure = Failure {
            message: "app boom".to_owned(),
            cause: Some(Box::new(Failure {
                message: "activity failed".to_owned(),
                failure_info: Some(FailureInfo::ActivityFailureInfo(
                    ActivityFailureInfo::default(),
                )),
                ..Default::default()
            })),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        let IncomingError::Application(app) = decoded else {
            panic!("expected application error");
        };
        assert_eq!(app.failure(), Some(&failure));
        let Some(IncomingError::Activity(activity)) = app.cause() else {
            panic!("expected activity cause");
        };
        assert_eq!(activity.failure().message, "activity failed");
    }

    #[rstest]
    #[case(
        FailureInfo::ApplicationFailureInfo(ApplicationFailureInfo::default()),
        IncomingKind::Application
    )]
    #[case(
        FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo::default()),
        IncomingKind::Timeout
    )]
    #[case(
        FailureInfo::CanceledFailureInfo(CanceledFailureInfo::default()),
        IncomingKind::Cancelled
    )]
    #[case(
        FailureInfo::TerminatedFailureInfo(TerminatedFailureInfo::default()),
        IncomingKind::Terminated
    )]
    #[case(
        FailureInfo::ServerFailureInfo(ServerFailureInfo::default()),
        IncomingKind::Server
    )]
    #[case(
        FailureInfo::ResetWorkflowFailureInfo(ResetWorkflowFailureInfo::default()),
        IncomingKind::ResetWorkflow
    )]
    #[case(
        FailureInfo::ActivityFailureInfo(ActivityFailureInfo::default()),
        IncomingKind::Activity
    )]
    #[case(
        FailureInfo::ChildWorkflowExecutionFailureInfo(
            ChildWorkflowExecutionFailureInfo::default()
        ),
        IncomingKind::ChildWorkflowExecution
    )]
    #[case(
        FailureInfo::NexusOperationExecutionFailureInfo(NexusOperationFailureInfo::default()),
        IncomingKind::NexusOperationExecution
    )]
    #[case(
        FailureInfo::NexusHandlerFailureInfo(NexusHandlerFailureInfo::default()),
        IncomingKind::NexusHandler
    )]
    fn failure_info_decodes_to_expected_incoming_error(
        #[case] failure_info: FailureInfo,
        #[case] expected: IncomingKind,
    ) {
        let failure = Failure {
            message: "boom".to_owned(),
            failure_info: Some(failure_info),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        assert_incoming_kind(&decoded, expected);
        assert_eq!(decoded.failure(), &failure);
    }

    #[test]
    fn activity_decode_hint_preserves_timeout_reason() {
        let failure = Failure {
            message: "activity failed".to_owned(),
            cause: Some(Box::new(Failure {
                message: "timed out".to_owned(),
                failure_info: Some(FailureInfo::TimeoutFailureInfo(
                    TimeoutFailureInfo::default(),
                )),
                ..Default::default()
            })),
            failure_info: Some(FailureInfo::ActivityFailureInfo(ActivityFailureInfo {
                activity_id: "act-1".to_owned(),
                activity_type: Some(crate::protos::temporal::api::common::v1::ActivityType {
                    name: "test-activity".to_owned(),
                }),
                scheduled_event_id: 5,
                started_event_id: 6,
                identity: "worker-1".to_owned(),
                retry_state: crate::protos::temporal::api::enums::v1::RetryState::Timeout.into(),
            })),
            ..Default::default()
        };
        let data_converter = crate::data_converters::DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            crate::data_converters::DefaultPayloadCodec,
        );

        let decoded = data_converter
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ActivityExecutionDecodeHint { cancelled: false },
            )
            .unwrap();

        let ActivityExecutionError::Failed(decoded_failure) = decoded else {
            panic!("expected failed activity execution error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
        assert_eq!(decoded_failure.activity_id(), "act-1");
        assert_eq!(decoded_failure.activity_type(), Some("test-activity"));
        assert_eq!(decoded_failure.scheduled_event_id(), 5);
        assert_eq!(decoded_failure.started_event_id(), 6);
        assert_eq!(decoded_failure.identity(), "worker-1");
        assert_eq!(
            decoded_failure.retry_state(),
            crate::error::RetryState::Timeout
        );
        assert!(matches!(
            decoded_failure.cause(),
            Some(IncomingError::Timeout(_))
        ));
    }

    #[rstest]
    #[case(
        cancelled_failure("activity cancelled"),
        ActivityExecutionKind::Cancelled,
        None
    )]
    #[case(timeout_failure("timed out"), ActivityExecutionKind::Failed, None)]
    #[case(
        Failure {
            message: "activity task cancelled".to_owned(),
            cause: Some(Box::new(cancelled_failure("activity cancelled"))),
            failure_info: Some(FailureInfo::ActivityFailureInfo(
                ActivityFailureInfo::default(),
            )),
            ..Default::default()
        },
        ActivityExecutionKind::Cancelled,
        Some(cancelled_failure("activity cancelled"))
    )]
    fn activity_cancelled_decode_hint_adapts_expected_shape(
        #[case] failure: Failure,
        #[case] expected_kind: ActivityExecutionKind,
        #[case] expected_failure: Option<Failure>,
    ) {
        let decoded = data_converter()
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ActivityExecutionDecodeHint { cancelled: true },
            )
            .unwrap();

        match expected_kind {
            ActivityExecutionKind::Failed => {
                let ActivityExecutionError::Failed(decoded_failure) = decoded else {
                    panic!("expected failed activity execution error");
                };
                assert_eq!(decoded_failure.failure(), &failure);
                assert!(decoded_failure.cause().is_none());
            }
            ActivityExecutionKind::Cancelled => {
                let ActivityExecutionError::Cancelled(decoded_failure) = decoded else {
                    panic!("expected cancelled activity execution error");
                };
                assert_eq!(
                    decoded_failure.failure(),
                    expected_failure.as_ref().unwrap_or(&failure)
                );
                assert!(decoded_failure.cause().is_none());
            }
        }
    }

    #[test]
    fn timeout_error_exposes_timeout_info_fields() {
        let heartbeat_details = crate::protos::temporal::api::common::v1::Payloads {
            payloads: vec![Payload {
                data: b"hb".to_vec(),
                ..Default::default()
            }],
        };
        let failure = Failure {
            message: "timed out".to_owned(),
            failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                timeout_type: crate::protos::temporal::api::enums::v1::TimeoutType::Heartbeat
                    .into(),
                last_heartbeat_details: Some(heartbeat_details.clone()),
            })),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        let IncomingError::Timeout(timeout) = decoded else {
            panic!("expected timeout error");
        };
        assert_eq!(timeout.timeout_type(), crate::error::TimeoutType::Heartbeat);
        assert_eq!(
            timeout.raw_last_heartbeat_details(),
            Some(heartbeat_details.payloads.as_slice())
        );
        assert_eq!(timeout.failure(), &failure);
    }

    #[test]
    fn cancelled_error_exposes_details() {
        let details = crate::protos::temporal::api::common::v1::Payloads {
            payloads: vec![Payload {
                data: b"cancel".to_vec(),
                ..Default::default()
            }],
        };
        let failure = Failure {
            message: "cancelled".to_owned(),
            failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                details: Some(details.clone()),
                identity: Default::default(),
            })),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        let IncomingError::Cancelled(cancelled) = decoded else {
            panic!("expected cancelled error");
        };
        assert_eq!(cancelled.raw_details(), Some(details.payloads.as_slice()));
        assert_eq!(cancelled.failure(), &failure);
    }

    #[test]
    fn child_workflow_decode_hint_preserves_child_failure_proto() {
        let failure = Failure {
            message: "child workflow failed".to_owned(),
            failure_info: Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
                ChildWorkflowExecutionFailureInfo {
                    namespace: "default".to_owned(),
                    workflow_execution: Some(
                        crate::protos::temporal::api::common::v1::WorkflowExecution {
                            workflow_id: "child-id".to_owned(),
                            run_id: "run-id".to_owned(),
                        },
                    ),
                    workflow_type: Some(crate::protos::temporal::api::common::v1::WorkflowType {
                        name: "child-type".to_owned(),
                    }),
                    initiated_event_id: 11,
                    started_event_id: 22,
                    retry_state: crate::protos::temporal::api::enums::v1::RetryState::Timeout
                        .into(),
                },
            )),
            ..Default::default()
        };
        let decoded = data_converter()
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ChildWorkflowExecutionDecodeHint,
            )
            .unwrap();

        let ChildWorkflowExecutionError::Failed(decoded_failure) = decoded else {
            panic!("expected failed child-workflow execution error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
        assert_eq!(decoded_failure.namespace(), "default");
        assert_eq!(
            decoded_failure
                .workflow_execution()
                .map(|wf| wf.workflow_id()),
            Some("child-id")
        );
        assert_eq!(
            decoded_failure.workflow_execution().map(|wf| wf.run_id()),
            Some("run-id")
        );
        assert_eq!(decoded_failure.workflow_type(), Some("child-type"));
        assert_eq!(decoded_failure.initiated_event_id(), 11);
        assert_eq!(decoded_failure.started_event_id(), 22);
        assert_eq!(
            decoded_failure.retry_state(),
            crate::error::RetryState::Timeout
        );
    }

    #[rstest]
    #[case(
        Failure {
            message: "child workflow cancelled".to_owned(),
            cause: Some(Box::new(cancelled_failure("child workflow cancelled"))),
            failure_info: Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
                ChildWorkflowExecutionFailureInfo::default(),
            )),
            ..Default::default()
        },
        Some(IncomingKind::Cancelled)
    )]
    #[case(timeout_failure("timed out"), None)]
    fn child_workflow_execution_decode_hint_adapts_expected_cause(
        #[case] failure: Failure,
        #[case] expected_cause: Option<IncomingKind>,
    ) {
        let decoded = data_converter()
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ChildWorkflowExecutionDecodeHint,
            )
            .unwrap();

        let ChildWorkflowExecutionError::Failed(decoded_failure) = decoded else {
            panic!("expected failed child-workflow execution error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
        match expected_cause {
            Some(expected) => {
                let cause = decoded_failure
                    .cause()
                    .expect("expected child failure cause");
                assert_incoming_kind(cause, expected);
            }
            None => assert!(decoded_failure.cause().is_none()),
        }
    }

    #[test]
    fn child_workflow_start_decode_hint_preserves_top_level_cancellation() {
        let failure = Failure {
            message: "child start cancelled".to_owned(),
            failure_info: Some(FailureInfo::CanceledFailureInfo(
                CanceledFailureInfo::default(),
            )),
            ..Default::default()
        };
        let decoded = data_converter()
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ChildWorkflowStartDecodeHint,
            )
            .unwrap();

        let ChildWorkflowStartError::Cancelled(decoded_failure) = decoded else {
            panic!("expected cancelled child-workflow start error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
        assert!(decoded_failure.cause().is_none());
    }

    #[test]
    fn child_workflow_signal_decode_hint_preserves_failure_proto() {
        let failure = Failure {
            message: "child workflow signal failed".to_owned(),
            cause: Some(Box::new(Failure {
                message: "timed out".to_owned(),
                failure_info: Some(FailureInfo::TimeoutFailureInfo(
                    TimeoutFailureInfo::default(),
                )),
                ..Default::default()
            })),
            ..Default::default()
        };
        let decoded = data_converter()
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                WorkflowSignalDecodeHint,
            )
            .unwrap();

        let WorkflowSignalError::Failed(decoded_failure) = decoded else {
            panic!("expected failed child-workflow signal error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
        assert!(matches!(
            decoded_failure.error(),
            IncomingError::Application(_)
        ));
        assert!(matches!(
            decoded_failure.cause(),
            Some(IncomingError::Timeout(_))
        ));
        assert!(std::error::Error::source(&decoded_failure).is_some());
    }

    #[test]
    fn outgoing_cancelled_activity_errors_encode_to_cancelled_failures() {
        let failure = DefaultFailureConverter.to_failure(
            OutgoingError::Activity(OutgoingActivityError::Cancelled { details: None }),
            &PayloadConverter::default(),
            &SerializationContextData::Activity,
        );

        assert_eq!(failure.message, "Activity cancelled");
        assert!(matches!(
            failure.failure_info,
            Some(FailureInfo::CanceledFailureInfo(_))
        ));
    }

    #[test]
    fn outgoing_cancelled_activity_errors_encode_serializable_details_with_payload_converter() {
        let failure = DefaultFailureConverter.to_failure(
            OutgoingError::Activity(OutgoingActivityError::Cancelled {
                details: Some("detail".to_string().into()),
            }),
            &PayloadConverter::default(),
            &SerializationContextData::Activity,
        );

        let err = DefaultFailureConverter
            .to_error(
                failure,
                &PayloadConverter::default(),
                &SerializationContextData::Activity,
            )
            .unwrap();
        let cancelled = err.as_cancelled().unwrap();
        let details: String = cancelled.details().unwrap().unwrap();
        assert_eq!(details, "detail");
    }
}
