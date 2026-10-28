//! Contains errors that can be returned by clients.

use crate::{PluginApplyError, WorkflowExecutionStatus, workflow_handle::WorkflowResultDetails};
use http::uri::InvalidUri;
use temporalio_common::{
    data_converters::{DecodablePayloads, PayloadConversionError},
    error::{IncomingError, TimeoutType},
    protos::{
        temporal::api::{
            errordetails::v1::ActivityExecutionAlreadyStartedFailure, failure::v1::Failure,
        },
        utilities::decode_status_detail,
    },
};
use tonic::Code;

/// Errors thrown while attempting to establish a connection to the server
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ClientConnectError {
    /// A plugin failed while configuring connection options.
    #[error(transparent)]
    Plugin(#[from] PluginApplyError),
    /// Invalid URI. Configuration error, fatal.
    #[error("Invalid URI: {0:?}")]
    InvalidUri(#[from] InvalidUri),
    /// Invalid gRPC metadata headers. Configuration error.
    #[error("Invalid headers: {0}")]
    InvalidHeaders(#[from] InvalidHeaderError),
    /// Server connection error. Crashing and restarting the worker is likely best.
    #[error("Server connection error: {0:?}")]
    TonicTransportError(#[from] tonic::transport::Error),
    /// We couldn't successfully make the `get_system_info` call at connection time to establish
    /// server capabilities / verify server is responding.
    #[error("`get_system_info` call error after connection: {0:?}")]
    SystemInfoCallError(tonic::Status),
    /// DNS resolution failed when attempting load-balanced connection.
    #[error("DNS resolution error for '{host}': {source}")]
    DnsResolutionError {
        /// The host that failed to resolve.
        host: String,
        /// The underlying IO error.
        #[source]
        source: std::io::Error,
    },
    /// Invalid client configuration.
    #[error("Invalid client configuration: {0}")]
    InvalidConfig(String),
}

impl From<ClientNewError> for ClientConnectError {
    fn from(value: ClientNewError) -> Self {
        match value {
            ClientNewError::Plugin(err) => Self::Plugin(err),
        }
    }
}

/// Errors thrown when a gRPC metadata header is invalid.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum InvalidHeaderError {
    /// A binary header key was invalid
    #[error("Invalid binary header key '{key}': {source}")]
    InvalidBinaryHeaderKey {
        /// The invalid key
        key: String,
        /// The source error from tonic
        source: tonic::metadata::errors::InvalidMetadataKey,
    },
    /// An ASCII header key was invalid
    #[error("Invalid ASCII header key '{key}': {source}")]
    InvalidAsciiHeaderKey {
        /// The invalid key
        key: String,
        /// The source error from tonic
        source: tonic::metadata::errors::InvalidMetadataKey,
    },
    /// An ASCII header value was invalid
    #[error("Invalid ASCII header value for key '{key}': {source}")]
    InvalidAsciiHeaderValue {
        /// The key
        key: String,
        /// The invalid value
        value: String,
        /// The source error from tonic
        source: tonic::metadata::errors::InvalidMetadataValue,
    },
}

/// Errors that can occur when starting a workflow.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum WorkflowStartError {
    /// The workflow already exists.
    #[error("Workflow already started with run ID: {run_id:?}")]
    AlreadyStarted {
        /// Run ID of the already-started workflow if this was raised by the client.
        run_id: Option<String>,
        /// The original gRPC status from the server.
        #[source]
        source: tonic::Status,
    },
    /// Error converting the input to a payload.
    #[error("Failed to serialize workflow input: {0}")]
    PayloadConversion(#[from] PayloadConversionError),
    /// An uncategorized rpc error from the server.
    #[error("Server error: {0}")]
    Rpc(#[from] tonic::Status),
}

/// Errors returned by query operations on [crate::WorkflowHandle].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowQueryError {
    /// The workflow was not found.
    #[error("Workflow not found")]
    NotFound(#[source] tonic::Status),

    /// The query was rejected based on the rejection condition.
    #[error("Query rejected: workflow status {status:?}")]
    Rejected {
        /// The workflow status that caused the query rejection, if reported.
        status: Option<WorkflowExecutionStatus>,
    },

    /// Error serializing input or deserializing output.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// An uncategorized RPC error from the server.
    #[error("Server error: {0}")]
    Rpc(tonic::Status),

    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl WorkflowQueryError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        if status.code() == Code::NotFound {
            Self::NotFound(status)
        } else {
            Self::Rpc(status)
        }
    }
}

/// Errors returned by update operations on [crate::WorkflowHandle].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowUpdateError {
    /// The workflow was not found.
    #[error("Workflow not found")]
    NotFound(#[source] tonic::Status),

    /// The update failed with an application-level failure.
    #[error("Update failed: {0:?}")]
    Failed(Box<Failure>),

    /// Error serializing input or deserializing output.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// An uncategorized RPC error from the server.
    #[error("Server error: {0}")]
    Rpc(tonic::Status),

    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl WorkflowUpdateError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        if status.code() == Code::NotFound {
            Self::NotFound(status)
        } else {
            Self::Rpc(status)
        }
    }
}

/// Errors returned by workflow get_result operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowGetResultError {
    /// The workflow finished in failure.
    #[error("Workflow failed: {0}")]
    Failed(#[source] Box<IncomingError>),

    /// The workflow was cancelled.
    #[error("Workflow cancelled")]
    Cancelled {
        /// Details provided at cancellation time.
        details: WorkflowResultDetails,
    },

    /// The workflow was terminated.
    #[error("Workflow terminated")]
    Terminated {
        /// Details provided at termination time.
        details: WorkflowResultDetails,
    },

    /// The workflow timed out.
    #[error("Workflow timed out")]
    TimedOut,

    /// The workflow continued as new.
    #[error("Workflow continued as new")]
    ContinuedAsNew,

    /// The workflow was not found.
    #[error("Workflow not found")]
    NotFound(#[source] tonic::Status),

    /// Error serializing input or deserializing output.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// An uncategorized RPC error from the server.
    #[error("Server error: {0}")]
    Rpc(tonic::Status),

    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl From<WorkflowInteractionError> for WorkflowGetResultError {
    fn from(err: WorkflowInteractionError) -> Self {
        match err {
            WorkflowInteractionError::NotFound(s) => Self::NotFound(s),
            WorkflowInteractionError::PayloadConversion(e) => Self::PayloadConversion(e),
            WorkflowInteractionError::Rpc(s) => Self::Rpc(s),
            WorkflowInteractionError::Other(e) => Self::Other(e),
        }
    }
}

impl WorkflowGetResultError {
    /// Returns `true` if this error represents a workflow-level non-success outcome
    /// (Failed, Cancelled, Terminated, TimedOut, or ContinuedAsNew) rather than an
    /// infrastructure/RPC error.
    pub fn is_workflow_outcome(&self) -> bool {
        matches!(
            self,
            Self::Failed(_)
                | Self::Cancelled { .. }
                | Self::Terminated { .. }
                | Self::TimedOut
                | Self::ContinuedAsNew
        )
    }
}

/// Errors returned by client methods that don't need more specific error types.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// Error decoding payloads returned by the server.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),
    /// An uncategorized rpc error from the server.
    #[error("Server error: {0}")]
    Rpc(#[from] tonic::Status),
}

/// Errors returned by methods on [crate::WorkflowHandle] for general operations
/// like signal, cancel, terminate, describe, fetch_history, and get_result.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowInteractionError {
    /// The workflow was not found.
    #[error("Workflow not found")]
    NotFound(#[source] tonic::Status),

    /// Error serializing input or deserializing output.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// An uncategorized RPC error from the server.
    #[error("Server error: {0}")]
    Rpc(tonic::Status),

    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl WorkflowInteractionError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        if status.code() == Code::NotFound {
            Self::NotFound(status)
        } else {
            Self::Rpc(status)
        }
    }
}

/// Errors that can occur when completing an activity asynchronously.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AsyncActivityError {
    /// The activity was not found (e.g., already completed, cancelled, or never existed).
    #[error("Activity not found")]
    NotFound(#[source] tonic::Status),
    /// Error serializing an activity result, failure, or details.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),
    /// An uncategorized rpc error from the server.
    #[error("Server error: {0}")]
    Rpc(#[from] tonic::Status),
}

impl AsyncActivityError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        if status.code() == Code::NotFound {
            Self::NotFound(status)
        } else {
            Self::Rpc(status)
        }
    }
}

/// Errors that can occur when constructing a [`crate::Client`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientNewError {
    /// A plugin failed while configuring client options.
    #[error(transparent)]
    Plugin(#[from] PluginApplyError),
}

/// Errors returned by methods on [crate::ActivityHandle] that don't need more specific error types.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ActivityInteractionError {
    /// The activity was not found.
    #[error("Activity not found")]
    NotFound(#[source] tonic::Status),

    /// Error deserializing output.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// An uncategorized RPC error from the server.
    #[error("Server error: {0}")]
    Rpc(#[source] tonic::Status),

    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl From<tonic::Status> for ActivityInteractionError {
    fn from(status: tonic::Status) -> Self {
        if status.code() == Code::NotFound {
            Self::NotFound(status)
        } else {
            Self::Rpc(status)
        }
    }
}

/// Errors that can occur when starting a standalone activity.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StartActivityError {
    /// There's a conflicting activity execution with the same ID according to chosen ID reuse
    /// policy and ID conflict policy.
    #[error("Activity already started with run_id={run_id}")]
    AlreadyStarted {
        /// Run ID of the existing execution with the same activity ID.
        run_id: String,
        /// Raw error from the server.
        #[source]
        source: tonic::Status,
    },

    /// Error serializing input.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// An uncategorized RPC error from the server.
    #[error("Server error: {0}")]
    Rpc(#[source] tonic::Status),

    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl From<tonic::Status> for StartActivityError {
    fn from(status: tonic::Status) -> Self {
        if status.code() == tonic::Code::AlreadyExists
            && let Some(details) =
                decode_status_detail::<ActivityExecutionAlreadyStartedFailure>(status.details())
        {
            StartActivityError::AlreadyStarted {
                run_id: details.run_id,
                source: status,
            }
        } else {
            StartActivityError::Rpc(status)
        }
    }
}

/// Errors returned by [`crate::ActivityHandle::result`].
#[allow(clippy::large_enum_variant)]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ActivityResultError {
    /// Activity execution did not complete successfully.
    #[error("Activity failed: {0}")]
    ActivityFailed(#[source] IncomingError),

    /// The activity was canceled.
    #[error("Activity canceled")]
    Cancelled {
        /// Details provided at cancellation time.
        details: DecodablePayloads,
    },

    /// The workflow was terminated.
    #[error("Activity terminated")]
    Terminated,

    /// The activity timed out.
    #[error("Activity timed out: {0:?}")]
    TimedOut(TimeoutType),

    /// The activity was not found.
    #[error("Activity not found")]
    NotFound(#[source] tonic::Status),

    /// Error deserializing output.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// An uncategorized RPC error from the server.
    #[error("Server error: {0}")]
    Rpc(#[source] tonic::Status),

    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl From<tonic::Status> for ActivityResultError {
    fn from(status: tonic::Status) -> Self {
        if status.code() == Code::NotFound {
            Self::NotFound(status)
        } else {
            Self::Rpc(status)
        }
    }
}
