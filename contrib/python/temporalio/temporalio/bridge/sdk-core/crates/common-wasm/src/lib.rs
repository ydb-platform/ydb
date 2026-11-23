#![warn(missing_docs)] // error if there are missing docs

//! This crate contains the shared definitions and serialization/proto surface needed by the
//! workflow authoring APIs, including WASM-targeted builds.

#[allow(unused_imports)] // Not used by all flag combinations, which is fine.
#[macro_use]
extern crate tracing;

use std::time::Duration;

mod activity_definition;
pub mod data_converters;
pub mod error;
mod memo;
mod priority;
mod retry_policy;
mod workflow_execution;
pub mod protos {
    //! Protobuf definitions re-exported from `temporalio-protos`.

    pub use temporalio_protos::*;
}
pub mod search_attributes;
pub mod worker;
mod workflow_definition;

pub use activity_definition::{ActivityDefinition, ActivityError, UntypedActivity};
pub use memo::Memo;
pub use priority::Priority;
pub use retry_policy::RetryPolicy;
pub use search_attributes::{
    SearchAttributeError, SearchAttributeKey, SearchAttributeUpdate, SearchAttributeValue,
    SearchAttributes, Timestamp,
};
pub use worker::WorkerDeploymentVersion;
pub use workflow_definition::{
    HasWorkflowDefinition, QueryDefinition, SignalDefinition, UntypedWorkflow, UpdateDefinition,
    WorkflowDefinition,
};
pub use workflow_execution::WorkflowExecution;

#[allow(unused_macros)]
macro_rules! dbg_panic {
  ($($arg:tt)*) => {
      use tracing::error;
      error!($($arg)*);
      debug_assert!(false, $($arg)*);
  };
}
#[allow(unused_imports)]
pub(crate) use dbg_panic;

/// Represents Activity schedule-to-close and start-to-close timeouts for the purposes of specifying
/// Activity options. Specifying at least one of them is required, but specifying both is also
/// allowed. Note that this type does not cover all available timeout options for an Activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityCloseTimeouts {
    /// Total time the Activity is allowed to run, including retries.
    ScheduleToClose(Duration),
    /// Maximum time of a single Activity execution attempt. Note that the Temporal Server doesn't
    /// detect Worker process failures directly. It relies on this timeout to detect that an
    /// Activity that didn't complete on time. So this timeout should be as short as the longest
    /// possible execution of the Activity body. Potentially long running Activities must specify
    /// `heartbeat_timeout` in options and heartbeat from the activity periodically for timely
    /// failure detection.
    StartToClose(Duration),
    /// Applies both execution-attempt and overall-completion bounds.
    Both {
        /// Total time the Activity is allowed to run, including retries.
        schedule_to_close: Duration,
        /// Maximum time of a single Activity execution attempt.
        start_to_close: Duration,
    },
}

impl ActivityCloseTimeouts {
    /// Returns value of [`Self::ScheduleToClose`]  or [`Self::Both::schedule_to_close`].
    pub fn schedule_to_close(&self) -> Option<Duration> {
        match self {
            ActivityCloseTimeouts::ScheduleToClose(schedule_to_close)
            | ActivityCloseTimeouts::Both {
                schedule_to_close, ..
            } => Some(*schedule_to_close),
            _ => None,
        }
    }

    /// Returns value of [`Self::StartToClose`]  or [`Self::Both::start_to_close`].
    pub fn start_to_close(&self) -> Option<Duration> {
        match self {
            ActivityCloseTimeouts::StartToClose(start_to_close)
            | ActivityCloseTimeouts::Both { start_to_close, .. } => Some(*start_to_close),
            _ => None,
        }
    }
}
