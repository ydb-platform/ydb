//! Host-side command sink trait mirroring the checked-in WIT interface.

use temporalio_common_wasm::protos::coresdk::workflow_commands::WorkflowCommand;

/// Runtime-facing workflow host interface for native and WASM backends.
pub trait WorkflowHost {
    /// Update the details string surfaced through the workflow metadata query.
    fn set_current_details(&self, details: String);
    /// Emit a workflow command for the current activation.
    fn push_command(&self, command: WorkflowCommand);
}
