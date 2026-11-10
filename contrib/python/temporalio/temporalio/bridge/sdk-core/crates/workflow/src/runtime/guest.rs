//! High-level guest-side runtime traits mirroring the checked-in WIT interface.

use crate::runtime::types::{
    ActivationResult, RoutineId, RoutinePollResult, WorkflowActivation, WorkflowFailure,
};
use std::task::Waker;

/// Runtime-facing single-workflow execution interface for native and WASM backends.
pub trait WorkflowInstance {
    /// Apply one ordered workflow activation, using the provided waker for any construction-only
    /// interceptor polls.
    fn activate(
        &mut self,
        activation: WorkflowActivation,
        waker: &Waker,
    ) -> Result<ActivationResult, WorkflowFailure>;
    /// Poll exactly one guest routine, using the provided waker for native integrations that need
    /// wake tracking.
    fn poll_routine(
        &mut self,
        routine_id: RoutineId,
        waker: &Waker,
    ) -> Result<RoutinePollResult, WorkflowFailure>;
}
