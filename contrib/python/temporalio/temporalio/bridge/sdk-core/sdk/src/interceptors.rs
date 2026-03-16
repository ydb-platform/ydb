//! User-definable interceptors are defined in this module

use crate::Worker;
use anyhow::bail;
use std::sync::{Arc, OnceLock};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{WorkflowActivation, remove_from_cache::EvictionReason},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::common::v1::Payload,
};

/// Implementors can intercept certain actions that happen within the Worker.
///
/// Advanced usage only.
#[async_trait::async_trait(?Send)]
pub trait WorkerInterceptor {
    /// Called every time a workflow activation completes (just before sending the completion to
    /// core).
    async fn on_workflow_activation_completion(&self, _completion: &WorkflowActivationCompletion) {}
    /// Called after the worker has initiated shutdown and the workflow/activity polling loops
    /// have exited, but just before waiting for the inner core worker shutdown
    fn on_shutdown(&self, _sdk_worker: &Worker) {}
    /// Called every time a workflow is about to be activated
    async fn on_workflow_activation(
        &self,
        _activation: &WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

/// Supports the composition of interceptors
pub struct InterceptorWithNext {
    inner: Box<dyn WorkerInterceptor>,
    next: Option<Box<InterceptorWithNext>>,
}

impl InterceptorWithNext {
    /// Create from an existing interceptor, can be used to initialize a chain of interceptors
    pub fn new(inner: Box<dyn WorkerInterceptor>) -> Self {
        Self { inner, next: None }
    }

    /// Sets the next interceptor, and then returns that interceptor, wrapped by
    /// [InterceptorWithNext]. You can keep calling this method on it to extend the chain.
    pub fn set_next(&mut self, next: Box<dyn WorkerInterceptor>) -> &mut InterceptorWithNext {
        self.next.insert(Box::new(Self::new(next)))
    }
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for InterceptorWithNext {
    async fn on_workflow_activation_completion(&self, c: &WorkflowActivationCompletion) {
        self.inner.on_workflow_activation_completion(c).await;
        if let Some(next) = &self.next {
            next.on_workflow_activation_completion(c).await;
        }
    }

    fn on_shutdown(&self, w: &Worker) {
        self.inner.on_shutdown(w);
        if let Some(next) = &self.next {
            next.on_shutdown(w);
        }
    }

    async fn on_workflow_activation(&self, a: &WorkflowActivation) -> Result<(), anyhow::Error> {
        self.inner.on_workflow_activation(a).await?;
        if let Some(next) = &self.next {
            next.on_workflow_activation(a).await?;
        }
        Ok(())
    }
}

/// An interceptor which causes the worker's run function to exit early if nondeterminism errors are
/// encountered
pub struct FailOnNondeterminismInterceptor {}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for FailOnNondeterminismInterceptor {
    async fn on_workflow_activation(
        &self,
        activation: &WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        if matches!(
            activation.eviction_reason(),
            Some(EvictionReason::Nondeterminism)
        ) {
            bail!(
                "Workflow is being evicted because of nondeterminism! {}",
                activation
            );
        }
        Ok(())
    }
}

/// An interceptor that allows you to fetch the exit value of the workflow if and when it is set
#[derive(Default)]
pub struct ReturnWorkflowExitValueInterceptor {
    result_value: Arc<OnceLock<Payload>>,
}

impl ReturnWorkflowExitValueInterceptor {
    /// Can be used to fetch the workflow result if/when it is determined
    pub fn get_result_handle(&self) -> Arc<OnceLock<Payload>> {
        self.result_value.clone()
    }
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for ReturnWorkflowExitValueInterceptor {
    async fn on_workflow_activation_completion(&self, c: &WorkflowActivationCompletion) {
        if let Some(v) = c.complete_workflow_execution_value() {
            let _ = self.result_value.set(v.clone());
        }
    }
}
