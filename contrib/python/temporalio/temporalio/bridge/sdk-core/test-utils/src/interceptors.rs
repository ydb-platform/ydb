use anyhow::Error;
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicBool, Ordering},
};
use temporal_sdk::interceptors::WorkerInterceptor;
use temporal_sdk_core_protos::coresdk::workflow_activation::WorkflowActivation;

#[derive(Default)]
pub struct ActivationAssertionsInterceptor {
    #[allow(clippy::type_complexity)]
    assertions: Mutex<VecDeque<Box<dyn FnOnce(&WorkflowActivation)>>>,
    used: AtomicBool,
}

impl ActivationAssertionsInterceptor {
    pub fn skip_one(&mut self) -> &mut Self {
        self.assertions.lock().push_back(Box::new(|_| {}));
        self
    }

    pub fn then(&mut self, assert: impl FnOnce(&WorkflowActivation) + 'static) -> &mut Self {
        self.assertions.lock().push_back(Box::new(assert));
        self
    }
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for ActivationAssertionsInterceptor {
    async fn on_workflow_activation(&self, act: &WorkflowActivation) -> Result<(), Error> {
        self.used.store(true, Ordering::Relaxed);
        if let Some(fun) = self.assertions.lock().pop_front() {
            fun(act);
        }
        Ok(())
    }
}

impl Drop for ActivationAssertionsInterceptor {
    fn drop(&mut self) {
        if !self.used.load(Ordering::Relaxed) {
            panic!("Activation assertions interceptor was never used!")
        }
    }
}
