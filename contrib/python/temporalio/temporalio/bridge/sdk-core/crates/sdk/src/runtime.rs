//! Runtime configuration and low-level Core worker building blocks.
//!
//! These types are grouped here to keep Core-specific configuration separate from the SDK's
//! primary workflow and activity APIs. Create a [`crate::Runtime`] before connecting a client,
//! then pass it to [`crate::Worker::new`].

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use temporalio_common::telemetry::{TelemetryInstance, TelemetryOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions as CoreRuntimeOptions};

pub use temporalio_sdk_core::{
    ActivitySlotKind, FixedSizeSlotSupplier, LocalActivitySlotKind, NexusSlotKind, PollerBehavior,
    ResourceBasedSlotsOptions, ResourceBasedSlotsOptionsBuilder, ResourceBasedTuner,
    ResourceBasedTunerConfig, ResourceController, ResourceSlotOptions, SlotInfo, SlotInfoTrait,
    SlotKind, SlotKindType, SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext,
    SlotSupplier, SlotSupplierOptions, SlotSupplierPermit, TokioRuntimeBuilder, TunerBuilder,
    TunerHolder, TunerHolderOptions, TunerHolderOptionsBuilder, Worker as CoreWorker, WorkerConfig,
    WorkerConfigBuilder, WorkerTuner, WorkerVersioningStrategy, WorkflowErrorType,
    WorkflowSlotKind, init_replay_worker, replay,
};

/// Configuration for the Rust SDK runtime. Construct with [`RuntimeOptions::builder`].
#[derive(bon::Builder)]
#[builder(finish_fn(vis = "", name = build_internal))]
#[non_exhaustive]
pub struct RuntimeOptions {
    /// Telemetry configuration options.
    #[builder(default)]
    telemetry_options: TelemetryOptions,
    /// Optional worker heartbeat interval for all workers created with this runtime.
    ///
    /// The interval must be between 1 and 60 seconds, inclusive.
    #[builder(required, default = Some(Duration::from_secs(60)))]
    heartbeat_interval: Option<Duration>,
    /// Disable including runtime, hosting, and platform information in worker heartbeats.
    #[builder(default)]
    disable_environment_info: bool,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self::builder().build().expect("builder defaults are valid")
    }
}

impl<S: runtime_options_builder::State> RuntimeOptionsBuilder<S> {
    /// Builds the runtime options.
    ///
    /// # Errors
    /// Returns an error if `heartbeat_interval` is set but is not between 1 and 60 seconds,
    /// inclusive.
    pub fn build(self) -> Result<RuntimeOptions, String> {
        let options = self.build_internal();
        if let Some(interval) = options.heartbeat_interval
            && (interval < Duration::from_secs(1) || interval > Duration::from_secs(60))
        {
            return Err(format!(
                "heartbeat_interval ({interval:?}) must be between 1s and 60s",
            ));
        }
        Ok(options)
    }
}

impl From<RuntimeOptions> for CoreRuntimeOptions {
    fn from(options: RuntimeOptions) -> Self {
        CoreRuntimeOptions::builder()
            .telemetry_options(options.telemetry_options)
            .heartbeat_interval(options.heartbeat_interval)
            .disable_environment_info(options.disable_environment_info)
            .build()
            .expect("SDK runtime options have already been validated")
    }
}

/// Holds shared state and components used by Rust SDK workers.
pub struct Runtime(CoreRuntime);

impl Runtime {
    /// Creates a runtime with a newly constructed Tokio runtime.
    pub fn new<F>(
        options: RuntimeOptions,
        tokio_builder: TokioRuntimeBuilder<F>,
    ) -> Result<Self, anyhow::Error>
    where
        F: Fn() + Send + Sync + 'static,
    {
        CoreRuntime::new(options.into(), tokio_builder).map(Self)
    }

    /// Creates a runtime using the currently active Tokio runtime.
    ///
    /// # Panics
    /// Panics if there is no currently active Tokio runtime.
    pub fn new_assume_tokio(options: RuntimeOptions) -> Result<Self, anyhow::Error> {
        CoreRuntime::new_assume_tokio(options.into()).map(Self)
    }

    /// Creates a runtime from an initialized telemetry instance using the currently active Tokio
    /// runtime.
    ///
    /// # Panics
    /// Panics if there is no currently active Tokio runtime.
    pub fn new_assume_tokio_initialized_telem(
        telemetry: TelemetryInstance,
        heartbeat_interval: Option<Duration>,
    ) -> Self {
        Self(CoreRuntime::new_assume_tokio_initialized_telem(
            telemetry,
            heartbeat_interval,
        ))
    }
}

impl Deref for Runtime {
    type Target = CoreRuntime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Runtime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
