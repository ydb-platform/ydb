#![warn(missing_docs)] // error if there are missing docs
#![allow(clippy::upper_case_acronyms)]

//! This crate provides a basis for creating new Temporal SDKs without completely starting from
//! scratch. APIs provided by this crate are not considered stable and may break at any time.
//!
//! If you are looking for the Temporal Rust SDK, please use `temporalio-sdk`.

#[cfg(test)]
#[macro_use]
pub extern crate assert_matches;
#[macro_use]
extern crate tracing;
extern crate core;

mod abstractions;
#[cfg(feature = "antithesis_assertions")]
mod antithesis;
#[cfg(feature = "debug-plugin")]
pub mod debug_client;
mod environment;
#[cfg(feature = "ephemeral-server")]
pub mod ephemeral_server;
mod internal_flags;
mod pollers;
mod protosext;
pub mod replay;
pub(crate) mod retry_logic;
pub mod telemetry;
mod worker;

#[cfg(test)]
mod core_tests;
#[cfg(any(feature = "test-utilities", test))]
#[macro_use]
pub mod test_help;

pub use crate::worker::client::{
    PollActivityOptions, PollOptions, PollWorkflowOptions, WorkerClient, WorkflowTaskCompletion,
};
pub use pollers::{Client, ClientOptions, ClientTlsOptions, RetryOptions, TlsOptions};
pub use temporalio_common::protos::TaskToken;
pub use url::Url;
pub use worker::{
    ActivitySlotKind, CompleteActivityError, CompleteNexusError, CompleteWfError,
    FixedSizeSlotSupplier, LocalActivitySlotKind, NamespaceCapabilities, NexusSlotKind, PollError,
    PollerBehavior, ResourceBasedSlotsOptions, ResourceBasedSlotsOptionsBuilder,
    ResourceBasedTuner, ResourceBasedTunerConfig, ResourceController, ResourceSlotOptions,
    SlotInfo, SlotInfoTrait, SlotKind, SlotKindType, SlotMarkUsedContext, SlotReleaseContext,
    SlotReservationContext, SlotSupplier, SlotSupplierOptions, SlotSupplierPermit, TunerBuilder,
    TunerHolder, TunerHolderOptions, TunerHolderOptionsBuilder, Worker, WorkerConfig,
    WorkerConfigBuilder, WorkerTuner, WorkerValidationError, WorkerVersioningStrategy,
    WorkflowErrorType, WorkflowSlotKind,
};

use crate::{
    replay::{HistoryForReplay, ReplayWorkerInput},
    telemetry::metrics::MetricsContext,
    worker::client::WorkerClientBag,
};
use anyhow::bail;
use futures_util::Stream;
use std::{sync::Arc, time::Duration};
use temporalio_client::{Connection, SharedReplaceableClient};
use temporalio_common::{
    protos::{
        coresdk::ActivityHeartbeat,
        temporal::api::worker::v1::{EnvironmentInfo, environment_info::Runtime},
    },
    telemetry::{
        TelemetryInstance, TelemetryOptions, remove_trace_subscriber_for_current_thread,
        set_trace_subscriber_for_current_thread, telemetry_init,
    },
};

/// Initialize a worker bound to a task queue.
///
/// You will need to have already initialized a [CoreRuntime] which will be used for this worker.
/// After the worker is initialized, you should use [CoreRuntime::tokio_handle] to run the worker's
/// async functions.
///
/// Lang implementations must pass in a [Client] When they do so, this function will always
/// overwrite the client retry configuration, force the client to use the namespace defined in the
/// worker config, and set the client identity appropriately.
pub fn init_worker(
    runtime: &CoreRuntime,
    worker_config: WorkerConfig,
    mut connection: Connection,
) -> Result<Worker, anyhow::Error> {
    let namespace = worker_config.namespace.clone();
    if namespace.is_empty() {
        bail!("Worker namespace cannot be empty");
    }

    *connection.retry_options_mut() = RetryOptions::default();
    init_worker_client(
        &mut connection,
        worker_config.client_identity_override.clone(),
    );
    let client = SharedReplaceableClient::new(connection);
    let client_ident = client.inner_cow().identity().to_owned();
    if client_ident.is_empty() {
        bail!("Client identity cannot be empty. Either lang or user should be setting this value");
    }
    let sticky_q = sticky_q_name_for_worker(&client_ident, worker_config.max_cached_workflows);

    let worker_instance_key = uuid::Uuid::new_v4();
    let client_bag = Arc::new(WorkerClientBag::new(
        client,
        namespace.clone(),
        worker_config.versioning_strategy.clone(),
        worker_instance_key,
    ));

    Worker::new(
        worker_config.clone(),
        sticky_q,
        client_bag.clone(),
        Some(&runtime.telemetry),
        runtime.heartbeat_interval,
        runtime.environment_info.clone(),
    )
}

/// Create a worker for replaying one or more existing histories. It will auto-shutdown as soon as
/// all histories have finished being replayed.
///
/// You do not necessarily need a [CoreRuntime] for replay workers, but it's advisable to create
/// one and use it to run the replay worker's async functions the same way you would for a normal
/// worker.
pub fn init_replay_worker<I>(rwi: ReplayWorkerInput<I>) -> Result<Worker, anyhow::Error>
where
    I: Stream<Item = HistoryForReplay> + Send + 'static,
{
    info!(
        task_queue = rwi.config.task_queue.as_str(),
        "Registering replay worker"
    );
    rwi.into_core_worker()
}

pub(crate) fn init_worker_client(
    connection: &mut Connection,
    client_identity_override: Option<String>,
) {
    if let Some(ref id_override) = client_identity_override {
        connection.identity_mut().clone_from(id_override);
    }
}

/// Creates a unique sticky queue name for a worker, iff the config allows for 1 or more cached
/// workflows.
pub(crate) fn sticky_q_name_for_worker(
    process_identity: &str,
    max_cached_workflows: usize,
) -> Option<String> {
    if max_cached_workflows > 0 {
        Some(format!(
            "{}-{}",
            &process_identity,
            uuid::Uuid::new_v4().simple()
        ))
    } else {
        None
    }
}

/// Holds shared state/components needed to back instances of workers and clients. More than one
/// may be instantiated, but typically only one is needed. More than one runtime instance may be
/// useful if multiple different telemetry settings are required.
pub struct CoreRuntime {
    telemetry: TelemetryInstance,
    runtime: Option<tokio::runtime::Runtime>,
    runtime_handle: tokio::runtime::Handle,
    heartbeat_interval: Option<Duration>,
    environment_info: Option<Arc<EnvironmentInfo>>,
}

/// Holds telemetry and process-wide worker options. Construct with [RuntimeOptions::builder].
#[derive(bon::Builder)]
#[builder(finish_fn(vis = "", name = build_internal))]
#[non_exhaustive]
pub struct RuntimeOptions {
    /// Telemetry configuration options.
    #[builder(default)]
    telemetry_options: TelemetryOptions,
    /// Optional worker heartbeat interval - This configures the heartbeat setting of all
    /// workers created using this runtime.
    ///
    /// Interval must be between 1s and 60s, inclusive.
    #[builder(required, default = Some(Duration::from_secs(60)))]
    heartbeat_interval: Option<Duration>,
    /// Disable including runtime, hosting, and platform information in worker heartbeats.
    #[builder(default)]
    disable_environment_info: bool,
    /// Runtime information supplied by language SDK bridges.
    #[doc(hidden)]
    #[builder(skip = vec![environment::native_runtime()])]
    runtimes: Vec<Runtime>,
}

impl RuntimeOptions {
    /// Supplies runtime information from a language SDK bridge.
    #[doc(hidden)]
    pub fn with_runtimes(mut self, runtimes: Vec<Runtime>) -> Self {
        self.runtimes = runtimes;
        self
    }
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self::builder().build().expect("builder defaults are valid")
    }
}

impl<S: runtime_options_builder::State> RuntimeOptionsBuilder<S> {
    /// Builds the RuntimeOptions
    ///
    /// # Errors
    /// Returns an error if heartbeat_interval is set but not between 1s and 60s inclusive.
    pub fn build(self) -> Result<RuntimeOptions, String> {
        let options = self.build_internal();
        {
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
}

/// Wraps a [tokio::runtime::Builder] to allow layering multiple on_thread_start functions
pub struct TokioRuntimeBuilder<F> {
    /// The underlying tokio runtime builder
    pub inner: tokio::runtime::Builder,
    /// A function to be called when setting the runtime builder's on thread start
    pub lang_on_thread_start: Option<F>,
}

impl Default for TokioRuntimeBuilder<Box<dyn Fn() + Send + Sync>> {
    fn default() -> Self {
        TokioRuntimeBuilder {
            inner: tokio::runtime::Builder::new_multi_thread(),
            lang_on_thread_start: None,
        }
    }
}

impl CoreRuntime {
    /// Create a new core runtime with the provided telemetry options and tokio runtime builder.
    /// Also initialize telemetry for the thread this is being called on.
    ///
    /// Note that this function will call the [tokio::runtime::Builder::enable_all] builder option
    /// on the Tokio runtime builder, and will call [tokio::runtime::Builder::on_thread_start] to
    /// ensure telemetry subscribers are set on every tokio thread.
    ///
    /// **Important**: You need to call this *before* calling any async functions on workers or
    /// clients, otherwise the tracing subscribers will not be properly attached.
    ///
    /// # Panics
    /// If a tokio runtime has already been initialized. To re-use an existing runtime, call
    /// [CoreRuntime::new_assume_tokio].
    pub fn new<F>(
        runtime_options: RuntimeOptions,
        mut tokio_builder: TokioRuntimeBuilder<F>,
    ) -> Result<Self, anyhow::Error>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let RuntimeOptions {
            telemetry_options,
            heartbeat_interval,
            disable_environment_info,
            runtimes,
        } = runtime_options;
        let telemetry = telemetry_init(telemetry_options)?;
        let subscriber = telemetry.trace_subscriber();
        let runtime = tokio_builder
            .inner
            .enable_all()
            .on_thread_start(move || {
                if let Some(sub) = subscriber.as_ref() {
                    set_trace_subscriber_for_current_thread(sub.clone());
                }
                if let Some(lang_on_thread_start) = tokio_builder.lang_on_thread_start.as_ref() {
                    lang_on_thread_start();
                }
            })
            .build()?;
        let _rg = runtime.enter();
        let mut me = Self::new_assume_tokio_initialized_telem_with_environment(
            telemetry,
            heartbeat_interval,
            disable_environment_info,
            runtimes,
        );
        me.runtime = Some(runtime);
        Ok(me)
    }

    /// Initialize telemetry for the thread this is being called on, assuming a tokio runtime is
    /// already active and this call exists in its context. See [Self::new] for more.
    ///
    /// # Panics
    /// If there is no currently active Tokio runtime
    pub fn new_assume_tokio(runtime_options: RuntimeOptions) -> Result<Self, anyhow::Error> {
        let RuntimeOptions {
            telemetry_options,
            heartbeat_interval,
            disable_environment_info,
            runtimes,
        } = runtime_options;
        let telemetry = telemetry_init(telemetry_options)?;
        Ok(Self::new_assume_tokio_initialized_telem_with_environment(
            telemetry,
            heartbeat_interval,
            disable_environment_info,
            runtimes,
        ))
    }

    /// Construct a runtime from an already-initialized telemetry instance, assuming a tokio runtime
    /// is already active and this call exists in its context. See [Self::new] for more.
    ///
    /// # Panics
    /// If there is no currently active Tokio runtime
    pub fn new_assume_tokio_initialized_telem(
        telemetry: TelemetryInstance,
        heartbeat_interval: Option<Duration>,
    ) -> Self {
        Self::new_assume_tokio_initialized_telem_with_environment(
            telemetry,
            heartbeat_interval,
            false,
            vec![environment::native_runtime()],
        )
    }

    fn new_assume_tokio_initialized_telem_with_environment(
        telemetry: TelemetryInstance,
        heartbeat_interval: Option<Duration>,
        disable_environment_info: bool,
        runtimes: Vec<Runtime>,
    ) -> Self {
        let runtime_handle = tokio::runtime::Handle::current();
        if let Some(sub) = telemetry.trace_subscriber() {
            set_trace_subscriber_for_current_thread(sub);
        }
        let environment_info = heartbeat_interval
            .filter(|_| !disable_environment_info)
            .map(|_| Arc::new(environment::detect(runtimes)));
        Self {
            telemetry,
            runtime: None,
            runtime_handle,
            heartbeat_interval,
            environment_info,
        }
    }

    /// Get a handle to the tokio runtime used by this Core runtime.
    pub fn tokio_handle(&self) -> tokio::runtime::Handle {
        self.runtime_handle.clone()
    }

    /// Return a reference to the owned [TelemetryInstance]
    pub fn telemetry(&self) -> &TelemetryInstance {
        &self.telemetry
    }

    /// Return a mutable reference to the owned [TelemetryInstance]
    pub fn telemetry_mut(&mut self) -> &mut TelemetryInstance {
        &mut self.telemetry
    }
}

impl Drop for CoreRuntime {
    fn drop(&mut self) {
        remove_trace_subscriber_for_current_thread();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn runtime_options_default_matches_builder_default() {
        let default = RuntimeOptions::default();
        let built = RuntimeOptions::builder().build().unwrap();
        assert_eq!(default.heartbeat_interval, built.heartbeat_interval);
    }
}
