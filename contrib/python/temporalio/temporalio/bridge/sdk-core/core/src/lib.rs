#![warn(missing_docs)] // error if there are missing docs
#![allow(clippy::upper_case_acronyms)]

//! This crate provides a basis for creating new Temporal SDKs without completely starting from
//! scratch

#[cfg(test)]
#[macro_use]
pub extern crate assert_matches;
#[macro_use]
extern crate tracing;
extern crate core;

mod abstractions;
#[cfg(feature = "debug-plugin")]
pub mod debug_client;
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
#[cfg(test)]
#[macro_use]
mod test_help;

pub(crate) use temporal_sdk_core_api::errors;

pub use pollers::{
    Client, ClientOptions, ClientOptionsBuilder, ClientTlsConfig, RetryClient, RetryConfig,
    TlsConfig, WorkflowClientTrait,
};
pub use temporal_sdk_core_api as api;
pub use temporal_sdk_core_protos as protos;
pub use temporal_sdk_core_protos::TaskToken;
pub use url::Url;
pub use worker::{
    FixedSizeSlotSupplier, RealSysInfo, ResourceBasedSlotsOptions,
    ResourceBasedSlotsOptionsBuilder, ResourceBasedTuner, ResourceSlotOptions, SlotSupplierOptions,
    TunerBuilder, TunerHolder, TunerHolderOptions, TunerHolderOptionsBuilder, Worker, WorkerConfig,
    WorkerConfigBuilder,
};

/// Expose [WorkerClient] symbols
pub use crate::worker::client::{
    PollActivityOptions, PollOptions, PollWorkflowOptions, WorkerClient, WorkflowTaskCompletion,
};
use crate::{
    replay::{HistoryForReplay, ReplayWorkerInput},
    telemetry::{
        TelemetryInstance, metrics::MetricsContext, remove_trace_subscriber_for_current_thread,
        set_trace_subscriber_for_current_thread, telemetry_init,
    },
    worker::client::WorkerClientBag,
};
use anyhow::bail;
use futures_util::Stream;
use std::sync::{Arc, OnceLock};
use temporal_client::{ConfiguredClient, NamespacedClient, TemporalServiceClientWithMetrics};
use temporal_sdk_core_api::{
    Worker as WorkerTrait,
    errors::{CompleteActivityError, PollError},
    telemetry::TelemetryOptions,
};
use temporal_sdk_core_protos::coresdk::ActivityHeartbeat;

/// Initialize a worker bound to a task queue.
///
/// You will need to have already initialized a [CoreRuntime] which will be used for this worker.
/// After the worker is initialized, you should use [CoreRuntime::tokio_handle] to run the worker's
/// async functions.
///
/// Lang implementations may pass in a [ConfiguredClient] directly (or a
/// [RetryClient] wrapping one, or a handful of other variants of the same idea). When they do so,
/// this function will always overwrite the client retry configuration, force the client to use the
/// namespace defined in the worker config, and set the client identity appropriately. IE: Use
/// [ClientOptions::connect_no_namespace], not [ClientOptions::connect].
pub fn init_worker<CT>(
    runtime: &CoreRuntime,
    worker_config: WorkerConfig,
    client: CT,
) -> Result<Worker, anyhow::Error>
where
    CT: Into<sealed::AnyClient>,
{
    let client = init_worker_client(&worker_config, *client.into().into_inner());
    if client.namespace() != worker_config.namespace {
        bail!("Passed in client is not bound to the same namespace as the worker");
    }
    if client.namespace() == "" {
        bail!("Client namespace cannot be empty");
    }
    let client_ident = client.get_identity().to_owned();
    let sticky_q = sticky_q_name_for_worker(&client_ident, &worker_config);

    if client_ident.is_empty() {
        bail!("Client identity cannot be empty. Either lang or user should be setting this value");
    }

    let heartbeat_fn = worker_config
        .heartbeat_interval
        .map(|_| Arc::new(OnceLock::new()));

    let client_bag = Arc::new(WorkerClientBag::new(
        client,
        worker_config.namespace.clone(),
        client_ident,
        worker_config.versioning_strategy.clone(),
        heartbeat_fn.clone(),
    ));

    Ok(Worker::new(
        worker_config,
        sticky_q,
        client_bag,
        Some(&runtime.telemetry),
        heartbeat_fn,
    ))
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
    config: &WorkerConfig,
    client: ConfiguredClient<TemporalServiceClientWithMetrics>,
) -> RetryClient<Client> {
    let mut client = Client::new(client, config.namespace.clone());
    if let Some(ref id_override) = config.client_identity_override {
        client.options_mut().identity.clone_from(id_override);
    }
    RetryClient::new(client, RetryConfig::default())
}

/// Creates a unique sticky queue name for a worker, iff the config allows for 1 or more cached
/// workflows.
pub(crate) fn sticky_q_name_for_worker(
    process_identity: &str,
    config: &WorkerConfig,
) -> Option<String> {
    if config.max_cached_workflows > 0 {
        Some(format!(
            "{}-{}",
            &process_identity,
            uuid::Uuid::new_v4().simple()
        ))
    } else {
        None
    }
}

mod sealed {
    use super::*;

    /// Allows passing different kinds of clients into things that want to be flexible. Motivating
    /// use-case was worker initialization.
    ///
    /// Needs to exist in this crate to avoid blanket impl conflicts.
    pub struct AnyClient {
        pub(crate) inner: Box<ConfiguredClient<TemporalServiceClientWithMetrics>>,
    }
    impl AnyClient {
        pub(crate) fn into_inner(self) -> Box<ConfiguredClient<TemporalServiceClientWithMetrics>> {
            self.inner
        }
    }

    impl From<RetryClient<ConfiguredClient<TemporalServiceClientWithMetrics>>> for AnyClient {
        fn from(c: RetryClient<ConfiguredClient<TemporalServiceClientWithMetrics>>) -> Self {
            Self {
                inner: Box::new(c.into_inner()),
            }
        }
    }
    impl From<RetryClient<Client>> for AnyClient {
        fn from(c: RetryClient<Client>) -> Self {
            Self {
                inner: Box::new(c.into_inner().into_inner()),
            }
        }
    }
    impl From<Arc<RetryClient<Client>>> for AnyClient {
        fn from(c: Arc<RetryClient<Client>>) -> Self {
            Self {
                inner: Box::new(c.get_client().inner().clone()),
            }
        }
    }
    impl From<ConfiguredClient<TemporalServiceClientWithMetrics>> for AnyClient {
        fn from(c: ConfiguredClient<TemporalServiceClientWithMetrics>) -> Self {
            Self { inner: Box::new(c) }
        }
    }
}

/// Holds shared state/components needed to back instances of workers and clients. More than one
/// may be instantiated, but typically only one is needed. More than one runtime instance may be
/// useful if multiple different telemetry settings are required.
pub struct CoreRuntime {
    telemetry: TelemetryInstance,
    runtime: Option<tokio::runtime::Runtime>,
    runtime_handle: tokio::runtime::Handle,
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
        telemetry_options: TelemetryOptions,
        mut tokio_builder: TokioRuntimeBuilder<F>,
    ) -> Result<Self, anyhow::Error>
    where
        F: Fn() + Send + Sync + 'static,
    {
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
        let mut me = Self::new_assume_tokio_initialized_telem(telemetry);
        me.runtime = Some(runtime);
        Ok(me)
    }

    /// Initialize telemetry for the thread this is being called on, assuming a tokio runtime is
    /// already active and this call exists in its context. See [Self::new] for more.
    ///
    /// # Panics
    /// If there is no currently active Tokio runtime
    pub fn new_assume_tokio(telemetry_options: TelemetryOptions) -> Result<Self, anyhow::Error> {
        let telemetry = telemetry_init(telemetry_options)?;
        Ok(Self::new_assume_tokio_initialized_telem(telemetry))
    }

    /// Construct a runtime from an already-initialized telemetry instance, assuming a tokio runtime
    /// is already active and this call exists in its context. See [Self::new] for more.
    ///
    /// # Panics
    /// If there is no currently active Tokio runtime
    pub fn new_assume_tokio_initialized_telem(telemetry: TelemetryInstance) -> Self {
        let runtime_handle = tokio::runtime::Handle::current();
        if let Some(sub) = telemetry.trace_subscriber() {
            set_trace_subscriber_for_current_thread(sub);
        }
        Self {
            telemetry,
            runtime: None,
            runtime_handle,
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
