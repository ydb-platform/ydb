#![allow(non_local_definitions)] // pymethods annotations causing issues with this lint

use anyhow::Context;
use prost::Message;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use temporalio_common::protos::coresdk::workflow_completion::WorkflowActivationCompletion;
use temporalio_common::protos::coresdk::{
    nexus::NexusTaskCompletion, ActivityHeartbeat, ActivityTaskCompletion,
};
use temporalio_common::protos::temporal::api::history::v1::History;
use temporalio_common::protos::temporal::api::worker::v1::{PluginInfo, StorageDriverInfo};
use temporalio_sdk_core::replay::{HistoryForReplay, ReplayWorkerInput};
use temporalio_sdk_core::{
    PollError, SlotInfo, SlotInfoTrait, SlotKind, SlotKindType, SlotMarkUsedContext,
    SlotReleaseContext, SlotReservationContext, SlotSupplier as SlotSupplierTrait,
    SlotSupplierPermit, WorkflowErrorType,
};
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tracing::error;

use crate::client;
use crate::runtime;

pyo3::create_exception!(temporal_sdk_bridge, PollShutdownError, PyException);

#[pyclass]
pub struct WorkerRef {
    worker: Option<Arc<temporalio_sdk_core::Worker>>,
    /// Set upon the call to `validate`, with the task locals for the event loop at that time, which
    /// is whatever event loop the user is running their worker in. This loop might be needed by
    /// other rust-created threads that want to run async python code.
    event_loop_task_locals: Arc<OnceLock<pyo3_async_runtimes::TaskLocals>>,
    runtime: runtime::Runtime,
}

#[derive(FromPyObject)]
pub struct WorkerConfig {
    namespace: String,
    task_queue: String,
    versioning_strategy: WorkerVersioningStrategy,
    identity_override: Option<String>,
    max_cached_workflows: usize,
    tuner: TunerHolder,
    workflow_task_poller_behavior: PollerBehavior,
    nonsticky_to_sticky_poll_ratio: f32,
    activity_task_poller_behavior: PollerBehavior,
    task_types: WorkerTaskTypes,
    sticky_queue_schedule_to_start_timeout_millis: u64,
    max_heartbeat_throttle_interval_millis: u64,
    default_heartbeat_throttle_interval_millis: u64,
    max_activities_per_second: Option<f64>,
    max_task_queue_activities_per_second: Option<f64>,
    max_eager_activity_reservations_per_workflow_task: usize,
    graceful_shutdown_period_millis: u64,
    nondeterminism_as_workflow_fail: bool,
    nondeterminism_as_workflow_fail_for_types: HashSet<String>,
    nexus_task_poller_behavior: PollerBehavior,
    plugins: Vec<String>,
    storage_drivers: HashSet<String>,
    disable_payload_error_limit: bool,
}

#[derive(FromPyObject)]
pub struct PollerBehaviorSimpleMaximum {
    pub simple_maximum: usize,
}

#[derive(FromPyObject)]
pub struct PollerBehaviorAutoscaling {
    pub minimum: usize,
    pub maximum: usize,
    pub initial: usize,
}

/// Recreates [temporalio_sdk_core::PollerBehavior]
#[derive(FromPyObject)]
pub enum PollerBehavior {
    SimpleMaximum(PollerBehaviorSimpleMaximum),
    Autoscaling(PollerBehaviorAutoscaling),
}

impl From<PollerBehavior> for temporalio_sdk_core::PollerBehavior {
    fn from(value: PollerBehavior) -> Self {
        match value {
            PollerBehavior::SimpleMaximum(simple) => {
                temporalio_sdk_core::PollerBehavior::SimpleMaximum(simple.simple_maximum)
            }
            PollerBehavior::Autoscaling(auto) => temporalio_sdk_core::PollerBehavior::Autoscaling {
                minimum: auto.minimum,
                maximum: auto.maximum,
                initial: auto.initial,
            },
        }
    }
}

/// Recreates [temporalio_sdk_core::WorkerVersioningStrategy]
#[derive(FromPyObject)]
pub enum WorkerVersioningStrategy {
    None(WorkerVersioningNone),
    DeploymentBased(WorkerDeploymentOptions),
    LegacyBuildIdBased(LegacyBuildIdBased),
}

#[derive(FromPyObject)]
pub struct WorkerVersioningNone {
    pub build_id_no_versioning: String,
}

/// Recreates [temporalio_common::worker::WorkerDeploymentOptions]
#[derive(FromPyObject)]
pub struct WorkerDeploymentOptions {
    pub version: WorkerDeploymentVersion,
    pub use_worker_versioning: bool,
    /// This is a [enums::v1::VersioningBehavior] represented as i32
    pub default_versioning_behavior: i32,
}

#[derive(FromPyObject)]
pub struct LegacyBuildIdBased {
    pub build_id_with_versioning: String,
}

/// Recreates [temporalio_common::worker::WorkerDeploymentVersion]
#[derive(FromPyObject, IntoPyObject, Clone)]
pub struct WorkerDeploymentVersion {
    pub deployment_name: String,
    pub build_id: String,
}

impl From<temporalio_common::worker::WorkerDeploymentVersion> for WorkerDeploymentVersion {
    fn from(version: temporalio_common::worker::WorkerDeploymentVersion) -> Self {
        WorkerDeploymentVersion {
            deployment_name: version.deployment_name,
            build_id: version.build_id,
        }
    }
}

#[derive(FromPyObject)]
pub struct TunerHolder {
    workflow_slot_supplier: SlotSupplier,
    activity_slot_supplier: SlotSupplier,
    local_activity_slot_supplier: SlotSupplier,
    nexus_slot_supplier: SlotSupplier,
}

#[derive(FromPyObject)]
pub enum SlotSupplier {
    FixedSize(FixedSizeSlotSupplier),
    ResourceBased(ResourceBasedSlotSupplier),
    Custom(CustomSlotSupplier),
}

#[derive(FromPyObject)]
pub struct FixedSizeSlotSupplier {
    num_slots: usize,
}

#[derive(FromPyObject)]
pub struct ResourceBasedSlotSupplier {
    minimum_slots: usize,
    maximum_slots: usize,
    // Need pyo3 0.21+ for this to be std Duration
    ramp_throttle_ms: u64,
    tuner_config: ResourceBasedTunerConfig,
}

#[derive(FromPyObject)]
pub struct WorkerTaskTypes {
    enable_workflows: bool,
    enable_local_activities: bool,
    enable_remote_activities: bool,
    enable_nexus: bool,
}

impl From<WorkerTaskTypes> for temporalio_common::worker::WorkerTaskTypes {
    fn from(t: WorkerTaskTypes) -> Self {
        Self {
            enable_workflows: t.enable_workflows,
            enable_local_activities: t.enable_local_activities,
            enable_remote_activities: t.enable_remote_activities,
            enable_nexus: t.enable_nexus,
        }
    }
}

#[pyclass]
pub struct SlotReserveCtx {
    #[pyo3(get)]
    pub slot_type: String,
    #[pyo3(get)]
    pub task_queue: String,
    #[pyo3(get)]
    pub worker_identity: String,
    #[pyo3(get)]
    pub worker_build_id: String,
    #[pyo3(get)]
    pub worker_deployment_version: Option<WorkerDeploymentVersion>,
    #[pyo3(get)]
    pub is_sticky: bool,
}

impl SlotReserveCtx {
    fn from_ctx(slot_type: SlotKindType, ctx: &dyn SlotReservationContext) -> Self {
        SlotReserveCtx {
            slot_type: match slot_type {
                SlotKindType::Workflow => "workflow".to_string(),
                SlotKindType::Activity => "activity".to_string(),
                SlotKindType::LocalActivity => "local-activity".to_string(),
                SlotKindType::Nexus => "nexus".to_string(),
            },
            task_queue: ctx.task_queue().to_string(),
            worker_identity: ctx.worker_identity().to_string(),
            worker_build_id: ctx
                .worker_deployment_version()
                .clone()
                .map(|v| v.build_id)
                .unwrap_or_default(),
            worker_deployment_version: ctx.worker_deployment_version().clone().map(Into::into),
            is_sticky: ctx.is_sticky(),
        }
    }
}

#[pyclass]
pub struct SlotMarkUsedCtx {
    #[pyo3(get)]
    slot_info: Py<PyAny>,
    #[pyo3(get)]
    permit: Py<PyAny>,
}

// NOTE: this is dumb because we already have the generated proto code, we just can't use
//   it b/c it's not pyclassable. In theory maybe we could compile-flag enable it in the core
//   protos crate but... that's a lot for just this. Maybe if there are other use cases.

#[pyclass]
pub struct WorkflowSlotInfo {
    #[pyo3(get)]
    pub workflow_type: String,
    #[pyo3(get)]
    pub is_sticky: bool,
}
#[pyclass]
pub struct ActivitySlotInfo {
    #[pyo3(get)]
    pub activity_type: String,
}
#[pyclass]
pub struct LocalActivitySlotInfo {
    #[pyo3(get)]
    pub activity_type: String,
}
#[pyclass]
pub struct NexusSlotInfo {
    #[pyo3(get)]
    pub service: String,
    #[pyo3(get)]
    pub operation: String,
}

#[pyclass]
pub struct SlotReleaseCtx {
    #[pyo3(get)]
    slot_info: Option<Py<PyAny>>,
    #[pyo3(get)]
    permit: Py<PyAny>,
}

fn slot_info_to_py_obj<'py>(py: Python<'py>, info: SlotInfo) -> PyResult<Bound<'py, PyAny>> {
    Ok(match info {
        SlotInfo::Workflow(w) => WorkflowSlotInfo {
            workflow_type: w.workflow_type.clone(),
            is_sticky: w.is_sticky,
        }
        .into_pyobject(py)?
        .into_any(),
        SlotInfo::Activity(a) => ActivitySlotInfo {
            activity_type: a.activity_type.clone(),
        }
        .into_pyobject(py)?
        .into_any(),
        SlotInfo::LocalActivity(a) => LocalActivitySlotInfo {
            activity_type: a.activity_type.clone(),
        }
        .into_pyobject(py)?
        .into_any(),
        SlotInfo::Nexus(n) => NexusSlotInfo {
            service: n.service.clone(),
            operation: n.operation.clone(),
        }
        .into_pyobject(py)?
        .into_any(),
    })
}

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct CustomSlotSupplier {
    inner: Arc<Py<PyAny>>,
}

struct CustomSlotSupplierOfType<SK: SlotKind> {
    inner: Arc<Py<PyAny>>,
    event_loop_task_locals: Arc<OnceLock<pyo3_async_runtimes::TaskLocals>>,
    _phantom: PhantomData<SK>,
}

#[pymethods]
impl CustomSlotSupplier {
    #[new]
    fn new(inner: Py<PyAny>) -> Self {
        CustomSlotSupplier {
            inner: Arc::new(inner),
        }
    }
}

// Shouldn't really need this callback nonsense, it should be possible to do this from the pyo3
// asyncio library, but we'd have to vendor the whole thing to make the right improvements. When
// pyo3 is upgraded and we are using https://github.com/PyO3/pyo3-async-runtimes (the replacement)
// consider upstreaming a way to do this.

#[pyclass]
struct CreatedTaskForSlotCallback {
    stored_task: Arc<OnceLock<Py<PyAny>>>,
}

#[pymethods]
impl CreatedTaskForSlotCallback {
    fn __call__(&self, task: Py<PyAny>) -> PyResult<()> {
        self.stored_task.set(task).expect("must only be set once");
        Ok(())
    }
}

struct TaskCanceller {
    stored_task: Arc<OnceLock<Py<PyAny>>>,
}

impl TaskCanceller {
    fn new(stored_task: Arc<OnceLock<Py<PyAny>>>) -> Self {
        TaskCanceller { stored_task }
    }
}

impl Drop for TaskCanceller {
    fn drop(&mut self) {
        if let Some(task) = self.stored_task.get() {
            Python::attach(|py| {
                task.call_method0(py, "cancel")
                    .expect("Failed to cancel task");
            });
        }
    }
}

#[async_trait::async_trait]
impl<SK: SlotKind + Send + Sync> SlotSupplierTrait for CustomSlotSupplierOfType<SK> {
    type SlotKind = SK;

    async fn reserve_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit {
        loop {
            let stored_task = Arc::new(OnceLock::new());
            let _task_canceller = TaskCanceller::new(stored_task.clone());
            let pypermit = match Python::attach(|py| {
                let py_obj = self.inner.bind(py);
                let called = py_obj.call_method1(
                    "reserve_slot",
                    (
                        SlotReserveCtx::from_ctx(SK::kind(), ctx),
                        CreatedTaskForSlotCallback { stored_task },
                    ),
                )?;
                let tl = self
                    .event_loop_task_locals
                    .get()
                    .expect("task locals must be set");
                pyo3_async_runtimes::into_future_with_locals(tl, called)
            }) {
                Ok(f) => f,
                Err(e) => {
                    error!("Unexpected error in custom slot supplier `reserve_slot`: {e}");
                    continue;
                }
            }
            .await;
            match pypermit {
                Ok(p) => {
                    return SlotSupplierPermit::with_user_data(p);
                }
                Err(_) => {
                    // This is a user thrown error, re-raised by the logging wrapper so we can
                    // loop, so do that.
                }
            }
        }
    }

    fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        Python::attach(|py| {
            let py_obj = self.inner.bind(py);
            let pa = py_obj.call_method1(
                "try_reserve_slot",
                (SlotReserveCtx::from_ctx(SK::kind(), ctx),),
            )?;

            if pa.is_none() {
                return Ok(None);
            }
            PyResult::Ok(Some(SlotSupplierPermit::with_user_data(
                pa.into_pyobject(py)?.unbind(),
            )))
        })
        .unwrap_or_else(|e| {
            error!("Uncaught error in custom slot supplier `try_reserve_slot`: {e}");
            None
        })
    }

    fn mark_slot_used(&self, ctx: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {
        if let Err(e) = Python::attach(|py| {
            let permit = ctx
                .permit()
                .user_data::<Py<PyAny>>()
                .map(|o| o.clone_ref(py))
                .unwrap_or_else(|| py.None());
            let py_obj = self.inner.bind(py);
            py_obj.call_method1(
                "mark_slot_used",
                (SlotMarkUsedCtx {
                    slot_info: slot_info_to_py_obj(py, ctx.info().downcast())?.unbind(),
                    permit,
                },),
            )?;
            PyResult::Ok(())
        }) {
            error!("Uncaught error in custom slot supplier `mark_slot_used`: {e}");
        }
    }

    fn release_slot(&self, ctx: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {
        if let Err(e) = Python::attach(|py| {
            let permit = ctx
                .permit()
                .user_data::<Py<PyAny>>()
                .map(|o| o.clone_ref(py))
                .unwrap_or_else(|| py.None());
            let py_obj = self.inner.bind(py);
            let slot_info = if let Some(info) = ctx.info() {
                Some(slot_info_to_py_obj(py, info.downcast())?.unbind())
            } else {
                None
            };
            py_obj.call_method1("release_slot", (SlotReleaseCtx { slot_info, permit },))?;
            PyResult::Ok(())
        }) {
            error!("Uncaught error in custom slot supplier `release_slot`: {e}");
        }
    }

    fn available_slots(&self) -> Option<usize> {
        None
    }
}

#[derive(FromPyObject, Clone, Copy, PartialEq)]
pub struct ResourceBasedTunerConfig {
    target_memory_usage: f64,
    target_cpu_usage: f64,
}

macro_rules! enter_sync {
    ($runtime:expr) => {
        if let Some(subscriber) = $runtime.core.telemetry().trace_subscriber() {
            temporalio_common::telemetry::set_trace_subscriber_for_current_thread(subscriber);
        }
        let _guard = $runtime.core.tokio_handle().enter();
    };
}

pub fn new_worker(
    runtime_ref: &runtime::RuntimeRef,
    client: &client::ClientRef,
    config: WorkerConfig,
) -> PyResult<WorkerRef> {
    enter_sync!(runtime_ref.runtime);
    runtime_ref.runtime.assert_same_process("create worker")?;
    let event_loop_task_locals = Arc::new(OnceLock::new());
    let config = convert_worker_config(config, event_loop_task_locals.clone())?;
    let worker = temporalio_sdk_core::init_worker(
        &runtime_ref.runtime.core,
        config,
        client.connection.clone(),
    )
    .context("Failed creating worker")?;
    Ok(WorkerRef {
        worker: Some(Arc::new(worker)),
        event_loop_task_locals,
        runtime: runtime_ref.runtime.clone(),
    })
}

pub fn new_replay_worker<'a>(
    py: Python<'a>,
    runtime_ref: &runtime::RuntimeRef,
    config: WorkerConfig,
) -> PyResult<Bound<'a, PyTuple>> {
    enter_sync!(runtime_ref.runtime);
    runtime_ref
        .runtime
        .assert_same_process("create replay worker")?;
    let event_loop_task_locals = Arc::new(OnceLock::new());
    let config = convert_worker_config(config, event_loop_task_locals.clone())?;
    let (history_pusher, stream) = HistoryPusher::new(runtime_ref.runtime.clone());
    let worker = WorkerRef {
        worker: Some(Arc::new(
            temporalio_sdk_core::init_replay_worker(ReplayWorkerInput::new(config, stream))
                .map_err(|err| {
                    PyValueError::new_err(format!("Failed creating replay worker: {err}"))
                })?,
        )),
        event_loop_task_locals: Default::default(),
        runtime: runtime_ref.runtime.clone(),
    };
    PyTuple::new(
        py,
        [
            worker.into_pyobject(py)?.into_any(),
            history_pusher.into_pyobject(py)?.into_any(),
        ],
    )
}

#[pymethods]
impl WorkerRef {
    fn validate<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("use worker")?;
        let worker = self.worker.as_ref().unwrap().clone();
        // Set custom slot supplier task locals so they can run futures.
        // Event loop is assumed to be running at this point.
        let task_locals =
            pyo3_async_runtimes::TaskLocals::with_running_loop(py)?.copy_context(py)?;
        self.event_loop_task_locals
            .set(task_locals)
            .expect("must only be set once");

        self.runtime.future_into_py(py, async move {
            let bytes = match worker.validate().await {
                Ok(info) => info.encode_to_vec(),
                Err(err) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "Worker validation failed: {err}"
                    )))
                }
            };
            Ok(bytes)
        })
    }

    fn poll_workflow_activation<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("use worker")?;
        let worker = self.worker.as_ref().unwrap().clone();
        self.runtime.future_into_py(py, async move {
            let bytes = match worker.poll_workflow_activation().await {
                Ok(act) => act.encode_to_vec(),
                Err(PollError::ShutDown) => return Err(PollShutdownError::new_err(())),
                Err(err) => return Err(PyRuntimeError::new_err(format!("Poll failure: {err}"))),
            };
            Ok(bytes)
        })
    }

    fn poll_activity_task<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("use worker")?;
        let worker = self.worker.as_ref().unwrap().clone();
        self.runtime.future_into_py(py, async move {
            let bytes = match worker.poll_activity_task().await {
                Ok(task) => task.encode_to_vec(),
                Err(PollError::ShutDown) => return Err(PollShutdownError::new_err(())),
                Err(err) => return Err(PyRuntimeError::new_err(format!("Poll failure: {err}"))),
            };
            Ok(bytes)
        })
    }

    fn poll_nexus_task<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("use worker")?;
        let worker = self.worker.as_ref().unwrap().clone();
        self.runtime.future_into_py(py, async move {
            let bytes = match worker.poll_nexus_task().await {
                Ok(task) => task.encode_to_vec(),
                Err(PollError::ShutDown) => return Err(PollShutdownError::new_err(())),
                Err(err) => return Err(PyRuntimeError::new_err(format!("Poll failure: {err}"))),
            };
            Ok(bytes)
        })
    }

    fn complete_workflow_activation<'p>(
        &self,
        py: Python<'p>,
        proto: &Bound<'_, PyBytes>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let worker = self.worker.as_ref().unwrap().clone();
        let completion = WorkflowActivationCompletion::decode(proto.as_bytes())
            .map_err(|err| PyValueError::new_err(format!("Invalid proto: {err}")))?;
        self.runtime.future_into_py(py, async move {
            worker
                .complete_workflow_activation(completion)
                .await
                .context("Completion failure")
                .map_err(Into::into)
        })
    }

    fn complete_activity_task<'p>(
        &self,
        py: Python<'p>,
        proto: &Bound<'_, PyBytes>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let worker = self.worker.as_ref().unwrap().clone();
        let completion = ActivityTaskCompletion::decode(proto.as_bytes())
            .map_err(|err| PyValueError::new_err(format!("Invalid proto: {err}")))?;
        self.runtime.future_into_py(py, async move {
            worker
                .complete_activity_task(completion)
                .await
                .context("Completion failure")
                .map_err(Into::into)
        })
    }

    fn complete_nexus_task<'p>(
        &self,
        py: Python<'p>,
        proto: &Bound<'_, PyBytes>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let worker = self.worker.as_ref().unwrap().clone();
        let completion = NexusTaskCompletion::decode(proto.as_bytes())
            .map_err(|err| PyValueError::new_err(format!("Invalid proto: {err}")))?;
        self.runtime.future_into_py(py, async move {
            worker
                .complete_nexus_task(completion)
                .await
                .context("Completion failure")
                .map_err(Into::into)
        })
    }

    fn record_activity_heartbeat(&self, proto: &Bound<'_, PyBytes>) -> PyResult<()> {
        enter_sync!(self.runtime);
        let heartbeat = ActivityHeartbeat::decode(proto.as_bytes())
            .map_err(|err| PyValueError::new_err(format!("Invalid proto: {err}")))?;
        let worker = self.worker.as_ref().unwrap().clone();
        // Detach from the GIL during the core call. Core may block on internal
        // locks whose holders can call back into Python (e.g. a custom slot
        // supplier's mark_slot_used runs while core's outstanding-activity
        // lock is held); holding the GIL here would deadlock the worker.
        proto
            .py()
            .detach(move || worker.record_activity_heartbeat(heartbeat));
        Ok(())
    }

    fn request_workflow_eviction(&self, run_id: &str) -> PyResult<()> {
        enter_sync!(self.runtime);
        self.worker
            .as_ref()
            .unwrap()
            .request_workflow_eviction(run_id);
        Ok(())
    }

    fn replace_client(&self, client: &client::ClientRef) -> PyResult<()> {
        enter_sync!(self.runtime);
        self.worker
            .as_ref()
            .expect("missing worker")
            .replace_client(client.connection.clone())
            .map_err(|err| PyValueError::new_err(format!("Failed replacing client: {err}")))
    }

    fn initiate_shutdown(&self) -> PyResult<()> {
        enter_sync!(self.runtime);
        let worker = self.worker.as_ref().unwrap().clone();
        worker.initiate_shutdown();
        Ok(())
    }

    fn finalize_shutdown<'p>(&mut self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        // Take the worker out of the option and leave None. This should be the
        // only reference remaining to the worker so try_unwrap will work.
        let worker = Arc::try_unwrap(self.worker.take().unwrap()).map_err(|arc| {
            PyValueError::new_err(format!(
                "Cannot finalize, expected 1 reference, got {}",
                Arc::strong_count(&arc)
            ))
        })?;
        self.runtime.future_into_py(py, async move {
            worker.finalize_shutdown().await;
            Ok(())
        })
    }
}

fn convert_worker_config(
    conf: WorkerConfig,
    task_locals: Arc<OnceLock<pyo3_async_runtimes::TaskLocals>>,
) -> PyResult<temporalio_sdk_core::WorkerConfig> {
    let converted_tuner = convert_tuner_holder(conf.tuner, task_locals)?;
    let converted_versioning_strategy = convert_versioning_strategy(conf.versioning_strategy);
    temporalio_sdk_core::WorkerConfig::builder()
        .namespace(conf.namespace)
        .task_queue(conf.task_queue)
        .versioning_strategy(converted_versioning_strategy)
        .maybe_client_identity_override(conf.identity_override)
        .max_cached_workflows(conf.max_cached_workflows)
        .workflow_task_poller_behavior(conf.workflow_task_poller_behavior.into())
        .tuner(Arc::new(converted_tuner))
        .nonsticky_to_sticky_poll_ratio(conf.nonsticky_to_sticky_poll_ratio)
        .activity_task_poller_behavior(conf.activity_task_poller_behavior.into())
        .task_types(conf.task_types.into())
        .sticky_queue_schedule_to_start_timeout(Duration::from_millis(
            conf.sticky_queue_schedule_to_start_timeout_millis,
        ))
        .max_heartbeat_throttle_interval(Duration::from_millis(
            conf.max_heartbeat_throttle_interval_millis,
        ))
        .default_heartbeat_throttle_interval(Duration::from_millis(
            conf.default_heartbeat_throttle_interval_millis,
        ))
        .maybe_max_worker_activities_per_second(conf.max_activities_per_second)
        .maybe_max_task_queue_activities_per_second(conf.max_task_queue_activities_per_second)
        .max_eager_activity_reservations_per_workflow_task(
            conf.max_eager_activity_reservations_per_workflow_task,
        )
        // Even though grace period is optional, if it is not set then the
        // auto-cancel-activity behavior of shutdown will not occur, so we
        // always set it even if 0.
        .graceful_shutdown_period(Duration::from_millis(conf.graceful_shutdown_period_millis))
        .workflow_failure_errors(if conf.nondeterminism_as_workflow_fail {
            HashSet::from([WorkflowErrorType::Nondeterminism])
        } else {
            HashSet::new()
        })
        .workflow_types_to_failure_errors(
            conf.nondeterminism_as_workflow_fail_for_types
                .iter()
                .map(|s| {
                    (
                        s.to_owned(),
                        HashSet::from([WorkflowErrorType::Nondeterminism]),
                    )
                })
                .collect(),
        )
        .nexus_task_poller_behavior(conf.nexus_task_poller_behavior.into())
        .plugins(
            conf.plugins
                .into_iter()
                .map(|name| PluginInfo {
                    name,
                    version: String::new(),
                })
                .collect(),
        )
        .storage_drivers(
            conf.storage_drivers
                .into_iter()
                .map(|r#type| StorageDriverInfo { r#type })
                .collect::<HashSet<_>>(),
        )
        .disable_payload_error_limit(conf.disable_payload_error_limit)
        .build()
        .map_err(|err| PyValueError::new_err(format!("Invalid worker config: {err}")))
}

fn convert_tuner_holder(
    holder: TunerHolder,
    task_locals: Arc<OnceLock<pyo3_async_runtimes::TaskLocals>>,
) -> PyResult<temporalio_sdk_core::TunerHolder> {
    // Verify all resource-based options are the same if any are set
    let maybe_wf_resource_opts =
        if let SlotSupplier::ResourceBased(ref ss) = holder.workflow_slot_supplier {
            Some(&ss.tuner_config)
        } else {
            None
        };
    let maybe_act_resource_opts =
        if let SlotSupplier::ResourceBased(ref ss) = holder.activity_slot_supplier {
            Some(&ss.tuner_config)
        } else {
            None
        };
    let maybe_local_act_resource_opts =
        if let SlotSupplier::ResourceBased(ref ss) = holder.local_activity_slot_supplier {
            Some(&ss.tuner_config)
        } else {
            None
        };
    let maybe_nexus_resource_opts =
        if let SlotSupplier::ResourceBased(ref ss) = holder.nexus_slot_supplier {
            Some(&ss.tuner_config)
        } else {
            None
        };
    let all_resource_opts = [
        maybe_wf_resource_opts,
        maybe_act_resource_opts,
        maybe_local_act_resource_opts,
        maybe_nexus_resource_opts,
    ];
    let mut set_resource_opts = all_resource_opts.iter().flatten();
    let first = set_resource_opts.next();
    let all_are_same = if let Some(first) = first {
        set_resource_opts.all(|elem| elem == first)
    } else {
        true
    };
    if !all_are_same {
        return Err(PyValueError::new_err(
            "All resource-based slot suppliers must have the same ResourceBasedTunerOptions",
        ));
    }

    Ok(temporalio_sdk_core::TunerHolderOptions::builder()
        .maybe_resource_based_config(first.map(|first| {
            temporalio_sdk_core::ResourceBasedTunerConfig::Options(
                temporalio_sdk_core::ResourceBasedSlotsOptions::builder()
                    .target_mem_usage(first.target_memory_usage)
                    .target_cpu_usage(first.target_cpu_usage)
                    .build(),
            )
        }))
        .workflow_slot_options(convert_slot_supplier(
            holder.workflow_slot_supplier,
            task_locals.clone(),
        )?)
        .activity_slot_options(convert_slot_supplier(
            holder.activity_slot_supplier,
            task_locals.clone(),
        )?)
        .local_activity_slot_options(convert_slot_supplier(
            holder.local_activity_slot_supplier,
            task_locals.clone(),
        )?)
        .nexus_slot_options(convert_slot_supplier(
            holder.nexus_slot_supplier,
            task_locals,
        )?)
        .build()
        .map_err(|e| PyValueError::new_err(format!("Invalid tuner holder options: {e}")))?
        .build_tuner_holder()
        .context("Failed building tuner holder")?)
}

fn convert_slot_supplier<SK: SlotKind + Send + Sync + 'static>(
    supplier: SlotSupplier,
    task_locals: Arc<OnceLock<pyo3_async_runtimes::TaskLocals>>,
) -> PyResult<temporalio_sdk_core::SlotSupplierOptions<SK>> {
    Ok(match supplier {
        SlotSupplier::FixedSize(fs) => temporalio_sdk_core::SlotSupplierOptions::FixedSize {
            slots: fs.num_slots,
        },
        SlotSupplier::ResourceBased(ss) => temporalio_sdk_core::SlotSupplierOptions::ResourceBased(
            temporalio_sdk_core::ResourceSlotOptions::new(
                ss.minimum_slots,
                ss.maximum_slots,
                Duration::from_millis(ss.ramp_throttle_ms),
            ),
        ),
        SlotSupplier::Custom(cs) => {
            temporalio_sdk_core::SlotSupplierOptions::Custom(Arc::new(CustomSlotSupplierOfType::<
                SK,
            > {
                inner: cs.inner,
                event_loop_task_locals: task_locals,
                _phantom: PhantomData,
            }))
        }
    })
}

fn convert_versioning_strategy(
    strategy: WorkerVersioningStrategy,
) -> temporalio_sdk_core::WorkerVersioningStrategy {
    match strategy {
        WorkerVersioningStrategy::None(vn) => temporalio_sdk_core::WorkerVersioningStrategy::None {
            build_id: vn.build_id_no_versioning,
        },
        WorkerVersioningStrategy::DeploymentBased(options) => {
            temporalio_sdk_core::WorkerVersioningStrategy::WorkerDeploymentBased(
                temporalio_common::worker::WorkerDeploymentOptions::new(
                    temporalio_common::worker::WorkerDeploymentVersion {
                        deployment_name: options.version.deployment_name,
                        build_id: options.version.build_id,
                    },
                )
                .use_worker_versioning(options.use_worker_versioning)
                .maybe_default_versioning_behavior(if options.use_worker_versioning {
                    Some(
                        temporalio_common::protos::temporal::api::enums::v1::VersioningBehavior::try_from(
                            options.default_versioning_behavior,
                        )
                        .unwrap_or_default()
                        .into(),
                    )
                } else {
                    None
                })
                .build(),
            )
        }
        WorkerVersioningStrategy::LegacyBuildIdBased(lb) => {
            temporalio_sdk_core::WorkerVersioningStrategy::LegacyBuildIdBased {
                build_id: lb.build_id_with_versioning,
            }
        }
    }
}

/// For feeding histories into core during replay
#[pyclass]
pub struct HistoryPusher {
    tx: Option<Sender<HistoryForReplay>>,
    runtime: runtime::Runtime,
}

impl HistoryPusher {
    fn new(runtime: runtime::Runtime) -> (Self, ReceiverStream<HistoryForReplay>) {
        let (tx, rx) = channel(1);
        (
            Self {
                tx: Some(tx),
                runtime,
            },
            ReceiverStream::new(rx),
        )
    }
}

#[pymethods]
impl HistoryPusher {
    fn push_history<'p>(
        &self,
        py: Python<'p>,
        workflow_id: &str,
        history_proto: &Bound<'_, PyBytes>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let history = History::decode(history_proto.as_bytes())
            .map_err(|err| PyValueError::new_err(format!("Invalid proto: {err}")))?;
        let wfid = workflow_id.to_string();
        let tx = if let Some(tx) = self.tx.as_ref() {
            tx.clone()
        } else {
            return Err(PyRuntimeError::new_err(
                "Replay worker is no longer accepting new histories",
            ));
        };
        // We accept this doesn't have logging/tracing
        self.runtime.future_into_py(py, async move {
            tx.send(HistoryForReplay::new(history, wfid))
                .await
                .map_err(|_| {
                    PyRuntimeError::new_err(
                        "Channel for history replay was dropped, this is an SDK bug.",
                    )
                })?;
            Ok(())
        })
    }

    fn close(&mut self) {
        self.tx.take();
    }
}
