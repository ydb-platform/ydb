use crate::{abstractions::dbg_panic, telemetry::TelemetryInstance};

use std::{
    fmt::{Debug, Display},
    iter::Iterator,
    sync::Arc,
    time::Duration,
};
use temporal_sdk_core_api::telemetry::metrics::{
    BufferAttributes, BufferInstrumentRef, CoreMeter, Counter, CounterBase, Gauge, GaugeBase,
    GaugeF64, GaugeF64Base, Histogram, HistogramBase, HistogramDuration, HistogramDurationBase,
    HistogramF64, HistogramF64Base, LazyBufferInstrument, MetricAttributable, MetricAttributes,
    MetricCallBufferer, MetricEvent, MetricKeyValue, MetricKind, MetricParameters, MetricUpdateVal,
    NewAttributes, NoOpCoreMeter,
};
use temporal_sdk_core_protos::temporal::api::{
    enums::v1::WorkflowTaskFailedCause, failure::v1::Failure,
};

/// Used to track context associated with metrics, and record/update them
#[derive(Clone)]
pub(crate) struct MetricsContext {
    meter: Arc<dyn CoreMeter>,
    kvs: MetricAttributes,
    instruments: Arc<Instruments>,
}

#[derive(Clone)]
struct Instruments {
    wf_completed_counter: Counter,
    wf_canceled_counter: Counter,
    wf_failed_counter: Counter,
    wf_cont_counter: Counter,
    wf_e2e_latency: HistogramDuration,
    wf_task_queue_poll_empty_counter: Counter,
    wf_task_queue_poll_succeed_counter: Counter,
    wf_task_execution_failure_counter: Counter,
    wf_task_sched_to_start_latency: HistogramDuration,
    wf_task_replay_latency: HistogramDuration,
    wf_task_execution_latency: HistogramDuration,
    act_poll_no_task: Counter,
    act_task_received_counter: Counter,
    act_execution_failed: Counter,
    act_sched_to_start_latency: HistogramDuration,
    act_exec_latency: HistogramDuration,
    act_exec_succeeded_latency: HistogramDuration,
    la_execution_cancelled: Counter,
    la_execution_failed: Counter,
    la_exec_latency: HistogramDuration,
    la_exec_succeeded_latency: HistogramDuration,
    la_total: Counter,
    nexus_poll_no_task: Counter,
    nexus_task_schedule_to_start_latency: HistogramDuration,
    nexus_task_e2e_latency: HistogramDuration,
    nexus_task_execution_latency: HistogramDuration,
    nexus_task_execution_failed: Counter,
    worker_registered: Counter,
    num_pollers: Gauge,
    task_slots_available: Gauge,
    task_slots_used: Gauge,
    sticky_cache_hit: Counter,
    sticky_cache_miss: Counter,
    sticky_cache_size: Gauge,
    sticky_cache_forced_evictions: Counter,
}

impl MetricsContext {
    pub(crate) fn no_op() -> Self {
        let meter = Arc::new(NoOpCoreMeter);
        let kvs = meter.new_attributes(Default::default());
        let instruments = Arc::new(Instruments::new(meter.as_ref()));
        Self {
            kvs,
            instruments,
            meter,
        }
    }

    pub(crate) fn top_level(namespace: String, tq: String, telemetry: &TelemetryInstance) -> Self {
        if let Some(mut meter) = telemetry.get_temporal_metric_meter() {
            meter
                .default_attribs
                .attributes
                .push(MetricKeyValue::new(KEY_NAMESPACE, namespace));
            meter.default_attribs.attributes.push(task_queue(tq));
            let kvs = meter.inner.new_attributes(meter.default_attribs);
            let mut instruments = Instruments::new(meter.inner.as_ref());
            instruments.update_attributes(&kvs);
            Self {
                kvs,
                instruments: Arc::new(instruments),
                meter: meter.inner,
            }
        } else {
            Self::no_op()
        }
    }

    /// Extend an existing metrics context with new attributes
    pub(crate) fn with_new_attrs(
        &self,
        new_attrs: impl IntoIterator<Item = MetricKeyValue>,
    ) -> Self {
        let kvs = self
            .meter
            .extend_attributes(self.kvs.clone(), new_attrs.into());
        let mut instruments = (*self.instruments).clone();
        instruments.update_attributes(&kvs);
        Self {
            instruments: Arc::new(instruments),
            kvs,
            meter: self.meter.clone(),
        }
    }

    /// A workflow task queue poll succeeded
    pub(crate) fn wf_tq_poll_ok(&self) {
        self.instruments.wf_task_queue_poll_succeed_counter.adds(1);
    }

    /// A workflow task queue poll timed out / had empty response
    pub(crate) fn wf_tq_poll_empty(&self) {
        self.instruments.wf_task_queue_poll_empty_counter.adds(1);
    }

    /// A workflow task execution failed
    pub(crate) fn wf_task_failed(&self) {
        self.instruments.wf_task_execution_failure_counter.adds(1);
    }

    /// A workflow completed successfully
    pub(crate) fn wf_completed(&self) {
        self.instruments.wf_completed_counter.adds(1);
    }

    /// A workflow ended cancelled
    pub(crate) fn wf_canceled(&self) {
        self.instruments.wf_canceled_counter.adds(1);
    }

    /// A workflow ended failed
    pub(crate) fn wf_failed(&self) {
        self.instruments.wf_failed_counter.adds(1);
    }

    /// A workflow continued as new
    pub(crate) fn wf_continued_as_new(&self) {
        self.instruments.wf_cont_counter.adds(1);
    }

    /// Record workflow total execution time in milliseconds
    pub(crate) fn wf_e2e_latency(&self, dur: Duration) {
        self.instruments.wf_e2e_latency.records(dur);
    }

    /// Record workflow task schedule to start time in millis
    pub(crate) fn wf_task_sched_to_start_latency(&self, dur: Duration) {
        self.instruments.wf_task_sched_to_start_latency.records(dur);
    }

    /// Record workflow task execution time in milliseconds
    pub(crate) fn wf_task_latency(&self, dur: Duration) {
        self.instruments.wf_task_execution_latency.records(dur);
    }

    /// Record time it takes to catch up on replaying a WFT
    pub(crate) fn wf_task_replay_latency(&self, dur: Duration) {
        self.instruments.wf_task_replay_latency.records(dur);
    }

    /// An activity long poll timed out
    pub(crate) fn act_poll_timeout(&self) {
        self.instruments.act_poll_no_task.adds(1);
    }

    /// A count of activity tasks received
    pub(crate) fn act_task_received(&self) {
        self.instruments.act_task_received_counter.adds(1);
    }

    /// An activity execution failed
    pub(crate) fn act_execution_failed(&self) {
        self.instruments.act_execution_failed.adds(1);
    }

    /// Record end-to-end (sched-to-complete) time for successful activity executions
    pub(crate) fn act_execution_succeeded(&self, dur: Duration) {
        self.instruments.act_exec_succeeded_latency.records(dur);
    }

    /// Record activity task schedule to start time in millis
    pub(crate) fn act_sched_to_start_latency(&self, dur: Duration) {
        self.instruments.act_sched_to_start_latency.records(dur);
    }

    /// Record time it took to complete activity execution, from the time core generated the
    /// activity task, to the time lang responded with a completion (failure or success).
    pub(crate) fn act_execution_latency(&self, dur: Duration) {
        self.instruments.act_exec_latency.records(dur);
    }

    pub(crate) fn la_execution_cancelled(&self) {
        self.instruments.la_execution_cancelled.adds(1);
    }

    pub(crate) fn la_execution_failed(&self) {
        self.instruments.la_execution_failed.adds(1);
    }

    pub(crate) fn la_exec_latency(&self, dur: Duration) {
        self.instruments.la_exec_latency.records(dur);
    }

    pub(crate) fn la_exec_succeeded_latency(&self, dur: Duration) {
        self.instruments.la_exec_succeeded_latency.records(dur);
    }

    pub(crate) fn la_executed(&self) {
        self.instruments.la_total.adds(1);
    }

    /// A nexus long poll timed out
    pub(crate) fn nexus_poll_timeout(&self) {
        self.instruments.nexus_poll_no_task.adds(1);
    }

    /// Record nexus task schedule to start time
    pub(crate) fn nexus_task_sched_to_start_latency(&self, dur: Duration) {
        self.instruments
            .nexus_task_schedule_to_start_latency
            .records(dur);
    }

    /// Record nexus task end-to-end time
    pub(crate) fn nexus_task_e2e_latency(&self, dur: Duration) {
        self.instruments.nexus_task_e2e_latency.records(dur);
    }

    /// Record nexus task execution time
    pub(crate) fn nexus_task_execution_latency(&self, dur: Duration) {
        self.instruments.nexus_task_execution_latency.records(dur);
    }

    /// Record a nexus task execution failure
    pub(crate) fn nexus_task_execution_failed(&self) {
        self.instruments.nexus_task_execution_failed.adds(1);
    }

    /// A worker was registered
    pub(crate) fn worker_registered(&self) {
        self.instruments.worker_registered.adds(1);
    }

    /// Record current number of available task slots. Context should have worker type set.
    pub(crate) fn available_task_slots(&self, num: usize) {
        self.instruments.task_slots_available.records(num as u64)
    }

    /// Record current number of used task slots. Context should have worker type set.
    pub(crate) fn task_slots_used(&self, num: u64) {
        self.instruments.task_slots_used.records(num)
    }

    /// Record current number of pollers. Context should include poller type / task queue tag.
    pub(crate) fn record_num_pollers(&self, num: usize) {
        self.instruments.num_pollers.records(num as u64);
    }

    /// A workflow task found a cached workflow to run against
    pub(crate) fn sticky_cache_hit(&self) {
        self.instruments.sticky_cache_hit.adds(1);
    }

    /// A workflow task did not find a cached workflow
    pub(crate) fn sticky_cache_miss(&self) {
        self.instruments.sticky_cache_miss.adds(1);
    }

    /// Record current cache size (in number of wfs, not bytes)
    pub(crate) fn cache_size(&self, size: u64) {
        self.instruments.sticky_cache_size.records(size);
    }

    /// Count a workflow being evicted from the cache
    pub(crate) fn forced_cache_eviction(&self) {
        self.instruments.sticky_cache_forced_evictions.adds(1);
    }
}

impl Instruments {
    fn new(meter: &dyn CoreMeter) -> Self {
        Self {
            wf_completed_counter: meter.counter(MetricParameters {
                name: "workflow_completed".into(),
                description: "Count of successfully completed workflows".into(),
                unit: "".into(),
            }),
            wf_canceled_counter: meter.counter(MetricParameters {
                name: "workflow_canceled".into(),
                description: "Count of canceled workflows".into(),
                unit: "".into(),
            }),
            wf_failed_counter: meter.counter(MetricParameters {
                name: "workflow_failed".into(),
                description: "Count of failed workflows".into(),
                unit: "".into(),
            }),
            wf_cont_counter: meter.counter(MetricParameters {
                name: "workflow_continue_as_new".into(),
                description: "Count of continued-as-new workflows".into(),
                unit: "".into(),
            }),
            wf_e2e_latency: meter.histogram_duration(MetricParameters {
                name: WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME.into(),
                unit: "duration".into(),
                description: "Histogram of total workflow execution latencies".into(),
            }),
            wf_task_queue_poll_empty_counter: meter.counter(MetricParameters {
                name: "workflow_task_queue_poll_empty".into(),
                description: "Count of workflow task queue poll timeouts (no new task)".into(),
                unit: "".into(),
            }),
            wf_task_queue_poll_succeed_counter: meter.counter(MetricParameters {
                name: "workflow_task_queue_poll_succeed".into(),
                description: "Count of workflow task queue poll successes".into(),
                unit: "".into(),
            }),
            wf_task_execution_failure_counter: meter.counter(MetricParameters {
                name: "workflow_task_execution_failed".into(),
                description: "Count of workflow task execution failures".into(),
                unit: "".into(),
            }),
            wf_task_sched_to_start_latency: meter.histogram_duration(MetricParameters {
                name: WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME.into(),
                unit: "duration".into(),
                description: "Histogram of workflow task schedule-to-start latencies".into(),
            }),
            wf_task_replay_latency: meter.histogram_duration(MetricParameters {
                name: WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME.into(),
                unit: "duration".into(),
                description: "Histogram of workflow task replay latencies".into(),
            }),
            wf_task_execution_latency: meter.histogram_duration(MetricParameters {
                name: WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME.into(),
                unit: "duration".into(),
                description: "Histogram of workflow task execution (not replay) latencies".into(),
            }),
            act_poll_no_task: meter.counter(MetricParameters {
                name: "activity_poll_no_task".into(),
                description: "Count of activity task queue poll timeouts (no new task)".into(),
                unit: "".into(),
            }),
            act_task_received_counter: meter.counter(MetricParameters {
                name: "activity_task_received".into(),
                description: "Count of activity task queue poll successes".into(),
                unit: "".into(),
            }),
            act_execution_failed: meter.counter(MetricParameters {
                name: "activity_execution_failed".into(),
                description: "Count of activity task execution failures".into(),
                unit: "".into(),
            }),
            act_sched_to_start_latency: meter.histogram_duration(MetricParameters {
                name: ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME.into(),
                unit: "duration".into(),
                description: "Histogram of activity schedule-to-start latencies".into(),
            }),
            act_exec_latency: meter.histogram_duration(MetricParameters {
                name: ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME.into(),
                unit: "duration".into(),
                description: "Histogram of activity execution latencies".into(),
            }),
            act_exec_succeeded_latency: meter.histogram_duration(MetricParameters {
                name: "activity_succeed_endtoend_latency".into(),
                unit: "duration".into(),
                description: "Histogram of activity execution latencies for successful activities"
                    .into(),
            }),
            la_execution_cancelled: meter.counter(MetricParameters {
                name: "local_activity_execution_cancelled".into(),
                description: "Count of local activity executions that were cancelled".into(),
                unit: "".into(),
            }),
            la_execution_failed: meter.counter(MetricParameters {
                name: "local_activity_execution_failed".into(),
                description: "Count of local activity executions that failed".into(),
                unit: "".into(),
            }),
            la_exec_latency: meter.histogram_duration(MetricParameters {
                name: "local_activity_execution_latency".into(),
                unit: "duration".into(),
                description: "Histogram of local activity execution latencies".into(),
            }),
            la_exec_succeeded_latency: meter.histogram_duration(MetricParameters {
                name: "local_activity_succeed_endtoend_latency".into(),
                unit: "duration".into(),
                description:
                    "Histogram of local activity execution latencies for successful local activities"
                        .into(),
            }),
            la_total: meter.counter(MetricParameters {
                name: "local_activity_total".into(),
                description: "Count of local activities executed".into(),
                unit: "".into(),
            }),
            nexus_poll_no_task: meter.counter(MetricParameters {
                name: "nexus_poll_no_task".into(),
                description: "Count of nexus task queue poll timeouts (no new task)".into(),
                unit: "".into(),
            }),
            nexus_task_schedule_to_start_latency: meter.histogram_duration(MetricParameters {
                name: "nexus_task_schedule_to_start_latency".into(),
                unit: "duration".into(),
                description: "Histogram of nexus task schedule-to-start latencies".into(),
            }),
            nexus_task_e2e_latency: meter.histogram_duration(MetricParameters {
                name: "nexus_task_endtoend_latency".into(),
                unit: "duration".into(),
                description: "Histogram of nexus task end-to-end latencies".into(),
            }),
            nexus_task_execution_latency: meter.histogram_duration(MetricParameters {
                name: "nexus_task_execution_latency".into(),
                unit: "duration".into(),
                description: "Histogram of nexus task execution latencies".into(),
            }),
            nexus_task_execution_failed: meter.counter(MetricParameters {
                name: "nexus_task_execution_failed".into(),
                description: "Count of nexus task execution failures".into(),
                unit: "".into(),
            }),
            // name kept as worker start for compat with old sdk / what users expect
            worker_registered: meter.counter(MetricParameters {
                name: "worker_start".into(),
                description: "Count of the number of initialized workers".into(),
                unit: "".into(),
            }),
            num_pollers: meter.gauge(MetricParameters {
                name: NUM_POLLERS_NAME.into(),
                description: "Current number of active pollers per queue type".into(),
                unit: "".into(),
            }),
            task_slots_available: meter.gauge(MetricParameters {
                name: TASK_SLOTS_AVAILABLE_NAME.into(),
                description: "Current number of available slots per task type".into(),
                unit: "".into(),
            }),
            task_slots_used: meter.gauge(MetricParameters {
                name: TASK_SLOTS_USED_NAME.into(),
                description: "Current number of used slots per task type".into(),
                unit: "".into(),
            }),
            sticky_cache_hit: meter.counter(MetricParameters {
                name: "sticky_cache_hit".into(),
                description: "Count of times the workflow cache was used for a new workflow task"
                    .into(),
                unit: "".into(),
            }),
            sticky_cache_miss: meter.counter(MetricParameters {
                name: "sticky_cache_miss".into(),
                description:
                    "Count of times the workflow cache was missing a workflow for a sticky task"
                        .into(),
                unit: "".into(),
            }),
            sticky_cache_size: meter.gauge(MetricParameters {
                name: STICKY_CACHE_SIZE_NAME.into(),
                description: "Current number of cached workflows".into(),
                unit: "".into(),
            }),
            sticky_cache_forced_evictions: meter.counter(MetricParameters {
                name: "sticky_cache_total_forced_eviction".into(),
                description: "Count of evictions of cached workflows".into(),
                unit: "".into(),
            }),
        }
    }

    fn update_attributes(&mut self, new_attributes: &MetricAttributes) {
        self.wf_completed_counter
            .update_attributes(new_attributes.clone());
        self.wf_canceled_counter
            .update_attributes(new_attributes.clone());
        self.wf_failed_counter
            .update_attributes(new_attributes.clone());
        self.wf_cont_counter
            .update_attributes(new_attributes.clone());
        self.wf_e2e_latency
            .update_attributes(new_attributes.clone());
        self.wf_task_queue_poll_empty_counter
            .update_attributes(new_attributes.clone());
        self.wf_task_queue_poll_succeed_counter
            .update_attributes(new_attributes.clone());
        self.wf_task_execution_failure_counter
            .update_attributes(new_attributes.clone());
        self.wf_task_sched_to_start_latency
            .update_attributes(new_attributes.clone());
        self.wf_task_replay_latency
            .update_attributes(new_attributes.clone());
        self.wf_task_execution_latency
            .update_attributes(new_attributes.clone());
        self.act_poll_no_task
            .update_attributes(new_attributes.clone());
        self.act_task_received_counter
            .update_attributes(new_attributes.clone());
        self.act_execution_failed
            .update_attributes(new_attributes.clone());
        self.act_sched_to_start_latency
            .update_attributes(new_attributes.clone());
        self.act_exec_latency
            .update_attributes(new_attributes.clone());
        self.act_exec_succeeded_latency
            .update_attributes(new_attributes.clone());
        self.la_execution_cancelled
            .update_attributes(new_attributes.clone());
        self.la_execution_failed
            .update_attributes(new_attributes.clone());
        self.la_exec_latency
            .update_attributes(new_attributes.clone());
        self.la_exec_succeeded_latency
            .update_attributes(new_attributes.clone());
        self.la_total.update_attributes(new_attributes.clone());
        self.nexus_poll_no_task
            .update_attributes(new_attributes.clone());
        self.nexus_task_schedule_to_start_latency
            .update_attributes(new_attributes.clone());
        self.nexus_task_e2e_latency
            .update_attributes(new_attributes.clone());
        self.nexus_task_execution_latency
            .update_attributes(new_attributes.clone());
        self.nexus_task_execution_failed
            .update_attributes(new_attributes.clone());
        self.worker_registered
            .update_attributes(new_attributes.clone());
        self.num_pollers.update_attributes(new_attributes.clone());
        self.task_slots_available
            .update_attributes(new_attributes.clone());
        self.task_slots_used
            .update_attributes(new_attributes.clone());
        self.sticky_cache_hit
            .update_attributes(new_attributes.clone());
        self.sticky_cache_miss
            .update_attributes(new_attributes.clone());
        self.sticky_cache_size
            .update_attributes(new_attributes.clone());
        self.sticky_cache_forced_evictions
            .update_attributes(new_attributes.clone());
    }
}

const KEY_NAMESPACE: &str = "namespace";
const KEY_WF_TYPE: &str = "workflow_type";
const KEY_TASK_QUEUE: &str = "task_queue";
const KEY_ACT_TYPE: &str = "activity_type";
const KEY_POLLER_TYPE: &str = "poller_type";
const KEY_WORKER_TYPE: &str = "worker_type";
const KEY_EAGER: &str = "eager";
const KEY_TASK_FAILURE_TYPE: &str = "failure_reason";

pub(crate) fn workflow_poller() -> MetricKeyValue {
    MetricKeyValue::new(KEY_POLLER_TYPE, "workflow_task")
}
pub(crate) fn workflow_sticky_poller() -> MetricKeyValue {
    MetricKeyValue::new(KEY_POLLER_TYPE, "sticky_workflow_task")
}
pub(crate) fn activity_poller() -> MetricKeyValue {
    MetricKeyValue::new(KEY_POLLER_TYPE, "activity_task")
}
pub(crate) fn nexus_poller() -> MetricKeyValue {
    MetricKeyValue::new(KEY_POLLER_TYPE, "nexus_task")
}
pub(crate) fn task_queue(tq: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_TASK_QUEUE, tq)
}
pub(crate) fn activity_type(ty: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_ACT_TYPE, ty)
}
pub(crate) fn workflow_type(ty: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_WF_TYPE, ty)
}
pub(crate) fn workflow_worker_type() -> MetricKeyValue {
    MetricKeyValue::new(KEY_WORKER_TYPE, "WorkflowWorker")
}
pub(crate) fn activity_worker_type() -> MetricKeyValue {
    MetricKeyValue::new(KEY_WORKER_TYPE, "ActivityWorker")
}
pub(crate) fn local_activity_worker_type() -> MetricKeyValue {
    MetricKeyValue::new(KEY_WORKER_TYPE, "LocalActivityWorker")
}
pub(crate) fn nexus_worker_type() -> MetricKeyValue {
    MetricKeyValue::new(KEY_WORKER_TYPE, "NexusWorker")
}
pub(crate) fn eager(is_eager: bool) -> MetricKeyValue {
    MetricKeyValue::new(KEY_EAGER, is_eager)
}
pub(crate) enum FailureReason {
    Nondeterminism,
    Workflow,
    Timeout,
    NexusOperation(String),
    NexusHandlerError(String),
}
impl Display for FailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            FailureReason::Nondeterminism => "NonDeterminismError".to_owned(),
            FailureReason::Workflow => "WorkflowError".to_owned(),
            FailureReason::Timeout => "timeout".to_owned(),
            FailureReason::NexusOperation(op) => format!("operation_{op}"),
            FailureReason::NexusHandlerError(op) => format!("handler_error_{op}"),
        };
        write!(f, "{str}")
    }
}
impl From<WorkflowTaskFailedCause> for FailureReason {
    fn from(v: WorkflowTaskFailedCause) -> Self {
        match v {
            WorkflowTaskFailedCause::NonDeterministicError => FailureReason::Nondeterminism,
            _ => FailureReason::Workflow,
        }
    }
}
pub(crate) fn failure_reason(reason: FailureReason) -> MetricKeyValue {
    MetricKeyValue::new(KEY_TASK_FAILURE_TYPE, reason.to_string())
}

/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME: &str = "workflow_endtoend_latency";
/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME: &str =
    "workflow_task_schedule_to_start_latency";
/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME: &str = "workflow_task_replay_latency";
/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME: &str = "workflow_task_execution_latency";
/// The string name (which may be prefixed) for this metric
pub const ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME: &str =
    "activity_schedule_to_start_latency";
/// The string name (which may be prefixed) for this metric
pub const ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME: &str = "activity_execution_latency";
pub(super) const NUM_POLLERS_NAME: &str = "num_pollers";
pub(super) const TASK_SLOTS_AVAILABLE_NAME: &str = "worker_task_slots_available";
pub(super) const TASK_SLOTS_USED_NAME: &str = "worker_task_slots_used";
pub(super) const STICKY_CACHE_SIZE_NAME: &str = "sticky_cache_size";

/// Track a failure metric if the failure is not a benign application failure.
pub(crate) fn should_record_failure_metric(failure: &Option<Failure>) -> bool {
    !failure
        .as_ref()
        .is_some_and(|f| f.is_benign_application_failure())
}

/// Helps define buckets once in terms of millis, but also generates a seconds version
macro_rules! define_latency_buckets {
    ($(($metric_name:pat, $name:ident, $sec_name:ident, [$($bucket:expr),*])),*) => {
        $(
            pub(super) static $name: &[f64] = &[$($bucket,)*];
            pub(super) static $sec_name: &[f64] = &[$( $bucket / 1000.0, )*];
        )*

        /// Returns the default histogram buckets that lang should use for a given metric name if
        /// they have not been overridden by the user. If `use_seconds` is true, returns buckets
        /// in terms of seconds rather than milliseconds.
        ///
        /// The name must *not* be prefixed with `temporal_`
        pub fn default_buckets_for(histo_name: &str, use_seconds: bool) -> &'static [f64] {
            match histo_name {
                $(
                    $metric_name => { if use_seconds { &$sec_name } else { &$name } },
                )*
            }
        }
    };
}

define_latency_buckets!(
    (
        WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME,
        WF_LATENCY_MS_BUCKETS,
        WF_LATENCY_S_BUCKETS,
        [
            100.,
            500.,
            1000.,
            1500.,
            2000.,
            5000.,
            10_000.,
            30_000.,
            60_000.,
            120_000.,
            300_000.,
            600_000.,
            1_800_000.,  // 30 min
            3_600_000.,  //  1 hr
            30_600_000., // 10 hrs
            8.64e7       // 24 hrs
        ]
    ),
    (
        WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME
            | WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME,
        WF_TASK_MS_BUCKETS,
        WF_TASK_S_BUCKETS,
        [1., 10., 20., 50., 100., 200., 500., 1000.]
    ),
    (
        ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME,
        ACT_EXE_MS_BUCKETS,
        ACT_EXE_S_BUCKETS,
        [50., 100., 500., 1000., 5000., 10_000., 60_000.]
    ),
    (
        WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME
            | ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
        TASK_SCHED_TO_START_MS_BUCKETS,
        TASK_SCHED_TO_START_S_BUCKETS,
        [100., 500., 1000., 5000., 10_000., 100_000., 1_000_000.]
    ),
    (
        _,
        DEFAULT_MS_BUCKETS,
        DEFAULT_S_BUCKETS,
        [50., 100., 500., 1000., 2500., 10_000.]
    )
);

/// Buffers [MetricEvent]s for periodic consumption by lang
#[derive(Debug)]
pub struct MetricsCallBuffer<I>
where
    I: BufferInstrumentRef,
{
    calls_rx: crossbeam_channel::Receiver<MetricEvent<I>>,
    calls_tx: LogErrOnFullSender<MetricEvent<I>>,
}
#[derive(Clone, Debug)]
struct LogErrOnFullSender<I>(crossbeam_channel::Sender<I>);
impl<I> LogErrOnFullSender<I> {
    fn send(&self, v: I) {
        if let Err(crossbeam_channel::TrySendError::Full(_)) = self.0.try_send(v) {
            error!(
                "Core's metrics buffer is full! Dropping call to record metrics. \
                 Make sure you drain the metric buffer often!"
            );
        }
    }
}

impl<I> MetricsCallBuffer<I>
where
    I: Clone + BufferInstrumentRef,
{
    /// Create a new buffer with the given capacity
    pub fn new(buffer_size: usize) -> Self {
        let (calls_tx, calls_rx) = crossbeam_channel::bounded(buffer_size);
        MetricsCallBuffer {
            calls_rx,
            calls_tx: LogErrOnFullSender(calls_tx),
        }
    }
    fn new_instrument(&self, params: MetricParameters, kind: MetricKind) -> BufferInstrument<I> {
        let hole = LazyBufferInstrument::hole();
        self.calls_tx.send(MetricEvent::Create {
            params,
            kind,
            populate_into: hole.clone(),
        });
        BufferInstrument {
            instrument_ref: hole,
            tx: self.calls_tx.clone(),
        }
    }
}

impl<I> CoreMeter for MetricsCallBuffer<I>
where
    I: BufferInstrumentRef + Debug + Send + Sync + Clone + 'static,
{
    fn new_attributes(&self, opts: NewAttributes) -> MetricAttributes {
        let ba = BufferAttributes::hole();
        self.calls_tx.send(MetricEvent::CreateAttributes {
            populate_into: ba.clone(),
            append_from: None,
            attributes: opts.attributes,
        });
        MetricAttributes::Buffer(ba)
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        if let MetricAttributes::Buffer(ol) = existing {
            let ba = BufferAttributes::hole();
            self.calls_tx.send(MetricEvent::CreateAttributes {
                populate_into: ba.clone(),
                append_from: Some(ol),
                attributes: attribs.attributes,
            });
            MetricAttributes::Buffer(ba)
        } else {
            dbg_panic!("Must use buffer attributes with a buffer metric implementation");
            existing
        }
    }

    fn counter(&self, params: MetricParameters) -> Counter {
        Counter::new(Arc::new(self.new_instrument(params, MetricKind::Counter)))
    }

    fn histogram(&self, params: MetricParameters) -> Histogram {
        Histogram::new(Arc::new(self.new_instrument(params, MetricKind::Histogram)))
    }

    fn histogram_f64(&self, params: MetricParameters) -> HistogramF64 {
        HistogramF64::new(Arc::new(self.new_instrument(params, MetricKind::Histogram)))
    }

    fn histogram_duration(&self, params: MetricParameters) -> HistogramDuration {
        HistogramDuration::new(Arc::new(
            self.new_instrument(params, MetricKind::HistogramDuration),
        ))
    }

    fn gauge(&self, params: MetricParameters) -> Gauge {
        Gauge::new(Arc::new(self.new_instrument(params, MetricKind::Gauge)))
    }

    fn gauge_f64(&self, params: MetricParameters) -> GaugeF64 {
        GaugeF64::new(Arc::new(self.new_instrument(params, MetricKind::Gauge)))
    }
}
impl<I> MetricCallBufferer<I> for MetricsCallBuffer<I>
where
    I: Send + Sync + BufferInstrumentRef,
{
    fn retrieve(&self) -> Vec<MetricEvent<I>> {
        self.calls_rx.try_iter().collect()
    }
}

#[derive(Clone)]
struct BufferInstrument<I: BufferInstrumentRef> {
    instrument_ref: LazyBufferInstrument<I>,
    tx: LogErrOnFullSender<MetricEvent<I>>,
}
impl<I> BufferInstrument<I>
where
    I: Clone + BufferInstrumentRef,
{
    fn send(&self, value: MetricUpdateVal, attributes: &MetricAttributes) {
        let attributes = match attributes {
            MetricAttributes::Buffer(l) => l.clone(),
            _ => panic!("MetricsCallBuffer only works with MetricAttributes::Buffer"),
        };
        self.tx.send(MetricEvent::Update {
            instrument: self.instrument_ref.clone(),
            update: value,
            attributes: attributes.clone(),
        });
    }
}

#[derive(Clone)]
struct InstrumentWithAttributes<I> {
    inner: I,
    attributes: MetricAttributes,
}

impl<I> MetricAttributable<Box<dyn CounterBase>> for BufferInstrument<I>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn CounterBase>, Box<dyn std::error::Error>> {
        Ok(Box::new(InstrumentWithAttributes {
            inner: self.clone(),
            attributes: attributes.clone(),
        }))
    }
}

impl<I> MetricAttributable<Box<dyn HistogramBase>> for BufferInstrument<I>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn HistogramBase>, Box<dyn std::error::Error>> {
        Ok(Box::new(InstrumentWithAttributes {
            inner: self.clone(),
            attributes: attributes.clone(),
        }))
    }
}

impl<I> MetricAttributable<Box<dyn HistogramF64Base>> for BufferInstrument<I>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn HistogramF64Base>, Box<dyn std::error::Error>> {
        Ok(Box::new(InstrumentWithAttributes {
            inner: self.clone(),
            attributes: attributes.clone(),
        }))
    }
}

impl<I> MetricAttributable<Box<dyn HistogramDurationBase>> for BufferInstrument<I>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn HistogramDurationBase>, Box<dyn std::error::Error>> {
        Ok(Box::new(InstrumentWithAttributes {
            inner: self.clone(),
            attributes: attributes.clone(),
        }))
    }
}

impl<I> MetricAttributable<Box<dyn GaugeBase>> for BufferInstrument<I>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn GaugeBase>, Box<dyn std::error::Error>> {
        Ok(Box::new(InstrumentWithAttributes {
            inner: self.clone(),
            attributes: attributes.clone(),
        }))
    }
}

impl<I> MetricAttributable<Box<dyn GaugeF64Base>> for BufferInstrument<I>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn GaugeF64Base>, Box<dyn std::error::Error>> {
        Ok(Box::new(InstrumentWithAttributes {
            inner: self.clone(),
            attributes: attributes.clone(),
        }))
    }
}
impl<I> CounterBase for InstrumentWithAttributes<BufferInstrument<I>>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn adds(&self, value: u64) {
        self.inner
            .send(MetricUpdateVal::Delta(value), &self.attributes)
    }
}
impl<I> GaugeBase for InstrumentWithAttributes<BufferInstrument<I>>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn records(&self, value: u64) {
        self.inner
            .send(MetricUpdateVal::Value(value), &self.attributes)
    }
}
impl<I> GaugeF64Base for InstrumentWithAttributes<BufferInstrument<I>>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn records(&self, value: f64) {
        self.inner
            .send(MetricUpdateVal::ValueF64(value), &self.attributes)
    }
}
impl<I> HistogramBase for InstrumentWithAttributes<BufferInstrument<I>>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn records(&self, value: u64) {
        self.inner
            .send(MetricUpdateVal::Value(value), &self.attributes)
    }
}
impl<I> HistogramF64Base for InstrumentWithAttributes<BufferInstrument<I>>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn records(&self, value: f64) {
        self.inner
            .send(MetricUpdateVal::ValueF64(value), &self.attributes)
    }
}
impl<I> HistogramDurationBase for InstrumentWithAttributes<BufferInstrument<I>>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn records(&self, value: Duration) {
        self.inner
            .send(MetricUpdateVal::Duration(value), &self.attributes)
    }
}

#[derive(Debug, derive_more::Constructor)]
pub(crate) struct PrefixedMetricsMeter<CM> {
    prefix: String,
    meter: CM,
}
impl<CM: CoreMeter> CoreMeter for PrefixedMetricsMeter<CM> {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        self.meter.new_attributes(attribs)
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        self.meter.extend_attributes(existing, attribs)
    }

    fn counter(&self, mut params: MetricParameters) -> Counter {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.counter(params)
    }

    fn histogram(&self, mut params: MetricParameters) -> Histogram {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.histogram(params)
    }

    fn histogram_f64(&self, mut params: MetricParameters) -> HistogramF64 {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.histogram_f64(params)
    }

    fn histogram_duration(&self, mut params: MetricParameters) -> HistogramDuration {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.histogram_duration(params)
    }

    fn gauge(&self, mut params: MetricParameters) -> Gauge {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.gauge(params)
    }

    fn gauge_f64(&self, mut params: MetricParameters) -> GaugeF64 {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.gauge_f64(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use temporal_sdk_core_api::telemetry::{
        METRIC_PREFIX,
        metrics::{BufferInstrumentRef, CustomMetricAttributes},
    };
    use tracing::subscriber::NoSubscriber;

    #[derive(Debug)]
    struct DummyCustomAttrs(usize);
    impl CustomMetricAttributes for DummyCustomAttrs {
        fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
            self as Arc<dyn Any + Send + Sync>
        }
    }
    impl DummyCustomAttrs {
        fn as_id(ba: &BufferAttributes) -> usize {
            let as_dum = ba
                .get()
                .clone()
                .as_any()
                .downcast::<DummyCustomAttrs>()
                .unwrap();
            as_dum.0
        }
    }

    #[derive(Debug, Clone)]
    struct DummyInstrumentRef(usize);
    impl BufferInstrumentRef for DummyInstrumentRef {}

    #[test]
    fn test_buffered_core_context() {
        let no_op_subscriber = Arc::new(NoSubscriber::new());
        let call_buffer = Arc::new(MetricsCallBuffer::new(100));
        let telem_instance = TelemetryInstance::new(
            Some(no_op_subscriber),
            None,
            METRIC_PREFIX.to_string(),
            Some(call_buffer.clone()),
            true,
        );
        let mc = MetricsContext::top_level("foo".to_string(), "q".to_string(), &telem_instance);
        mc.forced_cache_eviction();
        let events = call_buffer.retrieve();
        let a1 = assert_matches!(
            &events[0],
            MetricEvent::CreateAttributes {
                populate_into,
                append_from: None,
                attributes,
            }
            if attributes[0].key == "service_name" &&
               attributes[1].key == "namespace" &&
               attributes[2].key == "task_queue"
            => populate_into
        );
        a1.set(Arc::new(DummyCustomAttrs(1))).unwrap();
        // Verify all metrics are created. This number will need to get updated any time a metric
        // is added.
        let num_metrics = 35;
        #[allow(clippy::needless_range_loop)] // Sorry clippy, this reads easier.
        for metric_num in 1..=num_metrics {
            let hole = assert_matches!(&events[metric_num],
                MetricEvent::Create { populate_into, .. }
                => populate_into
            );
            hole.set(Arc::new(DummyInstrumentRef(metric_num))).unwrap();
        }
        assert_matches!(
            &events[num_metrics + 1], // +1 for attrib creation (at start), then this update
            MetricEvent::Update {
                instrument,
                attributes,
                update: MetricUpdateVal::Delta(1)
            }
            if DummyCustomAttrs::as_id(attributes) == 1 && instrument.get().0 == num_metrics
        );
        // Verify creating a new context with new attributes merges them properly
        let mc2 = mc.with_new_attrs([MetricKeyValue::new("gotta", "go fast")]);
        mc2.wf_task_latency(Duration::from_secs(1));
        let events = call_buffer.retrieve();
        let a2 = assert_matches!(
            &events[0],
            MetricEvent::CreateAttributes {
                populate_into,
                append_from: Some(eh),
                attributes
            }
            if attributes[0].key == "gotta" && DummyCustomAttrs::as_id(eh) == 1
            => populate_into
        );
        a2.set(Arc::new(DummyCustomAttrs(2))).unwrap();
        dbg!(&events);
        assert_matches!(
            &events[1],
            MetricEvent::Update {
                instrument,
                attributes,
                update: MetricUpdateVal::Duration(d)
            }
            if DummyCustomAttrs::as_id(attributes) == 2 && instrument.get().0 == 11
               && d == &Duration::from_secs(1)
        );
    }

    #[test]
    fn metric_buffer() {
        let call_buffer = MetricsCallBuffer::new(10);
        let ctr = call_buffer.counter(MetricParameters {
            name: "ctr".into(),
            description: "a counter".into(),
            unit: "grognaks".into(),
        });
        let histo = call_buffer.histogram(MetricParameters {
            name: "histo".into(),
            description: "a histogram".into(),
            unit: "flubarbs".into(),
        });
        let gauge = call_buffer.gauge(MetricParameters {
            name: "gauge".into(),
            description: "a counter".into(),
            unit: "bleezles".into(),
        });
        let histo_dur = call_buffer.histogram_duration(MetricParameters {
            name: "histo_dur".into(),
            description: "a duration histogram".into(),
            unit: "seconds".into(),
        });
        let attrs_1 = call_buffer.new_attributes(NewAttributes {
            attributes: vec![MetricKeyValue::new("hi", "yo")],
        });
        let attrs_2 = call_buffer.new_attributes(NewAttributes {
            attributes: vec![MetricKeyValue::new("run", "fast")],
        });
        ctr.add(1, &attrs_1);
        histo.record(2, &attrs_1);
        gauge.record(3, &attrs_2);
        histo_dur.record(Duration::from_secs_f64(1.2), &attrs_1);

        let mut calls = call_buffer.retrieve();
        calls.reverse();
        let ctr_1 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::Create {
                params,
                populate_into,
                kind: MetricKind::Counter
            })
            if params.name == "ctr"
            => populate_into
        );
        ctr_1.set(Arc::new(DummyInstrumentRef(1))).unwrap();
        let hist_2 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::Create {
                params,
                populate_into,
                kind: MetricKind::Histogram
            })
            if params.name == "histo"
            => populate_into
        );
        hist_2.set(Arc::new(DummyInstrumentRef(2))).unwrap();
        let gauge_3 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::Create {
                params,
                populate_into,
                kind: MetricKind::Gauge
            })
            if params.name == "gauge"
            => populate_into
        );
        gauge_3.set(Arc::new(DummyInstrumentRef(3))).unwrap();
        let hist_4 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::Create {
                params,
                populate_into,
                kind: MetricKind::HistogramDuration
            })
            if params.name == "histo_dur"
            => populate_into
        );
        hist_4.set(Arc::new(DummyInstrumentRef(4))).unwrap();
        let a1 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::CreateAttributes {
                populate_into,
                append_from: None,
                attributes
            })
            if attributes[0].key == "hi"
            => populate_into
        );
        a1.set(Arc::new(DummyCustomAttrs(1))).unwrap();
        let a2 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::CreateAttributes {
                populate_into,
                append_from: None,
                attributes
            })
            if attributes[0].key == "run"
            => populate_into
        );
        a2.set(Arc::new(DummyCustomAttrs(2))).unwrap();
        assert_matches!(
            calls.pop(),
            Some(MetricEvent::Update{
                instrument,
                attributes,
                update: MetricUpdateVal::Delta(1)
            })
            if DummyCustomAttrs::as_id(&attributes) == 1 && instrument.get().0 == 1
        );
        assert_matches!(
            calls.pop(),
            Some(MetricEvent::Update{
                instrument,
                attributes,
                update: MetricUpdateVal::Value(2)
            })
            if DummyCustomAttrs::as_id(&attributes) == 1 && instrument.get().0 == 2
        );
        assert_matches!(
            calls.pop(),
            Some(MetricEvent::Update{
                instrument,
                attributes,
                update: MetricUpdateVal::Value(3)
            })
            if DummyCustomAttrs::as_id(&attributes) == 2 && instrument.get().0 == 3
        );
        assert_matches!(
            calls.pop(),
            Some(MetricEvent::Update{
                instrument,
                attributes,
                update: MetricUpdateVal::Duration(d)
            })
            if DummyCustomAttrs::as_id(&attributes) == 1 && instrument.get().0 == 4
               && d == Duration::from_secs_f64(1.2)
        );
    }
}
