#[cfg(test)]
use crate::TelemetryInstance;
use crate::abstractions::dbg_panic;

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    iter::Iterator,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};
use temporalio_common::{
    protos::temporal::api::{enums::v1::WorkflowTaskFailedCause, failure::v1::Failure},
    telemetry::metrics::{core::*, *},
};

pub(super) const NUM_POLLERS_NAME: &str = "num_pollers";
pub(super) const TASK_SLOTS_AVAILABLE_NAME: &str = "worker_task_slots_available";
pub(super) const TASK_SLOTS_USED_NAME: &str = "worker_task_slots_used";
pub(super) const STICKY_CACHE_SIZE_NAME: &str = "sticky_cache_size";

/// Used to track context associated with metrics, and record/update them
#[derive(Clone)]
pub(crate) struct MetricsContext {
    meter: TemporalMeter,
    instruments: Arc<Instruments>,
    in_memory_metrics: Option<Arc<WorkerHeartbeatMetrics>>,
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
        let meter = TemporalMeter::no_op();
        let in_memory_metrics = Some(Arc::new(WorkerHeartbeatMetrics::default()));
        let instruments = Arc::new(Instruments::new(&meter, in_memory_metrics.clone()));
        Self {
            instruments,
            meter,
            in_memory_metrics,
        }
    }

    #[cfg(test)]
    pub(crate) fn top_level(namespace: String, tq: String, telemetry: &TelemetryInstance) -> Self {
        MetricsContext::top_level_with_meter(namespace, tq, telemetry.get_temporal_metric_meter())
    }

    pub(crate) fn top_level_with_meter(
        namespace: String,
        tq: String,
        temporal_meter: Option<TemporalMeter>,
    ) -> Self {
        if let Some(mut meter) = temporal_meter {
            let addtl_attributes = [
                MetricKeyValue::new(KEY_NAMESPACE, namespace),
                task_queue(tq),
            ];
            meter.merge_attributes(addtl_attributes.into());
            let in_memory_metrics = Some(Arc::new(WorkerHeartbeatMetrics::default()));
            let mut instruments = Instruments::new(&meter, in_memory_metrics.clone());
            instruments.update_attributes(meter.get_default_attributes());
            Self {
                instruments: Arc::new(instruments),
                meter,
                in_memory_metrics,
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
        let mut tm = self.meter.clone();
        tm.merge_attributes(new_attrs.into());
        let mut instruments = (*self.instruments).clone();
        instruments.update_attributes(tm.get_default_attributes());
        Self {
            instruments: Arc::new(instruments),
            meter: tm,
            in_memory_metrics: self.in_memory_metrics.clone(),
        }
    }

    pub(crate) fn in_memory_meter(&self) -> Option<Arc<WorkerHeartbeatMetrics>> {
        self.in_memory_metrics.clone()
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
    fn new(meter: &TemporalMeter, in_memory: Option<Arc<WorkerHeartbeatMetrics>>) -> Self {
        let counter_with_in_mem = |params: MetricParameters| -> Counter {
            in_memory
                .clone()
                .and_then(|in_mem| in_mem.get_metric(&params.name))
                .map(|metric| meter.counter_with_in_memory(params.clone(), metric))
                .unwrap_or_else(|| meter.counter(params))
        };

        let gauge_with_in_mem = |params: MetricParameters| -> Gauge {
            in_memory
                .clone()
                .and_then(|in_mem| in_mem.get_metric(&params.name))
                .map(|metric| meter.gauge_with_in_memory(params.clone(), metric))
                .unwrap_or_else(|| meter.gauge(params))
        };

        let histogram_with_in_mem = |params: MetricParameters| -> HistogramDuration {
            in_memory
                .clone()
                .and_then(|in_mem| in_mem.get_metric(&params.name))
                .map(|metric| meter.histogram_duration_with_in_memory(params.clone(), metric))
                .unwrap_or_else(|| meter.histogram_duration(params))
        };

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
            wf_task_queue_poll_succeed_counter: counter_with_in_mem(MetricParameters {
                name: "workflow_task_queue_poll_succeed".into(),
                description: "Count of workflow task queue poll successes".into(),
                unit: "".into(),
            }),
            wf_task_execution_failure_counter: counter_with_in_mem(MetricParameters {
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
            wf_task_execution_latency: histogram_with_in_mem(MetricParameters {
                name: WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME.into(),
                unit: "duration".into(),
                description: "Histogram of workflow task execution (not replay) latencies".into(),
            }),
            act_poll_no_task: meter.counter(MetricParameters {
                name: "activity_poll_no_task".into(),
                description: "Count of activity task queue poll timeouts (no new task)".into(),
                unit: "".into(),
            }),
            act_task_received_counter: counter_with_in_mem(MetricParameters {
                name: "activity_task_received".into(),
                description: "Count of activity task queue poll successes".into(),
                unit: "".into(),
            }),
            act_execution_failed: counter_with_in_mem(MetricParameters {
                name: "activity_execution_failed".into(),
                description: "Count of activity task execution failures".into(),
                unit: "".into(),
            }),
            act_sched_to_start_latency: meter.histogram_duration(MetricParameters {
                name: ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME.into(),
                unit: "duration".into(),
                description: "Histogram of activity schedule-to-start latencies".into(),
            }),
            act_exec_latency: histogram_with_in_mem(MetricParameters {
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
            la_exec_latency: histogram_with_in_mem(MetricParameters {
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
            la_total: counter_with_in_mem(MetricParameters {
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
            nexus_task_execution_latency: histogram_with_in_mem(MetricParameters {
                name: "nexus_task_execution_latency".into(),
                unit: "duration".into(),
                description: "Histogram of nexus task execution latencies".into(),
            }),
            nexus_task_execution_failed: counter_with_in_mem(MetricParameters {
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
            num_pollers: gauge_with_in_mem(MetricParameters {
                name: NUM_POLLERS_NAME.into(),
                description: "Current number of active pollers per queue type".into(),
                unit: "".into(),
            }),
            task_slots_available: gauge_with_in_mem(MetricParameters {
                name: TASK_SLOTS_AVAILABLE_NAME.into(),
                description: "Current number of available slots per task type".into(),
                unit: "".into(),
            }),
            task_slots_used: gauge_with_in_mem(MetricParameters {
                name: TASK_SLOTS_USED_NAME.into(),
                description: "Current number of used slots per task type".into(),
                unit: "".into(),
            }),
            sticky_cache_hit: counter_with_in_mem(MetricParameters {
                name: "sticky_cache_hit".into(),
                description: "Count of times the workflow cache was used for a new workflow task"
                    .into(),
                unit: "".into(),
            }),
            sticky_cache_miss: counter_with_in_mem(MetricParameters {
                name: "sticky_cache_miss".into(),
                description:
                "Count of times the workflow cache was missing a workflow for a sticky task".into(),
                unit: "".into(),
            }),
            sticky_cache_size: gauge_with_in_mem(MetricParameters {
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

#[derive(Default, Debug)]
pub(crate) struct NumPollersMetric {
    pub wft_current_pollers: Arc<AtomicU64>,
    pub sticky_wft_current_pollers: Arc<AtomicU64>,
    pub activity_current_pollers: Arc<AtomicU64>,
    pub nexus_current_pollers: Arc<AtomicU64>,
}

impl NumPollersMetric {
    pub(crate) fn as_map(&self) -> HashMap<String, Arc<AtomicU64>> {
        HashMap::from([
            (
                "workflow_task".to_string(),
                self.wft_current_pollers.clone(),
            ),
            (
                "sticky_workflow_task".to_string(),
                self.sticky_wft_current_pollers.clone(),
            ),
            (
                "activity_task".to_string(),
                self.activity_current_pollers.clone(),
            ),
            ("nexus_task".to_string(), self.nexus_current_pollers.clone()),
        ])
    }
}

#[derive(Default, Debug)]
pub(crate) struct SlotMetrics {
    pub workflow_worker: Arc<AtomicU64>,
    pub activity_worker: Arc<AtomicU64>,
    pub nexus_worker: Arc<AtomicU64>,
    pub local_activity_worker: Arc<AtomicU64>,
}

impl SlotMetrics {
    pub(crate) fn as_map(&self) -> HashMap<String, Arc<AtomicU64>> {
        HashMap::from([
            ("WorkflowWorker".to_string(), self.workflow_worker.clone()),
            ("ActivityWorker".to_string(), self.activity_worker.clone()),
            ("NexusWorker".to_string(), self.nexus_worker.clone()),
            (
                "LocalActivityWorker".to_string(),
                self.local_activity_worker.clone(),
            ),
        ])
    }
}

#[derive(Default, Debug)]
pub(crate) struct WorkerHeartbeatMetrics {
    pub sticky_cache_size: Arc<AtomicU64>,
    pub total_sticky_cache_hit: Arc<AtomicU64>,
    pub total_sticky_cache_miss: Arc<AtomicU64>,
    pub num_pollers: NumPollersMetric,
    pub worker_task_slots_used: SlotMetrics,
    pub worker_task_slots_available: SlotMetrics,
    pub workflow_task_execution_failed: Arc<AtomicU64>,
    pub activity_execution_failed: Arc<AtomicU64>,
    pub nexus_task_execution_failed: Arc<AtomicU64>,
    pub local_activity_execution_failed: Arc<AtomicU64>,
    // Although latency metrics here are histograms, we are using the number of times they're called
    // to represent the `total_processed_tasks` heartbeat field
    pub activity_execution_latency: Arc<AtomicU64>,
    pub local_activity_execution_latency: Arc<AtomicU64>,
    pub workflow_task_execution_latency: Arc<AtomicU64>,
    pub nexus_task_execution_latency: Arc<AtomicU64>,
}

impl WorkerHeartbeatMetrics {
    pub(crate) fn get_metric(&self, name: &str) -> Option<HeartbeatMetricType> {
        match name {
            "sticky_cache_size" => Some(HeartbeatMetricType::Individual(
                self.sticky_cache_size.clone(),
            )),
            "sticky_cache_hit" => Some(HeartbeatMetricType::Individual(
                self.total_sticky_cache_hit.clone(),
            )),
            "sticky_cache_miss" => Some(HeartbeatMetricType::Individual(
                self.total_sticky_cache_miss.clone(),
            )),
            "num_pollers" => Some(HeartbeatMetricType::WithLabel {
                label_key: "poller_type".to_string(),
                metrics: self.num_pollers.as_map(),
            }),
            "worker_task_slots_used" => Some(HeartbeatMetricType::WithLabel {
                label_key: "worker_type".to_string(),
                metrics: self.worker_task_slots_used.as_map(),
            }),
            "worker_task_slots_available" => Some(HeartbeatMetricType::WithLabel {
                label_key: "worker_type".to_string(),
                metrics: self.worker_task_slots_available.as_map(),
            }),
            "workflow_task_execution_failed" => Some(HeartbeatMetricType::Individual(
                self.workflow_task_execution_failed.clone(),
            )),
            "activity_execution_failed" => Some(HeartbeatMetricType::Individual(
                self.activity_execution_failed.clone(),
            )),
            "nexus_task_execution_failed" => Some(HeartbeatMetricType::Individual(
                self.nexus_task_execution_failed.clone(),
            )),
            "local_activity_execution_failed" => Some(HeartbeatMetricType::Individual(
                self.local_activity_execution_failed.clone(),
            )),
            "activity_execution_latency" => Some(HeartbeatMetricType::Individual(
                self.activity_execution_latency.clone(),
            )),
            "local_activity_execution_latency" => Some(HeartbeatMetricType::Individual(
                self.local_activity_execution_latency.clone(),
            )),
            "workflow_task_execution_latency" => Some(HeartbeatMetricType::Individual(
                self.workflow_task_execution_latency.clone(),
            )),
            "nexus_task_execution_latency" => Some(HeartbeatMetricType::Individual(
                self.nexus_task_execution_latency.clone(),
            )),
            _ => None,
        }
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
    GrpcMessageTooLarge,
    PayloadsTooLarge,
}
impl Display for FailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            FailureReason::Nondeterminism => "NonDeterminismError".to_owned(),
            FailureReason::Workflow => "WorkflowError".to_owned(),
            FailureReason::Timeout => "timeout".to_owned(),
            FailureReason::NexusOperation(op) => format!("operation_{op}"),
            FailureReason::NexusHandlerError(op) => format!("handler_error_{op}"),
            FailureReason::GrpcMessageTooLarge => "GrpcMessageTooLarge".to_owned(),
            FailureReason::PayloadsTooLarge => "PayloadsTooLarge".to_owned(),
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

/// Track a failure metric if the failure is not a benign application failure.
pub(crate) fn should_record_failure_metric(failure: &Option<Failure>) -> bool {
    !failure
        .as_ref()
        .is_some_and(|f| f.is_benign_application_failure())
}

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
        HistogramF64::new(Arc::new(
            self.new_instrument(params, MetricKind::HistogramF64),
        ))
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
        GaugeF64::new(Arc::new(self.new_instrument(params, MetricKind::GaugeF64)))
    }

    fn up_down_counter(&self, params: MetricParameters) -> UpDownCounter {
        UpDownCounter::new(Arc::new(
            self.new_instrument(params, MetricKind::UpDownCounter),
        ))
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
            e => panic!(
                "MetricsCallBuffer only works with MetricAttributes::Buffer, but used: {:?}",
                e
            ),
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

impl<I> MetricAttributable<Box<dyn UpDownCounterBase>> for BufferInstrument<I>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn UpDownCounterBase>, Box<dyn std::error::Error>> {
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

impl<I> UpDownCounterBase for InstrumentWithAttributes<BufferInstrument<I>>
where
    I: BufferInstrumentRef + Send + Sync + Clone + 'static,
{
    fn adds(&self, value: i64) {
        self.inner
            .send(MetricUpdateVal::SignedDelta(value), &self.attributes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use temporalio_common::telemetry::{
        TelemetryOptions,
        metrics::core::{BufferInstrumentRef, CustomMetricAttributes},
        telemetry_init,
    };

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
    fn with_new_attrs_adds_attrs_to_returned_context() {
        let mc = MetricsContext::no_op();
        let mc2 = mc.with_new_attrs([MetricKeyValue::new("workflow_type", "my_wf")]);
        let MetricAttributes::NoOp(labels) = mc2.meter.get_default_attributes() else {
            panic!("expected no-op metric attributes");
        };
        assert_eq!(
            labels.get("workflow_type").map(String::as_str),
            Some("my_wf")
        );
    }

    #[test]
    fn test_buffered_core_context() {
        let call_buffer = Arc::new(MetricsCallBuffer::new(100));
        let telem_instance = telemetry_init(
            TelemetryOptions::builder()
                .metrics(call_buffer.clone() as Arc<dyn CoreMeter>)
                .build(),
        )
        .unwrap();
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
            if attributes[0].key == "service_name"
            => populate_into
        );
        let a2 = assert_matches!(
            &events[1],
            MetricEvent::CreateAttributes {
                populate_into,
                append_from: Some(_),
                attributes,
            }
            if attributes[0].key == "namespace" &&
               attributes[1].key == "task_queue"
            => populate_into
        );
        a1.set(Arc::new(DummyCustomAttrs(1))).unwrap();
        a2.set(Arc::new(DummyCustomAttrs(2))).unwrap();
        // Verify all metrics are created. This number will need to get updated any time a metric
        // is added.
        let num_metrics = 35;
        #[allow(clippy::needless_range_loop)] // Sorry clippy, this reads easier.
        for metric_num in 2..=num_metrics + 1 {
            let hole = assert_matches!(&events[metric_num],
                MetricEvent::Create { populate_into, .. }
                => populate_into
            );
            hole.set(Arc::new(DummyInstrumentRef(metric_num))).unwrap();
        }
        assert_matches!(
            &events[num_metrics + 2], // +2 for attrib creation (at start), then this update
            MetricEvent::Update {
                instrument,
                attributes,
                update: MetricUpdateVal::Delta(1)
            }
            if DummyCustomAttrs::as_id(attributes) == 2 && instrument.get().0 == num_metrics + 1
        );
        // Verify creating a new context with new attributes merges them properly
        let mc2 = mc.with_new_attrs([MetricKeyValue::new("gotta", "go fast")]);
        mc2.wf_task_latency(Duration::from_secs(1));
        let events = call_buffer.retrieve();
        dbg!(&events);
        let a2 = assert_matches!(
            &events[0],
            MetricEvent::CreateAttributes {
                populate_into,
                append_from: Some(eh),
                attributes
            }
            if attributes[0].key == "gotta" && DummyCustomAttrs::as_id(eh) == 2
            => populate_into
        );
        a2.set(Arc::new(DummyCustomAttrs(3))).unwrap();
        assert_matches!(
            &events[1],
            MetricEvent::Update {
                instrument,
                attributes,
                update: MetricUpdateVal::Duration(d)
            }
            if DummyCustomAttrs::as_id(attributes) == 3 && instrument.get().0 == 12
               && d == &Duration::from_secs(1)
        );
    }

    #[test]
    fn metric_buffer() {
        let call_buffer = MetricsCallBuffer::new(12);
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
        let up_down_ctr = call_buffer.up_down_counter(MetricParameters {
            name: "up_down_ctr".into(),
            description: "an up down counter".into(),
            unit: "widgets".into(),
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
        up_down_ctr.add(-3, &attrs_2);

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
        let up_down_5 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::Create {
                params,
                populate_into,
                kind: MetricKind::UpDownCounter
            })
            if params.name == "up_down_ctr"
            => populate_into
        );
        up_down_5.set(Arc::new(DummyInstrumentRef(5))).unwrap();
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
        assert_matches!(
            calls.pop(),
            Some(MetricEvent::Update{
                instrument,
                attributes,
                update: MetricUpdateVal::SignedDelta(-3)
            })
            if DummyCustomAttrs::as_id(&attributes) == 2 && instrument.get().0 == 5
        );
    }
}
