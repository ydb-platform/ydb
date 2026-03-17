use crate::{
    MetricsContext,
    telemetry::metrics::workflow_type,
    worker::workflow::{
        HistoryUpdate, LocalActivityRequestSink, PermittedWFT, RequestEvictMsg, RunBasics,
        managed_run::{ManagedRun, RunUpdateAct},
    },
};
use lru::LruCache;
use std::{num::NonZeroUsize, rc::Rc, sync::Arc};
use temporal_sdk_core_api::worker::WorkerConfig;
use temporal_sdk_core_protos::{
    coresdk::workflow_activation::remove_from_cache::EvictionReason,
    temporal::api::workflowservice::v1::get_system_info_response,
};

pub(super) struct RunCache {
    worker_config: Arc<WorkerConfig>,
    sdk_name_and_version: (String, String),
    server_capabilities: get_system_info_response::Capabilities,
    /// Run id -> Data
    runs: LruCache<String, ManagedRun>,
    local_activity_request_sink: Rc<dyn LocalActivityRequestSink>,

    metrics: MetricsContext,
}

impl RunCache {
    pub(super) fn new(
        worker_config: Arc<WorkerConfig>,
        sdk_name_and_version: (String, String),
        server_capabilities: get_system_info_response::Capabilities,
        local_activity_request_sink: impl LocalActivityRequestSink,
        metrics: MetricsContext,
    ) -> Self {
        // The cache needs room for at least one run, otherwise we couldn't do anything. In
        // "0" size mode, the run is evicted once the workflow task is complete.
        let lru_size = if worker_config.max_cached_workflows > 0 {
            worker_config.max_cached_workflows
        } else {
            1
        };
        Self {
            worker_config,
            sdk_name_and_version,
            server_capabilities,
            runs: LruCache::new(
                NonZeroUsize::new(lru_size).expect("LRU size is guaranteed positive"),
            ),
            local_activity_request_sink: Rc::new(local_activity_request_sink),
            metrics,
        }
    }

    pub(super) fn instantiate_or_update(&mut self, pwft: PermittedWFT) -> RunUpdateAct {
        let cur_num_cached_runs = self.runs.len();
        let run_id = pwft.work.execution.run_id.clone();

        if let Some(run_handle) = self.runs.get_mut(&run_id) {
            let rur = run_handle.incoming_wft(pwft);
            self.metrics.cache_size(cur_num_cached_runs as u64);
            return rur;
        }

        // Create a new workflow machines instance for this workflow, initialize it, and
        // track it.
        let metrics = self
            .metrics
            .with_new_attrs([workflow_type(pwft.work.workflow_type.clone())]);
        let (mrh, rur) = ManagedRun::new(
            RunBasics {
                worker_config: self.worker_config.clone(),
                workflow_id: pwft.work.execution.workflow_id.clone(),
                workflow_type: pwft.work.workflow_type.clone(),
                run_id: run_id.clone(),
                history: HistoryUpdate::dummy(),
                metrics,
                capabilities: &self.server_capabilities,
                sdk_name: &self.sdk_name_and_version.0,
                sdk_version: &self.sdk_name_and_version.1,
            },
            pwft,
            self.local_activity_request_sink.clone(),
        );
        if self.runs.push(run_id, mrh).is_some() {
            panic!("Overflowed run cache! Cache owner is expected to avoid this!");
        }
        self.metrics.cache_size(cur_num_cached_runs as u64 + 1);
        rur
    }

    pub(super) fn remove(&mut self, k: &str) -> Option<ManagedRun> {
        let r = self.runs.pop(k);
        self.metrics.cache_size(self.len() as u64);
        if let Some(rh) = &r {
            // A workflow completing normally doesn't count as a forced eviction.
            if !matches!(
                rh.trying_to_evict(),
                Some(RequestEvictMsg {
                    reason: EvictionReason::WorkflowExecutionEnding,
                    ..
                })
            ) {
                self.metrics.forced_cache_eviction();
            }
        }
        r
    }

    pub(super) fn get_mut(&mut self, k: &str) -> Option<&mut ManagedRun> {
        self.runs.get_mut(k)
    }

    pub(super) fn get(&mut self, k: &str) -> Option<&ManagedRun> {
        self.runs.get(k)
    }

    /// Returns the current least-recently-used run. Returns `None` when cache empty.
    pub(super) fn current_lru_run(&self) -> Option<&str> {
        self.runs.peek_lru().map(|(run_id, _)| run_id.as_str())
    }

    /// Returns an iterator yielding cached runs in LRU order
    pub(super) fn runs_lru_order(&self) -> impl Iterator<Item = (&str, &ManagedRun)> {
        self.runs.iter().rev().map(|(k, v)| (k.as_str(), v))
    }

    pub(super) fn peek(&self, k: &str) -> Option<&ManagedRun> {
        self.runs.peek(k)
    }

    pub(super) fn has_run(&self, k: &str) -> bool {
        self.runs.contains(k)
    }

    pub(super) fn handles(&self) -> impl Iterator<Item = &ManagedRun> {
        self.runs.iter().map(|(_, v)| v)
    }

    pub(super) fn is_full(&self) -> bool {
        self.runs.cap().get() == self.runs.len()
    }

    pub(super) fn len(&self) -> usize {
        self.runs.len()
    }

    pub(super) fn cache_capacity(&self) -> usize {
        self.worker_config.max_cached_workflows
    }
}
