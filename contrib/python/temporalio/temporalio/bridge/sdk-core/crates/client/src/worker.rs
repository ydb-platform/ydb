//! Contains types and logic for interactions between clients and Core/SDK workers.
//!
//! This module is public for use by `temporalio-sdk-core` and is not intended to be used directly
//! by SDK users.

use anyhow::bail;
use parking_lot::RwLock;
use rand::seq::SliceRandom;
use std::{
    collections::{
        HashMap,
        hash_map::Entry::{Occupied, Vacant},
    },
    future::Future,
    sync::{Arc, Weak},
};
use temporalio_common::{
    protos::{
        TaskToken,
        temporal::api::{
            worker::v1::WorkerHeartbeat,
            workflowservice::v1::{DescribeNamespaceResponse, PollWorkflowTaskQueueResponse},
        },
    },
    worker::{WorkerDeploymentOptions, WorkerTaskTypes},
};
use tokio::sync::OnceCell;
use tonic::Code;
use uuid::Uuid;

/// This trait represents a slot reserved for processing a WFT by a worker.
#[cfg_attr(test, mockall::automock)]
pub trait Slot {
    /// Consumes this slot by dispatching a WFT to its worker. This can only be called once.
    fn schedule_wft(
        self: Box<Self>,
        task: PollWorkflowTaskQueueResponse,
    ) -> Result<(), anyhow::Error>;
}

/// Result of reserving a workflow task slot, including deployment options if applicable.
pub(crate) struct SlotReservation {
    /// The reserved slot for processing the workflow task
    pub slot: Box<dyn Slot + Send>,
    /// Worker deployment options, if the worker is using deployment-based versioning
    pub deployment_options: Option<WorkerDeploymentOptions>,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
struct SlotKey {
    namespace: String,
    task_queue: String,
}

impl SlotKey {
    fn new(namespace: String, task_queue: String) -> SlotKey {
        SlotKey {
            namespace,
            task_queue,
        }
    }
}

/// Information about a registered worker in the slot provider registry
#[derive(Debug, Clone)]
struct RegisteredWorkerInfo {
    /// Unique identifier for this worker instance
    worker_id: Uuid,
    /// Optional deployment build ID for versioning
    build_id: Option<String>,
    /// Task types this worker can handle
    task_types: WorkerTaskTypes,
}

impl RegisteredWorkerInfo {
    fn new(worker_id: Uuid, build_id: Option<String>, task_types: WorkerTaskTypes) -> Self {
        Self {
            worker_id,
            build_id,
            task_types,
        }
    }
}

/// This is an inner class for [ClientWorkerSet] needed to hide the mutex.
struct ClientWorkerSetImpl {
    /// Maps slot keys to registered worker information
    slot_providers: HashMap<SlotKey, Vec<RegisteredWorkerInfo>>,
    /// Maps worker_instance_key to registered workers
    all_workers: HashMap<Uuid, Arc<dyn ClientWorker + Send + Sync>>,
    /// Maps namespace to shared worker for worker heartbeating
    shared_worker: HashMap<String, Box<dyn SharedNamespaceWorkerTrait + Send + Sync>>,
    // Avoid retaining namespace limits and capabilities after the last worker using them is gone.
    namespace_descriptions: HashMap<String, Weak<NamespaceDescriptionSource>>,
}

impl ClientWorkerSetImpl {
    /// Factory method.
    fn new() -> Self {
        Self {
            slot_providers: Default::default(),
            all_workers: Default::default(),
            shared_worker: Default::default(),
            namespace_descriptions: Default::default(),
        }
    }

    fn namespace_description_source(&mut self, namespace: &str) -> Arc<NamespaceDescriptionSource> {
        if let Some(description) = self
            .namespace_descriptions
            .get(namespace)
            .and_then(Weak::upgrade)
        {
            return description;
        }

        let description = Arc::new(NamespaceDescriptionSource::unresolved());
        self.namespace_descriptions
            .insert(namespace.to_owned(), Arc::downgrade(&description));
        description
    }

    fn try_reserve_wft_slot(
        &self,
        namespace: String,
        task_queue: String,
    ) -> Option<SlotReservation> {
        let key = SlotKey::new(namespace, task_queue);
        if let Some(worker_list) = self.slot_providers.get(&key) {
            let workflow_workers: Vec<&RegisteredWorkerInfo> = worker_list
                .iter()
                .filter(|info| info.task_types.enable_workflows)
                .collect();

            for worker_id in Self::worker_ids_in_selection_order(&workflow_workers) {
                if let Some(worker) = self.all_workers.get(&worker_id)
                    && let Some(slot) = worker.try_reserve_wft_slot()
                {
                    let deployment_options = worker.deployment_options();
                    return Some(SlotReservation {
                        slot,
                        deployment_options,
                    });
                }
            }
        }
        None
    }

    fn worker_control_task_queue_enabled(&self, namespace: &str) -> bool {
        self.shared_worker
            .get(namespace)
            .is_some_and(|worker| worker.worker_control_task_queue_enabled())
    }

    fn worker_ids_in_selection_order(worker_list: &[&RegisteredWorkerInfo]) -> Vec<Uuid> {
        // For tests we return workers in the order they're registered, so we can test
        // the retry mechanism deterministically
        if cfg!(test) {
            worker_list.iter().map(|info| info.worker_id).collect()
        } else {
            let mut rng = rand::rng();
            let mut shuffled: Vec<_> = worker_list.to_vec();
            shuffled.shuffle(&mut rng);
            shuffled.iter().map(|info| info.worker_id).collect()
        }
    }

    fn register(
        &mut self,
        worker: Arc<dyn ClientWorker + Send + Sync>,
        skip_client_worker_set_check: bool,
    ) -> Result<(), anyhow::Error> {
        let slot_key = SlotKey::new(
            worker.namespace().to_string(),
            worker.task_queue().to_string(),
        );
        let build_id = worker
            .deployment_options()
            .map(|opts| opts.version.build_id);
        let task_types = worker.worker_task_types();

        if !task_types.enable_workflows
            && !task_types.enable_local_activities
            && !task_types.enable_remote_activities
            && !task_types.enable_nexus
        {
            bail!(
                "Worker must have at least one capability enabled (workflows, activities, or nexus)"
            );
        }

        if !task_types.enable_workflows && task_types.enable_local_activities {
            bail!("Local activities cannot be enabled without workflows")
        }

        if !skip_client_worker_set_check
            && let Some(existing_workers) = self.slot_providers.get(&slot_key)
        {
            for existing_worker_info in existing_workers {
                if existing_worker_info.build_id.as_ref() == build_id.as_ref()
                    && task_types.overlaps_with(&existing_worker_info.task_types)
                {
                    bail!(
                        "Registration of multiple workers with overlapping worker task types \
                        on the same namespace, task queue, and deployment build ID not allowed: \
                        {slot_key:?}, worker_instance_key: {:?} \
                        build_id: {build_id:?}, \
                        new task types: {task_types:?}, \
                        existing task types: {:?}.",
                        existing_worker_info.task_types,
                        worker.worker_instance_key()
                    );
                }
            }
        }

        if worker.heartbeat_enabled()
            && let Some(heartbeat_callback) = worker.heartbeat_callback()
        {
            let worker_instance_key = worker.worker_instance_key();
            let namespace = worker.namespace().to_string();

            let shared_worker = match self.shared_worker.entry(namespace) {
                Occupied(o) => o.into_mut(),
                Vacant(v) => {
                    let shared_worker = worker.new_shared_namespace_worker()?;
                    v.insert(shared_worker)
                }
            };
            shared_worker.register_callback(
                worker_instance_key,
                WorkerCallbacks {
                    heartbeat: heartbeat_callback,
                    heartbeat_success: worker.heartbeat_success_callback(),
                    cancel_activity: worker.cancel_activity_callback(),
                },
            );
        }

        let worker_info =
            RegisteredWorkerInfo::new(worker.worker_instance_key(), build_id, task_types);

        match self.slot_providers.entry(slot_key.clone()) {
            Occupied(o) => o.into_mut().push(worker_info),
            Vacant(v) => {
                v.insert(vec![worker_info]);
            }
        };

        self.all_workers
            .insert(worker.worker_instance_key(), worker);

        Ok(())
    }

    /// Slot provider should be unregistered at the beginning of worker shutdown, in order to disable
    /// eager workflow start.
    fn unregister_slot_provider(&mut self, worker_instance_key: Uuid) -> Result<(), anyhow::Error> {
        let worker = self.all_workers.get(&worker_instance_key).ok_or_else(|| {
            anyhow::anyhow!("Worker not in all_workers during slot provider unregister")
        })?;

        let slot_key = SlotKey::new(
            worker.namespace().to_string(),
            worker.task_queue().to_string(),
        );
        if let Some(slot_vec) = self.slot_providers.get_mut(&slot_key) {
            slot_vec.retain(|info| info.worker_id != worker_instance_key);
            if slot_vec.is_empty() {
                self.slot_providers.remove(&slot_key);
            }
        }
        Ok(())
    }

    fn finalize_unregister(
        &mut self,
        worker_instance_key: Uuid,
    ) -> Result<Arc<dyn ClientWorker + Send + Sync>, anyhow::Error> {
        if let Some(worker) = self.all_workers.get(&worker_instance_key)
            && let Some(slot_vec) = self.slot_providers.get(&SlotKey::new(
                worker.namespace().to_string(),
                worker.task_queue().to_string(),
            ))
            && slot_vec
                .iter()
                .any(|info| info.worker_id == worker_instance_key)
        {
            return Err(anyhow::anyhow!(
                "Worker still in slot_providers during finalize"
            ));
        }

        let worker = self
            .all_workers
            .remove(&worker_instance_key)
            .ok_or_else(|| anyhow::anyhow!("Worker not found in all_workers"))?;

        if let Some(w) = self.shared_worker.get_mut(worker.namespace()) {
            let (callback, is_empty) = w.unregister_callback(worker.worker_instance_key());
            if callback.is_some() && is_empty {
                self.shared_worker.remove(worker.namespace());
            }
        }

        Ok(worker)
    }

    #[cfg(test)]
    fn num_providers(&self) -> usize {
        self.slot_providers.values().map(|v| v.len()).sum()
    }

    #[cfg(test)]
    fn num_heartbeat_workers(&self) -> usize {
        self.shared_worker.values().map(|v| v.num_workers()).sum()
    }
}

/// A connection-scoped source for a namespace description shared by all workers in that namespace.
#[derive(Debug)]
#[doc(hidden)]
pub struct NamespaceDescriptionSource {
    description: OnceCell<DescribeNamespaceResponse>,
}

impl NamespaceDescriptionSource {
    /// Construct a source whose description has not yet been resolved.
    pub fn unresolved() -> Self {
        Self {
            description: OnceCell::new(),
        }
    }

    /// Construct a source whose description has already been resolved.
    pub fn resolved(description: DescribeNamespaceResponse) -> Self {
        Self {
            description: OnceCell::new_with(Some(description)),
        }
    }

    /// Resolve the namespace description once, allowing concurrent callers to await the same RPC.
    pub async fn resolve<F, Fut>(
        &self,
        fetch: F,
    ) -> Result<&DescribeNamespaceResponse, tonic::Status>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<DescribeNamespaceResponse, tonic::Status>>,
    {
        self.description
            .get_or_try_init(|| async {
                match fetch().await {
                    Err(status) if status.code() == Code::Unimplemented => {
                        Ok(DescribeNamespaceResponse::default())
                    }
                    result => result,
                }
            })
            .await
    }

    /// Return the resolved namespace description, if resolution has completed successfully.
    pub fn get(&self) -> Option<&DescribeNamespaceResponse> {
        self.description.get()
    }
}

/// This trait represents a shared namespace worker that sends worker heartbeats and
/// receives worker commands.
pub trait SharedNamespaceWorkerTrait {
    /// Namespace that the shared namespace worker is connected to.
    fn namespace(&self) -> String;

    /// Registers worker callbacks.
    fn register_callback(&self, worker_instance_key: Uuid, callbacks: WorkerCallbacks);

    /// Unregisters worker callbacks. Returns the callbacks removed, as well as a bool that
    /// indicates if there are no remaining callbacks in the SharedNamespaceWorker, indicating
    /// the shared worker itself can be shut down.
    fn unregister_callback(&self, worker_instance_key: Uuid) -> (Option<WorkerCallbacks>, bool);

    /// Returns the number of workers registered to this shared worker.
    fn num_workers(&self) -> usize;

    /// Returns whether this shared worker is polling the worker control task queue.
    fn worker_control_task_queue_enabled(&self) -> bool {
        false
    }
}

/// Enables local workers to make themselves visible to a shared client instance.
///
/// For slot managing, there can only be one worker registered per
/// namespace+queue_name+connection, others will return an error.
/// It also provides a convenient method to find compatible slots within the collection.
pub struct ClientWorkerSet {
    worker_grouping_key: Uuid,
    worker_manager: RwLock<ClientWorkerSetImpl>,
}

impl Default for ClientWorkerSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientWorkerSet {
    /// Factory method.
    pub fn new() -> Self {
        Self {
            worker_grouping_key: Uuid::new_v4(),
            worker_manager: RwLock::new(ClientWorkerSetImpl::new()),
        }
    }

    /// Return the shared namespace description source for this connection and namespace.
    #[doc(hidden)]
    pub fn namespace_description_source(&self, namespace: &str) -> Arc<NamespaceDescriptionSource> {
        self.worker_manager
            .write()
            .namespace_description_source(namespace)
    }

    /// Try to reserve a compatible processing slot in any of the registered workers.
    /// Returns the slot and the worker's deployment options (if using deployment-based versioning).
    pub(crate) fn try_reserve_wft_slot(
        &self,
        namespace: String,
        task_queue: String,
    ) -> Option<SlotReservation> {
        self.worker_manager
            .read()
            .try_reserve_wft_slot(namespace, task_queue)
    }

    /// Register a local worker that can provide WFT processing slots and potentially worker heartbeating.
    pub fn register_worker(
        &self,
        worker: Arc<dyn ClientWorker + Send + Sync>,
        skip_client_worker_set_check: bool,
    ) -> Result<(), anyhow::Error> {
        self.worker_manager
            .write()
            .register(worker, skip_client_worker_set_check)
    }

    /// Disables Eager Workflow Start for this worker. This must be called before
    /// `finalize_unregister`, otherwise `finalize_unregister` will return an err.
    pub fn unregister_slot_provider(&self, worker_instance_key: Uuid) -> Result<(), anyhow::Error> {
        self.worker_manager
            .write()
            .unregister_slot_provider(worker_instance_key)
    }

    /// Finalizes unregistering of worker from client. This must be called at the end of worker
    /// shutdown in order to finalize shutdown for worker heartbeat properly. Must call after
    /// `unregister_slot_provider`, otherwise an err will be returned.
    pub fn finalize_unregister(
        &self,
        worker_instance_key: Uuid,
    ) -> Result<Arc<dyn ClientWorker + Send + Sync>, anyhow::Error> {
        self.worker_manager
            .write()
            .finalize_unregister(worker_instance_key)
    }

    /// Returns the worker grouping key, which is unique for each worker.
    pub fn worker_grouping_key(&self) -> Uuid {
        self.worker_grouping_key
    }

    /// Returns whether the shared worker for `namespace` is polling the worker control task queue.
    pub fn worker_control_task_queue_enabled(&self, namespace: &str) -> bool {
        self.worker_manager
            .read()
            .worker_control_task_queue_enabled(namespace)
    }

    #[cfg(test)]
    /// Returns (num_providers, num_buckets), where a bucket key is namespace+task_queue.
    /// There is only one provider per bucket so `num_providers` should be equal to `num_buckets`.
    pub fn num_providers(&self) -> usize {
        self.worker_manager.read().num_providers()
    }

    #[cfg(test)]
    /// Returns the total number of heartbeat workers registered across all namespaces.
    pub fn num_heartbeat_workers(&self) -> usize {
        self.worker_manager.read().num_heartbeat_workers()
    }
}

impl std::fmt::Debug for ClientWorkerSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientWorkerSet")
            .field("worker_grouping_key", &self.worker_grouping_key)
            .finish()
    }
}

/// Contains a worker heartbeat callback, wrapped for mocking. Returns `None` until the worker has
/// started running.
pub type HeartbeatCallback = Arc<dyn Fn() -> Option<WorkerHeartbeat> + Send + Sync>;

/// Callback invoked after a worker heartbeat has been accepted by the server.
pub type HeartbeatSuccessCallback = Arc<dyn Fn() + Send + Sync>;

/// Callback to cancel an activity by task token. Returns true if the activity was found.
pub type CancelActivityCallback = Arc<dyn Fn(TaskToken) -> bool + Send + Sync>;

/// Bundles all per-worker callbacks registered with the SharedNamespaceWorker.
pub struct WorkerCallbacks {
    /// Callback to collect heartbeat data from the worker.
    pub heartbeat: HeartbeatCallback,
    /// Callback acknowledging successful delivery of the collected heartbeat.
    pub heartbeat_success: Option<HeartbeatSuccessCallback>,
    /// Callback to cancel an activity by task token.
    pub cancel_activity: Option<CancelActivityCallback>,
}

/// Represents a complete worker that can handle both slot management
/// and worker heartbeat functionality.
#[cfg_attr(test, mockall::automock)]
pub trait ClientWorker: Send + Sync {
    /// The namespace this worker operates in
    fn namespace(&self) -> &str;

    /// The task queue this worker listens to
    fn task_queue(&self) -> &str;

    /// Try to reserve a slot for workflow task processing.
    ///
    /// This method should return `Some(slot)` if a workflow task slot is available,
    /// or `None` if all slots are currently in use. The returned slot will be used
    /// to process exactly one workflow task.
    fn try_reserve_wft_slot(&self) -> Option<Box<dyn Slot + Send>>;

    /// Get the worker deployment options for this worker, if using deployment-based versioning.
    fn deployment_options(&self) -> Option<WorkerDeploymentOptions>;

    /// Unique identifier for this worker instance.
    /// This must be stable across the worker's lifetime and unique per instance.
    fn worker_instance_key(&self) -> Uuid;

    /// Indicates if worker heartbeating is enabled for this client worker.
    fn heartbeat_enabled(&self) -> bool;

    /// Returns the heartbeat callback that can be used to get WorkerHeartbeat data.
    fn heartbeat_callback(&self) -> Option<HeartbeatCallback>;

    /// Returns a callback notified after the heartbeat is accepted by the server.
    fn heartbeat_success_callback(&self) -> Option<HeartbeatSuccessCallback> {
        None
    }

    /// Returns a callback that can cancel an activity by task token.
    fn cancel_activity_callback(&self) -> Option<CancelActivityCallback>;

    /// Creates a new worker that implements the [SharedNamespaceWorkerTrait]
    fn new_shared_namespace_worker(
        &self,
    ) -> Result<Box<dyn SharedNamespaceWorkerTrait + Send + Sync>, anyhow::Error>;

    /// Returns the task types this worker can handle
    fn worker_task_types(&self) -> WorkerTaskTypes;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn namespace_description_source_resolves_once() {
        let source = NamespaceDescriptionSource::unresolved();
        let calls = AtomicUsize::new(0);

        let first = source.resolve(|| async {
            calls.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
            Ok(DescribeNamespaceResponse::default())
        });
        let second = source.resolve(|| async {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(DescribeNamespaceResponse::default())
        });

        let (first, second) = tokio::join!(first, second);
        first.unwrap();
        second.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn namespace_description_source_resolves_unimplemented_as_default() {
        let source = NamespaceDescriptionSource::unresolved();

        let description = source
            .resolve(|| async { Err(tonic::Status::unimplemented("unsupported")) })
            .await
            .unwrap();

        assert_eq!(description, &DescribeNamespaceResponse::default());
    }

    #[test]
    fn namespace_description_sources_are_scoped_by_namespace() {
        let workers = ClientWorkerSet::new();
        let first = workers.namespace_description_source("first");

        assert!(Arc::ptr_eq(
            &first,
            &workers.namespace_description_source("first")
        ));
        assert!(!Arc::ptr_eq(
            &first,
            &workers.namespace_description_source("second")
        ));
    }

    fn new_mock_slot(with_error: bool) -> Box<MockSlot> {
        let mut mock_slot = MockSlot::new();
        if with_error {
            mock_slot
                .expect_schedule_wft()
                .returning(|_| Err(anyhow::anyhow!("Changed my mind")));
        } else {
            mock_slot.expect_schedule_wft().returning(|_| Ok(()));
        }
        Box::new(mock_slot)
    }

    fn new_mock_provider(
        namespace: String,
        task_queue: String,
        with_error: bool,
        no_slots: bool,
        heartbeat_enabled: bool,
    ) -> MockClientWorker {
        let mut mock_provider = MockClientWorker::new();
        mock_provider
            .expect_try_reserve_wft_slot()
            .returning(move || {
                if no_slots {
                    None
                } else {
                    Some(new_mock_slot(with_error))
                }
            });
        mock_provider.expect_namespace().return_const(namespace);
        mock_provider.expect_task_queue().return_const(task_queue);
        mock_provider.expect_deployment_options().return_const(None);
        mock_provider
            .expect_heartbeat_enabled()
            .return_const(heartbeat_enabled);
        mock_provider
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        mock_provider
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: true,
                enable_nexus: true,
            });
        mock_provider
    }

    #[test]
    fn reserve_wft_slot_retries_another_worker_when_first_has_no_slot() {
        let mut manager = ClientWorkerSetImpl::new();
        let namespace = "retry_namespace".to_string();
        let task_queue = "retry_queue".to_string();

        let failing_worker_id = Uuid::new_v4();
        let mut failing_worker = MockClientWorker::new();
        failing_worker
            .expect_try_reserve_wft_slot()
            .times(1)
            .returning(|| None);
        failing_worker
            .expect_namespace()
            .return_const(namespace.clone());
        failing_worker
            .expect_task_queue()
            .return_const(task_queue.clone());
        failing_worker.expect_deployment_options().return_const(
            WorkerDeploymentOptions::new(temporalio_common::worker::WorkerDeploymentVersion {
                deployment_name: "test-deployment".to_string(),
                build_id: "build-fail".to_string(),
            })
            .use_worker_versioning(true)
            .build(),
        );
        failing_worker
            .expect_worker_instance_key()
            .return_const(failing_worker_id);
        failing_worker
            .expect_heartbeat_enabled()
            .return_const(false);
        failing_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: true,
                enable_nexus: true,
            });

        let succeeding_worker_id = Uuid::new_v4();
        let mut succeeding_worker = MockClientWorker::new();
        succeeding_worker
            .expect_try_reserve_wft_slot()
            .times(1)
            .returning(|| Some(new_mock_slot(false)));
        succeeding_worker
            .expect_namespace()
            .return_const(namespace.clone());
        succeeding_worker
            .expect_task_queue()
            .return_const(task_queue.clone());
        let success_deployment_options =
            WorkerDeploymentOptions::new(temporalio_common::worker::WorkerDeploymentVersion {
                deployment_name: "test-deployment".to_string(),
                build_id: "build-success".to_string(),
            })
            .use_worker_versioning(true)
            .build();
        succeeding_worker
            .expect_deployment_options()
            .return_const(success_deployment_options.clone());
        succeeding_worker
            .expect_worker_instance_key()
            .return_const(succeeding_worker_id);
        succeeding_worker
            .expect_heartbeat_enabled()
            .return_const(false);
        succeeding_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: true,
                enable_nexus: true,
            });

        manager
            .register(Arc::new(failing_worker), false)
            .expect("failing worker registration succeeds");
        manager
            .register(Arc::new(succeeding_worker), false)
            .expect("succeeding worker registration succeeds");

        let reservation = manager.try_reserve_wft_slot(namespace.clone(), task_queue.clone());

        let reservation_deployment_options = reservation
            .expect("succeeding worker was used after failing worker failed")
            .deployment_options
            .unwrap();
        assert_eq!(
            reservation_deployment_options, success_deployment_options,
            "deployment options bubble through from succeeding worker"
        );
    }

    #[test]
    fn reserve_wft_slot_retries_respects_slot_boundary() {
        let mut manager = ClientWorkerSetImpl::new();
        let namespace = "retry_namespace".to_string();
        let task_queue = "retry_queue".to_string();

        let failing_worker_id = Uuid::new_v4();
        let mut failing_worker = MockClientWorker::new();
        failing_worker
            .expect_try_reserve_wft_slot()
            .times(1)
            .returning(|| None);
        failing_worker
            .expect_namespace()
            .return_const(namespace.clone());
        failing_worker
            .expect_task_queue()
            .return_const(task_queue.clone());
        failing_worker.expect_deployment_options().return_const(
            WorkerDeploymentOptions::new(temporalio_common::worker::WorkerDeploymentVersion {
                deployment_name: "test-deployment".to_string(),
                build_id: "build-fail".to_string(),
            })
            .use_worker_versioning(true)
            .build(),
        );
        failing_worker
            .expect_worker_instance_key()
            .return_const(failing_worker_id);
        failing_worker
            .expect_heartbeat_enabled()
            .return_const(false);
        failing_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: true,
                enable_nexus: true,
            });

        // On a separate task queue
        let succeeding_worker_id = Uuid::new_v4();
        let mut succeeding_worker = MockClientWorker::new();
        succeeding_worker.expect_try_reserve_wft_slot().times(0);
        succeeding_worker
            .expect_namespace()
            .return_const(namespace.clone());
        succeeding_worker
            .expect_task_queue()
            .return_const("other_task_queue".to_string());
        succeeding_worker
            .expect_deployment_options()
            .return_const(None);
        succeeding_worker
            .expect_worker_instance_key()
            .return_const(succeeding_worker_id);
        succeeding_worker
            .expect_heartbeat_enabled()
            .return_const(false);
        succeeding_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: true,
                enable_nexus: true,
            });

        manager
            .register(Arc::new(failing_worker), false)
            .expect("failing worker registration succeeds");
        manager
            .register(Arc::new(succeeding_worker), false)
            .expect("succeeding worker registration succeeds");

        let reservation = manager.try_reserve_wft_slot(namespace.clone(), task_queue.clone());
        assert!(
            reservation.is_none(),
            "succeeding_worker should not be picked due to it being on a separate task queue"
        );
    }

    #[test]
    fn registry_keeps_one_provider_per_namespace() {
        let manager = ClientWorkerSet::new();
        let mut worker_keys = vec![];
        let mut successful_registrations = 0;

        for i in 0..10 {
            let namespace = format!("myId{}", i % 3);
            let mock_provider =
                new_mock_provider(namespace, "bar_q".to_string(), false, false, false);
            let worker_instance_key = mock_provider.worker_instance_key();

            let result = manager.register_worker(Arc::new(mock_provider), false);
            if let Err(err) = result {
                // Should get error for overlapping worker task types
                assert!(err.to_string().contains(
                    "Registration of multiple workers with overlapping worker task types"
                ));
            } else {
                successful_registrations += 1;
                worker_keys.push(worker_instance_key);
            }
        }

        assert_eq!(successful_registrations, 3);
        assert_eq!(3, manager.num_providers());

        let count = worker_keys.iter().fold(0, |count, key| {
            manager.unregister_slot_provider(*key).unwrap();
            manager.finalize_unregister(*key).unwrap();
            // expect error since worker is already unregistered
            let result = manager.unregister_slot_provider(*key);
            assert!(result.is_err());
            let result = manager.finalize_unregister(*key);
            assert!(result.is_err());
            count + 1
        });
        assert_eq!(3, count);
        assert_eq!(0, manager.num_providers());
    }

    struct MockSharedNamespaceWorker {
        namespace: String,
        callbacks: Arc<RwLock<HashMap<Uuid, WorkerCallbacks>>>,
        worker_control_task_queue_enabled: bool,
    }

    impl std::fmt::Debug for MockSharedNamespaceWorker {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockSharedNamespaceWorker")
                .field("namespace", &self.namespace)
                .field("callbacks_count", &self.callbacks.read().len())
                .finish()
        }
    }

    impl MockSharedNamespaceWorker {
        fn new(namespace: String) -> Self {
            Self {
                namespace,
                callbacks: Arc::new(RwLock::new(HashMap::new())),
                worker_control_task_queue_enabled: false,
            }
        }

        fn with_worker_control_task_queue_enabled(mut self) -> Self {
            self.worker_control_task_queue_enabled = true;
            self
        }
    }

    impl SharedNamespaceWorkerTrait for MockSharedNamespaceWorker {
        fn namespace(&self) -> String {
            self.namespace.clone()
        }

        fn register_callback(&self, worker_instance_key: Uuid, callbacks: WorkerCallbacks) {
            self.callbacks
                .write()
                .insert(worker_instance_key, callbacks);
        }

        fn unregister_callback(
            &self,
            worker_instance_key: Uuid,
        ) -> (Option<WorkerCallbacks>, bool) {
            let mut callbacks = self.callbacks.write();
            let callback = callbacks.remove(&worker_instance_key);
            let is_empty = callbacks.is_empty();
            (callback, is_empty)
        }

        fn num_workers(&self) -> usize {
            self.callbacks.read().len()
        }

        fn worker_control_task_queue_enabled(&self) -> bool {
            self.worker_control_task_queue_enabled
        }
    }

    #[test]
    fn worker_control_task_queue_enabled_reflects_shared_worker() {
        let manager = ClientWorkerSet::new();
        let namespace = "test_namespace";

        assert!(!manager.worker_control_task_queue_enabled(namespace));

        manager.worker_manager.write().shared_worker.insert(
            namespace.to_string(),
            Box::new(
                MockSharedNamespaceWorker::new(namespace.to_string())
                    .with_worker_control_task_queue_enabled(),
            ),
        );

        assert!(manager.worker_control_task_queue_enabled(namespace));
        assert!(!manager.worker_control_task_queue_enabled("other_namespace"));
    }

    fn new_mock_provider_with_heartbeat(
        namespace: String,
        task_queue: String,
        heartbeat_enabled: bool,
        build_id: Option<String>,
    ) -> MockClientWorker {
        let mut mock_provider = MockClientWorker::new();
        mock_provider
            .expect_try_reserve_wft_slot()
            .returning(|| Some(new_mock_slot(false)));
        mock_provider
            .expect_namespace()
            .return_const(namespace.clone());
        mock_provider.expect_task_queue().return_const(task_queue);
        mock_provider
            .expect_heartbeat_enabled()
            .return_const(heartbeat_enabled);
        mock_provider
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        let deployment_name = "test-deployment".to_string();
        let build_id_for_closure = build_id.clone();
        mock_provider
            .expect_deployment_options()
            .returning(move || {
                build_id_for_closure.as_ref().map(|build_id| {
                    WorkerDeploymentOptions::new(
                        temporalio_common::worker::WorkerDeploymentVersion {
                            deployment_name: deployment_name.clone(),
                            build_id: build_id.clone(),
                        },
                    )
                    .use_worker_versioning(true)
                    .build()
                })
            });

        if heartbeat_enabled {
            mock_provider
                .expect_heartbeat_callback()
                .returning(|| Some(Arc::new(|| Some(WorkerHeartbeat::default()))));
            mock_provider
                .expect_heartbeat_success_callback()
                .returning(|| None);
            mock_provider
                .expect_cancel_activity_callback()
                .returning(|| None);

            let namespace_clone = namespace.clone();
            mock_provider
                .expect_new_shared_namespace_worker()
                .returning(move || {
                    Ok(Box::new(MockSharedNamespaceWorker::new(
                        namespace_clone.clone(),
                    )))
                });
        }

        mock_provider
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: true,
                enable_nexus: true,
            });

        mock_provider
    }

    #[test]
    fn duplicate_namespace_task_queue_registration_fails() {
        let manager = ClientWorkerSet::new();

        let worker1 = new_mock_provider_with_heartbeat(
            "test_namespace".to_string(),
            "test_queue".to_string(),
            true,
            None,
        );

        // Same namespace+task_queue but different worker instance
        let worker2 = new_mock_provider_with_heartbeat(
            "test_namespace".to_string(),
            "test_queue".to_string(),
            true,
            None,
        );

        manager.register_worker(Arc::new(worker1), false).unwrap();

        // second worker register should fail due to overlapping worker task types
        let result = manager.register_worker(Arc::new(worker2), false);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Registration of multiple workers with overlapping worker task types")
        );

        assert_eq!(1, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 1);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.shared_worker.len(), 1);
        assert!(impl_ref.shared_worker.contains_key("test_namespace"));
    }

    #[test]
    fn duplicate_namespace_with_different_build_ids_succeeds() {
        let manager = ClientWorkerSet::new();
        let namespace = "test_namespace".to_string();
        let task_queue = "test_queue".to_string();

        let worker1 =
            new_mock_provider_with_heartbeat(namespace.clone(), task_queue.clone(), false, None);
        let worker1_instance_key = worker1.worker_instance_key();
        let worker2 = new_mock_provider_with_heartbeat(
            namespace.clone(),
            task_queue.clone(),
            false,
            Some("build-1".to_string()),
        );
        let worker2_instance_key = worker2.worker_instance_key();
        let worker3 =
            new_mock_provider_with_heartbeat(namespace.clone(), task_queue.clone(), false, None);
        let worker4 = new_mock_provider_with_heartbeat(
            namespace.clone(),
            task_queue.clone(),
            false,
            Some("build-1".to_string()),
        );

        manager.register_worker(Arc::new(worker1), false).unwrap();

        manager
            .register_worker(Arc::new(worker2), false)
            .expect("worker with new build ID should register");
        assert_eq!(2, manager.num_providers());

        assert!(
            manager
                .register_worker(Arc::new(worker3), false)
                .unwrap_err()
                .to_string()
                .contains("Registration of multiple workers with overlapping worker task types")
        );

        assert!(
            manager
                .register_worker(Arc::new(worker4), false)
                .unwrap_err()
                .to_string()
                .contains("Registration of multiple workers with overlapping worker task types")
        );
        assert_eq!(2, manager.num_providers());

        {
            let impl_ref = manager.worker_manager.read();
            let slot_key = SlotKey::new(namespace.clone(), task_queue.clone());
            let providers = impl_ref
                .slot_providers
                .get(&slot_key)
                .expect("slot providers should exist for namespace/task queue");
            assert_eq!(2, providers.len());

            assert_eq!(providers[0].worker_id, worker1_instance_key);
            assert_eq!(providers[0].build_id, None);
            assert_eq!(providers[1].worker_id, worker2_instance_key);
            assert_eq!(providers[1].build_id, Some("build-1".to_string()));
        }

        manager
            .unregister_slot_provider(worker2_instance_key)
            .unwrap();
        manager.finalize_unregister(worker2_instance_key).unwrap();

        {
            let impl_ref = manager.worker_manager.read();
            let slot_key = SlotKey::new(namespace.clone(), task_queue.clone());
            let providers = impl_ref
                .slot_providers
                .get(&slot_key)
                .expect("slot providers should exist for namespace/task queue");

            assert_eq!(1, providers.len());
            assert_eq!(providers[0].worker_id, worker1_instance_key);
            assert_eq!(providers[0].build_id, None);
        }
    }

    #[test]
    fn multiple_workers_same_namespace_share_heartbeat_manager() {
        let manager = ClientWorkerSet::new();

        let worker1 = new_mock_provider_with_heartbeat(
            "shared_namespace".to_string(),
            "queue1".to_string(),
            true,
            None,
        );

        // Same namespace but different task queue
        let worker2 = new_mock_provider_with_heartbeat(
            "shared_namespace".to_string(),
            "queue2".to_string(),
            true,
            None,
        );

        manager.register_worker(Arc::new(worker1), false).unwrap();
        manager.register_worker(Arc::new(worker2), false).unwrap();

        assert_eq!(2, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 2);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.shared_worker.len(), 1);
        assert!(impl_ref.shared_worker.contains_key("shared_namespace"));

        let shared_worker = impl_ref.shared_worker.get("shared_namespace").unwrap();
        assert_eq!(shared_worker.namespace(), "shared_namespace");
    }

    #[test]
    fn different_namespaces_get_separate_heartbeat_managers() {
        let manager = ClientWorkerSet::new();
        let worker1 = new_mock_provider_with_heartbeat(
            "namespace1".to_string(),
            "queue1".to_string(),
            true,
            None,
        );
        let worker2 = new_mock_provider_with_heartbeat(
            "namespace2".to_string(),
            "queue1".to_string(),
            true,
            None,
        );

        manager.register_worker(Arc::new(worker1), false).unwrap();
        manager.register_worker(Arc::new(worker2), false).unwrap();

        assert_eq!(2, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 2);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.num_heartbeat_workers(), 2);
        assert!(impl_ref.shared_worker.contains_key("namespace1"));
        assert!(impl_ref.shared_worker.contains_key("namespace2"));
    }

    #[test]
    fn unregister_heartbeat_workers_cleans_up_shared_worker_when_last_removed() {
        let manager = ClientWorkerSet::new();

        // Create two workers with same namespace but different task queues
        let worker1 = new_mock_provider_with_heartbeat(
            "test_namespace".to_string(),
            "queue1".to_string(),
            true,
            None,
        );
        let worker2 = new_mock_provider_with_heartbeat(
            "test_namespace".to_string(),
            "queue2".to_string(),
            true,
            None,
        );
        let worker_instance_key1 = worker1.worker_instance_key();
        let worker_instance_key2 = worker2.worker_instance_key();

        assert_ne!(worker_instance_key1, worker_instance_key2);

        manager.register_worker(Arc::new(worker1), false).unwrap();
        manager.register_worker(Arc::new(worker2), false).unwrap();

        // Verify initial state: 2 slot providers, 2 heartbeat workers, 1 shared worker
        assert_eq!(2, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 2);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.shared_worker.len(), 1);
        assert!(impl_ref.shared_worker.contains_key("test_namespace"));
        assert_eq!(
            impl_ref
                .shared_worker
                .get("test_namespace")
                .unwrap()
                .num_workers(),
            2
        );
        drop(impl_ref);

        // Unregister first worker
        manager
            .unregister_slot_provider(worker_instance_key1)
            .unwrap();
        manager.finalize_unregister(worker_instance_key1).unwrap();

        // After unregistering first worker: 1 slot provider, 1 heartbeat worker, shared worker still exists
        assert_eq!(1, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 1);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.num_heartbeat_workers(), 1); // SharedNamespaceWorker still exists
        assert!(impl_ref.shared_worker.contains_key("test_namespace"));
        assert_eq!(
            impl_ref
                .shared_worker
                .get("test_namespace")
                .unwrap()
                .num_workers(),
            1
        );
        drop(impl_ref);

        // Unregister second worker
        manager
            .unregister_slot_provider(worker_instance_key2)
            .unwrap();
        manager.finalize_unregister(worker_instance_key2).unwrap();

        // After unregistering last worker: 0 slot providers, 0 heartbeat workers, shared worker is removed
        assert_eq!(0, manager.num_providers());
        assert_eq!(manager.num_heartbeat_workers(), 0);

        let impl_ref = manager.worker_manager.read();
        assert_eq!(impl_ref.shared_worker.len(), 0); // SharedNamespaceWorker is cleaned up
        assert!(!impl_ref.shared_worker.contains_key("test_namespace"));
    }

    #[test]
    fn workflow_and_activity_only_workers_coexist() {
        let manager = ClientWorkerSet::new();
        let namespace = "test_namespace".to_string();
        let task_queue = "test_queue".to_string();

        let mut workflow_nexus_worker = MockClientWorker::new();
        workflow_nexus_worker
            .expect_namespace()
            .return_const(namespace.clone());
        workflow_nexus_worker
            .expect_task_queue()
            .return_const(task_queue.clone());
        workflow_nexus_worker
            .expect_deployment_options()
            .return_const(None);
        workflow_nexus_worker
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        workflow_nexus_worker
            .expect_heartbeat_enabled()
            .return_const(false);
        workflow_nexus_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: false,
                enable_remote_activities: false,
                enable_nexus: true,
            });

        let mut activity_worker = MockClientWorker::new();
        activity_worker
            .expect_namespace()
            .return_const(namespace.clone());
        activity_worker
            .expect_task_queue()
            .return_const(task_queue.clone());
        activity_worker
            .expect_deployment_options()
            .return_const(None);
        activity_worker
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        activity_worker
            .expect_heartbeat_enabled()
            .return_const(false);
        activity_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: false,
                enable_local_activities: false,
                enable_remote_activities: true,
                enable_nexus: false,
            });
        activity_worker.expect_try_reserve_wft_slot().times(0); // Should not be called for activity-only worker

        manager
            .register_worker(Arc::new(workflow_nexus_worker), false)
            .expect("workflow-nexus worker should register");
        manager
            .register_worker(Arc::new(activity_worker), false)
            .expect("activity-only worker should register");

        assert_eq!(2, manager.num_providers());
    }

    #[test]
    fn overlapping_capabilities_rejected() {
        let manager = ClientWorkerSet::new();
        let namespace = "test_namespace".to_string();
        let task_queue = "test_queue".to_string();

        // workflow+activity worker
        let mut worker1 = MockClientWorker::new();
        worker1.expect_namespace().return_const(namespace.clone());
        worker1.expect_task_queue().return_const(task_queue.clone());
        worker1.expect_deployment_options().return_const(None);
        worker1
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        worker1.expect_heartbeat_enabled().return_const(false);
        worker1
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: true,
                enable_nexus: false,
            });

        // workflow+activity worker
        let mut worker2 = MockClientWorker::new();
        worker2.expect_namespace().return_const(namespace.clone());
        worker2.expect_task_queue().return_const(task_queue.clone());
        worker2.expect_deployment_options().return_const(None);
        worker2
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        worker2.expect_heartbeat_enabled().return_const(false);
        worker2
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: true,
                enable_nexus: false,
            });

        manager
            .register_worker(Arc::new(worker1), false)
            .expect("first worker should register");

        let result = manager.register_worker(Arc::new(worker2), false);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("overlapping worker task types")
        );

        // activity-only worker
        let mut worker3 = MockClientWorker::new();
        worker3.expect_namespace().return_const(namespace.clone());
        worker3.expect_task_queue().return_const(task_queue.clone());
        worker3.expect_deployment_options().return_const(None);
        worker3
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        worker3.expect_heartbeat_enabled().return_const(false);
        worker3
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: false,
                enable_local_activities: false,
                enable_remote_activities: true,
                enable_nexus: false,
            });

        let result = manager.register_worker(Arc::new(worker3), false);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("overlapping worker task types")
        );
    }

    #[test]
    fn wft_slot_reservation_ignores_non_workflow_workers() {
        let mut manager_impl = ClientWorkerSetImpl::new();
        let namespace = "test_namespace".to_string();
        let task_queue = "test_queue".to_string();

        let mut activity_worker = MockClientWorker::new();
        activity_worker
            .expect_namespace()
            .return_const(namespace.clone());
        activity_worker
            .expect_task_queue()
            .return_const(task_queue.clone());
        activity_worker
            .expect_deployment_options()
            .return_const(None);
        activity_worker
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        activity_worker
            .expect_heartbeat_enabled()
            .return_const(false);
        activity_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: false,
                enable_local_activities: false,
                enable_remote_activities: true,
                enable_nexus: false,
            });

        let mut nexus_worker = MockClientWorker::new();
        nexus_worker
            .expect_namespace()
            .return_const(namespace.clone());
        nexus_worker
            .expect_task_queue()
            .return_const(task_queue.clone());
        nexus_worker.expect_deployment_options().return_const(None);
        nexus_worker
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        nexus_worker.expect_heartbeat_enabled().return_const(false);
        nexus_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: false,
                enable_local_activities: false,
                enable_remote_activities: false,
                enable_nexus: true,
            });

        manager_impl
            .register(Arc::new(activity_worker), false)
            .expect("activity worker should register");
        manager_impl
            .register(Arc::new(nexus_worker), false)
            .expect("nexus worker should register");

        let reservation = manager_impl.try_reserve_wft_slot(namespace.clone(), task_queue.clone());
        assert!(
            reservation.is_none(),
            "should not find workflow workers when only activity/nexus workers registered"
        );

        // Now register a workflow worker
        let mut workflow_worker = MockClientWorker::new();
        workflow_worker
            .expect_namespace()
            .return_const(namespace.clone());
        workflow_worker
            .expect_task_queue()
            .return_const(task_queue.clone());
        workflow_worker
            .expect_deployment_options()
            .return_const(None);
        workflow_worker
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        workflow_worker
            .expect_heartbeat_enabled()
            .return_const(false);
        workflow_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: false,
                enable_nexus: false,
            });
        workflow_worker
            .expect_try_reserve_wft_slot()
            .times(1)
            .returning(|| Some(new_mock_slot(false)));

        manager_impl
            .register(Arc::new(workflow_worker), false)
            .expect("workflow worker should register");

        let reservation = manager_impl.try_reserve_wft_slot(namespace.clone(), task_queue.clone());
        assert!(
            reservation.is_some(),
            "should find workflow worker after it's registered"
        );
    }

    #[test]
    fn worker_invalid_type_config_rejected() {
        let manager = ClientWorkerSet::new();

        // no types enabled
        let mut worker = MockClientWorker::new();
        worker
            .expect_namespace()
            .return_const("test_namespace".to_string());
        worker
            .expect_task_queue()
            .return_const("test_queue".to_string());
        worker.expect_deployment_options().return_const(None);
        worker
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        worker.expect_heartbeat_enabled().return_const(false);
        worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: false,
                enable_local_activities: false,
                enable_remote_activities: false,
                enable_nexus: false,
            });

        let result = manager.register_worker(Arc::new(worker), false);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must have at least one capability enabled")
        );

        // local activities enabled without workflows
        let mut worker = MockClientWorker::new();
        worker
            .expect_namespace()
            .return_const("test_namespace".to_string());
        worker
            .expect_task_queue()
            .return_const("test_queue".to_string());
        worker.expect_deployment_options().return_const(None);
        worker
            .expect_worker_instance_key()
            .return_const(Uuid::new_v4());
        worker.expect_heartbeat_enabled().return_const(false);
        worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: false,
                enable_local_activities: true,
                enable_remote_activities: true,
                enable_nexus: false,
            });

        let result = manager.register_worker(Arc::new(worker), false);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Local activities cannot be enabled without workflows".to_string()
        );
    }

    #[test]
    fn unregister_with_multiple_workers() {
        let manager = ClientWorkerSet::new();
        let namespace = "test_namespace".to_string();
        let task_queue = "test_queue".to_string();

        // workflow-only worker
        let mut workflow_worker = MockClientWorker::new();
        workflow_worker
            .expect_namespace()
            .return_const(namespace.clone());
        workflow_worker
            .expect_task_queue()
            .return_const(task_queue.clone());
        workflow_worker
            .expect_deployment_options()
            .return_const(None);
        let wf_worker_key = Uuid::new_v4();
        workflow_worker
            .expect_worker_instance_key()
            .return_const(wf_worker_key);
        workflow_worker
            .expect_heartbeat_enabled()
            .return_const(false);
        workflow_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: false,
                enable_nexus: false,
            });
        workflow_worker
            .expect_try_reserve_wft_slot()
            .returning(|| Some(new_mock_slot(false)));

        // activity-only worker
        let mut activity_worker = MockClientWorker::new();
        activity_worker
            .expect_namespace()
            .return_const(namespace.clone());
        activity_worker
            .expect_task_queue()
            .return_const(task_queue.clone());
        activity_worker
            .expect_deployment_options()
            .return_const(None);
        let act_worker_key = Uuid::new_v4();
        activity_worker
            .expect_worker_instance_key()
            .return_const(act_worker_key);
        activity_worker
            .expect_heartbeat_enabled()
            .return_const(false);
        activity_worker
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: false,
                enable_local_activities: false,
                enable_remote_activities: true,
                enable_nexus: false,
            });

        manager
            .register_worker(Arc::new(workflow_worker), false)
            .expect("workflow worker should register");
        manager
            .register_worker(Arc::new(activity_worker), false)
            .expect("activity worker should register");

        assert_eq!(2, manager.num_providers());

        let reservation = manager.try_reserve_wft_slot(namespace.clone(), task_queue.clone());
        assert!(
            reservation.is_some(),
            "should be able to reserve slot from workflow worker"
        );

        manager
            .unregister_slot_provider(wf_worker_key)
            .expect("should unregister slot provider for workflow worker");
        manager
            .finalize_unregister(wf_worker_key)
            .expect("should finalize unregister for workflow worker");

        // Activity worker should still be registered
        assert_eq!(1, manager.num_providers());

        let reservation = manager.try_reserve_wft_slot(namespace.clone(), task_queue.clone());
        assert!(
            reservation.is_none(),
            "should not find workflow worker after unregistration"
        );

        manager
            .unregister_slot_provider(act_worker_key)
            .expect("should unregister slot provider for activity worker");
        manager
            .finalize_unregister(act_worker_key)
            .expect("should finalize unregister for activity worker");

        assert_eq!(0, manager.num_providers());
    }

    #[test]
    fn worker_unregister_order() {
        let manager = ClientWorkerSet::new();
        let worker = new_mock_provider_with_heartbeat(
            "namespace1".to_string(),
            "queue1".to_string(),
            true,
            None,
        );
        let worker_instance_key = worker.worker_instance_key();
        manager.register_worker(Arc::new(worker), false).unwrap();

        let res = manager.finalize_unregister(worker_instance_key);
        assert!(res.is_err());
        let err_string = res.err().map(|e| e.to_string()).unwrap();
        assert!(err_string.contains("Worker still in slot_providers during finalize"));

        // previous incorrect call to finalize_unregister should not cause any state leaks when
        // properly removed later
        manager
            .unregister_slot_provider(worker_instance_key)
            .unwrap();
        manager.finalize_unregister(worker_instance_key).unwrap();
    }
}
