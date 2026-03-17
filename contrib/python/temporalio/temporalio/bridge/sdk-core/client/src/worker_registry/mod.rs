//! This module enables the tracking of workers that are associated with a client instance.
//! This is needed to implement Eager Workflow Start, a latency optimization in which the client,
//!  after reserving a slot, directly forwards a WFT to a local worker.

use parking_lot::RwLock;
use slotmap::SlotMap;
use std::collections::{HashMap, hash_map::Entry::Vacant};

use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;

slotmap::new_key_type! {
    /// Registration key for a worker
    pub struct WorkerKey;
}

/// This trait is implemented by an object associated with a worker, which provides WFT processing slots.
#[cfg_attr(test, mockall::automock)]
pub trait SlotProvider: std::fmt::Debug {
    /// The namespace for the WFTs that it can process.
    fn namespace(&self) -> &str;
    /// The task queue this provider listens to.
    fn task_queue(&self) -> &str;
    /// Try to reserve a slot on this worker.
    fn try_reserve_wft_slot(&self) -> Option<Box<dyn Slot + Send>>;
}

/// This trait represents a slot reserved for processing a WFT by a worker.
#[cfg_attr(test, mockall::automock)]
pub trait Slot {
    /// Consumes this slot by dispatching a WFT to its worker. This can only be called once.
    fn schedule_wft(
        self: Box<Self>,
        task: PollWorkflowTaskQueueResponse,
    ) -> Result<(), anyhow::Error>;
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

/// This is an inner class for [SlotManager] needed to hide the mutex.
#[derive(Default, Debug)]
struct SlotManagerImpl {
    /// Maps keys, i.e., namespace#task_queue, to provider.
    providers: HashMap<SlotKey, Box<dyn SlotProvider + Send + Sync>>,
    /// Maps ids to keys in `providers`.
    index: SlotMap<WorkerKey, SlotKey>,
}

impl SlotManagerImpl {
    /// Factory method.
    fn new() -> Self {
        Self {
            index: Default::default(),
            providers: Default::default(),
        }
    }

    fn try_reserve_wft_slot(
        &self,
        namespace: String,
        task_queue: String,
    ) -> Option<Box<dyn Slot + Send>> {
        let key = SlotKey::new(namespace, task_queue);
        if let Some(p) = self.providers.get(&key)
            && let Some(slot) = p.try_reserve_wft_slot()
        {
            return Some(slot);
        }
        None
    }

    fn register(&mut self, provider: Box<dyn SlotProvider + Send + Sync>) -> Option<WorkerKey> {
        let key = SlotKey::new(
            provider.namespace().to_string(),
            provider.task_queue().to_string(),
        );
        if let Vacant(p) = self.providers.entry(key.clone()) {
            p.insert(provider);
            Some(self.index.insert(key))
        } else {
            warn!("Ignoring registration for worker: {key:?}.");
            None
        }
    }

    fn unregister(&mut self, id: WorkerKey) -> Option<Box<dyn SlotProvider + Send + Sync>> {
        if let Some(key) = self.index.remove(id) {
            self.providers.remove(&key)
        } else {
            None
        }
    }

    #[cfg(test)]
    fn num_providers(&self) -> (usize, usize) {
        (self.index.len(), self.providers.len())
    }
}

/// Enables local workers to make themselves visible to a shared client instance.
/// There can only be one worker registered per namespace+queue_name+client, others will get ignored.
/// It also provides a convenient method to find compatible slots within the collection.
#[derive(Default, Debug)]
pub struct SlotManager {
    manager: RwLock<SlotManagerImpl>,
}

impl SlotManager {
    /// Factory method.
    pub fn new() -> Self {
        Self {
            manager: RwLock::new(SlotManagerImpl::new()),
        }
    }

    /// Try to reserve a compatible processing slot in any of the registered workers.
    pub(crate) fn try_reserve_wft_slot(
        &self,
        namespace: String,
        task_queue: String,
    ) -> Option<Box<dyn Slot + Send>> {
        self.manager
            .read()
            .try_reserve_wft_slot(namespace, task_queue)
    }

    /// Register a local worker that can provide WFT processing slots.
    pub fn register(&self, provider: Box<dyn SlotProvider + Send + Sync>) -> Option<WorkerKey> {
        self.manager.write().register(provider)
    }

    /// Unregister a provider, typically when its worker starts shutdown.
    pub fn unregister(&self, id: WorkerKey) -> Option<Box<dyn SlotProvider + Send + Sync>> {
        self.manager.write().unregister(id)
    }

    #[cfg(test)]
    /// Returns (num_providers, num_buckets), where a bucket key is namespace+task_queue.
    /// There is only one provider per bucket so `num_providers` should be equal to `num_buckets`.
    pub fn num_providers(&self) -> (usize, usize) {
        self.manager.read().num_providers()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    ) -> MockSlotProvider {
        let mut mock_provider = MockSlotProvider::new();
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
        mock_provider
    }

    #[test]
    fn registry_respects_registration_order() {
        let mock_provider1 =
            new_mock_provider("foo".to_string(), "bar_q".to_string(), false, false);
        let mock_provider2 = new_mock_provider("foo".to_string(), "bar_q".to_string(), false, true);

        let manager = SlotManager::new();
        let some_slots = manager.register(Box::new(mock_provider1));
        let no_slots = manager.register(Box::new(mock_provider2));
        assert!(no_slots.is_none());

        let mut found = 0;
        for _ in 0..10 {
            if manager
                .try_reserve_wft_slot("foo".to_string(), "bar_q".to_string())
                .is_some()
            {
                found += 1;
            }
        }
        assert_eq!(found, 10);
        assert_eq!((1, 1), manager.num_providers());

        manager.unregister(some_slots.unwrap());
        assert_eq!((0, 0), manager.num_providers());

        let mock_provider1 =
            new_mock_provider("foo".to_string(), "bar_q".to_string(), false, false);
        let mock_provider2 = new_mock_provider("foo".to_string(), "bar_q".to_string(), false, true);

        let no_slots = manager.register(Box::new(mock_provider2));
        let some_slots = manager.register(Box::new(mock_provider1));
        assert!(some_slots.is_none());

        let mut not_found = 0;
        for _ in 0..10 {
            if manager
                .try_reserve_wft_slot("foo".to_string(), "bar_q".to_string())
                .is_none()
            {
                not_found += 1;
            }
        }
        assert_eq!(not_found, 10);
        assert_eq!((1, 1), manager.num_providers());
        manager.unregister(no_slots.unwrap());
        assert_eq!((0, 0), manager.num_providers());
    }

    #[test]
    fn registry_keeps_one_provider_per_namespace() {
        let manager = SlotManager::new();
        let mut worker_keys = vec![];
        for i in 0..10 {
            let namespace = format!("myId{}", i % 3);
            let mock_provider = new_mock_provider(namespace, "bar_q".to_string(), false, false);
            worker_keys.push(manager.register(Box::new(mock_provider)));
        }
        assert_eq!((3, 3), manager.num_providers());

        let count = worker_keys
            .iter()
            .filter(|key| key.is_some())
            .fold(0, |count, key| {
                manager.unregister(key.unwrap());
                // Should be idempotent
                manager.unregister(key.unwrap());
                count + 1
            });
        assert_eq!(3, count);
        assert_eq!((0, 0), manager.num_providers());
    }
}
