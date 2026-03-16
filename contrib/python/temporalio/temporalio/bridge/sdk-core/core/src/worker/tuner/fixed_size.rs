use std::{marker::PhantomData, sync::Arc};
use temporal_sdk_core_api::worker::{
    SlotKind, SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext, SlotSupplier,
    SlotSupplierPermit,
};
use tokio::sync::Semaphore;

/// Implements [SlotSupplier] with a fixed number of slots
pub struct FixedSizeSlotSupplier<SK> {
    sem: Arc<Semaphore>,
    _pd: PhantomData<SK>,
}

impl<SK> FixedSizeSlotSupplier<SK> {
    /// Create a slot supplier which will only hand out at most the provided number of slots
    pub fn new(size: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(size)),
            _pd: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<SK> SlotSupplier for FixedSizeSlotSupplier<SK>
where
    SK: SlotKind + Send + Sync,
{
    type SlotKind = SK;

    async fn reserve_slot(&self, _: &dyn SlotReservationContext) -> SlotSupplierPermit {
        let perm = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .expect("inner semaphore is never closed");
        SlotSupplierPermit::with_user_data(perm)
    }

    fn try_reserve_slot(&self, _: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        let perm = self.sem.clone().try_acquire_owned();
        perm.ok().map(SlotSupplierPermit::with_user_data)
    }

    fn mark_slot_used(&self, _ctx: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {}

    fn release_slot(&self, _ctx: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {}

    fn available_slots(&self) -> Option<usize> {
        Some(self.sem.available_permits())
    }
}
