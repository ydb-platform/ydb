//! This module contains very generic helpers that can be used codebase-wide

pub(crate) mod take_cell;

use crate::MetricsContext;
use std::{
    fmt::{Debug, Formatter},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use temporal_sdk_core_api::{
    telemetry::metrics::TemporalMeter,
    worker::{
        SlotKind, SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext, SlotSupplier,
        SlotSupplierPermit, WorkerDeploymentVersion, WorkflowSlotKind,
    },
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Wraps a [SlotSupplier] and turns successful slot reservations into permit structs, as well
/// as handling associated metrics tracking.
#[derive(Clone)]
pub(crate) struct MeteredPermitDealer<SK: SlotKind> {
    supplier: Arc<dyn SlotSupplier<SlotKind = SK> + Send + Sync>,
    /// The number of permit owners who have acquired a permit, but are not yet meaningfully using
    /// that permit. This is useful for giving a more semantically accurate count of used task
    /// slots, since we typically wait for a permit first before polling, but that slot isn't used
    /// in the sense the user expects until we actually also get the corresponding task.
    unused_claimants: Arc<AtomicUsize>,
    /// The number of permits that have been handed out
    extant_permits: (watch::Sender<usize>, watch::Receiver<usize>),
    /// The maximum number of extant permits which are allowed. Once the number of extant permits
    /// is at this number, no more permits will be requested from the supplier until one is freed.
    /// This avoids requesting slots when we are at the workflow cache size limit. If and when
    /// we add user-defined cache sizing, that logic will need to live with the supplier and
    /// there will need to be some associated refactoring.
    max_permits: Option<usize>,
    metrics_ctx: MetricsContext,
    meter: Option<TemporalMeter>,
    /// Only applies to permit dealers for workflow tasks. True if this permit dealer is associated
    /// with a sticky queue poller.
    is_sticky_poller: bool,
    context_data: Arc<PermitDealerContextData>,
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(Default))]
pub(crate) struct PermitDealerContextData {
    pub(crate) task_queue: String,
    pub(crate) worker_identity: String,
    pub(crate) worker_deployment_version: Option<WorkerDeploymentVersion>,
}

impl<SK> MeteredPermitDealer<SK>
where
    SK: SlotKind + 'static,
{
    pub(crate) fn new(
        supplier: Arc<dyn SlotSupplier<SlotKind = SK> + Send + Sync>,
        metrics_ctx: MetricsContext,
        max_permits: Option<usize>,
        context_data: Arc<PermitDealerContextData>,
        meter: Option<TemporalMeter>,
    ) -> Self {
        Self {
            supplier,
            unused_claimants: Arc::new(AtomicUsize::new(0)),
            extant_permits: watch::channel(0),
            metrics_ctx,
            meter,
            max_permits,
            is_sticky_poller: false,
            context_data,
        }
    }

    pub(crate) fn available_permits(&self) -> Option<usize> {
        self.supplier.available_slots()
    }

    #[cfg(test)]
    pub(crate) fn unused_permits(&self) -> Option<usize> {
        self.available_permits()
            .map(|ap| ap + self.unused_claimants.load(Ordering::Acquire))
    }

    pub(crate) async fn acquire_owned(&self) -> OwnedMeteredSemPermit<SK> {
        if let Some(max) = self.max_permits {
            self.extant_permits
                .1
                .clone()
                .wait_for(|&ep| ep < max)
                .await
                .expect("Extant permit channel is never closed");
        }
        let res = self.supplier.reserve_slot(self).await;
        self.build_owned(res)
    }

    pub(crate) fn try_acquire_owned(&self) -> Result<OwnedMeteredSemPermit<SK>, ()> {
        if let Some(max) = self.max_permits
            && *self.extant_permits.1.borrow() >= max
        {
            return Err(());
        }
        if let Some(res) = self.supplier.try_reserve_slot(self) {
            Ok(self.build_owned(res))
        } else {
            Err(())
        }
    }

    pub(crate) fn get_extant_count_rcv(&self) -> watch::Receiver<usize> {
        self.extant_permits.1.clone()
    }

    fn build_owned(&self, res: SlotSupplierPermit) -> OwnedMeteredSemPermit<SK> {
        self.unused_claimants.fetch_add(1, Ordering::Release);
        self.extant_permits.0.send_modify(|ep| *ep += 1);
        // Eww
        let uc_c = self.unused_claimants.clone();
        let ep_rx_c = self.extant_permits.1.clone();
        let ep_tx_c = self.extant_permits.0.clone();
        let supp = self.supplier.clone();
        let supp_c = self.supplier.clone();
        let supp_c_c = self.supplier.clone();
        let mets = self.metrics_ctx.clone();
        let metric_rec =
            // When being called from the drop impl, the permit isn't actually dropped yet, so
            // account for that with the `add_one` parameter.
            move |add_one: bool| {
                let extra = usize::from(add_one);
                let unused = uc_c.load(Ordering::Acquire);
                if let Some(avail) = supp.available_slots() {
                    mets.available_task_slots(avail + unused + extra);
                }
                mets.task_slots_used((ep_rx_c.borrow().saturating_sub(unused) + extra) as u64);
            };
        let mrc = metric_rec.clone();
        mrc(false);

        OwnedMeteredSemPermit {
            unused_claimants: Some(self.unused_claimants.clone()),
            release_ctx: ReleaseCtx {
                permit: res,
                stored_info: None,
                meter: self.meter.clone(),
            },
            use_fn: Box::new(move |info| {
                supp_c.mark_slot_used(info);
                metric_rec(false)
            }),
            release_fn: Box::new(move |info| {
                supp_c_c.release_slot(info);
                ep_tx_c.send_modify(|ep| *ep -= 1);
                mrc(true)
            }),
        }
    }
}

impl MeteredPermitDealer<WorkflowSlotKind> {
    pub(crate) fn into_sticky(mut self) -> Self {
        self.is_sticky_poller = true;
        self
    }
}

impl<SK: SlotKind> SlotReservationContext for MeteredPermitDealer<SK> {
    fn task_queue(&self) -> &str {
        &self.context_data.task_queue
    }

    fn worker_identity(&self) -> &str {
        &self.context_data.worker_identity
    }

    fn worker_deployment_version(&self) -> &Option<WorkerDeploymentVersion> {
        &self.context_data.worker_deployment_version
    }

    fn num_issued_slots(&self) -> usize {
        *self.extant_permits.1.borrow()
    }

    fn is_sticky(&self) -> bool {
        self.is_sticky_poller
    }

    fn get_metrics_meter(&self) -> Option<TemporalMeter> {
        self.meter.clone()
    }
}

struct UseCtx<'a, SK: SlotKind> {
    stored_info: &'a SK::Info,
    permit: &'a SlotSupplierPermit,
    meter: Option<TemporalMeter>,
}

impl<SK: SlotKind> SlotMarkUsedContext for UseCtx<'_, SK> {
    type SlotKind = SK;

    fn permit(&self) -> &SlotSupplierPermit {
        self.permit
    }

    fn info(&self) -> &<Self::SlotKind as SlotKind>::Info {
        self.stored_info
    }

    fn get_metrics_meter(&self) -> Option<TemporalMeter> {
        self.meter.clone()
    }
}

struct ReleaseCtx<SK: SlotKind> {
    permit: SlotSupplierPermit,
    stored_info: Option<SK::Info>,
    meter: Option<TemporalMeter>,
}

impl<SK: SlotKind> SlotReleaseContext for ReleaseCtx<SK> {
    type SlotKind = SK;

    fn permit(&self) -> &SlotSupplierPermit {
        &self.permit
    }

    fn info(&self) -> Option<&<Self::SlotKind as SlotKind>::Info> {
        self.stored_info.as_ref()
    }

    fn get_metrics_meter(&self) -> Option<TemporalMeter> {
        self.meter.clone()
    }
}

/// A version of [MeteredPermitDealer] that can be closed and supports waiting for close to complete.
/// Once closed, no permits will be handed out.
/// Close completes when all permits have been returned.
pub(crate) struct ClosableMeteredPermitDealer<SK: SlotKind> {
    inner: Arc<MeteredPermitDealer<SK>>,
    outstanding_permits: AtomicUsize,
    close_requested: AtomicBool,
    close_complete_token: CancellationToken,
}

impl<SK> ClosableMeteredPermitDealer<SK>
where
    SK: SlotKind,
{
    pub(crate) fn new_arc(sem: Arc<MeteredPermitDealer<SK>>) -> Arc<Self> {
        Arc::new(Self {
            inner: sem,
            outstanding_permits: Default::default(),
            close_requested: AtomicBool::new(false),
            close_complete_token: CancellationToken::new(),
        })
    }
}

impl<SK> ClosableMeteredPermitDealer<SK>
where
    SK: SlotKind + 'static,
{
    #[cfg(test)]
    pub(crate) fn unused_permits(&self) -> Option<usize> {
        self.inner.unused_permits()
    }

    /// Request to close the semaphore and prevent new permits from being acquired.
    pub(crate) fn close(&self) {
        self.close_requested.store(true, Ordering::Release);
        if self.outstanding_permits.load(Ordering::Acquire) == 0 {
            self.close_complete_token.cancel();
        }
    }

    /// Returns after close has been requested and all outstanding permits have been returned.
    pub(crate) async fn close_complete(&self) {
        self.close_complete_token.cancelled().await;
    }

    /// Acquire a permit if one is available and close was not requested.
    pub(crate) fn try_acquire_owned(
        self: &Arc<Self>,
    ) -> Result<TrackedOwnedMeteredSemPermit<SK>, ()> {
        if self.close_requested.load(Ordering::Acquire) {
            return Err(());
        }
        self.outstanding_permits.fetch_add(1, Ordering::Release);
        let res = self.inner.try_acquire_owned();
        if res.is_err() {
            self.outstanding_permits.fetch_sub(1, Ordering::Release);
        }
        res.map(|permit| TrackedOwnedMeteredSemPermit {
            inner: Some(permit),
            on_drop: self.on_permit_dropped(),
        })
    }

    fn on_permit_dropped(self: &Arc<Self>) -> Box<dyn Fn() + Send + Sync> {
        let sem = self.clone();
        Box::new(move || {
            sem.outstanding_permits.fetch_sub(1, Ordering::Release);
            if sem.close_requested.load(Ordering::Acquire)
                && sem.outstanding_permits.load(Ordering::Acquire) == 0
            {
                sem.close_complete_token.cancel();
            }
        })
    }
}

/// Tracks an OwnedMeteredSemPermit and calls on_drop when dropped.
#[derive(derive_more::Debug)]
#[debug("Tracked({inner:?})")]
#[clippy::has_significant_drop]
pub(crate) struct TrackedOwnedMeteredSemPermit<SK: SlotKind> {
    inner: Option<OwnedMeteredSemPermit<SK>>,
    on_drop: Box<dyn Fn() + Send + Sync>,
}
impl<SK: SlotKind> From<TrackedOwnedMeteredSemPermit<SK>> for OwnedMeteredSemPermit<SK> {
    fn from(mut value: TrackedOwnedMeteredSemPermit<SK>) -> Self {
        value
            .inner
            .take()
            .expect("Inner permit should be available")
    }
}
impl<SK: SlotKind> Drop for TrackedOwnedMeteredSemPermit<SK> {
    fn drop(&mut self) {
        (self.on_drop)();
    }
}

/// Wraps an [SlotSupplierPermit] to update metrics & when it's dropped
#[clippy::has_significant_drop]
pub(crate) struct OwnedMeteredSemPermit<SK: SlotKind> {
    /// See [MeteredPermitDealer::unused_claimants]. If present when dropping, used to decrement the
    /// count.
    unused_claimants: Option<Arc<AtomicUsize>>,
    /// The actual [SlotSupplierPermit] is stored in here
    release_ctx: ReleaseCtx<SK>,
    #[allow(clippy::type_complexity)] // not really tho, bud
    use_fn: Box<dyn Fn(&UseCtx<SK>) + Send + Sync>,
    #[allow(clippy::type_complexity)] // not really tho, bud
    release_fn: Box<dyn Fn(&ReleaseCtx<SK>) + Send + Sync>,
}
impl<SK: SlotKind> Drop for OwnedMeteredSemPermit<SK> {
    fn drop(&mut self) {
        if let Some(uc) = self.unused_claimants.take() {
            uc.fetch_sub(1, Ordering::Release);
        }
        (self.release_fn)(&self.release_ctx);
    }
}
impl<SK: SlotKind> Debug for OwnedMeteredSemPermit<SK> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("OwnedMeteredSemPermit()")
    }
}
impl<SK: SlotKind> OwnedMeteredSemPermit<SK> {
    /// Should be called once this permit is actually being "used" for the work it was meant to
    /// permit.
    pub(crate) fn into_used(mut self, info: SK::Info) -> UsedMeteredSemPermit<SK> {
        if let Some(uc) = self.unused_claimants.take() {
            uc.fetch_sub(1, Ordering::Release);
        }
        let ctx = UseCtx {
            stored_info: &info,
            permit: &self.release_ctx.permit,
            meter: self.release_ctx.meter.clone(),
        };
        (self.use_fn)(&ctx);
        self.release_ctx.stored_info = Some(info);
        UsedMeteredSemPermit(self)
    }
}

#[derive(Debug)]
pub(crate) struct UsedMeteredSemPermit<SK: SlotKind>(#[allow(dead_code)] OwnedMeteredSemPermit<SK>);

macro_rules! dbg_panic {
  ($($arg:tt)*) => {
      error!($($arg)*);
      debug_assert!(false, $($arg)*);
  };
}
pub(crate) use dbg_panic;

pub(crate) struct ActiveCounter<F: Fn(usize)>(watch::Sender<usize>, Option<Arc<F>>);
impl<F> ActiveCounter<F>
where
    F: Fn(usize),
{
    pub(crate) fn new(a: watch::Sender<usize>, change_fn: Option<Arc<F>>) -> Self {
        a.send_modify(|v| {
            *v += 1;
            if let Some(cfn) = change_fn.as_ref() {
                cfn(*v);
            }
        });
        Self(a, change_fn)
    }
}
impl<F> Drop for ActiveCounter<F>
where
    F: Fn(usize),
{
    fn drop(&mut self) {
        self.0.send_modify(|v| {
            *v -= 1;
            if let Some(cfn) = self.1.as_ref() {
                cfn(*v)
            };
        });
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{advance_fut, worker::tuner::FixedSizeSlotSupplier};
    use futures_util::FutureExt;
    use temporal_sdk_core_api::worker::WorkflowSlotKind;

    pub(crate) fn fixed_size_permit_dealer<SK: SlotKind + Send + Sync + 'static>(
        size: usize,
    ) -> MeteredPermitDealer<SK> {
        MeteredPermitDealer::new(
            Arc::new(FixedSizeSlotSupplier::new(size)),
            MetricsContext::no_op(),
            None,
            Arc::new(Default::default()),
            None,
        )
    }

    #[test]
    fn closable_semaphore_permit_drop_returns_permit() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        let perm = sem.try_acquire_owned().unwrap();
        let permits = sem.outstanding_permits.load(Ordering::Acquire);
        assert_eq!(permits, 1);
        drop(perm);
        let permits = sem.outstanding_permits.load(Ordering::Acquire);
        assert_eq!(permits, 0);
    }

    #[tokio::test]
    async fn closable_semaphore_permit_drop_after_close_resolves_close_complete() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        let perm = sem.try_acquire_owned().unwrap();
        sem.close();
        drop(perm);
        sem.close_complete().await;
    }

    #[tokio::test]
    async fn closable_semaphore_close_complete_ready_if_unused() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        sem.close();
        sem.close_complete().await;
    }

    #[test]
    fn closable_semaphore_does_not_hand_out_permits_after_closed() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        sem.close();
        sem.try_acquire_owned().unwrap_err();
    }

    #[tokio::test]
    async fn respects_max_extant_permits() {
        let mut sem = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        sem.max_permits = Some(1);
        let perm = sem.try_acquire_owned().unwrap();
        sem.try_acquire_owned().unwrap_err();
        let acquire_fut = sem.acquire_owned();
        // Will be pending
        advance_fut!(acquire_fut);
        drop(perm);
        // Now it'll proceed
        acquire_fut.await;
    }
}
