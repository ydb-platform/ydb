#include "columnshard_impl.h"

#include "bg_tasks/manager/manager.h"

namespace NKikimr::NColumnShard {

bool TColumnShard::HoldUntilMediatorTimeReady(TAutoPtr<IEventHandle>& ev) {
    if (MediatorTime.Ready()) {
        return false;
    }

    // Adding an event here changes its order relative to queued events. Add it only if it must run early and can safely overtake them.
    switch (ev->GetTypeRewrite()) {
        // The shard needs the proxy's timecast entry to read mediator time.
        case TEvMediatorTimecast::TEvRegisterTabletResult::EventType:
        // After a restart, a coordinator reply supplies the step the shard must catch up to.
        case TEvMediatorTimecast::TEvSubscribeReadStepResult::EventType:
        // When the mediator reaches the requested step, this notification wakes the waiting shard.
        case TEvMediatorTimecast::TEvNotifyPlanStep::EventType:
        // The mediator can keep time frozen below this step until the shard acknowledges it.
        case TEvTxProcessing::TEvPlanStep::EventType:
        // Operators need the queue depth while catch-up is stalled.
        case TEvTablet::TEvGetCounters::EventType:
        // The tablet monitoring page must remain accessible to diagnose a stalled shard.
        case NMon::TEvRemoteHttpInfo::EventType:
        // A forced shutdown must still detach the executor if catch-up is stuck.
        case NActors::TEvents::TEvPoison::EventType:
        // The system tablet needs the stop acknowledgment to finish graceful shutdown.
        case TEvTablet::TEvTabletStop::EventType:
        // Once the system tablet dies, the shard must tear down its executor and exit.
        case TEvTablet::TEvTabletDead::EventType:
        // Losing the subdomain watch requires a shard restart even during catch-up.
        case TEvTxProxySchemeCache::TEvWatchNotifyUnavailable::EventType:
            return false;
        case TEvColumnShard::TEvProposeTransaction::EventType:
            // A fresh shard skips catch-up but needs this proposal's processing parameters for its first registration with the proxy.
            if (!ProcessingParams && ev->Get<TEvColumnShard::TEvProposeTransaction>()->Record.HasProcessingParams()) {
                return false;
            }
            break;
    }

    MediatorTime.HoldEvent(std::move(ev));
    return true;
}

void TColumnShard::TryMakeMediatorTimeReady(const TActorContext& ctx) {
    if (!MediatorTime.TryBecomeReady(ctx)) {
        return;
    }
    StartupTimeline.OnMediatorTimeReady();
    if (CurrentStateFunc() == &TThis::StateWork) {
        OnMediatorTimeReady();
    }
}

void TColumnShard::Handle(TEvMediatorTimecast::TEvRegisterTabletResult::TPtr& ev, const TActorContext& ctx) {
    Y_ABORT_UNLESS(ev->Get()->TabletId == TabletID());
    MediatorTime.OnRegistered(ev->Get()->Entry);
    TryMakeMediatorTimeReady(ctx);
}

void TColumnShard::Handle(TEvMediatorTimecast::TEvNotifyPlanStep::TPtr& ev, const TActorContext& ctx) {
    Y_ABORT_UNLESS(ev->Get()->TabletId == TabletID());
    MediatorTime.OnNotifyPlanStep();
    TryMakeMediatorTimeReady(ctx);
    if (CurrentStateFunc() == &TThis::StateWork) {
        RescheduleWaitingReads();
    }
}

void TColumnShard::Handle(TEvMediatorTimecast::TEvSubscribeReadStepResult::TPtr& ev, const TActorContext& ctx) {
    MediatorTime.OnNextReadStep(ev->Get()->CoordinatorId, ev->Get()->NextReadStep, ctx);
    TryMakeMediatorTimeReady(ctx);
}

void TColumnShard::OnMediatorTimeReady() {
    // dispatched here, not re-sent, so that events already in the mailbox cannot overtake them
    for (TAutoPtr<IEventHandle>& ev : MediatorTime.ExtractHeldEvents()) {
        StateWork(ev);
    }

    EnqueueBackgroundActivities();
    BackgroundSessionsManager->Start();
}

void TColumnShard::WaitPlanStep(ui64 step) {
    YDB_LOG_VERIFY(MediatorTime.Ready());
    if (!ProgressTxController->GetPlannedTx()) {
        MediatorTime.WaitStep(step, ActorContext());
    }
}

ui64 TColumnShard::GetOutdatedStep() const {
    return MediatorTime.Ready() ? Max(LastPlannedStep, MediatorTime.GetStepVerified()) : LastPlannedStep;
}

TDuration TColumnShard::GetTxCompleteLag() const {
    return ProgressTxController->GetTxCompleteLag(MediatorTime.GetStepVerified());
}

}   // namespace NKikimr::NColumnShard
