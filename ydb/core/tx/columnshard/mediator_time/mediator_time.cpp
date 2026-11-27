#include "mediator_time.h"

#include <ydb/core/tx/columnshard/counters/tablet_counters.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

void TMediatorTime::UpdateCounters() const {
    TabletCounters->SetCounter(COUNTER_EVENTS_WAITING_MEDIATOR_TIME, HeldUntilReady.size());
}

void TMediatorTime::Register(const NKikimrSubDomains::TProcessingParams& processingParams, const NActors::TActorContext& ctx) {
    YDB_LOG_VERIFY(State == EState::Unregistered);
    ctx.Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvRegisterTablet(TabletId, processingParams));
    State = EState::WaitingForReadiness;
}

void TMediatorTime::RegisterAndStartCatchUp(const NKikimrSubDomains::TProcessingParams& processingParams, const NActors::TActorContext& ctx) {
    Register(processingParams, ctx);
    const THashSet<ui64> coordinators(processingParams.GetCoordinators().begin(), processingParams.GetCoordinators().end());
    for (const ui64 coordinatorId : coordinators) {
        ctx.Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvSubscribeReadStep(coordinatorId));
    }
    CatchUp = TMediatorTimeCatchUp(coordinators);
    YDB_LOG_INFO("",
        {"event", "mediator_time_catch_up_started"},
        {"coordinators", coordinators.size()});
}

void TMediatorTime::StopCatchUp(const NActors::TActorContext& ctx) {
    if (CatchUp) {
        ctx.Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvUnsubscribeReadStep());
        CatchUp.reset();
    }
}

void TMediatorTime::Unregister(const NActors::TActorContext& ctx) {
    if (State == EState::Unregistered) {
        return;
    }
    StopCatchUp(ctx);
    ctx.Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvUnregisterTablet(TabletId));
    State = EState::Unregistered;
    Entry = nullptr;
}

ui64 TMediatorTime::GetStepVerified() const {
    YDB_LOG_VERIFY(Ready());
    return Entry->Get(TabletId);
}

void TMediatorTime::WaitStep(const ui64 step, const NActors::TActorContext& ctx) {
    YDB_LOG_VERIFY(State != EState::Unregistered && Entry);
    if (!WaitingSteps.empty() && *WaitingSteps.begin() <= step) {
        return;
    }
    WaitingSteps.insert(step);
    ctx.Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvWaitPlanStep(TabletId, step));
    YDB_LOG_DEBUG("Waiting for plan step from mediator time cast",
        {"planStep", step});
}

void TMediatorTime::OnRegistered(const TMediatorTimecastEntry::TCPtr& entry) {
    YDB_LOG_VERIFY(entry);
    Entry = entry;
    YDB_LOG_DEBUG("Registered with mediator time cast",
        {"mediatorStep", Entry->Get(TabletId)});
}

void TMediatorTime::OnNotifyPlanStep() {
    YDB_LOG_VERIFY(Entry);
    const ui64 step = Entry->Get(TabletId);
    YDB_LOG_DEBUG("Notified by mediator time cast",
        {"planStep", step});
    WaitingSteps.erase(WaitingSteps.begin(), WaitingSteps.upper_bound(step));
}

void TMediatorTime::OnNextReadStep(const ui64 coordinatorId, const ui64 nextReadStep, const NActors::TActorContext& ctx) {
    YDB_LOG_DEBUG("Got read step of coordinator",
        {"coordinatorId", coordinatorId},
        {"nextReadStep", nextReadStep});
    if (!CatchUp) {
        return;
    }
    CatchUp->OnNextReadStep(coordinatorId, nextReadStep);
    ctx.Send(MakeMediatorTimecastProxyID(), new TEvMediatorTimecast::TEvUnsubscribeReadStep(coordinatorId));
}

bool TMediatorTime::TryBecomeReady(const NActors::TActorContext& ctx) {
    if (State != EState::WaitingForReadiness || !Entry) {
        return false;
    }
    const ui64 mediatorStep = Entry->Get(TabletId);
    if (CatchUp) {
        const auto result = CatchUp->Check(mediatorStep);
        switch (result.Status) {
            case TMediatorTimeCatchUp::EStatus::WaitingForCoordinator:
                return false;
            case TMediatorTimeCatchUp::EStatus::WaitingForStep:
                WaitStep(result.TargetStep, ctx);
                return false;
            case TMediatorTimeCatchUp::EStatus::CaughtUp:
                break;
        }
        StopCatchUp(ctx);
        YDB_LOG_INFO("",
            {"event", "mediator_time_caught_up"},
            {"targetStep", result.TargetStep},
            {"mediatorStep", mediatorStep},
            {"heldEvents", HeldUntilReady.size()});
    }
    State = EState::Ready;
    return true;
}

void TMediatorTime::HoldEvent(TAutoPtr<NActors::IEventHandle> ev) {
    HeldUntilReady.emplace_back(std::move(ev));
    UpdateCounters();
}

std::vector<TAutoPtr<NActors::IEventHandle>> TMediatorTime::ExtractHeldEvents() {
    std::vector<TAutoPtr<NActors::IEventHandle>> result;
    result.swap(HeldUntilReady);
    UpdateCounters();
    return result;
}

}   // namespace NKikimr::NColumnShard
