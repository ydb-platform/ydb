#pragma once
#include "catch_up.h"

#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/tx/time_cast/time_cast.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>

#include <util/generic/set.h>

#include <memory>
#include <optional>
#include <vector>

namespace NKikimr::NColumnShard {

class TTabletCountersHandle;

class TMediatorTime {
private:
    enum class EState {
        Unregistered,
        WaitingForReadiness,
        Ready
    };

    const ui64 TabletId;
    const std::shared_ptr<const TTabletCountersHandle> TabletCounters;
    EState State = EState::Unregistered;
    TMediatorTimecastEntry::TCPtr Entry;
    TSet<ui64> WaitingSteps;
    std::optional<TMediatorTimeCatchUp> CatchUp;
    std::vector<TAutoPtr<NActors::IEventHandle>> HeldUntilReady;

    void UpdateCounters() const;
    void StopCatchUp(const NActors::TActorContext& ctx);

public:
    TMediatorTime(const ui64 tabletId, const std::shared_ptr<const TTabletCountersHandle>& tabletCounters)
        : TabletId(tabletId)
        , TabletCounters(tabletCounters)
    {
    }

    void Register(const NKikimrSubDomains::TProcessingParams& processingParams, const NActors::TActorContext& ctx);
    void RegisterAndStartCatchUp(const NKikimrSubDomains::TProcessingParams& processingParams, const NActors::TActorContext& ctx);
    void Unregister(const NActors::TActorContext& ctx);

    bool Ready() const {
        return State == EState::Ready;
    }

    ui64 GetStepVerified() const;
    void WaitStep(const ui64 step, const NActors::TActorContext& ctx);

    void OnRegistered(const TMediatorTimecastEntry::TCPtr& entry);
    void OnNotifyPlanStep();
    void OnNextReadStep(const ui64 coordinatorId, const ui64 nextReadStep, const NActors::TActorContext& ctx);

    bool TryBecomeReady(const NActors::TActorContext& ctx);

    void HoldEvent(TAutoPtr<NActors::IEventHandle> ev);
    [[nodiscard]] std::vector<TAutoPtr<NActors::IEventHandle>> ExtractHeldEvents();
};

}   // namespace NKikimr::NColumnShard
