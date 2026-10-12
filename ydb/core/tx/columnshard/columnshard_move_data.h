#pragma once

#include "columnshard_private_events.h"

#include "resource_subscriber/task.h"

#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/queue_sizes.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimrTabletBase {
class TEvMoveData;
}

namespace NKikimr::NColumnShard {

class TColumnShard;

// The driver and tablet share a mailbox; synchronous admission precedes queued gate checks.
class TMoveDataDriver: public TActorBootstrapped<TMoveDataDriver> {
private:
    // On restart Hive resends the request; session progress is not persisted.
    struct TSession {
        TActorId HiveSender;
        THashSet<ui32> TargetGroups;
        bool Active = false;
        bool VacuumCompleted = false;
        ui64 ReportedRejections = 0;
        bool TargetsChanged = false;
    };

    TColumnShard* const Self;
    // Dropped before the tablet stops the driver, so queued events cannot touch a dead owner.
    const std::shared_ptr<TAtomicCounter> TabletActivity;
    TSession State;
    const NOlap::NResourceBroker::NSubscribe::TTaskContext MetadataTaskSubscription;
    const std::shared_ptr<TAtomicCounter> MetadataRequestsInFlight = std::make_shared<TAtomicCounter>();
    static constexpr TDuration Cadence = TDuration::Seconds(5);

    void ScheduleWakeup(const TActorContext& ctx) {
        ctx.Schedule(Cadence, new TEvPrivate::TEvMoveDataWakeup());
    }

    bool IsOwnerAlive() const {
        return TabletActivity->Val() != 0;
    }

    void AcceptRequest(const TActorId& sender, const THashSet<ui32>& requested, const TActorContext& ctx);
    void RestartActualizer();
    NOlap::NActualizer::TMoveDataQueueSizes UpdateQueueSizes();
    void CheckGate(const TActorContext& ctx, const NOlap::NActualizer::TMoveDataQueueSizes& queues);
    void SetupMetadata();
    void SetupRewrites();
    void StartAndCheckGate(const TActorContext& ctx);

    void Handle(TEvPrivate::TEvMoveDataWakeup::TPtr&, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvMoveDataPoke::TPtr&, const TActorContext& ctx);

public:
    TMoveDataDriver(TColumnShard* self, const std::shared_ptr<TAtomicCounter>& tabletActivity);

    // Called on the owner's mailbox, before another queued driver turn can pass the gate.
    static void Request(TColumnShard& owner, const NKikimrTabletBase::TEvMoveData& request, const TActorId& sender, const TActorContext& ctx);
    static void Stop(TColumnShard& owner, const TActorContext& ctx);
    void OnVacuumCompleted(const TActorContext& ctx);

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWork);
        ScheduleWakeup(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvMoveDataWakeup, Handle);
            HFunc(TEvPrivate::TEvMoveDataPoke, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                break;
        }
    }
};

}   // namespace NKikimr::NColumnShard
