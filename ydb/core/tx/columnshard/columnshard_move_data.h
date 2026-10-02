#pragma once

#include "columnshard_private_events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NColumnShard {

class TColumnShard;

// Stateless v1: no persistence; on restart Hive re-sends TEvMoveData.
struct TMoveDataState {
    TActorId HiveSender;
    THashSet<ui32> TargetGroups;
    bool Active = false;
    // Set by the executor's MoveDataCompleted(): vacuum done, the blob gates still pending.
    bool VacuumCompleted = false;
    // The actualizer count is cumulative; track what was reported to keep the sensor a rate.
    ui64 ReportedRejections = 0;
    // The driver restarts the actualizer for the new set before any gate check may pass.
    bool TargetsChanged = false;
};

// Owns the move loop (relaunch, metadata re-arm, rewrites, gate) on its own cadence; same mailbox as the tablet, so not CPU offload.
class TMoveDataDriver: public TActorBootstrapped<TMoveDataDriver> {
private:
    TColumnShard* Self;
    // Dropped to zero by TColumnShard::Die before it poisons us; an event queued in between must not touch Self.
    const std::shared_ptr<TAtomicCounter> TabletActivity;
    // Periodic fallback interval; pokes run a turn sooner.
    static constexpr TDuration Cadence = TDuration::Seconds(5);

    void ScheduleWakeup(const TActorContext& ctx) {
        ctx.Schedule(Cadence, new TEvPrivate::TEvMoveDataWakeup());
    }

    bool IsOwnerAlive() const {
        return TabletActivity->Val() != 0;
    }

    void StartAndCheckGate(const TActorContext& ctx);

    void Handle(TEvPrivate::TEvMoveDataWakeup::TPtr&, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvMoveDataPoke::TPtr&, const TActorContext& ctx);

public:
    TMoveDataDriver(TColumnShard* self, const std::shared_ptr<TAtomicCounter>& tabletActivity)
        : Self(self)
        , TabletActivity(tabletActivity)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWork);
        // The tablet pokes after registering; this is the periodic fallback.
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
