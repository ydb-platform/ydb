#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash_set.h>
#include <util/generic/refcount.h>

#include <memory>

namespace NKikimrTabletBase {
class TEvMoveData;
}

namespace NKikimr::NColumnShard {

class TColumnShard;

struct TEvMoveData {
    enum EEv {
        EvWakeup = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvPoke,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE));

    // The move's own cadence, so it does not ride the tablet's periodic wakeup.
    struct TEvWakeup: public NActors::TEventLocal<TEvWakeup, EvWakeup> {};

    // Run one driver turn now instead of waiting for the cadence tick.
    struct TEvPoke: public NActors::TEventLocal<TEvPoke, EvPoke> {};
};

// Stateless: nothing is persisted; on restart Hive re-sends TEvMoveData.
struct TMoveDataState {
    TActorId HiveSender;
    THashSet<ui32> TargetGroups;
    bool Active = false;
    // Set by the executor's MoveDataCompleted(): the local-DB vacuum leg is done.
    bool VacuumCompleted = false;
    // The driver re-seeds the data path for the new set before any gate check may pass.
    bool TargetsChanged = false;
};

// Owns the move loop (re-seed, metadata re-arm, rewrites, gate) on its own cadence; same mailbox as the tablet, so not CPU offload.
class TMoveDataDriver: public TActorBootstrapped<TMoveDataDriver> {
private:
    TColumnShard* Self;
    TMoveDataState State;
    // Dropped to zero by TColumnShard::Die before it poisons us; an event queued in between must not touch Self.
    const std::shared_ptr<TAtomicCounter> TabletActivity;
    // Periodic fallback interval; pokes run a turn sooner.
    static constexpr TDuration Cadence = TDuration::Seconds(5);

    void ScheduleWakeup(const TActorContext& ctx) {
        ctx.Schedule(Cadence, new TEvMoveData::TEvWakeup());
    }

    bool IsOwnerAlive() const {
        return TabletActivity->Val() != 0;
    }

    void AcceptRequest(const TActorId& sender, const THashSet<ui32>& requested, const TActorContext& ctx);
    void RestartMoveDataActualizer();
    void SetupMoveDataMetadata();
    void SetupMoveDataRewrites();
    void CheckMoveDataGate(const TActorContext& ctx);
    void StartAndCheckGate(const TActorContext& ctx);

    void Handle(TEvMoveData::TEvWakeup::TPtr&, const TActorContext& ctx);
    void Handle(TEvMoveData::TEvPoke::TPtr&, const TActorContext& ctx);

public:
    TMoveDataDriver(TColumnShard* self, const std::shared_ptr<TAtomicCounter>& tabletActivity)
        : Self(self)
        , TabletActivity(tabletActivity)
    {
    }

    static void StartOrUpdateMigration(
        TColumnShard& owner, const NKikimrTabletBase::TEvMoveData& request, const TActorId& sender, const TActorContext& ctx);
    static void Stop(TColumnShard& owner, const TActorContext& ctx);
    void OnVacuumCompleted(const TActorContext& ctx);

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWork);
        // The tablet pokes after registering; this is the periodic fallback.
        ScheduleWakeup(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvMoveData::TEvWakeup, Handle);
            HFunc(TEvMoveData::TEvPoke, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                break;
        }
    }
};

}   // namespace NKikimr::NColumnShard
