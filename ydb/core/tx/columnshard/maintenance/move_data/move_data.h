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
        EvWakeup = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPoke,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE));

    // The move's own cadence, so it does not ride the tablet's periodic wakeup.
    struct TEvWakeup: public NActors::TEventLocal<TEvWakeup, EvWakeup> {};

    // Run one driver turn now instead of waiting for the cadence tick.
    struct TEvPoke: public NActors::TEventLocal<TEvPoke, EvPoke> {};
};

// Stateless: nothing is persisted; on restart Hive re-sends TEvMoveData.
struct TMoveDataState {
    NActors::TActorId HiveSender;
    THashSet<ui32> TargetGroups;
    bool Active = false;
    // Set by the executor's MoveDataCompleted(): the local-DB vacuum leg is done.
    bool VacuumCompleted = false;
    // The driver re-seeds the data path for the new set before any gate check may pass.
    bool TargetsChanged = false;
};

// Owns the move loop (re-seed, metadata re-arm, rewrites, gate) on its own cadence; same mailbox as the tablet, so not CPU offload.
class TMoveDataDriver: public NActors::TActorBootstrapped<TMoveDataDriver> {
private:
    TColumnShard* Self;
    TMoveDataState State;
    // Dropped to zero by TColumnShard::Die before it poisons us; an event queued in between must not touch Self.
    const std::shared_ptr<TAtomicCounter> TabletActivity;
    // Periodic fallback interval; pokes run a turn sooner.
    static constexpr TDuration Cadence = TDuration::Seconds(5);

    void ScheduleWakeup(const NActors::TActorContext& ctx) {
        ctx.Schedule(Cadence, new TEvMoveData::TEvWakeup());
    }

    bool IsOwnerAlive() const {
        return TabletActivity->Val() != 0;
    }

    void AcceptRequest(const NActors::TActorId& sender, const THashSet<ui32>& requested, const NActors::TActorContext& ctx);
    void RestartMoveDataActualizer();
    void SetupMoveDataMetadata();
    void SetupMoveDataRewrites();
    void CheckMoveDataGate(const NActors::TActorContext& ctx);
    void StartAndCheckGate(const NActors::TActorContext& ctx);

    void Handle(TEvMoveData::TEvWakeup::TPtr&, const NActors::TActorContext& ctx);
    void Handle(TEvMoveData::TEvPoke::TPtr&, const NActors::TActorContext& ctx);

public:
    TMoveDataDriver(TColumnShard* self, const std::shared_ptr<TAtomicCounter>& tabletActivity)
        : Self(self)
        , TabletActivity(tabletActivity)
    {
    }

    static void StartOrUpdateMigration(
        TColumnShard& owner, const NKikimrTabletBase::TEvMoveData& request, const NActors::TActorId& sender, const NActors::TActorContext& ctx);
    static void Stop(TColumnShard& owner, const NActors::TActorContext& ctx);
    void OnVacuumCompleted(const NActors::TActorContext& ctx);

    void Bootstrap(const NActors::TActorContext& ctx) {
        Become(&TThis::StateWork);
        // The tablet pokes after registering; this is the periodic fallback.
        ScheduleWakeup(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvMoveData::TEvWakeup, Handle);
            HFunc(TEvMoveData::TEvPoke, Handle);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
            default:
                break;
        }
    }
};

}   // namespace NKikimr::NColumnShard
