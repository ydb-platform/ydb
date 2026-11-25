#pragma once

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/queue_sizes.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash_set.h>
#include <util/generic/refcount.h>

#include <memory>

namespace NKikimr::NColumnShard {

class TColumnShard;

struct TEvMoveData {
    enum EEv {
        EvWakeup = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvPoke,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE));

    // The move's own cadence, so it no longer rides the tablet's periodic wakeup.
    struct TEvWakeup: public NActors::TEventLocal<TEvWakeup, EvWakeup> {};

    // Run one driver turn now instead of waiting for the cadence tick.
    struct TEvPoke: public NActors::TEventLocal<TEvPoke, EvPoke> {};
};

// The driver and tablet share a mailbox; synchronous admission precedes queued gate checks.
class TMoveDataDriver: public TActorBootstrapped<TMoveDataDriver> {
private:
    // On restart Hive resends the request; session progress is not persisted.
    struct TSession {
        THashSet<TActorId> Subscribers;
        THashSet<ui32> TargetGroups;
        bool VacuumCompleted = false;
        ui64 ReportedRejections = 0;
        bool TargetsChanged = false;
    };

    TColumnShard* Self;
    TSession State;
    const NOlap::NResourceBroker::NSubscribe::TTaskContext MetadataTaskSubscription;
    const std::shared_ptr<TAtomicCounter> MetadataSubscribersInFlight = std::make_shared<TAtomicCounter>();
    static constexpr TDuration Cadence = TDuration::Seconds(5);

private:
    void ScheduleWakeup(const TActorContext& ctx) {
        ctx.Schedule(Cadence, new TEvMoveData::TEvWakeup());
    }

    void RestartActualizer();
    NOlap::NActualizer::TMoveDataQueueSizes UpdateQueueSizes();
    enum class ECompletionBlocker {
        None,
        Vacuum,
        Portions,
        Cleanup,
        SharedBlobs,
    };

    ECompletionBlocker GetCompletionBlocker(const NOlap::NActualizer::TMoveDataQueueSizes& queues) const;
    void ReportProgress(const NOlap::NActualizer::TMoveDataQueueSizes& queues, ECompletionBlocker blocker);
    void FinishMigration(const TActorContext& ctx);
    void SetupMetadataRequests();
    void StartPortionRewrites();
    void RunMigrationStep(const TActorContext& ctx);

    void Handle(TEvMoveData::TEvWakeup::TPtr&, const TActorContext& ctx);
    void Handle(TEvMoveData::TEvPoke::TPtr&, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvMetadataAccessorsInfo::TPtr& ev, const TActorContext& ctx);

public:
    explicit TMoveDataDriver(TColumnShard* self);

    void AcceptRequest(const TActorId& sender, const THashSet<ui32>& requested, const TActorContext& ctx);
    static void Stop(TColumnShard& owner, const TActorContext& ctx);
    void OnVacuumCompleted(const TActorContext& ctx);

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWork);
        ScheduleWakeup(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvMoveData::TEvWakeup, Handle);
            HFunc(TEvMoveData::TEvPoke, Handle);
            HFunc(TEvPrivate::TEvMetadataAccessorsInfo, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                break;
        }
    }
};

}   // namespace NKikimr::NColumnShard
