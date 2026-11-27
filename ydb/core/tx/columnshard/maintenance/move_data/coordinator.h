#pragma once

#include "events.h"

#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/queue_sizes.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NColumnShard {

class TColumnShard;

class TMoveDataCoordinator {
private:
    // On restart Hive resends the request; session progress is not persisted.
    struct TSession {
        THashSet<TActorId> Subscribers;
        THashSet<ui32> TargetGroups;
        bool VacuumCompleted = false;
        bool TargetsChanged = false;
    };

    TColumnShard* Self;
    TSession State;
    TActorId ScanActor;
    size_t Pending = 0;
    bool WakeupScheduled = false;
    bool PokeScheduled = false;

    void RestartScan(const TActorContext& ctx);
    enum class ECompletionBlocker {
        None,
        Vacuum,
        Portions,
        SharedBlobs,
    };

    NOlap::NActualizer::TMoveDataQueueSizes UpdateQueueSizes();
    ECompletionBlocker GetCompletionBlocker(const NOlap::NActualizer::TMoveDataQueueSizes& queues) const;
    void ReportProgress(const NOlap::NActualizer::TMoveDataQueueSizes& queues, ECompletionBlocker blocker);
    void FinishMigration(const TActorContext& ctx);
    void StartPortionRewrites();
    void RunMigrationStep(const TActorContext& ctx);

public:
    explicit TMoveDataCoordinator(TColumnShard* self)
        : Self(self)
    {
    }

    bool IsActive() const {
        return !State.Subscribers.empty();
    }

    void AcceptRequest(const TActorId& sender, const THashSet<ui32>& requested, const TActorContext& ctx);
    void StopScan(const TActorContext& ctx);
    void OnVacuumCompleted(const TActorContext& ctx);
    void RequestStep(const TActorContext& ctx);
    void OnWakeup(const TActorContext& ctx);
    void OnPoke(const TActorContext& ctx);
    void OnBatch(TEvMoveData::TEvBatch::TPtr& ev, const TActorContext& ctx);
};

}   // namespace NKikimr::NColumnShard
