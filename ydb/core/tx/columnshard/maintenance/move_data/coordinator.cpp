#include "coordinator.h"
#include "scan.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

void TMoveDataCoordinator::AcceptRequest(const TActorId& sender, const THashSet<ui32>& requested, const TActorContext& ctx) {
    State.Subscribers.insert(sender);
    for (const auto groupId : requested) {
        State.TargetsChanged |= State.TargetGroups.emplace(groupId).second;
    }
    YDB_LOG_INFO("MoveData requested", {"tabletId", Self->TabletID()}, {"groups", State.TargetGroups.size()}, {"changed", State.TargetsChanged});
    RequestStep(ctx);
}

void TMoveDataCoordinator::StopScan(const TActorContext& ctx) {
    if (ScanActor) {
        ctx.Send(ScanActor, new TEvents::TEvPoisonPill());
        ScanActor = {};
    }
}

void TMoveDataCoordinator::RestartScan(const TActorContext& ctx) {
    StopScan(ctx);
    State.TargetsChanged = false;
    Pending = 0;
    if (!Self->HasIndex()) {
        return;
    }
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    index.StopMoveData();
    std::vector<TMoveDataScanPortion> portions;
    const auto append = [&](const auto& source) {
        for (const auto& [_, portion] : source) {
            if (portion->HasRemoveSnapshot()) {
                continue;
            }
            const auto schema = portion->GetSchema(index.GetVersionedIndex());
            if (portion->HasBlobsInBlobStorage(schema->GetIndexInfo())) {
                portions.push_back({ portion->GetPathId(), portion->GetPortionId(), portion->PredictAccessorsMemory(schema) });
            }
        }
    };
    for (const auto& [_, granule] : index.GetTables()) {
        append(granule->GetPortions());
        append(granule->GetInsertedPortions());
    }
    Pending = portions.size();
    if (Pending) {
        ScanActor = ctx.Register(CreateMoveDataScan(Self->SelfId(), std::move(portions), State.TargetGroups,
                                     NYDBTest::TControllers::GetColumnShardController()->GetMetadataRequestSoftMemoryLimit()),
            TMailboxType::HTSwap, AppDataVerified().BatchPoolId);
    }
}

void TMoveDataCoordinator::OnBatch(TEvMoveData::TEvBatch::TPtr& ev, const TActorContext& ctx) {
    if (ev->Sender != ScanActor || State.TargetsChanged) {
        return;
    }
    const auto& batch = *ev->Get();
    auto next = std::make_unique<TEvMoveData::TEvContinue>();
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    const auto visit = [&](const TMoveDataScanPortion& item, bool missing) {
        const auto granule = index.GetGranuleOptional(item.PathId);
        const auto portion = granule ? granule->GetPortionOptional(item.PortionId, false) : nullptr;
        if (portion && !portion->HasRemoveSnapshot()) {
            if (missing) {
                next->Retry.push_back(item);
            } else {
                granule->AddMoveDataCandidate(*portion);
            }
        }
    };
    for (const auto& item : batch.Matches) {
        visit(item, false);
    }
    for (const auto& item : batch.Missing) {
        visit(item, true);
    }
    const size_t classified = batch.Matches.size() + batch.Rejected + batch.Missing.size() - next->Retry.size();
    AFL_VERIFY(Pending >= classified);
    Pending -= classified;
    Self->Counters.GetCSCounters().OnMoveDataPortionsRejected(batch.Rejected);
    ctx.Send(ScanActor, next.release());
    if (!Pending) {
        ScanActor = {};
    }
    RunMigrationStep(ctx);
}

void TMoveDataCoordinator::RequestStep(const TActorContext& ctx) {
    if (IsActive() && !PokeScheduled) {
        PokeScheduled = true;
        ctx.Send(Self->SelfId(), new TEvMoveData::TEvPoke());
    }
}

void TMoveDataCoordinator::OnVacuumCompleted(const TActorContext& ctx) {
    State.VacuumCompleted = true;
    RequestStep(ctx);
}

NOlap::NActualizer::TMoveDataQueueSizes TMoveDataCoordinator::UpdateQueueSizes() {
    if (!Self->HasIndex()) {
        return {};
    }
    return Self->MutableIndexAs<NOlap::TColumnEngineForLogs>().UpdateMoveDataQueueSizes();
}

TMoveDataCoordinator::ECompletionBlocker TMoveDataCoordinator::GetCompletionBlocker(
    const NOlap::NActualizer::TMoveDataQueueSizes& queues) const {
    if (!State.VacuumCompleted) {
        return ECompletionBlocker::Vacuum;
    }
    if (queues.GetTotal() != 0) {
        return ECompletionBlocker::Portions;
    }
    const auto& sharedBlobs = Self->GetStoragesManager()->GetDefaultOperator()->GetSharedBlobs();
    AFL_VERIFY(sharedBlobs);
    if (sharedBlobs->HasBlobsForGroups(State.TargetGroups)) {
        return ECompletionBlocker::SharedBlobs;
    }
    return ECompletionBlocker::None;
}

void TMoveDataCoordinator::ReportProgress(const NOlap::NActualizer::TMoveDataQueueSizes& queues, ECompletionBlocker blocker) {
    Self->Counters.GetCSCounters().OnMoveDataGateChecked();
    Self->Counters.GetCSCounters().OnMoveDataQueues(queues);
    switch (blocker) {
        case ECompletionBlocker::None:
            break;
        case ECompletionBlocker::Vacuum:
            Self->Counters.GetCSCounters().OnMoveDataGateBlockedByVacuum();
            break;
        case ECompletionBlocker::Portions:
            Self->Counters.GetCSCounters().OnMoveDataGateBlockedByPortions();
            if (queues.Uncommitted) {
                YDB_LOG_INFO("MoveData gate waits for uncommitted writes", {"tabletId", Self->TabletID()}, {"uncommitted", queues.Uncommitted});
            }
            break;
        case ECompletionBlocker::SharedBlobs:
            Self->Counters.GetCSCounters().OnMoveDataGateBlockedByShared();
            YDB_LOG_INFO("MoveData gate waits for shared blobs", {"tabletId", Self->TabletID()});
            break;
    }
}

void TMoveDataCoordinator::FinishMigration(const TActorContext& ctx) {
    YDB_LOG_INFO("MoveData gate passed", {"tabletId", Self->TabletID()});

    if (Self->HasIndex()) {
        Self->MutableIndexAs<NOlap::TColumnEngineForLogs>().StopMoveData();
    }
    // Removal snapshots are durable. Cleanup can resume after Hive restarts the tablet.
    for (const auto& subscriber : State.Subscribers) {
        ctx.Send(subscriber, new TEvTablet::TEvMoveDataResponse(Self->TabletID(), NKikimrTabletBase::TEvMoveDataResponse::Success));
    }
    Self->Counters.GetCSCounters().OnMoveDataFinished();
    State = {};
}

void TMoveDataCoordinator::StartPortionRewrites() {
    const ui64 memoryUsageLimit = AppDataVerified().ColumnShardConfig.GetTieringsMemoryLimit();
    std::vector<std::shared_ptr<NOlap::TTTLColumnEngineChanges>> indexChanges = Self->TablesManager.MutablePrimaryIndex().StartTtl(
        {}, Self->DataLocksManager, memoryUsageLimit, NOlap::NActualizer::EActualizationScope::MoveDataOnly);
    if (indexChanges.empty()) {
        return;
    }
    Self->StartTtlChanges(std::move(indexChanges));
}

void TMoveDataCoordinator::RunMigrationStep(const TActorContext& ctx) {
    if (!IsActive()) {
        return;
    }
    if (!WakeupScheduled) {
        WakeupScheduled = true;
        ctx.Schedule(TDuration::Seconds(5), new TEvMoveData::TEvWakeup());
    }
    if (State.TargetsChanged) {
        // Initialize the portion queue only after deferred loading has populated the portion catalog.
        if (Self->HasIndex() && !Self->GetIndexAs<NOlap::TColumnEngineForLogs>().AreAllPortionsLoaded(std::nullopt)) {
            return;
        }
        RestartScan(ctx);
    }
    auto queues = UpdateQueueSizes();
    queues.Pending = Pending;
    if (queues.ConfirmedToMove || queues.RewriteStarted) {
        StartPortionRewrites();
    }
    const auto blocker = GetCompletionBlocker(queues);
    ReportProgress(queues, blocker);
    if (blocker == ECompletionBlocker::None) {
        FinishMigration(ctx);
    }
}

void TMoveDataCoordinator::OnWakeup(const TActorContext& ctx) {
    WakeupScheduled = false;
    RunMigrationStep(ctx);
}

void TMoveDataCoordinator::OnPoke(const TActorContext& ctx) {
    PokeScheduled = false;
    RunMigrationStep(ctx);
}

}   // namespace NKikimr::NColumnShard
