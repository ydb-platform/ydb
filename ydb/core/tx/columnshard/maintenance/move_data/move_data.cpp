#include "move_data.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

TMoveDataDriver::TMoveDataDriver(TColumnShard* self)
    : Self(self)
    , MetadataTaskSubscription("CS::MOVE_DATA", self->Counters.GetSubscribeCounters())
{
}

void TMoveDataDriver::AcceptRequest(const TActorId& sender, const THashSet<ui32>& requested, const TActorContext& ctx) {
    State.Subscribers.insert(sender);
    for (const auto groupId : requested) {
        State.TargetsChanged |= State.TargetGroups.emplace(groupId).second;
    }
    YDB_LOG_INFO("MoveData requested", {"tabletId", Self->TabletID()}, {"groups", State.TargetGroups.size()}, {"changed", State.TargetsChanged});
    ctx.Send(SelfId(), new TEvMoveData::TEvPoke());
}

void TMoveDataDriver::RestartActualizer() {
    State.TargetsChanged = false;
    // A fresh actualizer counts rejections from zero again.
    State.ReportedRejections = 0;
    if (!Self->HasIndex()) {
        return;
    }
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    // A changed target set replaces the actualizers and seeds fresh queues.
    index.StopMoveData();
    index.StartMoveData(State.TargetGroups);
}

void TMoveDataDriver::Stop(TColumnShard& owner, const TActorContext& ctx) {
    if (owner.MoveDataDriver == nullptr) {
        return;
    }
    // Same-mailbox callbacks cannot run between invalidation and tablet cleanup.
    owner.MoveDataDriver->Self = nullptr;
    ctx.Send(owner.MoveDataDriver->SelfId(), new TEvents::TEvPoison());
    owner.MoveDataDriver = nullptr;
}

void TMoveDataDriver::OnVacuumCompleted(const TActorContext& ctx) {
    State.VacuumCompleted = true;
    ctx.Send(SelfId(), new TEvMoveData::TEvPoke());
}

NOlap::NActualizer::TMoveDataQueueSizes TMoveDataDriver::UpdateQueueSizes() {
    if (!Self->HasIndex()) {
        return {};
    }
    return Self->MutableIndexAs<NOlap::TColumnEngineForLogs>().UpdateMoveDataQueueSizes();
}

TMoveDataDriver::ECompletionBlocker TMoveDataDriver::GetCompletionBlocker(const NOlap::NActualizer::TMoveDataQueueSizes& queues) const {
    if (!State.VacuumCompleted) {
        return ECompletionBlocker::Vacuum;
    }
    if (queues.GetTotal() != 0) {
        return ECompletionBlocker::Portions;
    }
    if (queues.Retired != 0) {
        return ECompletionBlocker::Cleanup;
    }
    const auto& sharedBlobs = Self->GetStoragesManager()->GetDefaultOperator()->GetSharedBlobs();
    AFL_VERIFY(sharedBlobs);
    if (sharedBlobs->HasBlobsForGroups(State.TargetGroups)) {
        return ECompletionBlocker::SharedBlobs;
    }
    return ECompletionBlocker::None;
}

void TMoveDataDriver::ReportProgress(const NOlap::NActualizer::TMoveDataQueueSizes& queues, ECompletionBlocker blocker) {
    Self->Counters.GetCSCounters().OnMoveDataGateChecked();
    Self->Counters.GetCSCounters().OnMoveDataQueues(queues);
    if (queues.Rejected > State.ReportedRejections) {
        Self->Counters.GetCSCounters().OnMoveDataPortionsRejected(queues.Rejected - State.ReportedRejections);
        State.ReportedRejections = queues.Rejected;
    }
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
        case ECompletionBlocker::Cleanup:
            Self->Counters.GetCSCounters().OnMoveDataGateBlockedByCleanup();
            YDB_LOG_INFO("MoveData gate waits for cleanup", {"tabletId", Self->TabletID()}, {"retired", queues.Retired});
            break;
        case ECompletionBlocker::SharedBlobs:
            Self->Counters.GetCSCounters().OnMoveDataGateBlockedByShared();
            YDB_LOG_INFO("MoveData gate waits for shared blobs", {"tabletId", Self->TabletID()});
            break;
    }
}

void TMoveDataDriver::FinishMigration(const TActorContext& ctx) {
    YDB_LOG_INFO("MoveData gate passed", {"tabletId", Self->TabletID()});

    if (Self->HasIndex()) {
        Self->MutableIndexAs<NOlap::TColumnEngineForLogs>().StopMoveData();
    }
    // Rewrites and cleanup are committed. GC acknowledgements and CutHistory may follow after Hive restarts us.
    for (const auto& subscriber : State.Subscribers) {
        ctx.Send(subscriber, new TEvTablet::TEvMoveDataResponse(Self->TabletID(), NKikimrTabletBase::TEvMoveDataResponse::Success));
    }
    Self->Counters.GetCSCounters().OnMoveDataFinished();
    Stop(*Self, ctx);
}

void TMoveDataDriver::SetupMetadataRequests() {
    if (MetadataSubscribersInFlight->Val()) {
        return;
    }
    NOlap::NActualizer::TMoveDataMetadataBudget budget(NYDBTest::TControllers::GetColumnShardController()->GetMetadataRequestSoftMemoryLimit());
    Self->StartMetadataRequests(Self->GetIndexAs<NOlap::TColumnEngineForLogs>().CollectMoveDataMetadataRequests(budget),
        MetadataTaskSubscription, MetadataSubscribersInFlight, SelfId());
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMetadataAccessorsInfo::TPtr& ev, const TActorContext& ctx) {
    if (Self == nullptr) {
        PassAway();
        return;
    }
    AFL_VERIFY(ev->Get()->GetGeneration() == Self->Generation())("ev", ev->Get()->GetGeneration())("tablet", Self->Generation());
    ev->Get()->GetProcessor()->ApplyResult(ev->Get()->ExtractResult(), Self->MutableIndexAs<NOlap::TColumnEngineForLogs>());
    RunMigrationStep(ctx);
}

void TMoveDataDriver::StartPortionRewrites() {
    const ui64 memoryUsageLimit = AppDataVerified().ColumnShardConfig.GetTieringsMemoryLimit();
    std::vector<std::shared_ptr<NOlap::TTTLColumnEngineChanges>> indexChanges = Self->TablesManager.MutablePrimaryIndex().StartTtl(
        {}, Self->DataLocksManager, memoryUsageLimit, NOlap::NActualizer::EActualizationScope::MoveDataOnly);
    if (indexChanges.empty()) {
        return;
    }
    Self->StartTtlChanges(std::move(indexChanges));
}

void TMoveDataDriver::RunMigrationStep(const TActorContext& ctx) {
    if (State.TargetsChanged) {
        // Seed the migration only after deferred loading has populated the portion catalog.
        if (Self->HasIndex() && !Self->GetIndexAs<NOlap::TColumnEngineForLogs>().AreAllPortionsLoaded(std::nullopt)) {
            return;
        }
        RestartActualizer();
    }
    const auto queues = UpdateQueueSizes();
    if (queues.Pending) {
        SetupMetadataRequests();
    }
    if (queues.ConfirmedToMove || queues.RewriteStarted) {
        StartPortionRewrites();
    }
    const auto blocker = GetCompletionBlocker(queues);
    ReportProgress(queues, blocker);
    if (blocker == ECompletionBlocker::None) {
        FinishMigration(ctx);
    }
}

void TMoveDataDriver::Handle(TEvMoveData::TEvWakeup::TPtr&, const TActorContext& ctx) {
    if (Self == nullptr) {
        PassAway();
        return;
    }
    RunMigrationStep(ctx);
    ScheduleWakeup(ctx);
}

void TMoveDataDriver::Handle(TEvMoveData::TEvPoke::TPtr&, const TActorContext& ctx) {
    if (Self == nullptr) {
        PassAway();
        return;
    }
    RunMigrationStep(ctx);
}

}   // namespace NKikimr::NColumnShard
