#include "columnshard_impl.h"
#include "columnshard_move_data.h"

#include "engines/column_engine_logs.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

#include <util/generic/algorithm.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

namespace {

THashSet<ui32> GetRequestedGroups(const NKikimrTabletBase::TEvMoveData& record) {
    return { record.GetGroups().begin(), record.GetGroups().end() };
}

// Same contract as keyvalue and blob_depot: a group that is still the latest entry keeps taking writes, so the move could never converge.
std::optional<ui32> FindActiveGroup(const TTabletStorageInfo& info, const THashSet<ui32>& groups) {
    if (groups.empty()) {
        return std::nullopt;
    }
    const auto* channel = FindIfPtr(info.Channels, [&groups](const auto& channel) {
        const auto* latest = channel.LatestEntry();
        return latest != nullptr && groups.contains(latest->GroupID);
    });
    if (channel == nullptr) {
        return std::nullopt;
    }
    return channel->LatestEntry()->GroupID;
}

void RefuseMoveData(const TActorId& sender, const ui64 tabletId, const ui32 activeGroup, const TActorContext& ctx) {
    const TString reason = TStringBuilder() << "group " << activeGroup << " is still the latest history entry at tablet " << tabletId;
    YDB_LOG_WARN("MoveData refused", {"tabletId", tabletId}, {"reason", reason});
    ctx.Send(sender, new TEvTablet::TEvMoveDataResponse(tabletId, NKikimrTabletBase::TEvMoveDataResponse::ErrorGroupIdMismatch, reason));
}

}   // namespace

TMoveDataDriver::TMoveDataDriver(TColumnShard* self)
    : Self(self)
    , MetadataTaskSubscription("CS::MOVE_DATA", self->Counters.GetSubscribeCounters())
{
}

void TColumnShard::Handle(TEvTablet::TEvMoveData::TPtr& ev, const TActorContext& ctx) {
    if (!AppDataVerified().FeatureFlags.GetEnableColumnshardMoveData()) {
        TTabletExecutedFlat::Handle(ev);
        return;
    }
    TMoveDataDriver::StartOrUpdateMigration(*this, ev->Get()->Record, ev->Sender, ctx);
}

void TMoveDataDriver::StartOrUpdateMigration(
    TColumnShard& owner, const NKikimrTabletBase::TEvMoveData& request, const TActorId& sender, const TActorContext& ctx) {
    const THashSet<ui32> requested = GetRequestedGroups(request);
    if (const auto activeGroup = FindActiveGroup(*owner.Info(), requested)) {
        RefuseMoveData(sender, owner.TabletID(), *activeGroup, ctx);
        return;
    }
    if (owner.MoveDataDriver == nullptr) {
        owner.MoveDataDriver = new TMoveDataDriver(&owner);
        ctx.RegisterWithSameMailbox(owner.MoveDataDriver);
        owner.Counters.GetCSCounters().OnMoveDataStarted();
        owner.Executor()->StartMoveDataVacuumFromOwner();
    }
    owner.MoveDataDriver->AcceptRequest(sender, requested, ctx);
}

void TMoveDataDriver::AcceptRequest(const TActorId& sender, const THashSet<ui32>& requested, const TActorContext& ctx) {
    State.HiveSender = sender;
    // Admission is synchronous even though the gate runs on the driver's cadence.
    for (const auto groupId : requested) {
        State.TargetsChanged |= State.TargetGroups.emplace(groupId).second;
    }
    YDB_LOG_INFO("MoveData requested", {"tabletId", Self->TabletID()}, {"groups", State.TargetGroups.size()}, {"changed", State.TargetsChanged});
    ctx.Send(SelfId(), new TEvPrivate::TEvMoveDataPoke());
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
    if (!State.TargetGroups.empty()) {
        index.StartMoveData(State.TargetGroups);
    }
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

void TColumnShard::MoveDataCompleted(const TActorContext& ctx) {
    if (MoveDataDriver != nullptr) {
        MoveDataDriver->OnVacuumCompleted(ctx);
    }
}

void TMoveDataDriver::OnVacuumCompleted(const TActorContext& ctx) {
    State.VacuumCompleted = true;
    ctx.Send(SelfId(), new TEvPrivate::TEvMoveDataPoke());
}

NOlap::NActualizer::TMoveDataQueueSizes TMoveDataDriver::UpdateQueueSizes() {
    if (!Self->HasIndex()) {
        return {};
    }
    return Self->MutableIndexAs<NOlap::TColumnEngineForLogs>().UpdateMoveDataQueueSizes();
}

bool TMoveDataDriver::CanCompleteMigration(const NOlap::NActualizer::TMoveDataQueueSizes& queues) {
    Self->Counters.GetCSCounters().OnMoveDataGateChecked();
    Self->Counters.GetCSCounters().OnMoveDataQueues(queues);
    if (queues.Rejected > State.ReportedRejections) {
        Self->Counters.GetCSCounters().OnMoveDataPortionsRejected(queues.Rejected - State.ReportedRejections);
        State.ReportedRejections = queues.Rejected;
    }
    if (!State.VacuumCompleted) {
        Self->Counters.GetCSCounters().OnMoveDataGateBlockedByVacuum();
        return false;
    }
    if (queues.GetTotal() != 0) {
        Self->Counters.GetCSCounters().OnMoveDataGateBlockedByPortions();
        if (queues.Uncommitted) {
            YDB_LOG_INFO("MoveData gate waits for uncommitted writes", {"tabletId", Self->TabletID()}, {"uncommitted", queues.Uncommitted});
        }
        return false;
    }
    // A retired seeded portion still in the granule has not reached the GC queues, so its blobs can still sit in a target group.
    if (queues.Retired != 0) {
        Self->Counters.GetCSCounters().OnMoveDataGateBlockedByCleanup();
        YDB_LOG_INFO("MoveData gate waits for cleanup", {"tabletId", Self->TabletID()}, {"retired", queues.Retired});
        return false;
    }
    const auto& defaultOperator = Self->GetStoragesManager()->GetDefaultOperator();
    // A shared or borrowed link is not ours to collect, so it gets its own sensor.
    const auto& sharedBlobs = defaultOperator->GetSharedBlobs();
    AFL_VERIFY(sharedBlobs);
    if (sharedBlobs->HasBlobsForGroups(State.TargetGroups)) {
        Self->Counters.GetCSCounters().OnMoveDataGateBlockedByShared();
        YDB_LOG_INFO("MoveData gate waits for shared blobs", {"tabletId", Self->TabletID()});
        return false;
    }
    return true;
}

void TMoveDataDriver::FinishMigration(const TActorContext& ctx) {
    YDB_LOG_INFO("MoveData gate passed", {"tabletId", Self->TabletID()});

    if (Self->HasIndex()) {
        Self->MutableIndexAs<NOlap::TColumnEngineForLogs>().StopMoveData();
    }
    // Rewrites and cleanup are committed. GC acknowledgements and CutHistory may follow after Hive restarts us.
    ctx.Send(State.HiveSender, new TEvTablet::TEvMoveDataResponse(Self->TabletID(), NKikimrTabletBase::TEvMoveDataResponse::Success));
    Self->Counters.GetCSCounters().OnMoveDataFinished();
    Stop(*Self, ctx);
}

void TMoveDataDriver::SetupMetadata() {
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
        RestartActualizer();
    }
    // Reconcile bounded batches of seeded portions, then select work and check the gate.
    const auto queues = UpdateQueueSizes();
    if (queues.Pending) {
        SetupMetadata();
    }
    if (queues.ConfirmedToMove || queues.RewriteStarted) {
        StartPortionRewrites();
    }
    if (CanCompleteMigration(queues)) {
        FinishMigration(ctx);
    }
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataWakeup::TPtr&, const TActorContext& ctx) {
    if (Self == nullptr) {
        PassAway();
        return;
    }
    RunMigrationStep(ctx);
    ScheduleWakeup(ctx);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataPoke::TPtr&, const TActorContext& ctx) {
    if (Self == nullptr) {
        PassAway();
        return;
    }
    RunMigrationStep(ctx);
}

}   // namespace NKikimr::NColumnShard
