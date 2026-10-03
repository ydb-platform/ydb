#include "columnshard_impl.h"

#include "engines/column_engine_logs.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

#include <util/generic/size_literals.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

namespace {

THashSet<ui32> RequestedGroups(const NKikimrTabletBase::TEvMoveData& record) {
    THashSet<ui32> groups;
    for (const auto groupId : record.GetGroups()) {
        groups.emplace(groupId);
    }
    return groups;
}

// Same contract as keyvalue and blob_depot: a group that is still the latest entry keeps taking writes, so the move could never converge.
std::optional<ui32> FindLiveGroup(const TTabletStorageInfo* info, const THashSet<ui32>& groups) {
    if (groups.empty() || !info) {
        return std::nullopt;
    }
    for (const auto& channel : info->Channels) {
        const auto* latest = channel.LatestEntry();
        if (latest && groups.contains(latest->GroupID)) {
            return latest->GroupID;
        }
    }
    return std::nullopt;
}

void RefuseMoveData(const TActorId& sender, const ui64 tabletId, const ui32 liveGroup, const TActorContext& ctx) {
    const TString reason = TStringBuilder() << "group " << liveGroup << " is still the latest history entry at tablet " << tabletId;
    YDB_LOG_WARN("MoveData refused", {"tabletId", tabletId}, {"reason", reason});
    ctx.Send(sender, new TEvTablet::TEvMoveDataResponse(tabletId, NKikimrTabletBase::TEvMoveDataResponse::ErrorGroupIdMismatch, reason));
}

// A request outside a session starts from the empty set; returns whether the target set changed.
bool MergeTargetGroups(TMoveDataState& state, const THashSet<ui32>& requested) {
    if (!state.Active) {
        state.TargetGroups.clear();
    }
    bool changed = !state.Active;
    for (const auto groupId : requested) {
        changed |= state.TargetGroups.emplace(groupId).second;
    }
    return changed;
}

}   // namespace

void TColumnShard::Handle(TEvTablet::TEvMoveData::TPtr& ev, const TActorContext& ctx) {
    if (!HasAppData() || !AppData()->FeatureFlags.GetEnableColumnshardMoveData()) {
        TTabletExecutedFlat::Handle(ev);
        return;
    }
    const THashSet<ui32> requested = RequestedGroups(ev->Get()->Record);
    if (const auto liveGroup = FindLiveGroup(Info(), requested)) {
        RefuseMoveData(ev->Sender, TabletID(), *liveGroup, ctx);
        return;
    }
    MoveDataState.HiveSender = ev->Sender;
    const bool changed = MergeTargetGroups(MoveDataState, requested);
    if (!MoveDataState.Active) {
        MoveDataState.Active = true;
        MoveDataState.VacuumCompleted = false;
        Counters.GetCSCounters().OnMoveDataStarted();
        // The vacuum leg belongs to the executor; everything else to the driver.
        Executor()->StartMoveDataVacuumFromOwner();
    }
    YDB_LOG_INFO("MoveData requested", {"tabletId", TabletID()}, {"groups", MoveDataState.TargetGroups.size()}, {"changed", changed});
    // Marked synchronously: a gate check already queued must not answer for a stale target set.
    MoveDataState.TargetsChanged |= changed;
    StartMoveDataDriver(ctx);
    ctx.Send(MoveDataDriverId, new TEvPrivate::TEvMoveDataPoke());
}

void TColumnShard::RestartMoveDataActualizer() {
    AFL_VERIFY(MoveDataState.Active);
    MoveDataState.TargetsChanged = false;
    // A fresh actualizer counts rejections from zero again.
    MoveDataState.ReportedRejections = 0;
    if (!HasIndex()) {
        return;
    }
    auto& index = MutableIndexAs<NOlap::TColumnEngineForLogs>();
    // A changed target set replaces the actualizers and seeds fresh queues.
    index.StopMoveData();
    if (!MoveDataState.TargetGroups.empty()) {
        index.StartMoveData(MoveDataState.TargetGroups);
    }
}

void TColumnShard::StartMoveDataDriver(const TActorContext& ctx) {
    if (!!MoveDataDriverId) {
        return;
    }
    MoveDataDriverId = ctx.RegisterWithSameMailbox(new TMoveDataDriver(this, TabletActivityImpl));
}

void TColumnShard::StopMoveDataDriver(const TActorContext& ctx) {
    if (!MoveDataDriverId) {
        return;
    }
    ctx.Send(MoveDataDriverId, new TEvents::TEvPoison());
    MoveDataDriverId = {};
}

void TColumnShard::MoveDataCompleted(const TActorContext& ctx) {
    if (!MoveDataState.Active) {
        return;
    }
    MoveDataState.VacuumCompleted = true;
    // The driver owns the gate; hand it the news rather than deciding here.
    if (!!MoveDataDriverId) {
        ctx.Send(MoveDataDriverId, new TEvPrivate::TEvMoveDataPoke());
    } else {
        CheckMoveDataGate(ctx);
    }
}

void TColumnShard::CheckMoveDataGate(const TActorContext& ctx) {
    if (!MoveDataState.Active) {
        return;
    }
    if (MoveDataState.TargetsChanged) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByReseed();
        return;
    }

    NOlap::NActualizer::TMoveDataQueueSizes queues;
    if (HasIndex()) {
        queues = GetIndexAs<NOlap::TColumnEngineForLogs>().GetMoveDataQueueSizes();
    }
    Counters.GetCSCounters().OnMoveDataQueues(queues.Pending, queues.ConfirmedToMove, queues.InFlight);
    if (queues.Rejected > MoveDataState.ReportedRejections) {
        Counters.GetCSCounters().OnMoveDataPortionsRejected(queues.Rejected - MoveDataState.ReportedRejections);
        MoveDataState.ReportedRejections = queues.Rejected;
    }
    if (!MoveDataState.VacuumCompleted) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByVacuum();
        return;
    }
    if (queues.GetTotal() != 0) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByPortions();
        if (queues.Uncommitted) {
            YDB_LOG_INFO("MoveData gate waits for uncommitted writes", {"tabletId", TabletID()}, {"uncommitted", queues.Uncommitted});
        }
        return;
    }
    // A retired seeded portion still in the granule has not reached the GC queues, so its blobs can still sit in a target group.
    if (queues.Retired != 0) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByCleanup();
        YDB_LOG_INFO("MoveData gate waits for cleanup", {"tabletId", TabletID()}, {"retired", queues.Retired});
        return;
    }
    if (!GetStoragesManager()->GetDefaultOperator()->HasCollectedBeforeCurrentGeneration()) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByFirstGCRound();
        YDB_LOG_INFO("MoveData gate waits for the first GC round", {"tabletId", TabletID()});
        return;
    }
    if (GetStoragesManager()->GetDefaultOperator()->HasBlobsForGroups(MoveDataState.TargetGroups)) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByGC();
        YDB_LOG_INFO("MoveData gate waits for pending GC", {"tabletId", TabletID()});
        return;
    }
    YDB_LOG_INFO("MoveData gate passed", {"tabletId", TabletID()});

    if (HasIndex()) {
        MutableIndexAs<NOlap::TColumnEngineForLogs>().StopMoveData();
    }
    // The boot-time CutHistory scan finds drained intervals by itself, so nothing needs persisting before Success.
    ctx.Send(MoveDataState.HiveSender, new TEvTablet::TEvMoveDataResponse(TabletID(), NKikimrTabletBase::TEvMoveDataResponse::Success));
    Counters.GetCSCounters().OnMoveDataFinished();
    MoveDataState = TMoveDataState{};
    StopMoveDataDriver(ctx);
}

void TColumnShard::SetupMoveDataMetadata() {
    if (!MoveDataState.Active || !HasIndex() || MoveDataMetadataRequestsInFlight->Val()) {
        return;
    }
    StartMetadataRequests(
        GetIndexAs<NOlap::TColumnEngineForLogs>().CollectMoveDataMetadataRequests(), MoveDataTaskSubscription, MoveDataMetadataRequestsInFlight);
}

void TColumnShard::SetupMoveDataRewrites() {
    if (!MoveDataState.Active || !HasIndex()) {
        return;
    }
    const ui64 memoryUsageLimit = HasAppData() ? AppDataVerified().ColumnShardConfig.GetTieringsMemoryLimit() : 512_MB;
    std::vector<std::shared_ptr<NOlap::TTTLColumnEngineChanges>> indexChanges = TablesManager.MutablePrimaryIndex().StartTtl(
        {}, DataLocksManager, memoryUsageLimit, NOlap::NActualizer::EActualizationScope::MoveDataOnly);
    if (indexChanges.empty()) {
        return;
    }
    StartTtlChanges(std::move(indexChanges));
}

void TMoveDataDriver::StartAndCheckGate(const TActorContext& ctx) {
    if (!Self->MoveDataState.Active) {
        return;
    }
    if (Self->MoveDataState.TargetsChanged) {
        Self->RestartMoveDataActualizer();
    }
    Self->SetupMoveDataMetadata();
    Self->SetupMoveDataRewrites();
    Self->CheckMoveDataGate(ctx);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataWakeup::TPtr&, const TActorContext& ctx) {
    if (!IsOwnerAlive()) {
        PassAway();
        return;
    }
    StartAndCheckGate(ctx);
    ScheduleWakeup(ctx);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataPoke::TPtr&, const TActorContext& ctx) {
    if (!IsOwnerAlive()) {
        PassAway();
        return;
    }
    StartAndCheckGate(ctx);
}

}   // namespace NKikimr::NColumnShard
