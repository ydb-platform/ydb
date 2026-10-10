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
std::optional<ui32> FindLiveGroup(const TTabletStorageInfo& info, const THashSet<ui32>& groups) {
    if (groups.empty()) {
        return std::nullopt;
    }
    for (const auto& channel : info.Channels) {
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

// Outside a session the set is already empty, so a first request is always a change; returns whether the target set changed.
bool MergeTargetGroups(TMoveDataState& state, const THashSet<ui32>& requested) {
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
    if (const auto liveGroup = FindLiveGroup(*Info(), requested)) {
        RefuseMoveData(ev->Sender, TabletID(), *liveGroup, ctx);
        return;
    }
    MoveDataState.HiveSender = ev->Sender;
    const bool changed = MergeTargetGroups(MoveDataState, requested);
    // Before Active is set, so an active session always has a driver to hand the gate to.
    StartMoveDataDriver(ctx);
    if (!MoveDataState.Active) {
        MoveDataState.Active = true;
        Counters.GetCSCounters().OnMoveDataStarted();
        // The vacuum leg belongs to the executor; everything else to the driver.
        Executor()->StartMoveDataVacuumFromOwner();
    }
    YDB_LOG_INFO("MoveData requested", {"tabletId", TabletID()}, {"groups", MoveDataState.TargetGroups.size()}, {"changed", changed});
    // Marked synchronously: a gate check already queued must not answer for a stale target set.
    MoveDataState.TargetsChanged |= changed;
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
    ctx.Send(MoveDataDriverId, new TEvPrivate::TEvMoveDataPoke());
}

NOlap::NActualizer::TMoveDataQueueSizes TColumnShard::RefreshMoveDataQueueSizes() {
    if (!HasIndex()) {
        return {};
    }
    return MutableIndexAs<NOlap::TColumnEngineForLogs>().RefreshMoveDataQueueSizes();
}

void TColumnShard::CheckMoveDataGate(const TActorContext& ctx, const NOlap::NActualizer::TMoveDataQueueSizes& queues) {
    if (!MoveDataState.Active) {
        return;
    }
    Counters.GetCSCounters().OnMoveDataGateChecked();
    if (MoveDataState.TargetsChanged) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByReseed();
        return;
    }

    Counters.GetCSCounters().OnMoveDataQueues(queues.Pending, queues.ConfirmedToMove, queues.InFlight, queues.Uncommitted, queues.Retired);
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
    const auto& defaultOperator = GetStoragesManager()->GetDefaultOperator();
    if (!defaultOperator->HasCollectedBeforeCurrentGeneration()) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByFirstGCRound();
        YDB_LOG_INFO("MoveData gate waits for the first GC round", {"tabletId", TabletID()});
        return;
    }
    // A shared or borrowed link is not ours to collect, so it gets its own sensor.
    const auto& sharedBlobs = defaultOperator->GetSharedBlobs();
    if (sharedBlobs && sharedBlobs->HasBlobsForGroups(MoveDataState.TargetGroups)) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByShared();
        YDB_LOG_INFO("MoveData gate waits for shared blobs", {"tabletId", TabletID()});
        return;
    }
    if (defaultOperator->HasGCBlobsForGroups(MoveDataState.TargetGroups)) {
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
    StartMetadataRequests(GetIndexAs<NOlap::TColumnEngineForLogs>().CollectMoveDataMetadataRequests(), MoveDataTaskSubscription,
        MoveDataMetadataRequestsInFlight, [](TColumnShard& tablet, const TActorContext& ctx) {
            if (!!tablet.MoveDataDriverId) {
                ctx.Send(tablet.MoveDataDriverId, new TEvPrivate::TEvMoveDataPoke());
            }
        });
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
    // One index walk per turn: prune retired ids and use the sizes to select work and check the gate.
    const NOlap::NActualizer::TMoveDataQueueSizes queues = Self->RefreshMoveDataQueueSizes();
    if (queues.Pending) {
        Self->SetupMoveDataMetadata();
    }
    if (queues.ConfirmedToMove) {
        Self->SetupMoveDataRewrites();
    }
    Self->CheckMoveDataGate(ctx, queues);
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
