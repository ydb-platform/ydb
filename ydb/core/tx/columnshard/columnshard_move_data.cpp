#include "columnshard_impl.h"

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
        // The vacuum leg belongs to the executor; everything else to the driver.
        Executor()->StartMoveDataVacuumFromOwner();
    }
    YDB_LOG_INFO("MoveData requested", {"tabletId", TabletID()}, {"groups", MoveDataState.TargetGroups.size()}, {"changed", changed});
    // Marked synchronously: a gate check already queued must not answer for a stale target set.
    MoveDataState.TargetsChanged |= changed;
    StartMoveDataDriver(ctx);
    ctx.Send(MoveDataDriverId, new TEvPrivate::TEvMoveDataPoke());
}

// Stub: the data path that re-seeds the move for the current target set lands separately.
void TColumnShard::RestartMoveDataActualizer() {
    AFL_VERIFY(MoveDataState.Active);
    MoveDataState.TargetsChanged = false;
}

// Stub: the move's own metadata-accessor requests land with the data path.
void TColumnShard::SetupMoveDataMetadata() {
}

// Stub: the rewrites of portions out of the target groups land with the data path.
void TColumnShard::SetupMoveDataRewrites() {
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

// Stub gate: only the vacuum leg is awaited; the portion, cleanup and GC clauses land with the data path.
void TColumnShard::CheckMoveDataGate(const TActorContext& ctx) {
    if (!MoveDataState.Active || MoveDataState.TargetsChanged) {
        return;
    }
    if (!MoveDataState.VacuumCompleted) {
        return;
    }
    YDB_LOG_INFO("MoveData gate passed", {"tabletId", TabletID()});
    ctx.Send(MoveDataState.HiveSender, new TEvTablet::TEvMoveDataResponse(TabletID(), NKikimrTabletBase::TEvMoveDataResponse::Success));
    MoveDataState = TMoveDataState{};
    StopMoveDataDriver(ctx);
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
