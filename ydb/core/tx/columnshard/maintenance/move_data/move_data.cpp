#include "move_data.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

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

void TMoveDataDriver::StartOrUpdateMigration(
    TColumnShard& owner, const NKikimrTabletBase::TEvMoveData& request, const TActorId& sender, const TActorContext& ctx) {
    const THashSet<ui32> requested = RequestedGroups(request);
    if (const auto liveGroup = FindLiveGroup(owner.Info(), requested)) {
        RefuseMoveData(sender, owner.TabletID(), *liveGroup, ctx);
        return;
    }
    if (owner.MoveDataDriver == nullptr) {
        owner.MoveDataDriver = new TMoveDataDriver(&owner, owner.TabletActivityImpl);
        ctx.RegisterWithSameMailbox(owner.MoveDataDriver);
    }
    owner.MoveDataDriver->AcceptRequest(sender, requested, ctx);
}

void TMoveDataDriver::AcceptRequest(const TActorId& sender, const THashSet<ui32>& requested, const TActorContext& ctx) {
    State.HiveSender = sender;
    const bool changed = MergeTargetGroups(State, requested);
    if (!State.Active) {
        State.Active = true;
        State.VacuumCompleted = false;
        // The vacuum leg belongs to the executor; everything else to the driver.
        Self->Executor()->StartMoveDataVacuumFromOwner();
    }
    YDB_LOG_INFO("MoveData requested", {"tabletId", Self->TabletID()}, {"groups", State.TargetGroups.size()}, {"changed", changed});
    // Marked synchronously: a gate check already queued must not answer for a stale target set.
    State.TargetsChanged |= changed;
    ctx.Send(SelfId(), new TEvMoveData::TEvPoke());
}

// Stub: the data path that re-seeds the move for the current target set lands separately.
void TMoveDataDriver::RestartMoveDataActualizer() {
    AFL_VERIFY(State.Active);
    State.TargetsChanged = false;
}

// Stub: the move's own metadata-accessor requests land with the data path.
void TMoveDataDriver::SetupMoveDataMetadata() {
}

// Stub: the rewrites of portions out of the target groups land with the data path.
void TMoveDataDriver::SetupMoveDataRewrites() {
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
    if (!State.Active) {
        return;
    }
    State.VacuumCompleted = true;
    // The driver owns the gate; hand it the news rather than deciding here.
    ctx.Send(SelfId(), new TEvMoveData::TEvPoke());
}

// Stub gate: only the vacuum leg is awaited; the portion, cleanup and GC clauses land with the data path.
void TMoveDataDriver::CheckMoveDataGate(const TActorContext& ctx) {
    if (!State.Active || State.TargetsChanged) {
        return;
    }
    if (!State.VacuumCompleted) {
        return;
    }
    YDB_LOG_INFO("MoveData gate passed", {"tabletId", Self->TabletID()});
    ctx.Send(State.HiveSender, new TEvTablet::TEvMoveDataResponse(Self->TabletID(), NKikimrTabletBase::TEvMoveDataResponse::Success));
    State = TMoveDataState{};
    Stop(*Self, ctx);
}

void TMoveDataDriver::StartAndCheckGate(const TActorContext& ctx) {
    if (!State.Active) {
        return;
    }
    if (State.TargetsChanged) {
        RestartMoveDataActualizer();
    }
    SetupMoveDataMetadata();
    SetupMoveDataRewrites();
    CheckMoveDataGate(ctx);
}

void TMoveDataDriver::Handle(TEvMoveData::TEvWakeup::TPtr&, const TActorContext& ctx) {
    if (Self == nullptr || !IsOwnerAlive()) {
        PassAway();
        return;
    }
    StartAndCheckGate(ctx);
    ScheduleWakeup(ctx);
}

void TMoveDataDriver::Handle(TEvMoveData::TEvPoke::TPtr&, const TActorContext& ctx) {
    if (Self == nullptr || !IsOwnerAlive()) {
        PassAway();
        return;
    }
    StartAndCheckGate(ctx);
}

}   // namespace NKikimr::NColumnShard
