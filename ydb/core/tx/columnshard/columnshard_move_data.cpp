#include "columnshard_impl.h"

#include "maintenance/move_data/move_data.h"

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

void TColumnShard::Handle(TEvTablet::TEvMoveData::TPtr& ev, const TActorContext& ctx) {
    if (!AppDataVerified().FeatureFlags.GetEnableColumnshardMoveData()) {
        TTabletExecutedFlat::Handle(ev);
        return;
    }
    const auto groups = GetRequestedGroups(ev->Get()->Record);
    if (const auto activeGroup = FindActiveGroup(*Info(), groups)) {
        RefuseMoveData(ev->Sender, TabletID(), *activeGroup, ctx);
        return;
    }
    if (!MoveDataCoordinator) {
        MoveDataCoordinator = std::make_unique<TMoveDataCoordinator>(this);
    }
    const bool startVacuum = !MoveDataCoordinator->IsActive();
    MoveDataCoordinator->AcceptRequest(ev->Sender, groups, ctx);
    if (startVacuum) {
        Counters.GetCSCounters().OnMoveDataStarted();
        Executor()->StartMoveDataVacuumFromOwner();
    }
}

void TColumnShard::MoveDataCompleted(const TActorContext& ctx) {
    if (MoveDataCoordinator && MoveDataCoordinator->IsActive()) {
        MoveDataCoordinator->OnVacuumCompleted(ctx);
    }
}

void TColumnShard::Handle(TEvMoveData::TEvBatch::TPtr& ev, const TActorContext& ctx) {
    MoveDataCoordinator->OnBatch(ev, ctx);
}

void TColumnShard::Handle(TEvMoveData::TEvWakeup::TPtr&, const TActorContext& ctx) {
    MoveDataCoordinator->OnWakeup(ctx);
}

void TColumnShard::Handle(TEvMoveData::TEvPoke::TPtr&, const TActorContext& ctx) {
    MoveDataCoordinator->OnPoke(ctx);
}

}   // namespace NKikimr::NColumnShard
