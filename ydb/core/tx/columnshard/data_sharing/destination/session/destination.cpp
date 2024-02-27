#include "destination.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/events/transfer.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/transactions/tx_finish_ack_from_initiator.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/transactions/tx_finish_from_source.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/transactions/tx_data_from_source.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NDataSharing {

NKikimr::TConclusionStatus TDestinationSession::DataReceived(THashMap<ui64, NEvents::TPathIdData>&& data, TColumnEngineForLogs& index, const std::shared_ptr<IStoragesManager>& /*manager*/) {
    auto guard = index.GranulesStorage->StartPackModification();
    for (auto&& i : data) {
        auto it = PathIds.find(i.first);
        AFL_VERIFY(it != PathIds.end())("path_id_undefined", i.first);
        for (auto&& portion : i.second.DetachPortions()) {
            ui32 contains = 0;
            ui32 notContains = 0;
            THashMap<TString, THashSet<TUnifiedBlobId>> blobIds;
            portion.FillBlobIdsByStorage(blobIds, index.GetVersionedIndex());
            for (auto&& s : blobIds) {
                auto it = CurrentBlobIds.find(s.first);
                if (it == CurrentBlobIds.end()) {
                    notContains += s.second.size();
                    continue;
                }
                for (auto&& b : s.second) {
                    if (it->second.contains(b)) {
                        ++contains;
                    }
                }
            }
            AFL_VERIFY(!contains || !notContains);
            if (!contains) {
                portion.SetPathId(it->second);
                index.UpsertPortion(std::move(portion));
            }
        }
    }
    return TConclusionStatus::Success();
}

void TDestinationSession::SendCurrentCursorAck(const NColumnShard::TColumnShard& shard, const std::optional<TTabletId> tabletId) {
    AFL_VERIFY(IsStarted() || IsStarting());
    bool found = false;
    bool allTransfersFinished = true;
    for (auto&& [_, cursor] : Cursors) {
        if (!cursor.GetDataFinished()) {
            allTransfersFinished = false;
        }
        if (tabletId && *tabletId != cursor.GetTabletId()) {
            continue;
        }
        found = true;
        if (cursor.GetDataFinished()) {
            auto ev = std::make_unique<NEvents::TEvAckFinishToSource>(GetSessionId());
            NActors::TActivationContext::AsActorContext().Send(MakePipePeNodeCacheID(false),
                new TEvPipeCache::TEvForward(ev.release(), (ui64)cursor.GetTabletId(), true), IEventHandle::FlagTrackDelivery, GetRuntimeId());
        } else if (cursor.GetPackIdx()) {
            auto ev = std::make_unique<NEvents::TEvAckDataToSource>(GetSessionId(), cursor.GetPackIdx());
            NActors::TActivationContext::AsActorContext().Send(MakePipePeNodeCacheID(false),
                new TEvPipeCache::TEvForward(ev.release(), (ui64)cursor.GetTabletId(), true), IEventHandle::FlagTrackDelivery, GetRuntimeId());
        } else {
            std::set<ui64> pathIdsBase;
            for (auto&& i : PathIds) {
                pathIdsBase.emplace(i.first);
            }
            TSourceSession source(GetSessionId(), TransferContext, cursor.GetTabletId(), pathIdsBase, (TTabletId)shard.TabletID());
            auto ev = std::make_unique<NEvents::TEvStartToSource>(source);
            NActors::TActivationContext::AsActorContext().Send(MakePipePeNodeCacheID(false),
                new TEvPipeCache::TEvForward(ev.release(), (ui64)cursor.GetTabletId(), true), IEventHandle::FlagTrackDelivery, GetRuntimeId());
        }
    }
    if (allTransfersFinished && !IsFinished()) {
        NYDBTest::TControllers::GetColumnShardController()->OnDataSharingFinished(shard.TabletID(), GetSessionId());
        Finish(shard.GetDataLocksManager());
        InitiatorController.Finished(GetSessionId());
    }
    AFL_VERIFY(found);
}

NKikimr::TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> TDestinationSession::ReceiveData(
    NColumnShard::TColumnShard* self, const THashMap<ui64, NEvents::TPathIdData>& data, const ui32 receivedPackIdx, const TTabletId sourceTabletId,
    const std::shared_ptr<TDestinationSession>& selfPtr) {
    auto result = GetCursorVerified(sourceTabletId).ReceiveData(receivedPackIdx);
    if (!result) {
        return result;
    }
    return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxDataFromSource(self, selfPtr, data, sourceTabletId));
}

NKikimr::TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> TDestinationSession::ReceiveFinished(NColumnShard::TColumnShard* self, const TTabletId sourceTabletId, const std::shared_ptr<TDestinationSession>& selfPtr) {
    auto result = GetCursorVerified(sourceTabletId).ReceiveFinished();
    if (!result) {
        return result;
    }
    return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxFinishFromSource(self, sourceTabletId, selfPtr));
}

NKikimr::TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> TDestinationSession::AckInitiatorFinished(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& selfPtr) {
    return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxFinishAckFromInitiator(self, selfPtr));
}

NKikimr::TConclusionStatus TDestinationSession::DeserializeDataFromProto(const NKikimrColumnShardDataSharingProto::TDestinationSession& proto, const TColumnEngineForLogs& index) {
    if (!InitiatorController.DeserializeFromProto(proto.GetInitiatorController())) {
        return TConclusionStatus::Fail("cannot parse initiator controller: " + proto.GetInitiatorController().DebugString());
    }
    auto parseBase = TBase::DeserializeFromProto(proto);
    if (!parseBase) {
        return parseBase;
    }

    for (auto&& i : TransferContext.GetSourceTabletIds()) {
        Cursors.emplace(i, TSourceCursorForDestination(i));
    }

    for (auto&& i : proto.GetPathIds()) {
        auto g = index.GetGranuleOptional(i.GetDestPathId());
        if (!g) {
            return TConclusionStatus::Fail("Incorrect remapping into undefined path id: " + ::ToString(i.GetDestPathId()));
        }
        if (!i.GetSourcePathId() || !i.GetDestPathId()) {
            return TConclusionStatus::Fail("PathIds remapping contains incorrect ids: " + i.DebugString());
        }
        if (!PathIds.emplace(i.GetSourcePathId(), i.GetDestPathId()).second) {
            return TConclusionStatus::Fail("PathIds contains duplicated values.");
        }
    }
    if (PathIds.empty()) {
        return TConclusionStatus::Fail("PathIds empty.");
    }
    return TConclusionStatus::Success();
}

NKikimrColumnShardDataSharingProto::TDestinationSession TDestinationSession::SerializeDataToProto() const {
    NKikimrColumnShardDataSharingProto::TDestinationSession result;
    InitiatorController.SerializeToProto(*result.MutableInitiatorController());
    TBase::SerializeToProto(result);
    for (auto&& i : PathIds) {
        auto* pathIdRemap = result.AddPathIds();
        pathIdRemap->SetSourcePathId(i.first);
        pathIdRemap->SetDestPathId(i.second);
    }
    return result;
}

NKikimrColumnShardDataSharingProto::TDestinationSession::TFullCursor TDestinationSession::SerializeCursorToProto() const {
    NKikimrColumnShardDataSharingProto::TDestinationSession::TFullCursor result;
    result.SetConfirmedFlag(ConfirmedFlag);
    for (auto&& i : Cursors) {
        *result.AddSourceCursors() = i.second.SerializeToProto();
    }
    return result;
}

NKikimr::TConclusionStatus TDestinationSession::DeserializeCursorFromProto(const NKikimrColumnShardDataSharingProto::TDestinationSession::TFullCursor& proto) {
    ConfirmedFlag = proto.GetConfirmedFlag();
    for (auto&& i : proto.GetSourceCursors()) {
        TSourceCursorForDestination cursor;
        auto parsed = cursor.DeserializeFromProto(i);
        if (!parsed) {
            return parsed;
        }
        auto it = Cursors.find(cursor.GetTabletId());
        AFL_VERIFY(it != Cursors.end());
        it->second = cursor;
    }
    return TConclusionStatus::Success();
}

bool TDestinationSession::DoStart(const NColumnShard::TColumnShard& shard, const THashMap<ui64, std::vector<std::shared_ptr<TPortionInfo>>>& portions) {
    AFL_VERIFY(IsConfirmed());
    NYDBTest::TControllers::GetColumnShardController()->OnDataSharingStarted(shard.TabletID(), GetSessionId());
    THashMap<TString, THashSet<TUnifiedBlobId>> local;
    for (auto&& i : portions) {
        for (auto&& p : i.second) {
            p->FillBlobIdsByStorage(local, shard.GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex());
        }
    }
    std::swap(CurrentBlobIds, local);
    SendCurrentCursorAck(shard, {});
    return true;
}

}