#include "source.h"
#include <ydb/core/tx/columnshard/data_sharing/source/transactions/tx_finish_ack_to_source.h>
#include <ydb/core/tx/columnshard/data_sharing/source/transactions/tx_data_ack_to_source.h>
#include <ydb/core/tx/columnshard/data_sharing/source/transactions/tx_write_source_cursor.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NDataSharing {

NKikimr::TConclusionStatus TSourceSession::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TSourceSession& proto, 
    const std::optional<NKikimrColumnShardDataSharingProto::TSourceSession::TCursor>& protoCursor,
    const TColumnEngineForLogs& index, const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager) {
    auto parseBase = TBase::DeserializeFromProto(proto);
    if (!parseBase) {
        return parseBase;
    }
    DestinationTabletId = (TTabletId)proto.GetDestinationTabletId();
    if (!(ui64)DestinationTabletId) {
        return TConclusionStatus::Fail("Incorrect DestinationTabletId in proto.");
    }
    for (auto&& i : proto.GetPathIds()) {
        if (!PathIds.emplace(i).second) {
            return TConclusionStatus::Fail("PathIds contains duplicated values.");
        }
    }
    if (PathIds.empty()) {
        return TConclusionStatus::Fail("PathIds empty.");
    }
    AFL_VERIFY(PathIds.size());
    Cursor = std::make_shared<TSourceCursor>(index, SelfTabletId, PathIds, TransferContext, GetSnapshotBarrier());
    if (protoCursor) {
        return Cursor->DeserializeFromProto(*protoCursor, sharedBlobsManager);
    } else {
        Cursor->Start(sharedBlobsManager);
        return TConclusionStatus::Success();
    }
}

TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> TSourceSession::AckFinished(NColumnShard::TColumnShard* self, const std::shared_ptr<TSourceSession>& selfPtr) {
    return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxFinishAckToSource(self, selfPtr));
}

TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> TSourceSession::AckData(NColumnShard::TColumnShard* self, const ui32 receivedPackIdx, const std::shared_ptr<TSourceSession>& selfPtr) {
    auto ackResult = Cursor->AckData(receivedPackIdx);
    if (!ackResult) {
        return ackResult;
    }
    if (Cursor->IsReadyForNext()) {
        return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxDataAckToSource(self, selfPtr));
    } else {
        return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxWriteSourceCursor(self, selfPtr));
    }
}

TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> TSourceSession::AckLinks(NColumnShard::TColumnShard* self, const TTabletId tabletId, const ui32 receivedPackIdx, const std::shared_ptr<TSourceSession>& selfPtr) {
    auto ackResult = Cursor->AckLinks(tabletId, receivedPackIdx);
    if (!ackResult) {
        return ackResult;
    }
    if (Cursor->IsReadyForNext()) {
        return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxDataAckToSource(self, selfPtr));
    } else {
        return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxWriteSourceCursor(self, selfPtr));
    }
}

void TSourceSession::ActualizeDestination() {
    if (Cursor->IsValid()) {
        if (Cursor->GetAckReceivedForPackIdx() < Cursor->GetPackIdx()) {
            const THashMap<ui64, NEvents::TPathIdData>& packPortions = Cursor->GetSelected();
            auto ev = std::make_unique<NEvents::TEvSendDataFromSource>(GetSessionId(), Cursor->GetPackIdx(), SelfTabletId, packPortions);
            NActors::TActivationContext::AsActorContext().Send(MakePipePeNodeCacheID(false),
                new TEvPipeCache::TEvForward(ev.release(), (ui64)DestinationTabletId, true), IEventHandle::FlagTrackDelivery, GetRuntimeId());
        }
        {
            const auto& links = Cursor->GetLinks();
            for (auto&& [tabletId, task] : links) {
                if (Cursor->GetLinksModifiedTablets().contains(tabletId)) {
                    continue;
                }
                auto ev = std::make_unique<NEvents::TEvApplyLinksModification>(SelfTabletId, GetSessionId(), Cursor->GetPackIdx(), task);
                NActors::TActivationContext::AsActorContext().Send(MakePipePeNodeCacheID(false),
                    new TEvPipeCache::TEvForward(ev.release(), (ui64)tabletId, true), IEventHandle::FlagTrackDelivery, GetRuntimeId());
            }
        }
    } else {
        auto ev = std::make_unique<NEvents::TEvFinishedFromSource>(GetSessionId(), SelfTabletId);
        NActors::TActivationContext::AsActorContext().Send(MakePipePeNodeCacheID(false),
            new TEvPipeCache::TEvForward(ev.release(), (ui64)DestinationTabletId, true), IEventHandle::FlagTrackDelivery, GetRuntimeId());
    }
}

}