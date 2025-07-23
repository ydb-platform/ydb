#include "source.h"

#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/data_sharing/source/transactions/tx_data_ack_to_source.h>
#include <ydb/core/tx/columnshard/data_sharing/source/transactions/tx_finish_ack_to_source.h>
#include <ydb/core/tx/columnshard/data_sharing/source/transactions/tx_start_source_cursor.h>
#include <ydb/core/tx/columnshard/data_sharing/source/transactions/tx_write_source_cursor.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NDataSharing {

NKikimr::TConclusionStatus TSourceSession::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TSourceSession& proto,
    const std::optional<NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic>& protoCursor,
    const std::optional<NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic>& protoCursorStatic) {
    auto parseBase = TBase::DeserializeFromProto(proto);
    if (!parseBase) {
        return parseBase;
    }
    DestinationTabletId = (TTabletId)proto.GetDestinationTabletId();
    if (!(ui64)DestinationTabletId) {
        return TConclusionStatus::Fail("Incorrect DestinationTabletId in proto.");
    }
    for (auto&& i : proto.GetPathIds()) {
        if (!PathIds.emplace(TInternalPathId::FromRawValue(i)).second) {
            return TConclusionStatus::Fail("PathIds contains duplicated values.");
        }
    }
    if (PathIds.empty()) {
        return TConclusionStatus::Fail("PathIds empty.");
    }
    AFL_VERIFY(PathIds.size());
    Cursor = std::make_shared<TSourceCursor>(SelfTabletId, PathIds, TransferContext);
    AFL_VERIFY(!!protoCursor == !!protoCursorStatic);
    if (protoCursor) {
        auto parsed = Cursor->DeserializeFromProto(*protoCursor, *protoCursorStatic);
        if (!parsed) {
            return parsed;
        }
    }
    return TConclusionStatus::Success();
}

TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> TSourceSession::AckFinished(
    NColumnShard::TColumnShard* self, const std::shared_ptr<TSourceSession>& selfPtr) {
    return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxFinishAckToSource(self, selfPtr, "ack_finished"));
}

TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> TSourceSession::AckData(
    NColumnShard::TColumnShard* self, const ui32 receivedPackIdx, const std::shared_ptr<TSourceSession>& selfPtr) {
    auto ackResult = Cursor->AckData(receivedPackIdx);
    if (!ackResult) {
        return ackResult;
    }
    if (Cursor->IsReadyForNext()) {
        Cursor->Next(self->GetStoragesManager(), self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex());
        return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxDataAckToSource(self, selfPtr, "ack_to_source_on_ack_data"));
    } else {
        return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxWriteSourceCursor(self, selfPtr, "write_source_cursor_on_ack_data"));
    }
}

TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> TSourceSession::AckLinks(
    NColumnShard::TColumnShard* self, const TTabletId tabletId, const ui32 receivedPackIdx, const std::shared_ptr<TSourceSession>& selfPtr) {
    auto ackResult = Cursor->AckLinks(tabletId, receivedPackIdx);
    if (!ackResult) {
        return ackResult;
    }
    if (Cursor->IsReadyForNext()) {
        Cursor->Next(self->GetStoragesManager(), self->GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex());
        return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxDataAckToSource(self, selfPtr, "ack_to_source_on_ack_links"));
    } else {
        return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxWriteSourceCursor(self, selfPtr, "write_source_cursor_on_ack_links"));
    }
}

void TSourceSession::SaveCursorToDatabase(NIceDb::TNiceDb& db) {
    GetCursorVerified()->SaveToDatabase(db, GetSessionId());
}

void TSourceSession::ActualizeDestination(
    const NColumnShard::TColumnShard& shard, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) {
    AFL_VERIFY(IsInProgress() || IsPrepared());
    AFL_VERIFY(Cursor);
    if (Cursor->IsValid()) {
        if (!Cursor->IsAckDataReceived()) {
            const THashMap<TInternalPathId, NEvents::TPathIdData>& packPortions = Cursor->GetSelected();

            auto ev = std::make_unique<NEvents::TEvSendDataFromSource>(
                GetSessionId(), Cursor->GetPackIdx(), SelfTabletId, packPortions, Cursor->GetSelectedSchemas());
            NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(ev.release(), (ui64)DestinationTabletId, true), IEventHandle::FlagTrackDelivery, GetRuntimeId());
        }
        {
            const auto& links = Cursor->GetLinks();
            for (auto&& [tabletId, task] : links) {
                if (Cursor->GetLinksModifiedTablets().contains(tabletId)) {
                    continue;
                }
                auto ev = std::make_unique<NEvents::TEvApplyLinksModification>(SelfTabletId, GetSessionId(), Cursor->GetPackIdx(), task);
                NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false),
                    new TEvPipeCache::TEvForward(ev.release(), (ui64)tabletId, true), IEventHandle::FlagTrackDelivery, GetRuntimeId());
            }
        }
    } else {
        auto ev = std::make_unique<NEvents::TEvFinishedFromSource>(GetSessionId(), SelfTabletId);
        NActors::TActivationContext::AsActorContext().Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(ev.release(), (ui64)DestinationTabletId, true), IEventHandle::FlagTrackDelivery, GetRuntimeId());
        Finish(shard, dataLocksManager);
    }
}

void TSourceSession::StartCursor(const NColumnShard::TColumnShard& shard,
    THashMap<TInternalPathId, std::vector<std::shared_ptr<TPortionDataAccessor>>>&& portions,
    std::vector<NOlap::TSchemaPresetVersionInfo>&& schemeHistory) {
    AFL_VERIFY(Cursor);
    AFL_VERIFY(Cursor->Start(shard.GetStoragesManager(), std::move(portions), std::move(schemeHistory), shard.GetIndexAs<TColumnEngineForLogs>().GetVersionedIndex()));
    ActualizeDestination(shard, shard.GetDataLocksManager());
}

TConclusionStatus TSourceSession::DoStart(
    NColumnShard::TColumnShard& shard, THashMap<TInternalPathId, std::vector<TPortionDataAccessor>>&& portions) {
    shard.Execute(new TTxStartSourceCursor(this, &shard, std::move(portions), "start_source_cursor"));
    return TConclusionStatus::Success();
}
}   // namespace NKikimr::NOlap::NDataSharing
