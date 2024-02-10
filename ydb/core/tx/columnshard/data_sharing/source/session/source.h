#pragma once
#include "cursor.h"
#include <ydb/core/tx/columnshard/data_sharing/common/session/common.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>

namespace NKikimr::NOlap::NDataSharing {

class TSharedBlobsManager;

class TSourceSession: public TCommonSession {
private:
    using TBase = TCommonSession;
    const TTabletId SelfTabletId;
    std::shared_ptr<TSourceCursor> Cursor;
    YDB_READONLY_DEF(std::set<ui64>, PathIds);
    TTabletId DestinationTabletId = TTabletId(0);
public:
    TSourceSession(const TTabletId selfTabletId)
        : SelfTabletId(selfTabletId)
    {

    }

    TSourceSession(const TCommonSession& baseSession, const TTabletId selfTabletId, const std::set<ui64>& pathIds, const TTabletId destTabletId)
        : TBase(baseSession)
        , SelfTabletId(selfTabletId)
        , PathIds(pathIds)
        , DestinationTabletId(destTabletId)
    {
    }

    TTabletId GetDestinationTabletId() const {
        return DestinationTabletId;
    }

    TString DebugString() const {
        return TStringBuilder() << "{base=" << TBase::DebugString() << ";destination_tablet_id=" << (ui64)DestinationTabletId << ";}";
    }

    bool IsEqualTo(const TSourceSession& item) const {
        return
            TBase::IsEqualTo(item) &&
            DestinationTabletId == item.DestinationTabletId &&
            PathIds == item.PathIds;
    }

    std::shared_ptr<TSourceCursor> GetCursorVerified() const {
        AFL_VERIFY(!!Cursor);
        return Cursor;
    }

    bool TryNextCursor(const ui32 packIdx, const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager) {
        AFL_VERIFY(Cursor);
        if (packIdx != Cursor->GetPackIdx()) {
            return false;
        }
        Cursor->Next(sharedBlobsManager);
        return true;
    }

    [[nodiscard]] TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> AckFinished(NColumnShard::TColumnShard* self, const std::shared_ptr<TSourceSession>& selfPtr);
    [[nodiscard]] TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> AckData(NColumnShard::TColumnShard* self, const ui32 receivedPackIdx, const std::shared_ptr<TSourceSession>& selfPtr);
    [[nodiscard]] TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> AckLinks(NColumnShard::TColumnShard* self, const TTabletId tabletId, const ui32 packIdx, const std::shared_ptr<TSourceSession>& selfPtr);

    void ActualizeDestination();

    NKikimrColumnShardDataSharingProto::TSourceSession SerializeDataToProto() const {
        NKikimrColumnShardDataSharingProto::TSourceSession result;
        TBase::SerializeToProto(result);
        result.SetDestinationTabletId((ui64)DestinationTabletId);
        for (auto&& i : PathIds) {
            result.AddPathIds(i);
        }
        return result;
    }

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TSourceSession& proto,
        const std::optional<NKikimrColumnShardDataSharingProto::TSourceSession::TCursor>& protoCursor, const TColumnEngineForLogs& index,
        const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager);
};
}