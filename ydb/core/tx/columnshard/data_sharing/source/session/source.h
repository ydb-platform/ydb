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
protected:
    virtual bool DoStart(const NColumnShard::TColumnShard& shard, const THashMap<ui64, std::vector<std::shared_ptr<TPortionInfo>>>& portions) override;
    virtual THashSet<ui64> GetPathIdsForStart() const override {
        THashSet<ui64> result;
        for (auto&& i : PathIds) {
            result.emplace(i);
        }
        return result;
    }
public:
    TSourceSession(const TTabletId selfTabletId)
        : TBase("source_proto")
        , SelfTabletId(selfTabletId)
    {

    }

    TSourceSession(const TString& sessionId, const TTransferContext& transfer, const TTabletId selfTabletId, const std::set<ui64>& pathIds, const TTabletId destTabletId)
        : TBase(sessionId, "source_base", transfer)
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
/*
    bool TryNextCursor(const ui32 packIdx, const std::shared_ptr<IStoragesManager>& storagesManager, const TVersionedIndex& index) {
        AFL_VERIFY(Cursor);
        if (packIdx != Cursor->GetPackIdx()) {
            return false;
        }
        Cursor->Next(storagesManager, index);
        return true;
    }
*/
    [[nodiscard]] TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> AckFinished(NColumnShard::TColumnShard* self, const std::shared_ptr<TSourceSession>& selfPtr);
    [[nodiscard]] TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> AckData(NColumnShard::TColumnShard* self, const ui32 receivedPackIdx, const std::shared_ptr<TSourceSession>& selfPtr);
    [[nodiscard]] TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> AckLinks(NColumnShard::TColumnShard* self, const TTabletId tabletId, const ui32 packIdx, const std::shared_ptr<TSourceSession>& selfPtr);

    void ActualizeDestination(const NColumnShard::TColumnShard& shard, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager);

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
        const std::optional<NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic>& protoCursor, 
        const std::optional<NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic>& protoCursorStatic);
};
}