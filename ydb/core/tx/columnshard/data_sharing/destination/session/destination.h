#pragma once
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/data_sharing/common/session/common.h>
#include <ydb/core/tx/columnshard/data_sharing/initiator/controller/abstract.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/sessions.pb.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>

#include <ydb/library/conclusion/result.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap {
class TColumnEngineForLogs;
class IStoragesManager;
}   // namespace NKikimr::NOlap

namespace NKikimr::NOlap::NDataSharing {

namespace NEvents {
class TPathIdData;
}

class TSourceCursorForDestination {
private:
    YDB_READONLY(TTabletId, TabletId, (TTabletId)0);
    YDB_READONLY(ui32, PackIdx, 0);
    YDB_READONLY(bool, DataFinished, false);

public:
    TSourceCursorForDestination() = default;
    TSourceCursorForDestination(const TTabletId tabletId)
        : TabletId(tabletId) {
    }

    TConclusionStatus ReceiveData(const ui32 packIdxReceived) {
        if (packIdxReceived != PackIdx + 1) {
            return TConclusionStatus::Fail("inconsistency packIdx");
        }
        PackIdx = packIdxReceived;
        return TConclusionStatus::Success();
    }

    TConclusionStatus ReceiveFinished() {
        if (DataFinished) {
            return TConclusionStatus::Fail("inconsistency DataFinished");
        }
        DataFinished = true;
        return TConclusionStatus::Success();
    }

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TDestinationSession::TSourceCursor& proto) {
        TabletId = (TTabletId)proto.GetTabletId();
        PackIdx = proto.GetPackIdx();
        DataFinished = proto.GetFinished();
        return TConclusionStatus::Success();
    }

    [[nodiscard]] NKikimrColumnShardDataSharingProto::TDestinationSession::TSourceCursor SerializeToProto() const {
        NKikimrColumnShardDataSharingProto::TDestinationSession::TSourceCursor result;
        result.SetTabletId((ui64)TabletId);
        result.SetPackIdx(PackIdx);
        result.SetFinished(DataFinished);
        return result;
    }
};

class TDestinationSession: public TCommonSession {
private:
    using TBase = TCommonSession;
    YDB_READONLY_DEF(TInitiatorControllerContainer, InitiatorController);
    using TPathIdsRemapper = THashMap<ui64, ui64>;
    YDB_READONLY_DEF(TPathIdsRemapper, PathIds);
    YDB_READONLY_FLAG(Confirmed, false);
    THashMap<TTabletId, TSourceCursorForDestination> Cursors;
    THashMap<TString, THashSet<TUnifiedBlobId>> CurrentBlobIds;

protected:
    virtual bool DoStart(const NColumnShard::TColumnShard& shard, const THashMap<ui64, std::vector<std::shared_ptr<TPortionInfo>>>& portions) override;
    virtual THashSet<ui64> GetPathIdsForStart() const override {
        THashSet<ui64> result;
        for (auto&& i : PathIds) {
            result.emplace(i.first);
        }
        return result;
    }

public:
    bool TryTakePortionBlobs(const TVersionedIndex& vIndex, const TPortionInfo& portion);

    void SetBarrierSnapshot(const TSnapshot& value) {
        TransferContext.SetSnapshotBarrier(value);
    }

    TSourceCursorForDestination& GetCursorVerified(const TTabletId& tabletId) {
        auto it = Cursors.find(tabletId);
        AFL_VERIFY(it != Cursors.end());
        return it->second;
    }

    TDestinationSession(const TInitiatorControllerContainer& controller, const TPathIdsRemapper& remapper, const TString& sessionId, const TTransferContext& context)
        : TBase(sessionId, "destination_base", context)
        , InitiatorController(controller)
        , PathIds(remapper) {
    }

    TDestinationSession()
        : TBase("dest_proto") {
    }

    void Confirm(const bool allowRepeat = false) {
        AFL_VERIFY(!ConfirmedFlag || allowRepeat);
        ConfirmedFlag = true;
    }

    [[nodiscard]] TConclusionStatus DataReceived(THashMap<ui64, NEvents::TPathIdData>&& data, TColumnEngineForLogs& index, const std::shared_ptr<IStoragesManager>& manager);

    ui32 GetSourcesInProgressCount() const;
    void SendCurrentCursorAck(const NColumnShard::TColumnShard& shard, const std::optional<TTabletId> tabletId);

    NKikimrColumnShardDataSharingProto::TDestinationSession SerializeDataToProto() const;

    [[nodiscard]] TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> ReceiveFinished(NColumnShard::TColumnShard* self, const TTabletId sourceTabletId, const std::shared_ptr<TDestinationSession>& selfPtr);

    [[nodiscard]] TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> AckInitiatorFinished(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& selfPtr);

    [[nodiscard]] TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> ReceiveData(NColumnShard::TColumnShard* self, const THashMap<ui64, NEvents::TPathIdData>& data,
        const ui32 receivedPackIdx, const TTabletId sourceTabletId, const std::shared_ptr<TDestinationSession>& selfPtr);

    NKikimrColumnShardDataSharingProto::TDestinationSession::TFullCursor SerializeCursorToProto() const;
    [[nodiscard]] TConclusionStatus DeserializeCursorFromProto(const NKikimrColumnShardDataSharingProto::TDestinationSession::TFullCursor& proto);

    [[nodiscard]] TConclusionStatus DeserializeDataFromProto(const NKikimrColumnShardDataSharingProto::TDestinationSession& proto, const TColumnEngineForLogs& index);
};

}   // namespace NKikimr::NOlap::NDataSharing
