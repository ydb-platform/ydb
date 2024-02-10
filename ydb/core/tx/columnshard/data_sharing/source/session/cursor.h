#pragma once
#include <ydb/core/tx/columnshard/data_sharing/destination/events/transfer.h>
#include <ydb/core/tx/columnshard/data_sharing/modification/tasks/modification.h>

namespace NKikimr::NOlap {
class TColumnEngineForLogs;
}

namespace NKikimr::NOlap::NDataSharing {

class TSharedBlobsManager;

class TSourceCursor {
private:
    std::map<ui64, std::map<ui32, std::shared_ptr<TPortionInfo>>> PortionsForSend;
    THashMap<ui64, NEvents::TPathIdData> PreviousSelected;
    THashMap<ui64, NEvents::TPathIdData> Selected;
    THashMap<TTabletId, TTaskForTablet> Links;
    YDB_READONLY(ui64, StartPathId, 0);
    YDB_READONLY(ui64, StartPortionId, 0);
    YDB_READONLY(ui64, PackIdx, 0);
    TTabletId SelfTabletId;
    TTransferContext TransferContext;
    std::optional<ui64> NextPathId = 0;
    std::optional<ui64> NextPortionId = 0;
    THashSet<TTabletId> LinksModifiedTablets;
    ui64 AckReceivedForPackIdx = 0;
    void BuildSelection(const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager);
public:
    TConclusionStatus AckData(const ui64 packIdxReceived) {
        AFL_VERIFY(packIdxReceived <= PackIdx);
        if (packIdxReceived == PackIdx) {
            AckReceivedForPackIdx = PackIdx;
        } else {
            return TConclusionStatus::Fail("incorrect packIdx received for AckData: " + ::ToString(packIdxReceived) + " but expected: " + ::ToString(PackIdx));
        }
        return TConclusionStatus::Success();
    }

    TConclusionStatus AckLinks(const TTabletId tabletId, const ui64 packIdxReceived) {
        if (packIdxReceived == PackIdx) {
            AckReceivedForPackIdx = PackIdx;
        } else {
            return TConclusionStatus::Fail("incorrect packIdx received for AckLinks: " + ::ToString(packIdxReceived) + " but expected: " + ::ToString(PackIdx));
        }
        AFL_VERIFY(Links.contains(tabletId));
        if (LinksModifiedTablets.emplace(tabletId).second) {
            return TConclusionStatus::Success();
        } else {
            return TConclusionStatus::Fail("AckLinks repeated table");
        }
    }

    bool IsReadyForNext() const {
        return AckReceivedForPackIdx == PackIdx && LinksModifiedTablets.size() == Links.size();
    }

    const THashSet<TTabletId>& GetLinksModifiedTablets() const {
        return LinksModifiedTablets;
    }

    void AddLinksModifiedTablet(const TTabletId tabletId) {
        LinksModifiedTablets.emplace(tabletId);
    }

    void SetAckReceivedForPackIdx(const ui32 idx) {
        AckReceivedForPackIdx = idx;
    }

    ui64 GetAckReceivedForPackIdx() const {
        return AckReceivedForPackIdx;
    }

    const THashMap<ui64, NEvents::TPathIdData> GetPreviousSelected() const {
        return PreviousSelected;
    }

    const THashMap<ui64, NEvents::TPathIdData>& GetSelected() const {
        return Selected;
    }

    const THashMap<TTabletId, TTaskForTablet>& GetLinks() const {
        return Links;
    }

    bool Next(const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager);

    bool IsValid() {
        return Selected.size();
    }

    TSourceCursor(const TColumnEngineForLogs& index, const TTabletId selfTabletId, const std::set<ui64>& pathIds, const TTransferContext transferContext, const TSnapshot& snapshotBorder);

    void Start(const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager);

    NKikimrColumnShardDataSharingProto::TSourceSession::TCursor SerializeToProto() const;

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TSourceSession::TCursor& proto, const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager);
};

}