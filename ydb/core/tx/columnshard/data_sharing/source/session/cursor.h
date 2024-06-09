#pragma once
#include <ydb/core/tx/columnshard/data_sharing/destination/events/transfer.h>
#include <ydb/core/tx/columnshard/data_sharing/modification/tasks/modification.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>

namespace NKikimr::NOlap {
class TColumnEngineForLogs;
class TVersionedIndex;
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
    std::set<ui64> PathIds;
    THashMap<ui64, TString> PathPortionHashes;
    bool IsStartedFlag = false;
    YDB_ACCESSOR(bool, StaticSaved, false);
    void BuildSelection(const std::shared_ptr<IStoragesManager>& storagesManager, const TVersionedIndex& index);
public:
    bool IsAckDataReceived() const {
        return AckReceivedForPackIdx == PackIdx;
    }

    bool IsStarted() const {
        return IsStartedFlag;
    }

    TConclusionStatus AckData(const ui64 packIdxReceived) {
        AFL_VERIFY(packIdxReceived <= PackIdx);
        if (packIdxReceived != PackIdx) {
            return TConclusionStatus::Fail("incorrect packIdx received for AckData: " + ::ToString(packIdxReceived) + " but expected: " + ::ToString(PackIdx));
        }
        AckReceivedForPackIdx = packIdxReceived;
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "SourceAckData")("pack", PackIdx)("pack_ack", AckReceivedForPackIdx)("links_ready", LinksModifiedTablets.size())("links_waiting", Links.size());
        return TConclusionStatus::Success();
    }

    TConclusionStatus AckLinks(const TTabletId tabletId, const ui64 packIdxReceived) {
        if (packIdxReceived != PackIdx) {
            return TConclusionStatus::Fail("incorrect packIdx received for AckLinks: " + ::ToString(packIdxReceived) + " but expected: " + ::ToString(PackIdx));
        }
        AFL_VERIFY(Links.contains(tabletId));
        if (LinksModifiedTablets.emplace(tabletId).second) {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "SourceAckData")("pack", PackIdx)("pack_ack", AckReceivedForPackIdx)("links_ready", LinksModifiedTablets.size())("links_waiting", Links.size());
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

    const THashMap<ui64, NEvents::TPathIdData> GetPreviousSelected() const {
        return PreviousSelected;
    }

    const THashMap<ui64, NEvents::TPathIdData>& GetSelected() const {
        return Selected;
    }

    const THashMap<TTabletId, TTaskForTablet>& GetLinks() const {
        return Links;
    }

    bool Next(const std::shared_ptr<IStoragesManager>& storagesManager, const TVersionedIndex& index);

    bool IsValid() {
        return Selected.size();
    }

    TSourceCursor(const TTabletId selfTabletId, const std::set<ui64>& pathIds, const TTransferContext transferContext);

    bool Start(const std::shared_ptr<IStoragesManager>& storagesManager, const THashMap<ui64, std::vector<std::shared_ptr<TPortionInfo>>>& portions, const TVersionedIndex& index);

    NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic SerializeDynamicToProto() const;
    NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic SerializeStaticToProto() const;

    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic& proto,
        const NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic& protoStatic);
};

}