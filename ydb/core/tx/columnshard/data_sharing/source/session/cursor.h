#pragma once
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/events/transfer.h>
#include <ydb/core/tx/columnshard/data_sharing/modification/tasks/modification.h>
#include <ydb/core/tx/columnshard/engines/scheme/schema_version.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap {
class TColumnEngineForLogs;
class TVersionedIndex;
} // namespace NKikimr::NOlap

namespace NKikimr::NIceDb {
class TNiceDb;
}

namespace NKikimr::NOlap::NDataSharing {

class TSharedBlobsManager;

class TSourceCursor {
private:
    std::map<TInternalPathId, std::map<ui32, std::shared_ptr<TPortionDataAccessor>>> PortionsForSend;
    THashMap<TInternalPathId, NEvents::TPathIdData> PreviousSelected;
    THashMap<TInternalPathId, NEvents::TPathIdData> Selected;
    THashMap<TTabletId, TTaskForTablet> Links;
    std::vector<NOlap::TSchemaPresetVersionInfo> SchemeHistory;
    YDB_READONLY(TInternalPathId, StartPathId, TInternalPathId{});
    YDB_READONLY(ui64, StartPortionId, 0);
    YDB_READONLY(ui64, PackIdx, 0);
    TTabletId SelfTabletId;
    TTransferContext TransferContext;
    std::optional<TInternalPathId> NextPathId = TInternalPathId{};
    std::optional<ui64> NextPortionId = 0;

    // Begin/End of the next slice of SchemeHistory
    ui64 NextSchemasIntervalBegin = 0;
    ui64 NextSchemasIntervalEnd = 0;

    THashSet<TTabletId> LinksModifiedTablets;
    ui64 AckReceivedForPackIdx = 0;
    std::set<TInternalPathId> PathIds;
    THashMap<TInternalPathId, TString> PathPortionHashes;
    bool IsStartedFlag = false;
    bool IsStaticSaved = false;
    void BuildSelection(const std::shared_ptr<IStoragesManager>& storagesManager, const TVersionedIndex& index);
    NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic SerializeDynamicToProto() const;
    NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic SerializeStaticToProto() const;

    bool NextSchemas();

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

    const THashMap<TInternalPathId, NEvents::TPathIdData> GetPreviousSelected() const {
        return PreviousSelected;
    }

    TArrayRef<const NOlap::TSchemaPresetVersionInfo> GetSelectedSchemas() const {
        return TArrayRef<const NOlap::TSchemaPresetVersionInfo>(SchemeHistory.data() + NextSchemasIntervalBegin, NextSchemasIntervalEnd - NextSchemasIntervalBegin);
    }

    const THashMap<TInternalPathId, NEvents::TPathIdData>& GetSelected() const {
        return Selected;
    }

    const THashMap<TTabletId, TTaskForTablet>& GetLinks() const {
        return Links;
    }

    bool Next(const std::shared_ptr<IStoragesManager>& storagesManager, const TVersionedIndex& index);

    bool IsValid() {
        AFL_VERIFY(NextSchemasIntervalBegin <= SchemeHistory.size());
        return NextSchemasIntervalBegin < SchemeHistory.size() || Selected.size();
    }

    TSourceCursor(const TTabletId selfTabletId, const std::set<TInternalPathId>& pathIds, const TTransferContext transferContext);

    void SaveToDatabase(class NIceDb::TNiceDb& db, const TString& sessionId);

    bool Start(const std::shared_ptr<IStoragesManager>& storagesManager, THashMap<TInternalPathId, std::vector<std::shared_ptr<TPortionDataAccessor>>>&& portions,
        std::vector<NOlap::TSchemaPresetVersionInfo>&& schemeHistory, const TVersionedIndex& index);
    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic& proto,
        const NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic& protoStatic);
};

} // namespace NKikimr::NOlap::NDataSharing
