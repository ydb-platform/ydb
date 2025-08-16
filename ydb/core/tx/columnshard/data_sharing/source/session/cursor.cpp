#include "source.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/events/transfer.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

namespace NKikimr::NOlap::NDataSharing {

void TSourceCursor::BuildSelection(const std::shared_ptr<IStoragesManager>& storagesManager, const TVersionedIndex& index) {
    THashMap<TInternalPathId, NEvents::TPathIdData> result;
    auto itCurrentPath = PortionsForSend.find(StartPathId);
    AFL_VERIFY(itCurrentPath != PortionsForSend.end());
    auto itPortion = itCurrentPath->second.find(StartPortionId);
    AFL_VERIFY(itPortion != itCurrentPath->second.end());
    ui32 count = 0;
    ui32 chunksCount = 0;
    bool selectMore = true;
    for (; itCurrentPath != PortionsForSend.end() && selectMore; ++itCurrentPath) {
        std::vector<std::shared_ptr<TPortionDataAccessor>> portions;
        for (; itPortion != itCurrentPath->second.end(); ++itPortion) {
            selectMore = (count < 10000 && chunksCount < 1000000);
            if (!selectMore) {
                NextPathId = itCurrentPath->first;
                NextPortionId = itPortion->first;
            } else {
                portions.emplace_back(itPortion->second);
                chunksCount += portions.back()->GetRecordsVerified().size();
                chunksCount += portions.back()->GetIndexesVerified().size();
                ++count;
            }
        }
        if (portions.size()) {
            NEvents::TPathIdData pathIdDataCurrent(itCurrentPath->first, portions);
            result.emplace(itCurrentPath->first, pathIdDataCurrent);
        }
    }
    if (selectMore) {
        AFL_VERIFY(!NextPathId);
        AFL_VERIFY(!NextPortionId);
    }

    THashMap<TTabletId, TTaskForTablet> tabletTasksResult;

    for (auto&& i : result) {
        THashMap<TTabletId, TTaskForTablet> tabletTasks = i.second.BuildLinkTabletTasks(storagesManager, SelfTabletId, TransferContext, index);
        for (auto&& t : tabletTasks) {
            auto it = tabletTasksResult.find(t.first);
            if (it == tabletTasksResult.end()) {
                tabletTasksResult.emplace(t.first, std::move(t.second));
            } else {
                it->second.Merge(t.second);
            }
        }
    }

    std::swap(Links, tabletTasksResult);
    std::swap(Selected, result);
}

bool TSourceCursor::NextSchemas() {
    NextSchemasIntervalBegin = NextSchemasIntervalEnd;

    if (NextSchemasIntervalEnd == SchemeHistory.size()) {
        return false;
    }

    i32 columnsToSend = 0;
    const i32 maxColumnsToSend = 10000;

    // limit the count of schemas to send based on their size in columns
    // maxColumnsToSend is pretty random value, so I don't care if columnsToSend would be greater then this value
    for (; NextSchemasIntervalEnd < SchemeHistory.size() && columnsToSend < maxColumnsToSend; ++NextSchemasIntervalEnd) {
        columnsToSend += SchemeHistory[NextSchemasIntervalEnd].ColumnsSize();
    }

    ++PackIdx;

    return true;
}

bool TSourceCursor::Next(const std::shared_ptr<IStoragesManager>& storagesManager, const TVersionedIndex& index) {
    if (NextSchemas()) {
        return true;
    }
    PreviousSelected = std::move(Selected);
    LinksModifiedTablets.clear();
    Selected.clear();
    if (!NextPathId) {
        AFL_VERIFY(!NextPortionId);
        return false;
    } else {
        AFL_VERIFY(NextPortionId);
    }
    StartPathId = *NextPathId;
    StartPortionId = *NextPortionId;
    NextPathId = {};
    NextPortionId = {};
    ++PackIdx;
    BuildSelection(storagesManager, index);
    AFL_VERIFY(IsValid());
    return true;
}

NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic TSourceCursor::SerializeDynamicToProto() const {
    NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic result;
    result.SetStartPathId(StartPathId.GetRawValue());
    result.SetStartPortionId(StartPortionId);
    result.SetNextSchemasIntervalBegin(NextSchemasIntervalBegin);
    result.SetNextSchemasIntervalEnd(NextSchemasIntervalEnd);
    if (NextPathId) {
        result.SetNextPathId(NextPathId->GetRawValue());
    }
    if (NextPortionId) {
        result.SetNextPortionId(*NextPortionId);
    }
    result.SetPackIdx(PackIdx);
    result.SetAckReceivedForPackIdx(AckReceivedForPackIdx);
    for (auto&& t : LinksModifiedTablets) {
        result.AddLinksModifiedTablets((ui64)t);
    }
    return result;
}

NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic TSourceCursor::SerializeStaticToProto() const {
    NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic result;
    for (auto&& i : PathPortionHashes) {
        auto* pathHash = result.AddPathHashes();
        pathHash->SetPathId(i.first.GetRawValue());
        pathHash->SetHash(i.second);
    }

    for (auto&& i : SchemeHistory) {
        *result.AddSchemeHistory() = i.GetProto();
    }
    return result;
}

NKikimr::TConclusionStatus TSourceCursor::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic& proto,
    const NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic& protoStatic) {
    StartPathId = TInternalPathId::FromRawValue(proto.GetStartPathId());
    StartPortionId = proto.GetStartPortionId();
    PackIdx = proto.GetPackIdx();
    NextSchemasIntervalBegin = proto.GetNextSchemasIntervalBegin();
    NextSchemasIntervalEnd = proto.GetNextSchemasIntervalEnd();
    if (!PackIdx) {
        return TConclusionStatus::Fail("Incorrect proto cursor PackIdx value: " + proto.DebugString());
    }
    if (proto.HasNextPathId()) {
        AFL_VERIFY(proto.GetNextPathId() == NextPathId->GetRawValue())("next_local", *NextPathId)("proto", proto.GetNextPathId());
    }
    if (proto.HasNextPortionId()) {
        AFL_VERIFY(proto.GetNextPortionId() == *NextPortionId)("next_local", *NextPortionId)("proto", proto.GetNextPortionId());
    }
    if (proto.HasAckReceivedForPackIdx()) {
        AckReceivedForPackIdx = proto.GetAckReceivedForPackIdx();
    } else {
        AckReceivedForPackIdx = 0;
    }
    for (auto&& i : proto.GetLinksModifiedTablets()) {
        LinksModifiedTablets.emplace((TTabletId)i);
    }
    for (auto&& i : protoStatic.GetPathHashes()) {
        PathPortionHashes.emplace(TInternalPathId::FromRawValue(i.GetPathId()), i.GetHash());
    }

    for (auto&& i : protoStatic.GetSchemeHistory()) {
        SchemeHistory.emplace_back(i);
    }
    if (PathPortionHashes.empty()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", "empty static cursor");
    } else {
        IsStaticSaved = true;
    }
    return TConclusionStatus::Success();
}

TSourceCursor::TSourceCursor(const TTabletId selfTabletId, const std::set<TInternalPathId>& pathIds, const TTransferContext transferContext)
    : SelfTabletId(selfTabletId)
    , TransferContext(transferContext)
    , PathIds(pathIds) {
}

void TSourceCursor::SaveToDatabase(NIceDb::TNiceDb& db, const TString& sessionId) {
    using SourceSessions = NKikimr::NColumnShard::Schema::SourceSessions;
    db.Table<SourceSessions>().Key(sessionId).Update(
        NIceDb::TUpdate<SourceSessions::CursorDynamic>(SerializeDynamicToProto().SerializeAsString()));
    if (!IsStaticSaved) {
        db.Table<SourceSessions>().Key(sessionId).Update(
            NIceDb::TUpdate<SourceSessions::CursorStatic>(SerializeStaticToProto().SerializeAsString()));
        IsStaticSaved = true;
    }
}

bool TSourceCursor::Start(const std::shared_ptr<IStoragesManager>& storagesManager,
    THashMap<TInternalPathId, std::vector<std::shared_ptr<TPortionDataAccessor>>>&& portions, std::vector<NOlap::TSchemaPresetVersionInfo>&& schemeHistory, const TVersionedIndex& index) {
    SchemeHistory = std::move(schemeHistory);
    AFL_VERIFY(!IsStartedFlag);
    std::map<TInternalPathId, std::map<ui32, std::shared_ptr<TPortionDataAccessor>>> local;
    NArrow::NHash::NXX64::TStreamStringHashCalcer hashCalcer(0);
    for (auto&& i : portions) {
        hashCalcer.Start();
        std::map<ui32, std::shared_ptr<TPortionDataAccessor>> portionsMap;
        for (auto&& p : i.second) {
            const ui64 portionId = p->GetPortionInfo().GetPortionId();
            hashCalcer.Update((ui8*)&portionId, sizeof(portionId));
            AFL_VERIFY(portionsMap.emplace(portionId, p).second);
        }
        auto it = PathPortionHashes.find(i.first);
        const ui64 hash = hashCalcer.Finish();
        if (it == PathPortionHashes.end()) {
            PathPortionHashes.emplace(i.first, ::ToString(hash));
        } else {
            AFL_VERIFY(::ToString(hash) == it->second);
        }
        local.emplace(i.first, std::move(portionsMap));
    }
    std::swap(PortionsForSend, local);

    if (PortionsForSend.empty()) {
        AFL_VERIFY(!StartPortionId);
        NextPathId = std::nullopt;
        NextPortionId = std::nullopt;
        // we don't need to send scheme history if we don't have data
        // this also invalidates cursor in this case
        SchemeHistory.clear();
        return true;
    } else if (!StartPathId) {
        AFL_VERIFY(PortionsForSend.begin()->second.size());
        NextPathId = PortionsForSend.begin()->first;
        NextPortionId = PortionsForSend.begin()->second.begin()->first;
        AFL_VERIFY(Next(storagesManager, index));
    } else {
        BuildSelection(storagesManager, index);
    }
    IsStartedFlag = true;
    return true;
}
} // namespace NKikimr::NOlap::NDataSharing
