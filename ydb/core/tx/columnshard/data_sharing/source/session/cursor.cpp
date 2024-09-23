#include "source.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/events/transfer.h>
#include <ydb/core/formats/arrow/hash/xx_hash.h>

namespace NKikimr::NOlap::NDataSharing {

void TSourceCursor::BuildSelection(const std::shared_ptr<IStoragesManager>& storagesManager, const TVersionedIndex& index) {
    THashMap<ui64, NEvents::TPathIdData> result;
    auto itCurrentPath = PortionsForSend.find(StartPathId);
    AFL_VERIFY(itCurrentPath != PortionsForSend.end());
    auto itPortion = itCurrentPath->second.find(StartPortionId);
    AFL_VERIFY(itPortion != itCurrentPath->second.end());
    ui32 count = 0;
    ui32 chunksCount = 0;
    bool selectMore = true;
    for (; itCurrentPath != PortionsForSend.end() && selectMore; ++itCurrentPath) {
        std::vector<TPortionInfo> portions;
        for (; itPortion != itCurrentPath->second.end(); ++itPortion) {
            selectMore = (count < 10000 && chunksCount < 1000000);
            if (!selectMore) {
                NextPathId = itCurrentPath->first;
                NextPortionId = itPortion->first;
            } else {
                portions.emplace_back(*itPortion->second);
                chunksCount += portions.back().GetRecords().size();
                chunksCount += portions.back().GetIndexes().size();
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

bool TSourceCursor::Next(const std::shared_ptr<IStoragesManager>& storagesManager, const TVersionedIndex& index) {
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
    result.SetStartPathId(StartPathId);
    result.SetStartPortionId(StartPortionId);
    if (NextPathId) {
        result.SetNextPathId(*NextPathId);
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
        pathHash->SetPathId(i.first);
        pathHash->SetHash(i.second);
    }
    return result;
}

NKikimr::TConclusionStatus TSourceCursor::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic& proto,
    const NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic& protoStatic) {
    StartPathId = proto.GetStartPathId();
    StartPortionId = proto.GetStartPortionId();
    PackIdx = proto.GetPackIdx();
    if (!PackIdx) {
        return TConclusionStatus::Fail("Incorrect proto cursor PackIdx value: " + proto.DebugString());
    }
    if (proto.HasNextPathId()) {
        AFL_VERIFY(proto.GetNextPathId() == *NextPathId)("next_local", *NextPathId)("proto", proto.GetNextPathId());
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
        PathPortionHashes.emplace(i.GetPathId(), i.GetHash());
    }
    AFL_VERIFY(PathPortionHashes.size());
    StaticSaved = true;
    return TConclusionStatus::Success();
}

TSourceCursor::TSourceCursor(const TTabletId selfTabletId, const std::set<ui64>& pathIds, const TTransferContext transferContext)
    : SelfTabletId(selfTabletId)
    , TransferContext(transferContext)
    , PathIds(pathIds)
{
}

bool TSourceCursor::Start(const std::shared_ptr<IStoragesManager>& storagesManager, const THashMap<ui64, std::vector<std::shared_ptr<TPortionInfo>>>& portions, const TVersionedIndex& index) {
    AFL_VERIFY(!IsStartedFlag);
    std::map<ui64, std::map<ui32, std::shared_ptr<TPortionInfo>>> local;
    std::vector<std::shared_ptr<TPortionInfo>> portionsLock;
    NArrow::NHash::NXX64::TStreamStringHashCalcer hashCalcer(0);
    for (auto&& i : portions) {
        hashCalcer.Start();
        std::map<ui32, std::shared_ptr<TPortionInfo>> portionsMap;
        for (auto&& p : i.second) {
            const ui64 portionId = p->GetPortionId();
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
    if (!StartPathId) {
        AFL_VERIFY(PortionsForSend.size());
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

}