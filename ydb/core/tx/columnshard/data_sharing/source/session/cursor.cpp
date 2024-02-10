#include "source.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/events/transfer.h>

namespace NKikimr::NOlap::NDataSharing {

void TSourceCursor::BuildSelection(const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager) {
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
        THashMap<TTabletId, TTaskForTablet> tabletTasks = i.second.BuildLinkTabletTasks(sharedBlobsManager, SelfTabletId, TransferContext);
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

bool TSourceCursor::Next(const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager) {
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
    BuildSelection(sharedBlobsManager);
    AFL_VERIFY(IsValid());
    return true;
}

NKikimrColumnShardDataSharingProto::TSourceSession::TCursor TSourceCursor::SerializeToProto() const {
    NKikimrColumnShardDataSharingProto::TSourceSession::TCursor result;
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

NKikimr::TConclusionStatus TSourceCursor::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TSourceSession::TCursor& proto, const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager) {
    StartPathId = proto.GetStartPathId();
    StartPortionId = proto.GetStartPortionId();
    PackIdx = proto.GetPackIdx();
    if (!PackIdx) {
        return TConclusionStatus::Fail("Incorrect proto cursor PackIdx value: " + proto.DebugString());
    }
    BuildSelection(sharedBlobsManager);
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
    return TConclusionStatus::Success();
}

TSourceCursor::TSourceCursor(const TColumnEngineForLogs& index, const TTabletId selfTabletId, const std::set<ui64>& pathIds, const TTransferContext transferContext, const TSnapshot& snapshotBorder)
    : SelfTabletId(selfTabletId)
    , TransferContext(transferContext)
{
    for (auto&& i : pathIds) {
        auto granule = index.GetGranuleOptional(i);
        if (!granule) {
            continue;
        }
        PortionsForSend.emplace(i, granule->GetPortionsOlderThenSnapshot(snapshotBorder));
    }
    AFL_VERIFY(PortionsForSend.size());
    AFL_VERIFY(PortionsForSend.begin()->second.size());
}

void TSourceCursor::Start(const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager) {
    NextPathId = PortionsForSend.begin()->first;
    NextPortionId = PortionsForSend.begin()->second.begin()->first;
    AFL_VERIFY(Next(sharedBlobsManager));
}

}