#include "gc.h"
#include "storage.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/blob_manager.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

void TGCTask::DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& /*self*/, NColumnShard::TBlobManagerDb& dbBlobs) {
    size_t numBlobs = 0;

    for (; KeepsToErase.size() && numBlobs < NColumnShard::TLimits::MAX_BLOBS_TO_DELETE; ++numBlobs) {
        dbBlobs.EraseBlobToKeep(KeepsToErase.front());
        KeepsToErase.pop_front();
    }

    for (; DeletesToErase.size() && numBlobs < NColumnShard::TLimits::MAX_BLOBS_TO_DELETE; ++numBlobs) {
        dbBlobs.EraseBlobToDelete(DeletesToErase.front());
        DeletesToErase.pop_front();
    }
    if (KeepsToErase.empty() && DeletesToErase.empty()) {
        Manager->OnGCFinished(CollectGenStepInFlight, dbBlobs);
    }
}

bool TGCTask::DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) {
    if (KeepsToErase.size() || DeletesToErase.size()) {
        TActorContext::AsActorContext().Send(self.SelfId(), std::make_unique<NColumnShard::TEvPrivate::TEvGarbageCollectionFinished>(taskAction));
        return false;
    } else {
        return true;
    }
}

TGCTask::TGCTask(const TString& storageId, TGCListsByGroup&& listsByGroupId, const NColumnShard::TGenStep& collectGenStepInFlight, std::deque<TUnifiedBlobId>&& keepsToErase, std::deque<TUnifiedBlobId>&& deletesToErase,
    const std::shared_ptr<NColumnShard::TBlobManager>& manager)
    : TBase(storageId)
    , ListsByGroupId(std::move(listsByGroupId))
    , CollectGenStepInFlight(collectGenStepInFlight)
    , KeepsToErase(std::move(keepsToErase))
    , DeletesToErase(std::move(deletesToErase))
    , Manager(manager)
{
}

void TGCTask::OnGCResult(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev) {
    AFL_VERIFY(ev->Get()->Status == NKikimrProto::OK)("status", ev->Get()->Status)("details", ev->Get()->ToString())("action_id", GetActionGuid());

    // Find the group for this result
    ui64 counterFromRequest = ev->Get()->PerGenerationCounter;
    auto itCounter = CounterToGroupInFlight.find(counterFromRequest);
    Y_ABORT_UNLESS(itCounter != CounterToGroupInFlight.end());
    const ui32 group = itCounter->second;

    auto itGroup = ListsByGroupId.find(group);
    Y_ABORT_UNLESS(itGroup != ListsByGroupId.end());
    const auto& keepList = itGroup->second.KeepList;
    const auto& dontKeepList = itGroup->second.DontKeepList;

    for (auto&& i : dontKeepList) {
        Counters->OnReply(i.BlobSize());
    }

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("actor", "OnGCResult")("keep_list", keepList.size())("dont_keep_list", dontKeepList.size());

    for (const auto& blobId : keepList) {
        KeepsToErase.emplace_back(TUnifiedBlobId(group, blobId));
    }
    for (const auto& blobId : dontKeepList) {
        DeletesToErase.emplace_back(TUnifiedBlobId(group, blobId));
    }

    ListsByGroupId.erase(itGroup);
    CounterToGroupInFlight.erase(itCounter);
}

THashMap<ui32, std::unique_ptr<NKikimr::TEvBlobStorage::TEvCollectGarbage>> TGCTask::BuildRequests(ui64& perGenerationCounter, const ui64 tabletId, const ui64 currentGen) {
    const ui32 channelIdx = NColumnShard::IBlobManager::BLOB_CHANNEL;
    // Make per group requests
    THashMap<ui32, std::unique_ptr<TEvBlobStorage::TEvCollectGarbage>> requests;
    for (const auto& gl : ListsByGroupId) {
        ui32 group = gl.first;
        requests[group] = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(
            tabletId, currentGen, perGenerationCounter,
            channelIdx, true,
            std::get<0>(CollectGenStepInFlight), std::get<1>(CollectGenStepInFlight),
            new TVector<TLogoBlobID>(gl.second.KeepList.begin(), gl.second.KeepList.end()),
            new TVector<TLogoBlobID>(gl.second.DontKeepList.begin(), gl.second.DontKeepList.end()),
            TInstant::Max(), true);
        for (auto&& i : gl.second.DontKeepList) {
            Counters->OnRequest(i.BlobSize());
        }
        Y_ABORT_UNLESS(CounterToGroupInFlight.emplace(perGenerationCounter, group).second);
        perGenerationCounter += requests[group]->PerGenerationCounterStepSize();
    }
    return std::move(requests);
}

}
