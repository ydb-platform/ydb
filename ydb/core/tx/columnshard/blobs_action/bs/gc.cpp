#include "gc.h"
#include "storage.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/blob_manager.h>

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

void TGCTask::DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& self, const std::shared_ptr<IBlobsGCAction>& taskAction) {
    if (KeepsToErase.size() || DeletesToErase.size()) {
        TActorContext::AsActorContext().Send(self.SelfId(), std::make_unique<NColumnShard::TEvPrivate::TEvGarbageCollectionFinished>(taskAction));
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
    Y_VERIFY(ev->Get()->Status == NKikimrProto::OK, "The caller must handle unsuccessful status");

    // Find the group for this result
    ui64 counterFromRequest = ev->Get()->PerGenerationCounter;
    auto itCounter = CounterToGroupInFlight.find(counterFromRequest);
    Y_VERIFY(itCounter != CounterToGroupInFlight.end());
    const ui32 group = itCounter->second;

    auto itGroup = ListsByGroupId.find(group);
    Y_VERIFY(itGroup != ListsByGroupId.end());
    const auto& keepList = itGroup->second.KeepList;
    const auto& dontKeepList = itGroup->second.DontKeepList;
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

        Y_VERIFY(CounterToGroupInFlight.emplace(perGenerationCounter, group).second);
        perGenerationCounter += requests[group]->PerGenerationCounterStepSize();
    }
    return std::move(requests);
}

}
