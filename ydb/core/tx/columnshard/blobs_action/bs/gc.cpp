#include "gc.h"
#include "storage.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

void TGCTask::RemoveBlobIdFromDB(const TTabletId tabletId, const TUnifiedBlobId& blobId, TBlobManagerDb& dbBlobs) {
    dbBlobs.EraseBlobToDelete(blobId, tabletId);
}

void TGCTask::DoOnExecuteTxAfterCleaning(NColumnShard::TColumnShard& /*self*/, TBlobManagerDb& dbBlobs) {
    for (auto&& i : KeepsToErase) {
        dbBlobs.EraseBlobToKeep(i);
    }
    Manager->OnGCFinishedOnExecute(CollectGenStepInFlight, dbBlobs);
}

bool TGCTask::DoOnCompleteTxAfterCleaning(NColumnShard::TColumnShard& /*self*/, const std::shared_ptr<IBlobsGCAction>& /*taskAction*/) {
    Manager->OnGCFinishedOnComplete(CollectGenStepInFlight);
    return true;
}

TGCTask::TGCTask(const TString& storageId, TGCListsByGroup&& listsByGroupId, const TGenStep& collectGenStepInFlight, std::deque<TUnifiedBlobId>&& keepsToErase,
    const std::shared_ptr<TBlobManager>& manager, TBlobsCategories&& blobsToRemove, const std::shared_ptr<TRemoveGCCounters>& counters)
    : TBase(storageId, std::move(blobsToRemove), counters)
    , ListsByGroupId(std::move(listsByGroupId))
    , CollectGenStepInFlight(collectGenStepInFlight)
    , KeepsToErase(std::move(keepsToErase))
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

    ListsByGroupId.erase(itGroup);
    CounterToGroupInFlight.erase(itCounter);
}

THashMap<ui32, std::unique_ptr<NKikimr::TEvBlobStorage::TEvCollectGarbage>> TGCTask::BuildRequests(ui64& perGenerationCounter, const ui64 tabletId, const ui64 currentGen) {
    const ui32 channelIdx = IBlobManager::BLOB_CHANNEL;
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
        Y_ABORT_UNLESS(CounterToGroupInFlight.emplace(perGenerationCounter, group).second);
        perGenerationCounter += requests[group]->PerGenerationCounterStepSize();
    }
    return std::move(requests);
}

}
