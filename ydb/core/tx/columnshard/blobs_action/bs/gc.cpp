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

TGCTask::TGCTask(const TString& storageId, TGCListsByGroup&& listsByGroupId, const std::optional<TGenStep>& collectGenStepInFlight, std::deque<TUnifiedBlobId>&& keepsToErase,
    const std::shared_ptr<TBlobManager>& manager, TBlobsCategories&& blobsToRemove, const std::shared_ptr<TRemoveGCCounters>& counters,
    const ui64 tabletId, const ui64 currentGen)
    : TBase(storageId, std::move(blobsToRemove), counters)
    , ListsByGroupId(std::move(listsByGroupId))
    , CollectGenStepInFlight(collectGenStepInFlight)
    , TabletId(tabletId)
    , CurrentGen(currentGen)
    , KeepsToErase(std::move(keepsToErase))
    , Manager(manager)
{
}

void TGCTask::OnGCResult(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev) {
    AFL_VERIFY(ev->Get()->Status == NKikimrProto::OK)("status", ev->Get()->Status)("details", ev->Get()->ToString())("action_id", GetActionGuid());
    auto itGroup = ListsByGroupId.find(ev->Cookie);
    Y_ABORT_UNLESS(itGroup != ListsByGroupId.end());
    ListsByGroupId.erase(itGroup);
}

namespace {
static TAtomicCounter PerGenerationCounter = 1;
}

std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> TGCTask::BuildRequest(const ui64 groupId) const {
    const ui32 channelIdx = IBlobManager::BLOB_CHANNEL;
    auto it = ListsByGroupId.find(groupId);
    AFL_VERIFY(it != ListsByGroupId.end());
    AFL_VERIFY(++it->second.RequestsCount < 10);
    auto result = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(
        TabletId, CurrentGen, PerGenerationCounter.Val(),
        channelIdx, !!CollectGenStepInFlight,
        CollectGenStepInFlight ? std::get<0>(*CollectGenStepInFlight) : 0, CollectGenStepInFlight ? std::get<1>(*CollectGenStepInFlight) : 0,
        new TVector<TLogoBlobID>(it->second.KeepList.begin(), it->second.KeepList.end()),
        new TVector<TLogoBlobID>(it->second.DontKeepList.begin(), it->second.DontKeepList.end()),
        TInstant::Max(), true);
    result->PerGenerationCounter = PerGenerationCounter.Add(result->PerGenerationCounterStepSize());
    return std::move(result);
}

}
