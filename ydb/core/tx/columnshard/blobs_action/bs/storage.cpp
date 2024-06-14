#include "storage.h"
#include "remove.h"
#include "write.h"
#include "read.h"
#include "gc.h"
#include "gc_actor.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

std::shared_ptr<NKikimr::NOlap::IBlobsDeclareRemovingAction> TOperator::DoStartDeclareRemovingAction(const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters) {
    return std::make_shared<TDeclareRemovingAction>(GetStorageId(), counters, *Manager);
}

std::shared_ptr<NKikimr::NOlap::IBlobsWritingAction> TOperator::DoStartWritingAction() {
    return std::make_shared<TWriteAction>(GetStorageId(), Manager);
}

std::shared_ptr<NKikimr::NOlap::IBlobsReadingAction> TOperator::DoStartReadingAction() {
    return std::make_shared<TReadingAction>(GetStorageId(), BlobCacheActorId);
}

void TOperator::DoStartGCAction(const std::shared_ptr<IBlobsGCAction>& action) const {
    auto gcTask = dynamic_pointer_cast<TGCTask>(action);
    AFL_VERIFY(!!gcTask);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("event", "StartGC")("requests_count", gcTask->GetListsByGroupId().size());
    TActorContext::AsActorContext().Register(new TGarbageCollectionActor(gcTask, TabletActorId, GetSelfTabletId()));
}

std::shared_ptr<IBlobsGCAction> TOperator::DoCreateGCAction(const std::shared_ptr<TRemoveGCCounters>& counters) const {
    auto gcTask = Manager->BuildGCTask(GetStorageId(), Manager, GetSharedBlobs(), counters);
    if (!gcTask) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("event", "StartGCSkipped");
        return nullptr;
    } else {
        AFL_VERIFY(!gcTask->IsEmpty());
    }
    return gcTask;
}

TOperator::TOperator(const TString& storageId, 
    const NActors::TActorId& tabletActorId, const TIntrusivePtr<TTabletStorageInfo>& tabletInfo, 
    const ui64 generation, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& sharedBlobs)
    : TBase(storageId, sharedBlobs)
    , Manager(std::make_shared<TBlobManager>(tabletInfo, generation, sharedBlobs->GetSelfTabletId()))
    , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId())
    , TabletActorId(tabletActorId)
{
}

}
