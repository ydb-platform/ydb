#include "storage.h"
#include "adapter.h"
#include "remove.h"
#include "write.h"
#include "read.h"
#include "gc.h"
#include "gc_actor.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/tiering/manager.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

NWrappers::NExternalStorage::IExternalStorageOperator::TPtr TOperator::GetCurrentOperator() const {
    TGuard<TSpinLock> changeLock(ChangeOperatorLock);
    return ExternalStorageOperator;
}

std::shared_ptr<IBlobsDeclareRemovingAction> TOperator::DoStartDeclareRemovingAction(const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters) {
    return std::make_shared<TDeclareRemovingAction>(GetStorageId(), GetSelfTabletId(), counters, GCInfo);
}

std::shared_ptr<IBlobsWritingAction> TOperator::DoStartWritingAction() {
    return std::make_shared<TWriteAction>(GetStorageId(), GetCurrentOperator(), (ui64)GetSelfTabletId(), GCInfo);
}

std::shared_ptr<IBlobsReadingAction> TOperator::DoStartReadingAction() {
    return std::make_shared<TReadingAction>(GetStorageId(), GetCurrentOperator());
}

std::shared_ptr<IBlobsGCAction> TOperator::DoStartGCAction(const std::shared_ptr<TRemoveGCCounters>& counters) const {
    std::deque<TUnifiedBlobId> draftBlobIds;
    TBlobsCategories categories(TTabletId(0));
    {
        TTabletsByBlob deleteBlobIds;
        if (!GCInfo->ExtractForGC(draftBlobIds, deleteBlobIds, 100000)) {
            return nullptr;
        }
        categories = GetSharedBlobs()->BuildRemoveCategories(std::move(deleteBlobIds));
    }
    auto gcTask = std::make_shared<TGCTask>(GetStorageId(), std::move(draftBlobIds), GetCurrentOperator(), std::move(categories), counters);
    TActorContext::AsActorContext().Register(new TGarbageCollectionActor(gcTask, TabletActorId, GetSelfTabletId()));
    return gcTask;
}

void TOperator::InitNewExternalOperator(const NColumnShard::NTiers::TManager* tierManager) {
    NKikimrSchemeOp::TS3Settings settings;
    if (tierManager) {
        settings = tierManager->GetS3Settings();
    } else {
        settings.SetEndpoint("nowhere");
    }
    auto extStorageConfig = NWrappers::NExternalStorage::IExternalStorageConfig::Construct(settings);
    AFL_VERIFY(extStorageConfig);
    auto extStorageOperator = extStorageConfig->ConstructStorageOperator(false);
    extStorageOperator->InitReplyAdapter(std::make_shared<NOlap::NBlobOperations::NTier::TRepliesAdapter>());
    TGuard<TSpinLock> changeLock(ChangeOperatorLock);
    ExternalStorageOperator = extStorageOperator;
}

TOperator::TOperator(const TString& storageId, const NColumnShard::TColumnShard& shard, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& storageSharedBlobsManager)
    : TBase(storageId, storageSharedBlobsManager)
    , TabletActorId(shard.SelfId())
{
    InitNewExternalOperator(shard.GetTierManagerPointer(storageId));
}

void TOperator::DoOnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& tiers) {
    auto* tierManager = tiers->GetManagerOptional(TBase::GetStorageId());
    if (tierManager) {
        InitNewExternalOperator(tierManager);
    } else {
        TGuard<TSpinLock> changeLock(ChangeOperatorLock);
        ExternalStorageOperator = nullptr;
    }
}

bool TOperator::DoLoad(IBlobManagerDb& dbBlobs) {
    TTabletsByBlob blobsToDelete;
    std::deque<TUnifiedBlobId> draftBlobIdsToRemove;
    if (!dbBlobs.LoadTierLists(GetStorageId(), blobsToDelete, draftBlobIdsToRemove, GetSelfTabletId())) {
        return false;
    }
    GCInfo->MutableBlobsToDelete() = std::move(blobsToDelete);
    GCInfo->MutableDraftBlobIdsToRemove() = std::move(draftBlobIdsToRemove);
    return true;
}

}
