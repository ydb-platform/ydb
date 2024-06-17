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
    return std::make_shared<TWriteAction>(GetStorageId(), GetCurrentOperator(), (ui64)GetSelfTabletId(), Generation, StepCounter.Inc(), GCInfo);
}

std::shared_ptr<IBlobsReadingAction> TOperator::DoStartReadingAction() {
    return std::make_shared<TReadingAction>(GetStorageId(), GetCurrentOperator());
}

std::shared_ptr<IBlobsGCAction> TOperator::DoCreateGCAction(const std::shared_ptr<TRemoveGCCounters>& counters) const {
    std::deque<TUnifiedBlobId> draftBlobIds;
    AFL_VERIFY(!!TabletActorId);
    TBlobsCategories categories(TTabletId(0));
    {
        TTabletsByBlob deleteBlobIds;
        if (!GCInfo->ExtractForGC(draftBlobIds, deleteBlobIds, 100000)) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("event", "start_gc_skipped")("reason", "cannot_extract");
            return nullptr;
        }
        categories = GetSharedBlobs()->BuildRemoveCategories(std::move(deleteBlobIds));
    }
    auto gcTask = std::make_shared<TGCTask>(GetStorageId(), std::move(draftBlobIds), GetCurrentOperator(), std::move(categories), counters);
    if (gcTask->IsEmpty()) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("event", "start_gc_skipped")("reason", "task_empty");
        return nullptr;
    }
    return gcTask;
}

void TOperator::DoStartGCAction(const std::shared_ptr<IBlobsGCAction>& action) const {
    auto gcTask = dynamic_pointer_cast<TGCTask>(action);
    AFL_VERIFY(!!gcTask);
    TActorContext::AsActorContext().Register(new TGarbageCollectionActor(gcTask, TabletActorId, GetSelfTabletId()));
}

void TOperator::InitNewExternalOperator(const NColumnShard::NTiers::TManager* tierManager) {
    NKikimrSchemeOp::TS3Settings settings;
    if (tierManager) {
        settings = tierManager->GetS3Settings();
    } else {
        settings.SetEndpoint("nowhere");
    }
    {
        TGuard<TSpinLock> changeLock(ChangeOperatorLock);
        if (CurrentS3Settings && CurrentS3Settings->SerializeAsString() == settings.SerializeAsString()) {
            return;
        }
    }
    auto extStorageConfig = NWrappers::NExternalStorage::IExternalStorageConfig::Construct(settings);
    AFL_VERIFY(extStorageConfig);
    auto extStorageOperator = extStorageConfig->ConstructStorageOperator(false);
    extStorageOperator->InitReplyAdapter(std::make_shared<NOlap::NBlobOperations::NTier::TRepliesAdapter>(GetStorageId()));
    TGuard<TSpinLock> changeLock(ChangeOperatorLock);
    CurrentS3Settings = settings;
    ExternalStorageOperator = extStorageOperator;
}

void TOperator::InitNewExternalOperator() {
    AFL_VERIFY(InitializationConfig);
    auto extStorageOperator = InitializationConfig->ConstructStorageOperator(false);
    extStorageOperator->InitReplyAdapter(std::make_shared<NOlap::NBlobOperations::NTier::TRepliesAdapter>(GetStorageId()));
    TGuard<TSpinLock> changeLock(ChangeOperatorLock);
    ExternalStorageOperator = extStorageOperator;
}

TOperator::TOperator(const TString& storageId, const NColumnShard::TColumnShard& shard, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& storageSharedBlobsManager)
    : TBase(storageId, storageSharedBlobsManager)
    , TabletActorId(shard.SelfId())
    , Generation(shard.Executor()->Generation())
{
    InitNewExternalOperator(shard.GetTierManagerPointer(storageId));
}

TOperator::TOperator(const TString& storageId, const TActorId& shardActorId, const std::shared_ptr<NWrappers::IExternalStorageConfig>& storageConfig,
    const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& storageSharedBlobsManager, const ui64 generation)
    : TBase(storageId, storageSharedBlobsManager)
    , TabletActorId(shardActorId)
    , Generation(generation)
    , InitializationConfig(storageConfig)
{
    InitNewExternalOperator();
}

void TOperator::DoOnTieringModified(const std::shared_ptr<NColumnShard::ITiersManager>& tiers) {
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
