#include "adapter.h"
#include "gc.h"
#include "gc_actor.h"
#include "read.h"
#include "remove.h"
#include "storage.h"
#include "write.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/counters/error_collector.h>
#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/wrappers/unavailable_storage.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

NWrappers::NExternalStorage::IExternalStorageOperator::TPtr TOperator::GetCurrentOperator() const {
    TGuard<TSpinLock> changeLock(ChangeOperatorLock);
    return ExternalStorageOperator->Get();
}

std::shared_ptr<IBlobsDeclareRemovingAction> TOperator::DoStartDeclareRemovingAction(
    const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters) {
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
    auto gcTask = std::make_shared<TGCTask>(GetStorageId(), std::move(draftBlobIds), ExternalStorageOperator, std::move(categories), counters);
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
    if (auto op = NYDBTest::TControllers::GetColumnShardController()->GetStorageOperatorOverride(GetStorageId())) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER)("event", "override_external_operator")("storage", GetStorageId());
        DoInitNewExternalOperator(op, std::nullopt);
    } else if (tierManager && tierManager->IsReady()) {
        const NKikimrSchemeOp::TS3Settings& settings = tierManager->GetS3Settings();
        {
            TGuard<TSpinLock> changeLock(ChangeOperatorLock);
            if (CurrentS3Settings && CurrentS3Settings->SerializeAsString() == settings.SerializeAsString()) {
                return;
            }
        }
        auto extStorageConfig = NWrappers::NExternalStorage::IExternalStorageConfig::Construct(settings);
        AFL_VERIFY(extStorageConfig);
        DoInitNewExternalOperator(extStorageConfig->ConstructStorageOperator(false), settings);
    } else {
        DoInitNewExternalOperator(std::make_shared<NWrappers::NExternalStorage::TUnavailableExternalStorageOperator>(
                                      NWrappers::NExternalStorage::TUnavailableExternalStorageOperator(
                                          "tier_unavailable", TStringBuilder() << "Tier is not configured: " << GetStorageId())),
            std::nullopt);
    }
}

void TOperator::InitNewExternalOperator() {
    AFL_VERIFY(InitializationConfig);
    DoInitNewExternalOperator(InitializationConfig->ConstructStorageOperator(false), std::nullopt);
}

void TOperator::DoInitNewExternalOperator(const NWrappers::NExternalStorage::IExternalStorageOperator::TPtr& storageOperator,
    const std::optional<NKikimrSchemeOp::TS3Settings>& settings) {
    storageOperator->InitReplyAdapter(std::make_shared<NOlap::NBlobOperations::NTier::TRepliesAdapter>(ErrorCollector, GetStorageId()));
    {
        TGuard<TSpinLock> changeLock(ChangeOperatorLock);
        CurrentS3Settings = settings;
    }

    ExternalStorageOperator->Emplace(storageOperator);
}

TOperator::TOperator(const TString& storageId, const NColumnShard::TColumnShard& shard,
    const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& storageSharedBlobsManager)
    : TBase(storageId, storageSharedBlobsManager)
    , ErrorCollector(shard.Counters.GetEvictionCounters().TieringErrors)
    , TabletActorId(shard.SelfId())
    , Generation(shard.Executor()->Generation())
    , ExternalStorageOperator(std::make_shared<TExternalStorageOperatorHolder>()) {
    InitNewExternalOperator(shard.GetTierManagerPointer(storageId));
}

TOperator::TOperator(const TString& storageId, const TActorId& shardActorId,
    const std::shared_ptr<NWrappers::IExternalStorageConfig>& storageConfig,
    const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& storageSharedBlobsManager, const ui64 generation,
    const std::shared_ptr<NKikimr::NColumnShard::TErrorCollector>& errorCollector)
    : TBase(storageId, storageSharedBlobsManager)
    , ErrorCollector(std::move(errorCollector))
    , TabletActorId(shardActorId)
    , Generation(generation)
    , InitializationConfig(storageConfig)
    , ExternalStorageOperator(std::make_shared<TExternalStorageOperatorHolder>()) {
    InitNewExternalOperator();
}

void TOperator::DoOnTieringModified(const std::shared_ptr<NColumnShard::ITiersManager>& tiers) {
    auto* tierManager = tiers->GetManagerOptional(TBase::GetStorageId());
    InitNewExternalOperator(tierManager);
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

}   // namespace NKikimr::NOlap::NBlobOperations::NTier
