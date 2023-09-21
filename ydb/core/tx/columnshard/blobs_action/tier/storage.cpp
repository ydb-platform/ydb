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
    const ui32 idx = CurrentOperatorIdx.Val();
    AFL_VERIFY(idx < ExternalStorageOperators.size())("idx", idx)("size", ExternalStorageOperators.size());
    auto result = ExternalStorageOperators[idx];
    Y_VERIFY(result);
    return result;
}

std::shared_ptr<IBlobsDeclareRemovingAction> TOperator::DoStartDeclareRemovingAction() {
    return std::make_shared<TDeclareRemovingAction>(GetStorageId(), GCInfo);
}

std::shared_ptr<IBlobsWritingAction> TOperator::DoStartWritingAction() {
    return std::make_shared<TWriteAction>(GetStorageId(), GetCurrentOperator(), TabletId, GCInfo);
}

std::shared_ptr<IBlobsReadingAction> TOperator::DoStartReadingAction() {
    return std::make_shared<TReadingAction>(GetStorageId(), GetCurrentOperator());
}

bool TOperator::DoStartGC() {
    std::vector<TUnifiedBlobId> draftBlobIds;
    std::vector<TUnifiedBlobId> deleteBlobIds;
    if (!GCInfo->ExtractForGC(draftBlobIds, deleteBlobIds, 100000)) {
        return false;
    }
    auto gcTask = std::make_shared<TGCTask>(GetStorageId(), std::move(draftBlobIds), std::move(deleteBlobIds), GetCurrentOperator());
    TActorContext::AsActorContext().Register(new TGarbageCollectionActor(gcTask, TabletActorId));
    return true;
}

void TOperator::InitNewExternalOperator(const NColumnShard::NTiers::TManager& tierManager) {
    auto extStorageConfig = NWrappers::NExternalStorage::IExternalStorageConfig::Construct(tierManager.GetS3Settings());
    AFL_VERIFY(extStorageConfig);
    auto extStorageOperator = extStorageConfig->ConstructStorageOperator(false);
    extStorageOperator->InitReplyAdapter(std::make_shared<NOlap::NBlobOperations::NTier::TRepliesAdapter>());
    ExternalStorageOperators.emplace_back(extStorageOperator);
    if (CurrentOperatorIdx.Val() + 1 < (i64)ExternalStorageOperators.size()) {
        CurrentOperatorIdx.Inc();
    }
}

TOperator::TOperator(const TString& storageId, const NColumnShard::TColumnShard& shard)
    : TBase(storageId)
    , TabletId(shard.TabletID())
    , TabletActorId(shard.SelfId())
{
    InitNewExternalOperator(shard.GetTierManagerVerified(storageId));
}

void TOperator::DoOnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& tiers) {
    AFL_VERIFY(ExternalStorageOperators.size());
    auto* tierManager = tiers->GetManagerOptional(TBase::GetStorageId());
    ui32 cleanCount = ExternalStorageOperators.size() - 1;
    if (tierManager) {
        InitNewExternalOperator(*tierManager);
    } else {
        cleanCount = ExternalStorageOperators.size();
    }
    for (ui32 i = 0; i < cleanCount; ++i) {
        if (ExternalStorageOperators[i].use_count() == 1) {
            ExternalStorageOperators[i] = nullptr;
        } else {
            break;
        }
    }
}

}
