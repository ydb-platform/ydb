#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/wrappers/abstract.h>
#include "gc_info.h"

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TOperator: public IBlobsStorageOperator {
private:
    using TBase = IBlobsStorageOperator;
    const NActors::TActorId TabletActorId;
    const ui64 Generation;
    std::shared_ptr<TGCInfo> GCInfo = std::make_shared<TGCInfo>();
    std::optional<NKikimrSchemeOp::TS3Settings> CurrentS3Settings;
    NWrappers::NExternalStorage::IExternalStorageConfig::TPtr InitializationConfig;
    NWrappers::NExternalStorage::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TSpinLock ChangeOperatorLock;
    NWrappers::NExternalStorage::IExternalStorageOperator::TPtr ExternalStorageOperator;

    NWrappers::NExternalStorage::IExternalStorageOperator::TPtr GetCurrentOperator() const;
    TAtomicCounter StepCounter;
    void InitNewExternalOperator(const NColumnShard::NTiers::TManager* tierManager);
    void InitNewExternalOperator();

    virtual TString DoDebugString() const override {
        return GetCurrentOperator()->DebugString();
    }
protected:
    virtual std::shared_ptr<IBlobsDeclareRemovingAction> DoStartDeclareRemovingAction(const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters) override;
    virtual std::shared_ptr<IBlobsWritingAction> DoStartWritingAction() override;
    virtual std::shared_ptr<IBlobsReadingAction> DoStartReadingAction() override;
    virtual std::shared_ptr<IBlobsGCAction> DoCreateGCAction(const std::shared_ptr<TRemoveGCCounters>& counters) const override;
    virtual void DoStartGCAction(const std::shared_ptr<IBlobsGCAction>& action) const override;
    virtual bool DoLoad(IBlobManagerDb& dbBlobs) override;
    virtual void DoOnTieringModified(const std::shared_ptr<NColumnShard::ITiersManager>& tiers) override;

public:
    TOperator(const TString& storageId, const NColumnShard::TColumnShard& shard, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& storageSharedBlobsManager);
    TOperator(const TString& storageId, const TActorId& shardActorId, const std::shared_ptr<NWrappers::IExternalStorageConfig>& storageConfig,
        const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& storageSharedBlobsManager, const ui64 generation);

    virtual TTabletsByBlob GetBlobsToDelete() const override {
        auto result = GCInfo->GetBlobsToDelete();
        result.Add(GCInfo->GetBlobsToDeleteInFuture());
        return result;
    }

    virtual std::shared_ptr<IBlobInUseTracker> GetBlobsTracker() const override {
        return GCInfo;
    }

    virtual bool HasToDelete(const TUnifiedBlobId& blobId, const TTabletId tabletId) const override {
        return GCInfo->HasToDelete(blobId, tabletId);
    }

};

}
