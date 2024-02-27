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
    std::shared_ptr<TGCInfo> GCInfo = std::make_shared<TGCInfo>();

    NWrappers::NExternalStorage::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TSpinLock ChangeOperatorLock;
    NWrappers::NExternalStorage::IExternalStorageOperator::TPtr ExternalStorageOperator;

    NWrappers::NExternalStorage::IExternalStorageOperator::TPtr GetCurrentOperator() const;
    void InitNewExternalOperator(const NColumnShard::NTiers::TManager* tierManager);

    virtual TString DoDebugString() const override {
        return GetCurrentOperator()->DebugString();
    }
protected:
    virtual std::shared_ptr<IBlobsDeclareRemovingAction> DoStartDeclareRemovingAction(const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters) override;
    virtual std::shared_ptr<IBlobsWritingAction> DoStartWritingAction() override;
    virtual std::shared_ptr<IBlobsReadingAction> DoStartReadingAction() override;
    virtual std::shared_ptr<IBlobsGCAction> DoStartGCAction(const std::shared_ptr<TRemoveGCCounters>& counters) const override;
    virtual bool DoLoad(IBlobManagerDb& dbBlobs) override;
    virtual void DoOnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& tiers) override;

public:
    TOperator(const TString& storageId, const ui64 tabletID, const TActorIdentity tabletActorID, const NColumnShard::NTiers::TManager* tierManager, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& storageSharedBlobsManager);

    virtual TTabletsByBlob GetBlobsToDelete() const override {
        auto result = GCInfo->GetBlobsToDelete();
        result.Add(GCInfo->GetBlobsToDeleteInFuture());
        return result;
    }

    virtual std::shared_ptr<IBlobInUseTracker> GetBlobsTracker() const override {
        return GCInfo;
    }
};

}
