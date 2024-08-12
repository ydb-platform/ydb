#pragma once
#include "blob_manager.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/gc.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TOperator: public IBlobsStorageOperator {
private:
    using TBase = IBlobsStorageOperator;
    std::shared_ptr<TBlobManager> Manager;
    const TActorId BlobCacheActorId;
    const TActorId TabletActorId;
protected:
    virtual std::shared_ptr<IBlobsDeclareRemovingAction> DoStartDeclareRemovingAction(const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters) override;
    virtual std::shared_ptr<IBlobsWritingAction> DoStartWritingAction() override;
    virtual std::shared_ptr<IBlobsReadingAction> DoStartReadingAction() override;
    virtual void DoStartGCAction(const std::shared_ptr<IBlobsGCAction>& action) const override;
    virtual std::shared_ptr<IBlobsGCAction> DoCreateGCAction(const std::shared_ptr<TRemoveGCCounters>& counters) const override;
    virtual bool DoLoad(IBlobManagerDb& dbBlobs) override {
        return Manager->LoadState(dbBlobs, GetSelfTabletId());
    }
    virtual void DoOnTieringModified(const std::shared_ptr<NColumnShard::ITiersManager>& /*tiers*/) override {
        return;
    }

public:
    TOperator(const TString& storageId, const NActors::TActorId& tabletActorId,
        const TIntrusivePtr<TTabletStorageInfo>& tabletInfo, const ui64 generation, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& sharedBlobs);

    virtual bool HasToDelete(const TUnifiedBlobId& blobId, const TTabletId tabletId) const override {
        return Manager->HasToDelete(blobId, tabletId);
    }

    virtual TTabletsByBlob GetBlobsToDelete() const override {
        return Manager->GetBlobsToDeleteAll();
    }

    virtual std::shared_ptr<IBlobInUseTracker> GetBlobsTracker() const override {
        return Manager;
    }
};

}
