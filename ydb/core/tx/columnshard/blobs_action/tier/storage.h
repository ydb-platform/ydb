#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blob_manager.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/wrappers/abstract.h>
#include "gc_info.h"

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TOperator: public IBlobsStorageOperator {
private:
    using TBase = IBlobsStorageOperator;
    const ui64 TabletId;
    const NActors::TActorId TabletActorId;
    TAtomicCounter CurrentOperatorIdx = 0;
    std::deque<NWrappers::NExternalStorage::IExternalStorageOperator::TPtr> ExternalStorageOperators;
    std::shared_ptr<TGCInfo> GCInfo = std::make_shared<TGCInfo>();
    std::optional<NKikimrSchemeOp::TS3Settings> CurrentS3Settings;
    NWrappers::NExternalStorage::IExternalStorageConfig::TPtr ExternalStorageConfig;
    NWrappers::NExternalStorage::IExternalStorageOperator::TPtr ExternalStorageOperator;

    NWrappers::NExternalStorage::IExternalStorageOperator::TPtr GetCurrentOperator() const;
    void InitNewExternalOperator(const NColumnShard::NTiers::TManager* tierManager);

    virtual TString DoDebugString() const override {
        return GetCurrentOperator()->DebugString();
    }
protected:
    virtual std::shared_ptr<IBlobsDeclareRemovingAction> DoStartDeclareRemovingAction() override;
    virtual std::shared_ptr<IBlobsWritingAction> DoStartWritingAction() override;
    virtual std::shared_ptr<IBlobsReadingAction> DoStartReadingAction() override;
    virtual std::shared_ptr<IBlobsGCAction> DoStartGCAction(const std::shared_ptr<TRemoveGCCounters>& counters) const override;
    virtual bool DoLoad(NColumnShard::IBlobManagerDb& dbBlobs) override {
        dbBlobs.LoadTierLists(GetStorageId(), GCInfo->MutableBlobsToDelete(), GCInfo->MutableDraftBlobIdsToRemove());
        return true;
    }
    virtual void DoOnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& tiers) override;

public:
    TOperator(const TString& storageId, const NColumnShard::TColumnShard& shard);

    virtual std::shared_ptr<IBlobInUseTracker> GetBlobsTracker() const override {
        return GCInfo;
    }
};

}
