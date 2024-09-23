#pragma once
#include "remove.h"
#include "write.h"
#include "read.h"
#include "gc.h"

#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/remove_gc.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/tiering/abstract/manager.h>

namespace NKikimr::NOlap {

class TCommonBlobsTracker: public IBlobInUseTracker {
private:
    // List of blobs that are used by in-flight requests
    THashMap<TUnifiedBlobId, i64> BlobsUseCount;
protected:
    virtual bool DoUseBlob(const TUnifiedBlobId& blobId) override;
    virtual bool DoFreeBlob(const TUnifiedBlobId& blobId) override;
public:
    virtual bool IsBlobInUsage(const NOlap::TUnifiedBlobId& blobId) const override;
    virtual void OnBlobFree(const TUnifiedBlobId& blobId) = 0;
};

class IBlobsStorageOperator {
private:
    YDB_READONLY_DEF(TTabletId, SelfTabletId);
    YDB_READONLY_DEF(TString, StorageId);
    std::shared_ptr<IBlobsGCAction> CurrentGCAction;
    YDB_READONLY(bool, Stopped, false);
    std::shared_ptr<NBlobOperations::TStorageCounters> Counters;
    YDB_ACCESSOR_DEF(std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>, SharedBlobs);
protected:
    virtual std::shared_ptr<IBlobsDeclareRemovingAction> DoStartDeclareRemovingAction(const std::shared_ptr<NBlobOperations::TRemoveDeclareCounters>& counters) = 0;
    virtual std::shared_ptr<IBlobsWritingAction> DoStartWritingAction() = 0;
    virtual std::shared_ptr<IBlobsReadingAction> DoStartReadingAction() = 0;
    virtual bool DoLoad(IBlobManagerDb& dbBlobs) = 0;
    virtual bool DoStop() {
        return true;
    }
    virtual const NSplitter::TSplitSettings& DoGetBlobSplitSettings() const {
        return Default<NSplitter::TSplitSettings>();
    }

    virtual void DoOnTieringModified(const std::shared_ptr<NColumnShard::ITiersManager>& tiers) = 0;
    virtual TString DoDebugString() const {
        return "";
    }

    virtual void DoStartGCAction(const std::shared_ptr<IBlobsGCAction>& counters) const = 0;

    void StartGCAction(const std::shared_ptr<IBlobsGCAction>& action) const {
        return DoStartGCAction(action);
    }

    virtual std::shared_ptr<IBlobsGCAction> DoCreateGCAction(const std::shared_ptr<NBlobOperations::TRemoveGCCounters>& counters) const = 0;
    std::shared_ptr<IBlobsGCAction> CreateGCAction(const std::shared_ptr<NBlobOperations::TRemoveGCCounters>& counters) const {
        return DoCreateGCAction(counters);
    }

public:
    IBlobsStorageOperator(const TString& storageId, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& sharedBlobs)
        : SelfTabletId(sharedBlobs->GetSelfTabletId())
        , StorageId(storageId)
        , SharedBlobs(sharedBlobs)
    {
        Counters = std::make_shared<NBlobOperations::TStorageCounters>(storageId);
    }

    void Stop();

    const NSplitter::TSplitSettings& GetBlobSplitSettings() const {
        return DoGetBlobSplitSettings();
    }

    virtual TTabletsByBlob GetBlobsToDelete() const = 0;
    virtual bool HasToDelete(const TUnifiedBlobId& blobId, const TTabletId initiatorTabletId) const = 0;
    virtual std::shared_ptr<IBlobInUseTracker> GetBlobsTracker() const = 0;

    virtual ~IBlobsStorageOperator() = default;

    TString DebugString() const {
        return TStringBuilder() << "(storage_id=" << StorageId << ";details=(" << DoDebugString() << "))";
    }

    bool Load(IBlobManagerDb& dbBlobs) {
        return DoLoad(dbBlobs);
    }
    void OnTieringModified(const std::shared_ptr<NColumnShard::ITiersManager>& tiers) {
        AFL_VERIFY(tiers);
        return DoOnTieringModified(tiers);
    }

    std::shared_ptr<IBlobsDeclareRemovingAction> StartDeclareRemovingAction(const NBlobOperations::EConsumer consumerId) {
        return DoStartDeclareRemovingAction(Counters->GetConsumerCounter(consumerId)->GetRemoveDeclareCounters());
    }
    std::shared_ptr<IBlobsWritingAction> StartWritingAction(const NBlobOperations::EConsumer consumerId) {
        auto result = DoStartWritingAction();
        result->SetCounters(Counters->GetConsumerCounter(consumerId)->GetWriteCounters());
        return result;
    }
    std::shared_ptr<IBlobsReadingAction> StartReadingAction(const NBlobOperations::EConsumer consumerId) {
        auto result = DoStartReadingAction();
        result->SetCounters(Counters->GetConsumerCounter(consumerId)->GetReadCounters());
        return result;
    }

    void StartGC(const std::shared_ptr<IBlobsGCAction>& action) {
        AFL_VERIFY(CurrentGCAction == action);
        AFL_VERIFY(!!action && action->IsInProgress());
        StartGCAction(action);
    }

    [[nodiscard]] std::shared_ptr<IBlobsGCAction> CreateGC() {
        NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_BLOBS)("storage_id", GetStorageId())("tablet_id", GetSelfTabletId());
        if (CurrentGCAction && CurrentGCAction->IsInProgress()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS)("event", "gc_in_progress");
            return nullptr;
        }
        if (Stopped) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS)("event", "stopped_on_gc");
            return nullptr;
        }
        auto task = CreateGCAction(Counters->GetConsumerCounter(NBlobOperations::EConsumer::GC)->GetRemoveGCCounters());
        CurrentGCAction = task;
        return CurrentGCAction;
    }
};

}
