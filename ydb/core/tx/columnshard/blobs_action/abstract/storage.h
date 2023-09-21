#pragma once
#include "remove.h"
#include "write.h"
#include "read.h"

#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NColumnShard {
class TTiersManager;
}

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
    YDB_READONLY_DEF(TString, StorageId);
    friend class IBlobsGCAction;
    bool GCActivity = false;

    void FinishGC() {
        Y_VERIFY(GCActivity);
        GCActivity = false;
    }
protected:
    virtual std::shared_ptr<IBlobsDeclareRemovingAction> DoStartDeclareRemovingAction() = 0;
    virtual std::shared_ptr<IBlobsWritingAction> DoStartWritingAction() = 0;
    virtual std::shared_ptr<IBlobsReadingAction> DoStartReadingAction() = 0;
    virtual bool DoStartGC() = 0;
    virtual bool DoLoad(NColumnShard::IBlobManagerDb& dbBlobs) = 0;

    virtual void DoOnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& tiers) = 0;
    virtual TString DoDebugString() const {
        return "";
    }
public:
    IBlobsStorageOperator(const TString& storageId)
        : StorageId(storageId) {

    }

    virtual std::shared_ptr<IBlobInUseTracker> GetBlobsTracker() const = 0;

    virtual ~IBlobsStorageOperator() = default;

    TString DebugString() const {
        return TStringBuilder() << "(storage_id=" << StorageId << ";details=(" << DoDebugString() << "))";
    }

    bool Load(NColumnShard::IBlobManagerDb& dbBlobs) {
        return DoLoad(dbBlobs);
    }
    void OnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& tiers) {
        return DoOnTieringModified(tiers);
    }
    std::shared_ptr<IBlobsDeclareRemovingAction> StartDeclareRemovingAction() {
        return DoStartDeclareRemovingAction();
    }
    std::shared_ptr<IBlobsWritingAction> StartWritingAction() {
        return DoStartWritingAction();
    }
    std::shared_ptr<IBlobsReadingAction> StartReadingAction() {
        return DoStartReadingAction();
    }
    bool StartGC() {
        if (!GCActivity) {
            GCActivity = DoStartGC();
            return GCActivity;
        }
        return false;
    }
};

}
