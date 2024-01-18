#pragma once
#include "common.h"
#include <util/generic/hash.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/write.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
class TBlobManagerDb;
}

namespace NKikimr::NOlap {

class IBlobsWritingAction: public ICommonBlobsAction {
private:
    using TBase = ICommonBlobsAction;
    bool WritingStarted = false;
    THashMap<TUnifiedBlobId, TMonotonic> WritingStart;
    ui64 SumSize = 0;
    ui32 BlobsWriteCount = 0;
    THashMap<TUnifiedBlobId, TString> BlobsForWrite;
    THashSet<TUnifiedBlobId> BlobsWaiting;
    bool Aborted = false;
    std::shared_ptr<NBlobOperations::TWriteCounters> Counters;
protected:
    virtual void DoOnExecuteTxBeforeWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) = 0;
    virtual void DoOnCompleteTxBeforeWrite(NColumnShard::TColumnShard& self) = 0;

    virtual void DoSendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) = 0;
    virtual void DoOnBlobWriteResult(const TUnifiedBlobId& blobId, const NKikimrProto::EReplyStatus status) = 0;

    virtual void DoOnExecuteTxAfterWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs, const bool success) = 0;
    virtual void DoOnCompleteTxAfterWrite(NColumnShard::TColumnShard& self, const bool success) = 0;

    virtual TUnifiedBlobId AllocateNextBlobId(const TString& data) = 0;
public:
    IBlobsWritingAction(const TString& storageId)
        : TBase(storageId)
    {

    }
    virtual ~IBlobsWritingAction();
    bool IsReady() const;

    void SetCounters(std::shared_ptr<NBlobOperations::TWriteCounters> counters) {
        Counters = counters;
    }

    const THashMap<TUnifiedBlobId, TString>& GetBlobsForWrite() const {
        return BlobsForWrite;
    }

    void Abort() {
        Aborted = true;
    }
    TUnifiedBlobId AddDataForWrite(const TString& data);

    void OnBlobWriteResult(const TUnifiedBlobId& blobId, const NKikimrProto::EReplyStatus status);

    void OnExecuteTxBeforeWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) {
        return DoOnExecuteTxBeforeWrite(self, dbBlobs);
    }

    ui32 GetBlobsCount() const {
        return BlobsWriteCount;
    }
    ui32 GetTotalSize() const {
        return SumSize;
    }

    virtual bool NeedDraftTransaction() const = 0;

    void OnCompleteTxBeforeWrite(NColumnShard::TColumnShard& self) {
        return DoOnCompleteTxBeforeWrite(self);
    }

    void OnExecuteTxAfterWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs, const bool success) {
        return DoOnExecuteTxAfterWrite(self, dbBlobs, success);
    }

    void OnCompleteTxAfterWrite(NColumnShard::TColumnShard& self, const bool success) {
        return DoOnCompleteTxAfterWrite(self, success);
    }

    void SendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId);
};

}
