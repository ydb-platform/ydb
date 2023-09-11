#pragma once

#include "abstract.h"
#include <ydb/core/tx/columnshard/blob_manager.h>

namespace NKikimr::NOlap {

class TBSWriteAction: public IBlobsAction {
private:
    NColumnShard::TBlobBatch BlobBatch;
protected:
    virtual void DoSendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) override {
        return BlobBatch.SendWriteBlobRequest(data, blobId, TInstant::Max(), TActorContext::AsActorContext());
    }

    virtual void DoOnBlobWriteResult(const TLogoBlobID& blobId, const NKikimrProto::EReplyStatus status) override {
        return BlobBatch.OnBlobWriteResult(blobId, status);
    }

    virtual void DoOnExecuteTxBeforeWrite(NColumnShard::TColumnShard& /*self*/, NColumnShard::TBlobManagerDb& /*dbBlobs*/) override {
        return;
    }

    virtual void DoOnCompleteTxBeforeWrite(NColumnShard::TColumnShard& /*self*/) override {
        return;
    }

    virtual void DoOnExecuteTxAfterWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) override;
    virtual void DoOnCompleteTxAfterWrite(NColumnShard::TColumnShard& /*self*/) override {

    }
public:
    virtual ui32 GetBlobsCount() const override {
        return BlobBatch.GetBlobCount();
    }
    virtual ui32 GetTotalSize() const override {
        return BlobBatch.GetTotalSize();
    }
    virtual bool NeedDraftTransaction() const override {
        return false;
    }

    virtual TUnifiedBlobId AllocateNextBlobId(const TString& data) override {
        return BlobBatch.AllocateNextBlobId(data);
    }
    virtual bool IsReady() const override {
        return BlobBatch.AllBlobWritesCompleted();
    }

    TBSWriteAction(NColumnShard::IBlobManager& blobManager)
        : BlobBatch(blobManager.StartBlobBatch())
    {

    }
};

}
