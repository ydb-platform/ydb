#pragma once

#include "blob_manager.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TWriteAction: public IBlobsWritingAction {
private:
    using TBase = IBlobsWritingAction;
    TBlobBatch BlobBatch;
    std::shared_ptr<IBlobManager> Manager;
protected:
    virtual void DoSendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) override;

    virtual void DoOnBlobWriteResult(const TUnifiedBlobId& blobId, const NKikimrProto::EReplyStatus status) override {
        return BlobBatch.OnBlobWriteResult(blobId.GetLogoBlobId(), status);
    }

    virtual void DoOnExecuteTxBeforeWrite(NColumnShard::TColumnShard& /*self*/, TBlobManagerDb& /*dbBlobs*/) override {
        return;
    }

    virtual void DoOnCompleteTxBeforeWrite(NColumnShard::TColumnShard& /*self*/) override {
        return;
    }

    virtual void DoOnExecuteTxAfterWrite(NColumnShard::TColumnShard& self, TBlobManagerDb& dbBlobs, const bool blobsWroteSuccessfully) override;
    virtual void DoOnCompleteTxAfterWrite(NColumnShard::TColumnShard& /*self*/, const bool blobsWroteSuccessfully) override;
public:
    virtual bool NeedDraftTransaction() const override {
        return false;
    }

    virtual TUnifiedBlobId AllocateNextBlobId(const TString& data) override {
        return BlobBatch.AllocateNextBlobId(data);
    }

    TWriteAction(const TString& storageId, const std::shared_ptr<IBlobManager>& manager)
        : TBase(storageId)
        , BlobBatch(manager->StartBlobBatch())
        , Manager(manager)
    {

    }
};

}
