#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/core/tx/columnshard/blob_manager.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TWriteAction: public IBlobsWritingAction {
private:
    using TBase = IBlobsWritingAction;
    NColumnShard::TBlobBatch BlobBatch;
    std::shared_ptr<NColumnShard::IBlobManager> Manager;
protected:
    virtual void DoSendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) override {
        return BlobBatch.SendWriteBlobRequest(data, blobId, TInstant::Max(), TActorContext::AsActorContext());
    }

    virtual void DoOnBlobWriteResult(const TUnifiedBlobId& blobId, const NKikimrProto::EReplyStatus status) override {
        return BlobBatch.OnBlobWriteResult(blobId.GetLogoBlobId(), status);
    }

    virtual void DoOnExecuteTxBeforeWrite(NColumnShard::TColumnShard& /*self*/, NColumnShard::TBlobManagerDb& /*dbBlobs*/) override {
        return;
    }

    virtual void DoOnCompleteTxBeforeWrite(NColumnShard::TColumnShard& /*self*/) override {
        return;
    }

    virtual void DoOnExecuteTxAfterWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs, const bool success) override;
    virtual void DoOnCompleteTxAfterWrite(NColumnShard::TColumnShard& /*self*/) override {

    }
public:
    virtual bool NeedDraftTransaction() const override {
        return false;
    }

    virtual TUnifiedBlobId AllocateNextBlobId(const TString& data) override {
        return BlobBatch.AllocateNextBlobId(data);
    }

    TWriteAction(const TString& storageId, const std::shared_ptr<NColumnShard::IBlobManager>& manager)
        : TBase(storageId)
        , BlobBatch(manager->StartBlobBatch())
        , Manager(manager)
    {

    }
};

}
