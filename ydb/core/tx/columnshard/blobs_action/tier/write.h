#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/core/tx/columnshard/blob_manager.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/wrappers/abstract.h>
#include "gc_info.h"

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TWriteAction: public IBlobsWritingAction {
private:
    using TBase = IBlobsWritingAction;
    const NWrappers::NExternalStorage::IExternalStorageOperator::TPtr ExternalStorageOperator;
    std::shared_ptr<TGCInfo> GCInfo;
    const ui64 TabletId;
protected:
    virtual void DoSendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) override;

    virtual void DoOnBlobWriteResult(const TUnifiedBlobId& /*blobId*/, const NKikimrProto::EReplyStatus status) override {
        Y_VERIFY(status == NKikimrProto::EReplyStatus::OK);
    }

    virtual void DoOnExecuteTxBeforeWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) override;
    virtual void DoOnCompleteTxBeforeWrite(NColumnShard::TColumnShard& /*self*/) override {
        return;
    }

    virtual void DoOnExecuteTxAfterWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs, const bool success) override;
    virtual void DoOnCompleteTxAfterWrite(NColumnShard::TColumnShard& /*self*/) override {

    }
public:
    virtual bool NeedDraftTransaction() const override {
        return true;
    }

    virtual TUnifiedBlobId AllocateNextBlobId(const TString& data) override;

    TWriteAction(const TString& storageId, const NWrappers::NExternalStorage::IExternalStorageOperator::TPtr& storageOperator, const ui64 tabletId, const std::shared_ptr<TGCInfo>& gcInfo)
        : TBase(storageId)
        , ExternalStorageOperator(storageOperator)
        , GCInfo(gcInfo)
        , TabletId(tabletId)
    {

    }
};

}
