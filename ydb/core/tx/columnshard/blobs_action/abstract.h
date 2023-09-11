#pragma once
#include "blob_manager_db.h"

#include <ydb/core/protos/base.pb.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/counters/blobs_manager.h>

#include <ydb/core/tablet_flat/flat_executor.h>
#include <ydb/core/util/backoff.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

#include <util/generic/string.h>

namespace NKikimr::NOlap {

using NOlap::TUnifiedBlobId;
using NOlap::TBlobRange;
using NOlap::TEvictedBlob;
using NOlap::EEvictState;
using NKikimrTxColumnShard::TEvictMetadata;

class IBlobsAction {
protected:
    virtual void DoOnExecuteTxBeforeWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) = 0;
    virtual void DoOnCompleteTxBeforeWrite(NColumnShard::TColumnShard& self) = 0;

    virtual void DoSendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) = 0;
    virtual void DoOnBlobWriteResult(const TLogoBlobID& blobId, const NKikimrProto::EReplyStatus status) = 0;

    virtual void DoOnExecuteTxAfterWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) = 0;
    virtual void DoOnCompleteTxAfterWrite(NColumnShard::TColumnShard& self) = 0;
public:
    virtual ~IBlobsAction() = default;
    virtual bool IsReady() const = 0;

    virtual TUnifiedBlobId AllocateNextBlobId(const TString& data) = 0;

    void OnBlobWriteResult(const TLogoBlobID& blobId, const NKikimrProto::EReplyStatus status) {
        return DoOnBlobWriteResult(blobId, status);
    }

    void OnExecuteTxBeforeWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) {
        return DoOnExecuteTxBeforeWrite(self, dbBlobs);
    }

    virtual ui32 GetBlobsCount() const = 0;
    virtual ui32 GetTotalSize() const = 0;

    virtual bool NeedDraftTransaction() const = 0;

    void OnCompleteTxBeforeWrite(NColumnShard::TColumnShard& self) {
        return DoOnCompleteTxBeforeWrite(self);
    }

    void OnExecuteTxAfterWrite(NColumnShard::TColumnShard& self, NColumnShard::TBlobManagerDb& dbBlobs) {
        return DoOnExecuteTxAfterWrite(self, dbBlobs);
    }

    void OnCompleteTxAfterWrite(NColumnShard::TColumnShard& self) {
        return DoOnCompleteTxAfterWrite(self);
    }

    void SendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) {
        return DoSendWriteBlobRequest(data, blobId);
    }
};

}
