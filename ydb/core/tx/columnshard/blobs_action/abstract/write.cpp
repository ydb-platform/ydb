#include "write.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

TUnifiedBlobId IBlobsWritingAction::AddDataForWrite(const TString& data, const std::optional<TUnifiedBlobId>& externalBlobId) {
    Y_ABORT_UNLESS(!WritingStarted);
    auto blobId = AllocateNextBlobId(data);
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_BLOBS)("generated_blob_id", blobId.ToStringNew());
    AddDataForWrite(externalBlobId.value_or(blobId), data);
    return externalBlobId.value_or(blobId);
}

void IBlobsWritingAction::AddDataForWrite(const TUnifiedBlobId& blobId, const TString& data) {
    AFL_VERIFY(blobId.IsValid())("blob_id", blobId.ToStringNew());
    AFL_VERIFY(blobId.BlobSize() == data.size());
    AFL_VERIFY(BlobsForWrite.emplace(blobId, data).second)("blob_id", blobId.ToStringNew());
    BlobsWaiting.emplace(blobId);
    BlobsWriteCount += 1;
    SumSize += data.size();
}

void IBlobsWritingAction::OnBlobWriteResult(const TUnifiedBlobId& blobId, const NKikimrProto::EReplyStatus status) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS)("event", "WriteBlobResult")("blob_id", blobId.ToStringNew())("status", status);
    AFL_VERIFY(Counters);
    auto it = WritingStart.find(blobId);
    AFL_VERIFY(it != WritingStart.end());
    if (status == NKikimrProto::EReplyStatus::OK) {
        Counters->OnReply(blobId.BlobSize(), TMonotonic::Now() - it->second);
    } else {
        Counters->OnFail(blobId.BlobSize(), TMonotonic::Now() - it->second);
    }
    WritingStart.erase(it);
    Y_ABORT_UNLESS(BlobsWaiting.erase(blobId));
    return DoOnBlobWriteResult(blobId, status);
}

bool IBlobsWritingAction::IsReady() const {
    Y_ABORT_UNLESS(WritingStarted);
    return BlobsWaiting.empty();
}

IBlobsWritingAction::~IBlobsWritingAction() {
//    AFL_VERIFY(!NActors::TlsActivationContext || BlobsWaiting.empty() || Aborted);
}

void IBlobsWritingAction::SendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS)("event", "SendWriteBlobRequest")("blob_id", blobId.ToStringNew());
    AFL_VERIFY(Counters);
    Counters->OnRequest(data.size());
    WritingStarted = true;
    AFL_VERIFY(WritingStart.emplace(blobId, TMonotonic::Now()).second);
    return DoSendWriteBlobRequest(data, blobId);
}

}
