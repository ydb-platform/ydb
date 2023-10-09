#include "write.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap {

TUnifiedBlobId IBlobsWritingAction::AddDataForWrite(const TString& data) {
    Y_ABORT_UNLESS(!WritingStarted);
    auto blobId = AllocateNextBlobId(data);
    AFL_VERIFY(BlobsForWrite.emplace(blobId, data).second);
    BlobsWaiting.emplace(blobId);
    BlobsWriteCount += 1;
    SumSize += data.size();
    return blobId;
}

void IBlobsWritingAction::OnBlobWriteResult(const TUnifiedBlobId& blobId, const NKikimrProto::EReplyStatus status) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "WriteBlobResult")("blob_id", blobId.ToStringNew())("status", status);
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
    AFL_VERIFY(!NActors::TlsActivationContext || BlobsWaiting.empty() || Aborted);
}

void IBlobsWritingAction::SendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "SendWriteBlobRequest")("blob_id", blobId.ToStringNew());
    AFL_VERIFY(Counters);
    Counters->OnRequest(data.size());
    WritingStarted = true;
    AFL_VERIFY(WritingStart.emplace(blobId, TMonotonic::Now()).second);
    return DoSendWriteBlobRequest(data, blobId);
}

}
