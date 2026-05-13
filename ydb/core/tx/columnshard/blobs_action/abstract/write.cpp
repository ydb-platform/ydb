#include "write.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS

namespace NKikimr::NOlap {

TUnifiedBlobId IBlobsWritingAction::AddDataForWrite(const TString& data, const std::optional<TUnifiedBlobId>& externalBlobId) {
    Y_ABORT_UNLESS(!WritingStarted);
    auto blobId = AllocateNextBlobId(data);
    YDB_LOG_TRACE("",
        {"generated_blob_id", blobId.ToStringNew()});
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
    YDB_LOG_DEBUG("",
        {"event", "WriteBlobResult"},
        {"blob_id", blobId.ToStringNew()},
        {"status", status});
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
    YDB_LOG_DEBUG("",
        {"event", "SendWriteBlobRequest"},
        {"blob_id", blobId.ToStringNew()});
    AFL_VERIFY(Counters);
    Counters->OnRequest(data.size());
    WritingStarted = true;
    AFL_VERIFY(WritingStart.emplace(blobId, TMonotonic::Now()).second);
    return DoSendWriteBlobRequest(data, blobId);
}

}   // namespace NKikimr::NOlap
