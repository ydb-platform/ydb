#include "write.h"

#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS

namespace NKikimr::NOlap {

TUnifiedBlobId IBlobsWritingAction::AddDataForWrite(const TString& data, const std::optional<TUnifiedBlobId>& externalBlobId) {
    Y_ABORT_UNLESS(!WritingStarted);
    auto blobId = AllocateNextBlobId(data);
    YDB_LOG_TRACE("",
        {"generatedBlobId", blobId.ToStringNew()});
    AddDataForWrite(externalBlobId.value_or(blobId), data);
    return externalBlobId.value_or(blobId);
}

void IBlobsWritingAction::AddDataForWrite(const TUnifiedBlobId& blobId, const TString& data) {
    YDB_LOG_VERIFY(blobId.IsValid(),
        {"blobId", blobId.ToStringNew()});
    YDB_LOG_VERIFY(blobId.BlobSize() == data.size());
    YDB_LOG_VERIFY(BlobsForWrite.emplace(blobId, data).second,
        {"blobId", blobId.ToStringNew()});
    BlobsWaiting.emplace(blobId);
    BlobsWriteCount += 1;
    SumSize += data.size();
}

void IBlobsWritingAction::UpdateChannelApproximateFreeSpace(const TUnifiedBlobId& blobId, float approximateFreeSpaceShare) {
    DoUpdateChannelApproximateFreeSpace(blobId, approximateFreeSpaceShare);
}

void IBlobsWritingAction::OnBlobWriteResult(const TUnifiedBlobId& blobId, const NKikimrProto::EReplyStatus status) {
    YDB_LOG_DEBUG("",
        {"event", "WriteBlobResult"},
        {"blobId", blobId.ToStringNew()},
        {"status", status});
    YDB_LOG_VERIFY(Counters);
    auto it = WritingStart.find(blobId);
    YDB_LOG_VERIFY(it != WritingStart.end());
    if (status == NKikimrProto::EReplyStatus::OK) {
        Counters->OnReply(blobId.BlobSize(), TMonotonic::Now() - it->second);
    } else {
        Counters->OnFail(blobId.BlobSize(), TMonotonic::Now() - it->second);
    }
    WritingStart.erase(it);
    Y_ABORT_UNLESS(BlobsWaiting.erase(blobId));
    if (status == NKikimrProto::EReplyStatus::OK && GetCacheAfterWrite()) {
        const auto& storageId = GetStorageId();
        if (!storageId || storageId == NBlobOperations::TGlobal::DefaultStorageId) {
            auto dataIt = BlobsForWrite.find(blobId);
            YDB_LOG_VERIFY(dataIt != BlobsForWrite.end(),
                {"blobId", blobId.ToStringNew()});
            NBlobCache::AddRangeToCache(TBlobRange::FromBlobId(blobId), dataIt->second);
        }
    }
    return DoOnBlobWriteResult(blobId, status);
}

bool IBlobsWritingAction::IsReady() const {
    Y_ABORT_UNLESS(WritingStarted);
    return BlobsWaiting.empty();
}

IBlobsWritingAction::~IBlobsWritingAction() {
    //    YDB_LOG_VERIFY(!NActors::TlsActivationContext || BlobsWaiting.empty() || Aborted);
}

void IBlobsWritingAction::SendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) {
    YDB_LOG_DEBUG("",
        {"event", "SendWriteBlobRequest"},
        {"blobId", blobId.ToStringNew()});
    YDB_LOG_VERIFY(Counters);
    Counters->OnRequest(data.size());
    WritingStarted = true;
    YDB_LOG_VERIFY(WritingStart.emplace(blobId, TMonotonic::Now()).second);
    return DoSendWriteBlobRequest(data, blobId);
}

}   // namespace NKikimr::NOlap
