#include "write.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap {

TUnifiedBlobId IBlobsWritingAction::AddDataForWrite(const TString& data) {
    Y_VERIFY(!WritingStarted);
    auto blobId = AllocateNextBlobId(data);
    AFL_VERIFY(BlobsForWrite.emplace(blobId, data).second);
    SumSize += data.size();
    return blobId;
}

void IBlobsWritingAction::OnBlobWriteResult(const TUnifiedBlobId& blobId, const NKikimrProto::EReplyStatus status) {
    Y_VERIFY(BlobsForWrite.erase(blobId));
    return DoOnBlobWriteResult(blobId, status);
}

bool IBlobsWritingAction::IsReady() const {
    Y_VERIFY(WritingStarted);
    return BlobsForWrite.empty();
}

IBlobsWritingAction::~IBlobsWritingAction() {
    AFL_VERIFY(BlobsForWrite.empty() || Aborted);
}

}
