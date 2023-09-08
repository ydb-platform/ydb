#include "task.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

THashMap<TUnifiedBlobId, std::vector<TBlobRange>> ITask::GetBlobsGroupped() const {
    Y_VERIFY(!BlobsFetchingStarted);
    THashMap<TUnifiedBlobId, std::vector<TBlobRange>> result;
    for (auto&& i : BlobsWaiting) {
        result[i.BlobId].emplace_back(i);
    }
    return result;
}

bool ITask::AddError(const TBlobRange& range, const TErrorStatus& status) {
    if (TaskFinishedWithError) {
        ACFL_WARN("event", "SkipError")("message", status.GetErrorMessage())("status", status.GetStatus());
        return false;
    } else {
        ACFL_ERROR("event", "NewError")("message", status.GetErrorMessage())("status", status.GetStatus());
    }
    Y_VERIFY(BlobsWaiting.erase(range));
    Y_VERIFY(BlobErrors.emplace(range, status).second);
    if (!OnError(range)) {
        TaskFinishedWithError = true;
        return false;
    }
    if (BlobsWaiting.empty()) {
        OnDataReady();
    }
    return true;
}

void ITask::AddData(const TBlobRange& range, const TString& data) {
    if (TaskFinishedWithError) {
        ACFL_WARN("event", "SkipDataAfterError");
        return;
    } else {
        ACFL_DEBUG("event", "NewData")("range", range.ToString());
    }
    Y_VERIFY(BlobsFetchingStarted);
    Y_VERIFY(BlobsWaiting.erase(range));
    Y_VERIFY(BlobsData.emplace(range, data).second);
    if (BlobsWaiting.empty()) {
        OnDataReady();
    }
}

void ITask::StartBlobsFetching() {
    BlobsFetchingStarted = true;
    if (BlobsWaiting.empty()) {
        OnDataReady();
    }
}

ITask::ITask(const THashSet<TBlobRange>& blobs)
    : BlobsWaiting(blobs)
{
    Y_VERIFY(BlobsWaiting.size());
}

}
