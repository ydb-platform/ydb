#include "task.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

const std::vector<std::shared_ptr<IBlobsReadingAction>>& ITask::GetAgents() const {
    Y_VERIFY(!BlobsFetchingStarted);
    return Agents;
}

bool ITask::AddError(const TBlobRange& range, const TErrorStatus& status) {
    if (TaskFinishedWithError) {
        ACFL_WARN("event", "SkipError")("message", status.GetErrorMessage())("status", status.GetStatus());
        return false;
    } else {
        ACFL_ERROR("event", "NewError")("message", status.GetErrorMessage())("status", status.GetStatus());
    }
    {
        auto it = BlobsWaiting.find(range);
        AFL_VERIFY(it != BlobsWaiting.end());
        it->second->OnReadError(range, status.GetStatus());
        BlobsWaiting.erase(it);
    }

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
        ACFL_TRACE("event", "NewData")("range", range.ToString());
    }
    Y_VERIFY(BlobsFetchingStarted);
    {
        auto it = BlobsWaiting.find(range);
        AFL_VERIFY(it != BlobsWaiting.end());
        it->second->OnReadResult(range, data);
        BlobsWaiting.erase(it);
    }
    Y_VERIFY(BlobsData.emplace(range, data).second);
    if (BlobsWaiting.empty()) {
        OnDataReady();
    }
}

void ITask::StartBlobsFetching(const THashSet<TBlobRange>& rangesInProgress) {
    Y_VERIFY(!BlobsFetchingStarted);
    BlobsFetchingStarted = true;
    for (auto&& agent : Agents) {
        for (auto&& b : agent->GetRangesForRead()) {
            for (auto&& r : b.second) {
                BlobsWaiting.emplace(r, agent);
            }
        }
        agent->Start(rangesInProgress);
    }
    if (BlobsWaiting.empty()) {
        OnDataReady();
    }
}

namespace {
TAtomicCounter TaskIdentifierBuilder = 0;
}

ITask::ITask(const std::vector<std::shared_ptr<IBlobsReadingAction>>& actions)
    : Agents(actions)
    , TaskIdentifier(TaskIdentifierBuilder.Inc())
{
    AFL_VERIFY(Agents.size());
    for (auto&& i : Agents) {
        AFL_VERIFY(i->GetExpectedBlobsCount());
    }
}

TString ITask::DebugString() const {
    TStringBuilder sb;
    sb << "finished_with_error=" << TaskFinishedWithError << ";"
        << "errors=" << BlobErrors.size() << ";"
        << "data=" << BlobsData.size() << ";"
        << "waiting=" << BlobsWaiting.size() << ";"
        << "additional_info=(" << DoDebugString() << ");"
        ;
    return sb;
}

void ITask::OnDataReady() {
    ACFL_DEBUG("event", "OnDataReady")("task", DebugString());
    Y_VERIFY(!DataIsReadyFlag);
    DataIsReadyFlag = true;
    DoOnDataReady();
}

bool ITask::OnError(const TBlobRange& range) {
    ACFL_DEBUG("event", "OnError")("task", DebugString());
    return DoOnError(range);
}

}
