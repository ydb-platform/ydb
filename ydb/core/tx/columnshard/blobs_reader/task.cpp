#include "task.h"
#include "events.h"
#include <library/cpp/actors/core/log.h>
#include "actor.h"

namespace NKikimr::NOlap::NBlobOperations::NRead {

const std::vector<std::shared_ptr<IBlobsReadingAction>>& ITask::GetAgents() const {
    Y_ABORT_UNLESS(!BlobsFetchingStarted);
    return Agents;
}

bool ITask::AddError(const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    ++BlobErrorsCount;
    if (TaskFinishedWithError || AbortFlag) {
        ACFL_WARN("event", "SkipError")("blob_range", range)("message", status.GetErrorMessage())("status", status.GetStatus())("external_task_id", ExternalTaskId)("consumer", TaskCustomer)
            ("abort", AbortFlag)("finished_with_error", TaskFinishedWithError);
        return false;
    } else {
        ACFL_ERROR("event", "NewError")("blob_range", range)("message", status.GetErrorMessage())("status", status.GetStatus())("external_task_id", ExternalTaskId)("consumer", TaskCustomer);
    }
    {
        auto it = BlobsWaiting.find(range);
        AFL_VERIFY(it != BlobsWaiting.end());
        it->second->OnReadError(range, status);
        BlobsWaiting.erase(it);
    }
    if (!OnError(range, status)) {
        TaskFinishedWithError = true;
        return false;
    }
    if (BlobsWaiting.empty()) {
        OnDataReady();
    }
    return true;
}

void ITask::AddData(const TBlobRange& range, const TString& data) {
    ++BlobsDataCount;
    if (TaskFinishedWithError || AbortFlag) {
        ACFL_WARN("event", "SkipDataAfterError")("external_task_id", ExternalTaskId)("abort", AbortFlag)("finished_with_error", TaskFinishedWithError);
        return;
    } else {
        ACFL_TRACE("event", "NewData")("range", range.ToString())("external_task_id", ExternalTaskId);
    }
    Y_ABORT_UNLESS(BlobsFetchingStarted);
    {
        auto it = BlobsWaiting.find(range);
        AFL_VERIFY(it != BlobsWaiting.end());
        it->second->OnReadResult(range, data);
        BlobsWaiting.erase(it);
    }
    if (BlobsWaiting.empty()) {
        OnDataReady();
    }
}

void ITask::StartBlobsFetching(const THashSet<TBlobRange>& rangesInProgress) {
    ACFL_TRACE("task_id", ExternalTaskId)("event", "start");
    Y_ABORT_UNLESS(!BlobsFetchingStarted);
    BlobsFetchingStarted = true;
    ui64 allRangesSize = 0;
    ui64 allRangesCount = 0;
    ui64 readRangesCount = 0;
    for (auto&& agent : Agents) {
        allRangesSize += agent->GetExpectedBlobsSize();
        allRangesCount += agent->GetExpectedBlobsCount();
        for (auto&& b : agent->GetRangesForRead()) {
            for (auto&& r : b.second) {
                BlobsWaiting.emplace(r, agent);
                ++readRangesCount;
            }
        }
        agent->Start(rangesInProgress);
    }
    ReadRangesCount = readRangesCount;
    AllRangesCount = allRangesCount;
    AllRangesSize = allRangesSize;
    if (BlobsWaiting.empty()) {
        OnDataReady();
    }
}

namespace {
TAtomicCounter TaskIdentifierBuilder = 0;

}

void ITask::TReadSubscriber::DoOnAllocationSuccess(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) {
    Task->ResourcesGuard = guard;
    TActorContext::AsActorContext().Register(new NRead::TActor(Task));
}

ITask::ITask(const std::vector<std::shared_ptr<IBlobsReadingAction>>& actions, const TString& taskCustomer, const TString& externalTaskId)
    : Agents(actions)
    , TaskIdentifier(TaskIdentifierBuilder.Inc())
    , ExternalTaskId(externalTaskId)
    , TaskCustomer(taskCustomer)
{
    AFL_VERIFY(Agents.size());
    for (auto&& i : Agents) {
        AFL_VERIFY(i->GetExpectedBlobsCount());
    }
}

TString ITask::DebugString() const {
    TStringBuilder sb;
    sb << "finished_with_error=" << TaskFinishedWithError << ";"
        << "errors=" << BlobErrorsCount << ";"
        << "data=" << BlobsDataCount << ";"
        << "waiting=" << BlobsWaiting.size() << ";"
        << "additional_info=(" << DoDebugString() << ");"
        ;
    return sb;
}

void ITask::OnDataReady() {
    ACFL_DEBUG("event", "OnDataReady")("task", DebugString())("external_task_id", ExternalTaskId);
    Y_ABORT_UNLESS(!DataIsReadyFlag);
    DataIsReadyFlag = true;
    DoOnDataReady(ResourcesGuard);
}

bool ITask::OnError(const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    ACFL_DEBUG("event", "OnError")("status", status.GetStatus())("task", DebugString());
    return DoOnError(range, status);
}

ITask::~ITask() {
    AFL_VERIFY(!NActors::TlsActivationContext || DataIsReadyFlag || TaskFinishedWithError || AbortFlag || !BlobsFetchingStarted);
}

THashMap<NKikimr::NOlap::TBlobRange, TString> ITask::ExtractBlobsData() {
    AFL_VERIFY(BlobsWaiting.empty());
    AFL_VERIFY(!ResultsExtracted);
    ResultsExtracted = true;
    THashMap<TBlobRange, TString> result;
    for (auto&& i : Agents) {
        i->ExtractBlobsDataTo(result);
    }
    return std::move(result);
}

}
