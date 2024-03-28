#include "task.h"
#include "events.h"
#include <ydb/library/actors/core/log.h>
#include "actor.h"

namespace NKikimr::NOlap::NBlobOperations::NRead {

bool ITask::AddError(const TString& storageIdExt, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    const TString storageId = storageIdExt ? storageIdExt : IStoragesManager::DefaultStorageId;
    AFL_VERIFY(--BlobsWaitingCount >= 0);
    if (TaskFinishedWithError || AbortFlag) {
        ACFL_WARN("event", "SkipError")("storage_id", storageId)("blob_range", range)("message", status.GetErrorMessage())("status", status.GetStatus())("external_task_id", ExternalTaskId)("consumer", TaskCustomer)
            ("abort", AbortFlag)("finished_with_error", TaskFinishedWithError);
        return false;
    } else {
        ACFL_ERROR("event", "NewError")("storage_id", storageId)("blob_range", range)("message", status.GetErrorMessage())("status", status.GetStatus())("external_task_id", ExternalTaskId)("consumer", TaskCustomer);
    }
    {
        auto it = AgentsWaiting.find(storageId);
        AFL_VERIFY(it != AgentsWaiting.end())("storage_id", storageId);
        it->second->OnReadError(range, status);
        if (it->second->IsFinished()) {
            AgentsWaiting.erase(it);
        }
    }
    if (!OnError(storageId, range, status)) {
        TaskFinishedWithError = true;
        return false;
    }
    if (AgentsWaiting.empty()) {
        OnDataReady();
    }
    return true;
}

void ITask::AddData(const TString& storageIdExt, const TBlobRange& range, const TString& data) {
    const TString storageId = storageIdExt ? storageIdExt : IStoragesManager::DefaultStorageId;
    AFL_VERIFY(--BlobsWaitingCount >= 0);
    if (TaskFinishedWithError || AbortFlag) {
        ACFL_WARN("event", "SkipDataAfterError")("storage_id", storageId)("external_task_id", ExternalTaskId)("abort", AbortFlag)("finished_with_error", TaskFinishedWithError);
        return;
    } else {
        ACFL_TRACE("event", "NewData")("storage_id", storageId)("range", range.ToString())("external_task_id", ExternalTaskId);
    }
    Y_ABORT_UNLESS(BlobsFetchingStarted);
    {
        auto it = AgentsWaiting.find(storageId);
        AFL_VERIFY(it != AgentsWaiting.end())("storage_id", storageId);
        it->second->OnReadResult(range, data);
        if (it->second->IsFinished()) {
            AgentsWaiting.erase(it);
        }
    }
    if (AgentsWaiting.empty()) {
        OnDataReady();
    }
}

void ITask::StartBlobsFetching(const THashSet<TBlobRange>& rangesInProgress) {
    ACFL_TRACE("task_id", ExternalTaskId)("event", "start");
    Y_ABORT_UNLESS(!BlobsFetchingStarted);
    BlobsFetchingStarted = true;
    AFL_VERIFY(BlobsWaitingCount == 0);
    for (auto&& agent : Agents) {
        agent.second->Start(rangesInProgress);
        if (!agent.second->IsFinished()) {
            AgentsWaiting.emplace(agent.second->GetStorageId(), agent.second);
            BlobsWaitingCount += agent.second->GetGroups().size();
        }
    }
    if (AgentsWaiting.empty()) {
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

ITask::ITask(const TReadActionsCollection& actions, const TString& taskCustomer, const TString& externalTaskId)
    : TaskIdentifier(TaskIdentifierBuilder.Inc())
    , ExternalTaskId(externalTaskId)
    , TaskCustomer(taskCustomer)
{
    Agents = actions;
    AFL_VERIFY(!Agents.IsEmpty());
    for (auto&& i : Agents) {
        AFL_VERIFY(i.second->GetExpectedBlobsCount());
    }
}

TString ITask::DebugString() const {
    TStringBuilder sb;
    if (TaskFinishedWithError) {
        sb << "finished_with_error=" << TaskFinishedWithError << ";";
    }
    sb << "agents_waiting=" << AgentsWaiting.size() << ";"
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

bool ITask::OnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    ACFL_DEBUG("event", "OnError")("status", status.GetStatus())("task", DebugString());
    return DoOnError(storageId, range, status);
}

ITask::~ITask() {
    AFL_VERIFY(!NActors::TlsActivationContext || DataIsReadyFlag || TaskFinishedWithError || AbortFlag || !BlobsFetchingStarted);
}

TCompositeReadBlobs ITask::ExtractBlobsData() {
    AFL_VERIFY(AgentsWaiting.empty());
    AFL_VERIFY(!ResultsExtracted);
    ResultsExtracted = true;
    TCompositeReadBlobs result;
    for (auto&& i : Agents) {
        result.Add(i.second->GetStorageId(), i.second->ExtractBlobsData());
    }
    return std::move(result);
}

}
