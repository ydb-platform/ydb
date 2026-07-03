#include "actor.h"
#include "task.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

bool ITask::AddError(const TString& storageIdExt, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    const TString storageId = storageIdExt ? storageIdExt : IStoragesManager::DefaultStorageId;
    AFL_VERIFY(--BlobsWaitingCount >= 0);
    if (TaskFinishedWithError || AbortFlag) {
        YDB_LOG_WARN_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "",
            {"event", "SkipError"},
            {"storageId", storageId},
            {"blobRange", range},
            {"message", status.GetErrorMessage()},
            {"status", status.GetStatus()},
            {"externalTaskId", ExternalTaskId},
            {"consumer", TaskCustomer},
            {"abort", AbortFlag},
            {"finishedWithError", TaskFinishedWithError});
        return false;
    } else {
        YDB_LOG_ERROR_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "",
            {"event", "NewError"},
            {"storageId", storageId},
            {"blobRange", range},
            {"message", status.GetErrorMessage()},
            {"status", status.GetStatus()},
            {"externalTaskId", ExternalTaskId},
            {"consumer", TaskCustomer});
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
        YDB_LOG_WARN_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "",
            {"event", "SkipDataAfterError"},
            {"storageId", storageId},
            {"externalTaskId", ExternalTaskId},
            {"abort", AbortFlag},
            {"finishedWithError", TaskFinishedWithError});
        return;
    } else {
        YDB_LOG_TRACE_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "Dump event, storageId, range, externalTaskId",
            {"event", "NewData"},
            {"storageId", storageId},
            {"range", range},
            {"externalTaskId", ExternalTaskId});
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
    YDB_LOG_TRACE_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "Dump taskId, event",
        {"taskId", ExternalTaskId},
        {"event", "start"});
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
    //    AFL_VERIFY(!Agents.IsEmpty());
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
       << "additional_info=(" << DoDebugString() << ");";
    return sb;
}

void ITask::OnDataReady() {
    YDB_LOG_DEBUG_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "Dump event, task, externalTaskId",
        {"event", "OnDataReady"},
        {"task", DebugString()},
        {"externalTaskId", ExternalTaskId});
    Y_ABORT_UNLESS(!DataIsReadyFlag);
    DataIsReadyFlag = true;
    DoOnDataReady(ResourcesGuard);
}

bool ITask::OnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    YDB_LOG_DEBUG_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "Dump event, status, task",
        {"event", "OnError"},
        {"status", status.GetStatus()},
        {"task", DebugString()});
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

}   // namespace NKikimr::NOlap::NBlobOperations::NRead
