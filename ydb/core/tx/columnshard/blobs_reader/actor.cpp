#include "actor.h"
#include "read_retry_policy.h"
#include <ydb/library/actors/struct_log/log_stack.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

TAtomicCounter TActor::WaitingBlobsCount = 0;

void TActor::HandleRetryTimer() {
    RetryState.OnWakeup();
    if (!Task || !RetryState.HasPendingRetries()) {
        return;
    }

    auto now = TActivationContext::Monotonic();
    auto ready = RetryState.ExtractReadyRetries(now);

    for (auto&& pending : ready) {
        auto action = Task->GetAgents().FindByStorageId(pending.StorageId);
        if (action) {
            YDB_LOG_DEBUG_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "Dump event, blobRange, storageId",
                {"event", "RetryS3Read"},
                {"blobRange", pending.Range},
                {"storageId", pending.StorageId});
            action->OnRetryExecute();
            action->RetryRead(pending.Range);
        } else {
            bool aborted = false;
            YDB_LOG_ERROR_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "",
                {"event", "RetryS3ReadNoAction"},
                {"blobRange", pending.Range},
                {"storageId", pending.StorageId});
            WaitingBlobsCount.Sub(Task->GetWaitingRangesCount());
            if (!Task->AddError(pending.StorageId, pending.Range,
                    IBlobsReadingAction::TErrorStatus::Fail(NKikimrProto::EReplyStatus::ERROR, "cannot retry read: storage action not found"))) {
                Task = nullptr;
                aborted = true;
            }
            if (aborted || Task->IsFinished()) {
                Task = nullptr;
                PassAway();
                return;
            }
        }
    }

    if (auto delay = RetryState.NeedsReschedule(now)) {
        Schedule(*delay, new TEvents::TEvWakeup());
    }
}

void TActor::Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev) {
    if (!Task) {
        return;
    }
    YDB_LOG_TRACE_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "Dump event, blobId",
        {"event", "TEvReadBlobRangeResult"},
        {"blobId", ev->Get()->BlobRange});

    auto& event = *ev->Get();

    bool aborted = false;
    if (event.Status != NKikimrProto::EReplyStatus::OK) {
        auto delay = RetryState.GetNextRetryDelay(event.BlobRange, event.IsRetriable);
        if (delay) {
            YDB_LOG_WARN_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "",
                {"event", "S3ReadRetriableError"},
                {"blobRange", event.BlobRange},
                {"storageId", event.DataSourceId},
                {"error", event.DetailedError},
                {"delayMs", delay->MilliSeconds()});
            auto now = TActivationContext::Monotonic();
            if (RetryState.EnqueueRetry(event.BlobRange, event.DataSourceId, now + *delay)) {
                if (auto action = Task->GetAgents().FindByStorageId(event.DataSourceId)) {
                    action->OnRetryEnqueue(event.BlobRange);
                }
                if (auto d = RetryState.NeedsReschedule(now)) {
                    Schedule(*d, new TEvents::TEvWakeup());
                }
            }
            return;
        }
        if (event.IsRetriable) {
            YDB_LOG_ERROR_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "",
                {"event", "S3ReadRetryExhausted"},
                {"blobRange", event.BlobRange},
                {"storageId", event.DataSourceId},
                {"error", event.DetailedError});
            if (auto action = Task->GetAgents().FindByStorageId(event.DataSourceId)) {
                action->OnRetryExhausted();
            }
        }

        WaitingBlobsCount.Sub(Task->GetWaitingRangesCount());
        if (!Task->AddError(event.DataSourceId, event.BlobRange,
                IBlobsReadingAction::TErrorStatus::Fail(
                    event.Status, "cannot get blob: " + event.Data.substr(0, 1024) + ", detailed error: " + event.DetailedError))) {
            aborted = true;
        }
    } else {
        WaitingBlobsCount.Dec();
        RetryState.ClearRetryState(event.BlobRange);
        Task->AddData(event.DataSourceId, event.BlobRange, event.Data);
    }
    if (aborted || Task->IsFinished()) {
        Task = nullptr;
        PassAway();
    }
}

TActor::TActor(const std::shared_ptr<ITask>& task)
    : Task(task)
    , RetryState(MakeReadRetryPolicy())
{
}

TActor::~TActor() {
    if (Task) {
        Task->Abort();
    }
}

void TActor::Bootstrap() {
    const auto& externalTaskId = Task->GetExternalTaskId();
    YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD,
        {"externalTaskId", externalTaskId});
    Task->StartBlobsFetching({});
    YDB_LOG_DEBUG_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "Dump task",
        {"task", Task->DebugString()});
    WaitingBlobsCount.Add(Task->GetWaitingRangesCount());
    Become(&TThis::StateWait);
    if (Task->IsFinished()) {
        PassAway();
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NRead
