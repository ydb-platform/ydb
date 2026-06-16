#include "actor.h"
#include "read_retry_policy.h"

namespace NKikimr::NOlap::NBlobOperations::NRead {

TAtomicCounter TActor::WaitingBlobsCount = 0;

std::optional<TDuration> TActor::GetNextRetryDelay(const TBlobRange& range, bool isRetriable) {
    if (!isRetriable) {
        return std::nullopt;
    }
    auto it = RetryStates.find(range);
    if (it == RetryStates.end()) {
        it = RetryStates.emplace(range, RetryPolicy->CreateRetryState()).first;
    }
    if (auto delay = it->second->GetNextRetryDelay()) {
        return *delay;
    }
    return std::nullopt;
}

void TActor::ScheduleNextRetry() {
    if (PendingRetries.empty()) {
        return;
    }
    TMonotonic earliest = TMonotonic::Max();
    for (const auto& [_, entry] : PendingRetries) {
        earliest = Min(earliest, entry.DueTime);
    }
    if (!ScheduledWakeup || earliest < *ScheduledWakeup) {
        auto now = TActivationContext::Monotonic();
        auto delay = (earliest > now) ? (earliest - now) : TDuration::Zero();
        Schedule(delay, new TEvents::TEvWakeup());
        ScheduledWakeup = earliest;
    }
}

void TActor::HandleRetryTimer() {
    ScheduledWakeup = std::nullopt;
    if (!Task || PendingRetries.empty()) {
        return;
    }

    auto now = TActivationContext::Monotonic();
    std::vector<TPendingRetry> ready;
    for (auto it = PendingRetries.begin(); it != PendingRetries.end();) {
        if (it->second.DueTime <= now) {
            ready.push_back(std::move(it->second));
            it = PendingRetries.erase(it);
        } else {
            ++it;
        }
    }

    for (auto&& pending : ready) {
        auto action = Task->GetAgents().FindByStorageId(pending.StorageId);
        if (action) {
            ACFL_DEBUG("event", "RetryS3Read")("blob_range", pending.Range)("storage_id", pending.StorageId);
            action->RetryRead(pending.Range);
        } else {
            ACFL_ERROR("event", "RetryS3ReadNoAction")("blob_range", pending.Range)("storage_id", pending.StorageId);
            WaitingBlobsCount.Sub(Task->GetWaitingRangesCount());
            if (!Task->AddError(pending.StorageId, pending.Range,
                    IBlobsReadingAction::TErrorStatus::Fail(NKikimrProto::EReplyStatus::ERROR, "cannot retry read: storage action not found"))) {
                Task = nullptr;
            }
            PassAway();
            return;
        }
    }

    ScheduleNextRetry();
}

void TActor::Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev) {
    if (!Task) {
        return;
    }
    ACFL_TRACE("event", "TEvReadBlobRangeResult")("blob_id", ev->Get()->BlobRange);

    auto& event = *ev->Get();

    bool aborted = false;
    if (event.Status != NKikimrProto::EReplyStatus::OK) {
        auto delay = GetNextRetryDelay(event.BlobRange, event.IsRetriable);
        if (delay) {
            ACFL_WARN("event", "S3ReadRetriableError")("blob_range", event.BlobRange)("storage_id", event.DataSourceId)(
                "error", event.DetailedError)("delay_ms", delay->MilliSeconds());
            auto dueTime = TActivationContext::Monotonic() + *delay;
            if (PendingRetries.emplace(event.BlobRange, TPendingRetry{ event.BlobRange, event.DataSourceId, dueTime }).second) {
                ScheduleNextRetry();
            }
            return;
        }
        if (event.IsRetriable) {
            ACFL_ERROR("event", "S3ReadRetryExhausted")("blob_range", event.BlobRange)("storage_id", event.DataSourceId)(
                "error", event.DetailedError);
        }

        WaitingBlobsCount.Sub(Task->GetWaitingRangesCount());
        if (!Task->AddError(event.DataSourceId, event.BlobRange,
                IBlobsReadingAction::TErrorStatus::Fail(
                    event.Status, "cannot get blob: " + event.Data.substr(0, 1024) + ", detailed error: " + event.DetailedError))) {
            aborted = true;
        }
    } else {
        WaitingBlobsCount.Dec();
        RetryStates.erase(event.BlobRange);
        Task->AddData(event.DataSourceId, event.BlobRange, event.Data);
    }
    if (aborted || Task->IsFinished()) {
        Task = nullptr;
        PassAway();
    }
}

TActor::TActor(const std::shared_ptr<ITask>& task)
    : Task(task)
    , RetryPolicy(MakeReadRetryPolicy())
{
}

TActor::~TActor() {
    if (Task) {
        Task->Abort();
    }
}

void TActor::Bootstrap() {
    const auto& externalTaskId = Task->GetExternalTaskId();
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("external_task_id", externalTaskId);
    Task->StartBlobsFetching({});
    ACFL_DEBUG("task", Task->DebugString());
    WaitingBlobsCount.Add(Task->GetWaitingRangesCount());
    Become(&TThis::StateWait);
    if (Task->IsFinished()) {
        PassAway();
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NRead
