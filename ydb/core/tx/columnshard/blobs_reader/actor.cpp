#include "actor.h"
#include "read_retry_policy.h"

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

    if (auto delay = RetryState.NeedsReschedule(now)) {
        Schedule(*delay, new TEvents::TEvWakeup());
    }
}

void TActor::Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev) {
    if (!Task) {
        return;
    }
    ACFL_TRACE("event", "TEvReadBlobRangeResult")("blob_id", ev->Get()->BlobRange);

    auto& event = *ev->Get();

    bool aborted = false;
    if (event.Status != NKikimrProto::EReplyStatus::OK) {
        auto delay = RetryState.GetNextRetryDelay(event.BlobRange, event.IsRetriable);
        if (delay) {
            ACFL_WARN("event", "S3ReadRetriableError")("blob_range", event.BlobRange)("storage_id", event.DataSourceId)(
                "error", event.DetailedError)("delay_ms", delay->MilliSeconds());
            auto now = TActivationContext::Monotonic();
            if (RetryState.EnqueueRetry(event.BlobRange, event.DataSourceId, now + *delay)) {
                if (auto d = RetryState.NeedsReschedule(now)) {
                    Schedule(*d, new TEvents::TEvWakeup());
                }
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
