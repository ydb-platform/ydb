#include "read_coordinator.h"
#include "read_retry_policy.h"

namespace NKikimr::NOlap::NBlobOperations::NRead {

std::optional<TDuration> TReadCoordinatorActor::GetNextRetryDelay(const TBlobRange& range, bool isRetriable) {
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

void TReadCoordinatorActor::HandleRetryTimer() {
    RetryScheduled = false;
    if (PendingRetries.empty()) {
        return;
    }

    auto retries = std::exchange(PendingRetries, {});
    for (auto&& pending : retries) {
        if (!BlobTasks.Contains(pending.StorageId, pending.Range)) {
            continue;
        }
        auto action = BlobTasks.FindAction(pending.StorageId, pending.Range);
        if (action) {
            ACFL_DEBUG("event", "RetryS3Read")("blob_range", pending.Range)("storage_id", pending.StorageId);
            action->RetryRead(pending.Range);
        } else {
            ACFL_ERROR("event", "RetryS3ReadNoAction")("blob_range", pending.Range)("storage_id", pending.StorageId);
            auto tasks = BlobTasks.Extract(pending.StorageId, pending.Range);
            for (auto&& task : tasks) {
                task->AddError(pending.StorageId, pending.Range,
                    IBlobsReadingAction::TErrorStatus::Fail(NKikimrProto::EReplyStatus::ERROR, "cannot retry read: storage action not found"));
            }
        }
    }
}

void TReadCoordinatorActor::Handle(TEvStartReadTask::TPtr& ev) {
    const auto& externalTaskId = ev->Get()->GetTask()->GetExternalTaskId();
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("external_task_id", externalTaskId);
    THashSet<TBlobRange> rangesInProgress;
    BlobTasks.AddTask(ev->Get()->GetTask());
    ev->Get()->GetTask()->StartBlobsFetching(rangesInProgress);
    ACFL_DEBUG("task", ev->Get()->GetTask()->DebugString());
}

void TReadCoordinatorActor::Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev) {
    ACFL_TRACE("event", "TEvReadBlobRangeResult")("blob_id", ev->Get()->BlobRange);

    auto& event = *ev->Get();

    if (event.Status != NKikimrProto::EReplyStatus::OK) {
        auto delay = GetNextRetryDelay(event.BlobRange, event.IsRetriable);
        if (delay) {
            ACFL_WARN("event", "S3ReadRetriableError")("blob_range", event.BlobRange)("storage_id", event.DataSourceId)(
                "error", event.DetailedError)("delay_ms", delay->MilliSeconds());
            PendingRetries.push_back(TPendingRetry{ event.BlobRange, event.DataSourceId });
            if (!RetryScheduled) {
                Schedule(*delay, new TEvents::TEvWakeup());
                RetryScheduled = true;
            }
            return;
        }
        if (event.IsRetriable) {
            ACFL_ERROR("event", "S3ReadRetryExhausted")("blob_range", event.BlobRange)("storage_id", event.DataSourceId)(
                "error", event.DetailedError);
        }
    }

    auto tasks = BlobTasks.Extract(event.DataSourceId, event.BlobRange);
    for (auto&& i : tasks) {
        if (event.Status != NKikimrProto::EReplyStatus::OK) {
            i->AddError(event.DataSourceId, event.BlobRange,
                IBlobsReadingAction::TErrorStatus::Fail(event.Status, "cannot get blob, detailed error: " + event.DetailedError));
        } else {
            RetryStates.erase(event.BlobRange);
            i->AddData(event.DataSourceId, event.BlobRange, event.Data);
        }
    }
}

TReadCoordinatorActor::TReadCoordinatorActor(ui64 tabletId, const TActorId& parent)
    : TabletId(tabletId)
    , Parent(parent)
    , RetryPolicy(MakeReadRetryPolicy())
{
}

TReadCoordinatorActor::~TReadCoordinatorActor() {
    auto tasks = BlobTasks.ExtractTasksAll();
    for (auto&& i : tasks) {
        i->Abort();
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NRead
