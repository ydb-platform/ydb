#include "actor.h"

#include <ydb/core/base/appdata_fwd.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

namespace {

IRetryPolicy<>::TPtr MakeReadRetryPolicy() {
    if (HasAppData()) {
        const auto& rp = AppDataVerified().ColumnShardConfig.GetReadRetryPolicy();
        return IRetryPolicy<>::GetExponentialBackoffPolicy([]() {
            return ERetryErrorClass::ShortRetry;
        }, TDuration::MilliSeconds(rp.GetInitialRetryDelayMs()), TDuration::MilliSeconds(rp.GetInitialRetryDelayMs()),
            TDuration::MilliSeconds(rp.GetMaxRetryDelayMs()), rp.GetMaxRetries());
    }
    return IRetryPolicy<>::GetExponentialBackoffPolicy([]() {
        return ERetryErrorClass::ShortRetry;
    }, TDuration::MilliSeconds(100), TDuration::MilliSeconds(100), TDuration::Seconds(5), 10);
}

}   // namespace

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

void TActor::HandleRetryTimer() {
    RetryScheduled = false;
    if (!Task || PendingRetries.empty()) {
        return;
    }

    auto retries = std::exchange(PendingRetries, {});
    for (auto&& pending : retries) {
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
                PassAway();
                return;
            }
        }
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
