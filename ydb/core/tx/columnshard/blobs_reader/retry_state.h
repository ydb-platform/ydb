#pragma once

#include <ydb/core/tx/columnshard/blob.h>

#include <library/cpp/retry/retry_policy.h>
#include <library/cpp/time_provider/monotonic.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

class TRetryState {
public:
    struct TPendingRetry {
        TBlobRange Range;
        TString StorageId;
        TMonotonic DueTime;
    };

    explicit TRetryState(IRetryPolicy<>::TPtr policy)
        : RetryPolicy(std::move(policy))
    {
    }

    std::optional<TDuration> GetNextRetryDelay(const TBlobRange& range, bool isRetriable) {
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

    bool EnqueueRetry(const TBlobRange& range, const TString& storageId, TMonotonic dueTime) {
        return PendingRetries.emplace(range, TPendingRetry{ range, storageId, dueTime }).second;
    }

    void OnWakeup() {
        ScheduledWakeup = std::nullopt;
    }

    std::vector<TPendingRetry> ExtractReadyRetries(TMonotonic now) {
        std::vector<TPendingRetry> ready;
        for (auto it = PendingRetries.begin(); it != PendingRetries.end();) {
            if (it->second.DueTime <= now) {
                ready.push_back(std::move(it->second));
                auto toErase = it;
                ++it;
                PendingRetries.erase(toErase);
            } else {
                ++it;
            }
        }
        return ready;
    }

    std::optional<TDuration> NeedsReschedule(TMonotonic now) {
        if (PendingRetries.empty()) {
            return std::nullopt;
        }
        TMonotonic earliest = TMonotonic::Max();
        for (const auto& [_, entry] : PendingRetries) {
            earliest = Min(earliest, entry.DueTime);
        }
        if (!ScheduledWakeup || earliest < *ScheduledWakeup) {
            ScheduledWakeup = earliest;
            return (earliest > now) ? (earliest - now) : TDuration::Zero();
        }
        return std::nullopt;
    }

    void ClearRetryState(const TBlobRange& range) {
        RetryStates.erase(range);
    }

    bool HasPendingRetries() const {
        return !PendingRetries.empty();
    }

private:
    IRetryPolicy<>::TPtr RetryPolicy;
    THashMap<TBlobRange, IRetryPolicy<>::IRetryState::TPtr> RetryStates;
    THashMap<TBlobRange, TPendingRetry> PendingRetries;
    std::optional<TMonotonic> ScheduledWakeup;
};

}   // namespace NKikimr::NOlap::NBlobOperations::NRead
