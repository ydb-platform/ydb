#include "database_readiness_tracker.h"

#include <ydb/services/workload_manager/common/helpers.h>


namespace NKikimr::NWorkloadManager::NPrivate {

TDatabaseReadinessTracker::TDatabaseReadinessTracker(TDuration requestTimeout)
    : RequestTimeout_(requestTimeout)
{}

std::optional<TString> TDatabaseReadinessTracker::AddSubscriber(const TString& databaseId, TPendingSubscriber subscriber, TInstant now) {
    auto [it, inserted] = Entries_.try_emplace(databaseId);
    auto& entry = it->second;
    if (inserted) {
        entry.State = EDatabaseState::Pending;
        entry.StateAt = now;
    }
    entry.Subscribers.push_back(subscriber);

    if (entry.State != EDatabaseState::Pending) {
        return std::nullopt;
    }
    return MarkFetchInFlight(DatabaseIdToDatabase(databaseId));
}

std::optional<TString> TDatabaseReadinessTracker::OnWarmup(const TString& path) {
    for (const auto& [databaseId, _] : Entries_) {
        if (DatabaseIdToDatabase(databaseId) == path) {
            return std::nullopt;
        }
    }
    return MarkFetchInFlight(path);
}

std::vector<TSubscriberReply> TDatabaseReadinessTracker::OnFetchResult(const TString& path, const TString& databaseId, Ydb::StatusIds::StatusCode status,
                                                                       const TString& message, bool serverless, TInstant now) {
    InFlightFetches_.erase(path);

    if (path != databaseId) {
        if (auto staleIt = Entries_.find(path); staleIt != Entries_.end()) {
            auto subscribers = std::move(staleIt->second.Subscribers);
            Entries_.erase(staleIt);
            auto& target = Entries_[databaseId].Subscribers;
            target.insert(target.end(), subscribers.begin(), subscribers.end());
        }
    }

    if (status == Ydb::StatusIds::UNSUPPORTED) {
        Entries_.try_emplace(databaseId);
        for (auto& [id, entry] : Entries_) {
            if (DatabaseIdToDatabase(id) != path || entry.State == EDatabaseState::Ready) {
                continue;
            }
            entry.State = EDatabaseState::Unsupported;
            entry.StateAt = now;
        }
        return {};
    }

    if (status != Ydb::StatusIds::SUCCESS) {
        const bool retryable = IsRetryable(status);
        const auto replyStatus = retryable ? Ydb::StatusIds::UNAVAILABLE : status;
        const TString replyMessage = retryable ? TString(WORKLOAD_MANAGER_NOT_READY_MESSAGE) : message;

        std::vector<TSubscriberReply> replies;
        for (auto it = Entries_.begin(); it != Entries_.end();) {
            if (DatabaseIdToDatabase(it->first) != path || it->second.State == EDatabaseState::Ready) {
                ++it;
                continue;
            }
            AppendReplies(it->second, replyStatus, replyMessage, replies);
            it = Entries_.erase(it);
        }
        return replies;
    }

    auto& entry = Entries_[databaseId];
    entry.State = EDatabaseState::Ready;
    entry.StateAt = now;
    entry.Serverless = serverless;
    PathToId_[path] = databaseId;
    return {};
}

std::vector<TPendingSubscriber> TDatabaseReadinessTracker::OnDatabaseDeleted(const TString& databaseId, const TString& path) {
    PathToId_.erase(path);

    const auto it = Entries_.find(databaseId);
    if (it == Entries_.end()) {
        return {};
    }
    auto subscribers = std::move(it->second.Subscribers);
    Entries_.erase(it);
    return subscribers;
}

std::vector<TSubscriberReply> TDatabaseReadinessTracker::TimeOutPending(TInstant now) {
    std::vector<TSubscriberReply> replies;
    for (auto it = Entries_.begin(); it != Entries_.end();) {
        if (it->second.State != EDatabaseState::Pending || now - it->second.StateAt <= RequestTimeout_) {
            ++it;
            continue;
        }
        AppendReplies(it->second, Ydb::StatusIds::UNAVAILABLE, TString(WORKLOAD_MANAGER_NOT_READY_MESSAGE), replies);
        it = Entries_.erase(it);
    }
    return replies;
}

bool TDatabaseReadinessTracker::HasPending() const {
    for (const auto& [_, entry] : Entries_) {
        if (entry.State == EDatabaseState::Pending) {
            return true;
        }
    }
    return false;
}

std::vector<TSubscriberReply> TDatabaseReadinessTracker::TakeSettledSubscribers(EMetadataState metadata, bool enableResourcePoolsOnServerless) {
    std::vector<TSubscriberReply> replies;
    for (auto& [_, entry] : Entries_) {
        if (entry.Subscribers.empty() || !IsSettled(entry, metadata, enableResourcePoolsOnServerless)) {
            continue;
        }
        if (entry.State == EDatabaseState::Ready && metadata == EMetadataState::TimedOut
            && !IsWorkloadManagerDisabled(entry, enableResourcePoolsOnServerless)) {
            AppendReplies(entry, Ydb::StatusIds::UNAVAILABLE, TString(WORKLOAD_MANAGER_NOT_READY_MESSAGE), replies);
        } else {
            AppendReplies(entry, Ydb::StatusIds::SUCCESS, {}, replies);
        }
    }
    return replies;
}

std::vector<TPendingSubscriber> TDatabaseReadinessTracker::TakeAllSubscribers() {
    std::vector<TPendingSubscriber> subscribers;
    for (auto& [_, entry] : Entries_) {
        subscribers.insert(subscribers.end(), entry.Subscribers.begin(), entry.Subscribers.end());
        entry.Subscribers.clear();
    }
    return subscribers;
}

void TDatabaseReadinessTracker::Fill(TSnapshot& snapshot) const {
    for (const auto& [databaseId, entry] : Entries_) {
        snapshot.Databases[databaseId] = TDatabaseInfo{
            .State = entry.State,
            .Serverless = entry.Serverless,
        };
    }
    for (const auto& [path, databaseId] : PathToId_) {
        if (const auto it = Entries_.find(databaseId); it != Entries_.end() && it->second.State == EDatabaseState::Ready) {
            snapshot.ReadyPaths.insert(path);
        }
    }
}

void TDatabaseReadinessTracker::AppendReplies(TEntry& entry, Ydb::StatusIds::StatusCode status, const TString& message,
                                              std::vector<TSubscriberReply>& replies) {
    for (const auto& sub : entry.Subscribers) {
        replies.push_back(TSubscriberReply{
            .Actor = sub.Actor,
            .Cookie = sub.Cookie,
            .Status = status,
            .Message = message,
        });
    }
    entry.Subscribers.clear();
}

std::optional<TString> TDatabaseReadinessTracker::MarkFetchInFlight(const TString& path) {
    if (!InFlightFetches_.emplace(path).second) {
        return std::nullopt;
    }
    return path;
}

bool TDatabaseReadinessTracker::IsRetryable(Ydb::StatusIds::StatusCode status) {
    switch (status) {
        case Ydb::StatusIds::UNAVAILABLE:
        case Ydb::StatusIds::OVERLOADED:
        case Ydb::StatusIds::TIMEOUT:
            return true;
        default:
            return false;
    }
}

bool TDatabaseReadinessTracker::IsWorkloadManagerDisabled(const TEntry& entry, bool enableResourcePoolsOnServerless) {
    return entry.Serverless && !enableResourcePoolsOnServerless;
}

bool TDatabaseReadinessTracker::IsSettled(const TEntry& entry, EMetadataState metadata, bool enableResourcePoolsOnServerless) {
    switch (entry.State) {
        case EDatabaseState::Pending:
            return false;
        case EDatabaseState::Ready:
            return metadata != EMetadataState::Pending || IsWorkloadManagerDisabled(entry, enableResourcePoolsOnServerless);
        case EDatabaseState::Unsupported:
            return true;
    }
    return false;
}

}
