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

std::optional<TString> TDatabaseReadinessTracker::OnWarmup(const TString& path, TInstant now) {
    if (const auto pathIt = PathToId_.find(path); pathIt != PathToId_.end()) {
        if (const auto it = Entries_.find(pathIt->second); it != Entries_.end() && it->second.State == EDatabaseState::Ready) {
            return std::nullopt;
        }
    }

    bool known = false;
    bool requery = false;
    for (auto& [databaseId, entry] : Entries_) {
        if (DatabaseIdToDatabase(databaseId) != path) {
            continue;
        }
        known = true;
        if ((entry.State == EDatabaseState::Failed || entry.State == EDatabaseState::TimedOut) && now - entry.StateAt > RequestTimeout_) {
            entry.StateAt = now;
            requery = true;
        }
    }

    if (known && !requery) {
        return std::nullopt;
    }
    return MarkFetchInFlight(path);
}

bool TDatabaseReadinessTracker::OnFetchResult(const TString& path, const TString& databaseId, Ydb::StatusIds::StatusCode status,
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

    if (status != Ydb::StatusIds::SUCCESS) {
        Entries_.try_emplace(databaseId);
        for (auto& [id, entry] : Entries_) {
            if (DatabaseIdToDatabase(id) != path || entry.State == EDatabaseState::Ready) {
                continue;
            }
            entry.StateAt = now;
            if (status == Ydb::StatusIds::UNSUPPORTED) {
                entry.State = EDatabaseState::Unsupported;
            } else if (IsRetryable(status)) {
                entry.State = EDatabaseState::TimedOut;
            } else {
                entry.State = EDatabaseState::Failed;
                entry.FailureStatus = status;
                entry.FailureMessage = message;
            }
        }
        return true;
    }

    auto& entry = Entries_[databaseId];
    entry.State = EDatabaseState::Ready;
    entry.StateAt = now;
    entry.Serverless = serverless;
    entry.FailureStatus = Ydb::StatusIds::SUCCESS;
    entry.FailureMessage.clear();
    PathToId_[path] = databaseId;
    return true;
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

bool TDatabaseReadinessTracker::TimeOutPending(TInstant now) {
    bool changed = false;
    for (auto& [_, entry] : Entries_) {
        if (entry.State == EDatabaseState::Pending && now - entry.StateAt > RequestTimeout_) {
            entry.State = EDatabaseState::TimedOut;
            entry.StateAt = now;
            changed = true;
        }
    }
    return changed;
}

bool TDatabaseReadinessTracker::HasPending() const {
    for (const auto& [_, entry] : Entries_) {
        if (entry.State == EDatabaseState::Pending) {
            return true;
        }
    }
    return false;
}

std::vector<TSubscriberReply> TDatabaseReadinessTracker::TakeSettledSubscribers(EMetadataState metadata) {
    std::vector<TSubscriberReply> replies;
    for (auto& [_, entry] : Entries_) {
        if (entry.Subscribers.empty() || !IsSettled(entry, metadata)) {
            continue;
        }
        auto status = Ydb::StatusIds::SUCCESS;
        TString message;
        if (entry.State == EDatabaseState::Failed) {
            status = entry.FailureStatus;
            message = entry.FailureMessage;
        } else if (entry.State == EDatabaseState::TimedOut || (entry.State == EDatabaseState::Ready && metadata == EMetadataState::TimedOut)) {
            status = Ydb::StatusIds::UNAVAILABLE;
            message = WORKLOAD_MANAGER_NOT_READY_MESSAGE;
        }
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
            .FailureStatus = entry.FailureStatus,
            .FailureMessage = entry.FailureMessage,
        };
    }
    for (const auto& [path, databaseId] : PathToId_) {
        if (const auto it = Entries_.find(databaseId); it != Entries_.end() && it->second.State == EDatabaseState::Ready) {
            snapshot.ReadyPaths.insert(path);
        }
    }
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

bool TDatabaseReadinessTracker::IsSettled(const TEntry& entry, EMetadataState metadata) {
    switch (entry.State) {
        case EDatabaseState::Pending:
            return false;
        case EDatabaseState::Ready:
            return metadata != EMetadataState::Pending;
        case EDatabaseState::Failed:
        case EDatabaseState::TimedOut:
        case EDatabaseState::Unsupported:
            return true;
    }
    return false;
}

}
