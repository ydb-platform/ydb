#pragma once

#include <ydb/services/workload_manager/gateway_internal.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace NKikimr::NWorkloadManager::NPrivate {

/// Subscriber waiting for a database to become ready (TEvSubscribeOnWorkloadManagerReady).
struct TPendingSubscriber {
    NActors::TActorId Actor;
    ui64 Cookie = 0;
};

/// Reply to send as TEvWorkloadManagerReady.
struct TSubscriberReply {
    NActors::TActorId Actor;
    ui64 Cookie = 0;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    TString Message;
};

///
/// Tracks per-database readiness for the workload manager state actor:
///
class TDatabaseReadinessTracker {
    struct TEntry {
        EDatabaseState State = EDatabaseState::Pending;
        TInstant StateAt;  // last state change; drives the Pending timeout
        bool Serverless = false;
        std::vector<TPendingSubscriber> Subscribers;
    };

public:
    explicit TDatabaseReadinessTracker(TDuration requestTimeout);

    /// Adds a subscriber, creating a Pending entry if new.
    /// Returns the path to fetch if a fetch must be started.
    std::optional<TString> AddSubscriber(const TString& databaseId, TPendingSubscriber subscriber, TInstant now);

    /// Returns the path to fetch for an unknown DB.
    std::optional<TString> OnWarmup(const TString& path);

    /// Applies a fetch result to all non-Ready entries of the path: Ready or Unsupported.
    /// Errors are not cached: the entries are removed and their subscribers are returned with
    /// retryable UNAVAILABLE for retryable errors or the fetch status otherwise.
    std::vector<TSubscriberReply> OnFetchResult(const TString& path, const TString& databaseId, Ydb::StatusIds::StatusCode status,
                                                const TString& message, bool serverless, TInstant now);

    /// Removes the entry; returns its subscribers (to be answered NOT_FOUND).
    std::vector<TPendingSubscriber> OnDatabaseDeleted(const TString& databaseId, const TString& path);

    /// Removes Pending entries past the timeout; returns their subscribers with retryable UNAVAILABLE.
    std::vector<TSubscriberReply> TimeOutPending(TInstant now);

    /// True if any DB fetch is still Pending.
    bool HasPending() const;

    /// Takes subscribers whose DB is settled given the metadata state; metadata TimedOut replies retryable UNAVAILABLE.
    /// Serverless DBs with pools disabled on serverless are settled regardless of metadata.
    std::vector<TSubscriberReply> TakeSettledSubscribers(EMetadataState metadata, bool enableResourcePoolsOnServerless);

    /// Takes all subscribers regardless of state (pools disabled, actor shutdown).
    std::vector<TPendingSubscriber> TakeAllSubscribers();

    /// Writes database states and paths of Ready databases into the snapshot.
    void Fill(TSnapshot& snapshot) const;

private:
    std::optional<TString> MarkFetchInFlight(const TString& path);
    static void AppendReplies(TEntry& entry, Ydb::StatusIds::StatusCode status, const TString& message,
                              std::vector<TSubscriberReply>& replies);
    static bool IsRetryable(Ydb::StatusIds::StatusCode status);
    static bool IsWorkloadManagerDisabled(const TEntry& entry, bool enableResourcePoolsOnServerless);
    static bool IsSettled(const TEntry& entry, EMetadataState metadata, bool enableResourcePoolsOnServerless);

private:
    const TDuration RequestTimeout_;
    std::unordered_map<TString, TEntry> Entries_;
    std::unordered_map<TString, TString> PathToId_;
    std::unordered_set<TString> InFlightFetches_;
};

}
