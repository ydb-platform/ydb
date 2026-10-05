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
        TInstant StateAt;  // last state change or requery; drives timeout and requery
        bool Serverless = false;
        Ydb::StatusIds::StatusCode FailureStatus = Ydb::StatusIds::SUCCESS;
        TString FailureMessage;
        std::vector<TPendingSubscriber> Subscribers;
    };

public:
    explicit TDatabaseReadinessTracker(TDuration requestTimeout);

    /// Adds a subscriber, creating a Pending entry if new.
    /// Returns the path to fetch if a fetch must be started.
    std::optional<TString> AddSubscriber(const TString& databaseId, TPendingSubscriber subscriber, TInstant now);

    /// Returns the path to fetch for an unknown DB, or to requery a Failed/TimedOut DB past the timeout.
    std::optional<TString> OnWarmup(const TString& path, TInstant now);

    /// Applies a fetch result to all non-Ready entries of the path: Ready, Unsupported,
    /// TimedOut for retryable errors (retryable UNAVAILABLE, requery later) or Failed for other errors.
    /// Returns true if the state changed.
    bool OnFetchResult(const TString& path, const TString& databaseId, Ydb::StatusIds::StatusCode status,
                       const TString& message, bool serverless, TInstant now);

    /// Removes the entry; returns its subscribers (to be answered NOT_FOUND).
    std::vector<TPendingSubscriber> OnDatabaseDeleted(const TString& databaseId, const TString& path);

    /// Moves Pending entries past the timeout to TimedOut. Returns true if any changed.
    bool TimeOutPending(TInstant now);

    /// True if any DB fetch is still Pending.
    bool HasPending() const;

    /// Takes subscribers whose DB is settled given the metadata state; Failed carries its error,
    /// TimedOut (DB or metadata) replies retryable UNAVAILABLE.
    std::vector<TSubscriberReply> TakeSettledSubscribers(EMetadataState metadata);

    /// Takes all subscribers regardless of state (pools disabled, actor shutdown).
    std::vector<TPendingSubscriber> TakeAllSubscribers();

    /// Writes database states and paths of Ready databases into the snapshot.
    void Fill(TSnapshot& snapshot) const;

private:
    std::optional<TString> MarkFetchInFlight(const TString& path);
    static bool IsRetryable(Ydb::StatusIds::StatusCode status);
    static bool IsSettled(const TEntry& entry, EMetadataState metadata);

private:
    const TDuration RequestTimeout_;
    std::unordered_map<TString, TEntry> Entries_;
    std::unordered_map<TString, TString> PathToId_;
    std::unordered_set<TString> InFlightFetches_;
};

}
