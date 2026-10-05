#include <ydb/services/workload_manager/actors/database_readiness_tracker.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager::NPrivate {

namespace {

constexpr TDuration TIMEOUT = TDuration::Seconds(5);
const TInstant T0 = TInstant::Seconds(1000);
const TString DATABASE = "/Root/db";

struct TDatabaseTrackerFixture : public NUnitTest::TBaseFixture {
    TDatabaseReadinessTracker Tracker{TIMEOUT};

    std::optional<TString> Subscribe(ui64 cookie, const TString& databaseId = DATABASE, TInstant now = T0) {
        return Tracker.AddSubscriber(databaseId, TPendingSubscriber{.Actor = NActors::TActorId(1, 0, cookie, 0), .Cookie = cookie}, now);
    }

    void FetchSucceeded(const TString& path = DATABASE, const TString& databaseId = DATABASE, bool serverless = false, TInstant now = T0) {
        Tracker.OnFetchResult(path, databaseId, Ydb::StatusIds::SUCCESS, {}, serverless, now);
    }

    void FetchFailed(Ydb::StatusIds::StatusCode status, const TString& message = {}, const TString& path = DATABASE, TInstant now = T0) {
        Tracker.OnFetchResult(path, path, status, message, false, now);
    }

    TSnapshot Snapshot() const {
        TSnapshot snapshot;
        Tracker.Fill(snapshot);
        return snapshot;
    }

    THashMap<TString, TDatabaseInfo> Databases() const {
        return Snapshot().Databases;
    }

    EDatabaseState State(const TString& databaseId = DATABASE) const {
        return Databases().at(databaseId).State;
    }

    bool Contains(const TString& databaseId) const {
        return Databases().contains(databaseId);
    }

    std::vector<TSubscriberReply> TakeSettled(EMetadataState metadata = EMetadataState::Ready) {
        return Tracker.TakeSettledSubscribers(metadata);
    }
};

}

Y_UNIT_TEST_SUITE(DatabaseReadinessTracker) {
    // Database info fetch. The tracker:
    // - starts one fetch for the first subscriber, none for the next,
    // - holds subscribers until the fetch completes,
    // - releases all of them with SUCCESS once Ready.
    Y_UNIT_TEST_F(TestFetchReleasesSubscribers, TDatabaseTrackerFixture) {
        const auto fetch = Subscribe(1);
        UNIT_ASSERT(fetch);
        UNIT_ASSERT_VALUES_EQUAL(*fetch, DATABASE);
        UNIT_ASSERT(!Subscribe(2));
        UNIT_ASSERT(TakeSettled().empty());

        FetchSucceeded();

        const auto replies = TakeSettled();
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 2);
        for (const auto& reply : replies) {
            UNIT_ASSERT_VALUES_EQUAL(reply.Status, Ydb::StatusIds::SUCCESS);
        }
        UNIT_ASSERT(TakeSettled().empty());
    }

    // Database is Ready, classifier metadata is not. Subscribers:
    // - are held while metadata is Pending,
    // - are released with retryable UNAVAILABLE once metadata times out.
    Y_UNIT_TEST_F(TestReadyWaitsForMetadata, TDatabaseTrackerFixture) {
        Subscribe(1);
        FetchSucceeded();

        UNIT_ASSERT(TakeSettled(EMetadataState::Pending).empty());

        const auto replies = TakeSettled(EMetadataState::TimedOut);
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Status, Ydb::StatusIds::UNAVAILABLE);
    }

    // Database info fetch fails. The tracker:
    // - marks the database Failed and replies with the fetch status and message for a non-retryable error,
    // - marks it TimedOut and replies with retryable UNAVAILABLE for a retryable error,
    // - marks UNSUPPORTED as Unsupported and replies with SUCCESS.
    Y_UNIT_TEST_F(TestFetchFailure, TDatabaseTrackerFixture) {
        const TString retryable = "/Root/retryable";
        const TString unsupported = "/Root/unsupported";
        Subscribe(1);
        Subscribe(2, retryable);
        Subscribe(3, unsupported);

        FetchFailed(Ydb::StatusIds::NOT_FOUND, "fetch failed");
        FetchFailed(Ydb::StatusIds::UNAVAILABLE, "retry limit exceeded", retryable);
        FetchFailed(Ydb::StatusIds::UNSUPPORTED, {}, unsupported);

        UNIT_ASSERT(State() == EDatabaseState::Failed);
        UNIT_ASSERT(State(retryable) == EDatabaseState::TimedOut);
        UNIT_ASSERT(State(unsupported) == EDatabaseState::Unsupported);

        const auto replies = TakeSettled(EMetadataState::Pending);
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 3);
        for (const auto& reply : replies) {
            if (reply.Cookie == 1) {
                UNIT_ASSERT_VALUES_EQUAL(reply.Status, Ydb::StatusIds::NOT_FOUND);
                UNIT_ASSERT_VALUES_EQUAL(reply.Message, "fetch failed");
            } else if (reply.Cookie == 2) {
                UNIT_ASSERT_VALUES_EQUAL(reply.Status, Ydb::StatusIds::UNAVAILABLE);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(reply.Status, Ydb::StatusIds::SUCCESS);
            }
        }
    }

    // Database info fetch hangs. The tracker:
    // - keeps the database Pending before the limit,
    // - moves it to TimedOut past the limit and releases subscribers with retryable UNAVAILABLE,
    // - accepts a late fetch result and becomes Ready.
    Y_UNIT_TEST_F(TestPendingTimesOut, TDatabaseTrackerFixture) {
        Subscribe(1);

        UNIT_ASSERT(!Tracker.TimeOutPending(T0 + TIMEOUT / 2));
        UNIT_ASSERT(Tracker.HasPending());

        UNIT_ASSERT(Tracker.TimeOutPending(T0 + TIMEOUT * 2));
        UNIT_ASSERT(!Tracker.HasPending());
        UNIT_ASSERT(State() == EDatabaseState::TimedOut);

        const auto replies = TakeSettled(EMetadataState::Pending);
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Status, Ydb::StatusIds::UNAVAILABLE);

        FetchSucceeded(DATABASE, DATABASE, false, T0 + TIMEOUT * 3);
        UNIT_ASSERT(State() == EDatabaseState::Ready);
    }

    // Database info fetch failed. Warmup:
    // - does not refetch before the limit,
    // - refetches once past the limit, keeping the Failed state until the result,
    // - turns the database Ready on a successful refetch.
    Y_UNIT_TEST_F(TestFailedRequeryRecovers, TDatabaseTrackerFixture) {
        Subscribe(1);
        FetchFailed(Ydb::StatusIds::NOT_FOUND, "fetch failed");
        TakeSettled();

        UNIT_ASSERT(!Tracker.OnWarmup(DATABASE, T0 + TIMEOUT / 2));

        const TInstant requeryAt = T0 + TIMEOUT * 2;
        const auto fetch = Tracker.OnWarmup(DATABASE, requeryAt);
        UNIT_ASSERT(fetch);
        UNIT_ASSERT_VALUES_EQUAL(*fetch, DATABASE);
        UNIT_ASSERT(!Tracker.OnWarmup(DATABASE, requeryAt));
        UNIT_ASSERT(State() == EDatabaseState::Failed);

        FetchSucceeded(DATABASE, DATABASE, false, requeryAt);
        UNIT_ASSERT(State() == EDatabaseState::Ready);
    }

    // Serverless database subscribed by its path before the composite id is known. On fetch success:
    // - subscribers move from the path entry to the composite id entry,
    // - the path entry is removed,
    // - the path is published as ready.
    Y_UNIT_TEST_F(TestServerlessMergeStaleEntry, TDatabaseTrackerFixture) {
        const TString path = "/Root/serverless";
        const TString databaseId = "72075186224037891:2:/Root/serverless";
        Subscribe(1, path);

        FetchSucceeded(path, databaseId, true);

        UNIT_ASSERT(!Contains(path));
        UNIT_ASSERT(State(databaseId) == EDatabaseState::Ready);
        UNIT_ASSERT(Databases().at(databaseId).Serverless);
        UNIT_ASSERT(Snapshot().ReadyPaths.contains(path));

        const auto replies = TakeSettled();
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Cookie, 1);
    }

    // Database deleted. The tracker:
    // - returns its waiting subscribers,
    // - removes the entry,
    // - fetches again on the next warmup.
    Y_UNIT_TEST_F(TestDatabaseDeleted, TDatabaseTrackerFixture) {
        Subscribe(1);
        FetchSucceeded();

        const auto subscribers = Tracker.OnDatabaseDeleted(DATABASE, DATABASE);
        UNIT_ASSERT_VALUES_EQUAL(subscribers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(subscribers[0].Cookie, 1);
        UNIT_ASSERT(!Contains(DATABASE));

        const auto fetch = Tracker.OnWarmup(DATABASE, T0);
        UNIT_ASSERT(fetch);
        UNIT_ASSERT_VALUES_EQUAL(*fetch, DATABASE);
    }
}

}
