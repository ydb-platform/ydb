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

    std::vector<TSubscriberReply> FetchFailed(Ydb::StatusIds::StatusCode status, const TString& message = {}, const TString& path = DATABASE, TInstant now = T0) {
        return Tracker.OnFetchResult(path, path, status, message, false, now);
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
    // - replies with the fetch status and message for a non-retryable error and forgets the database,
    // - replies with retryable UNAVAILABLE for a retryable error and forgets the database,
    // - marks UNSUPPORTED as Unsupported and replies with SUCCESS.
    Y_UNIT_TEST_F(TestFetchFailure, TDatabaseTrackerFixture) {
        const TString retryable = "/Root/retryable";
        const TString unsupported = "/Root/unsupported";
        Subscribe(1);
        Subscribe(2, retryable);
        Subscribe(3, unsupported);

        const auto failed = FetchFailed(Ydb::StatusIds::NOT_FOUND, "fetch failed");
        UNIT_ASSERT_VALUES_EQUAL(failed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(failed[0].Cookie, 1);
        UNIT_ASSERT_VALUES_EQUAL(failed[0].Status, Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(failed[0].Message, "fetch failed");
        UNIT_ASSERT(!Contains(DATABASE));

        const auto retried = FetchFailed(Ydb::StatusIds::UNAVAILABLE, "retry limit exceeded", retryable);
        UNIT_ASSERT_VALUES_EQUAL(retried.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(retried[0].Cookie, 2);
        UNIT_ASSERT_VALUES_EQUAL(retried[0].Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT(!Contains(retryable));

        UNIT_ASSERT(FetchFailed(Ydb::StatusIds::UNSUPPORTED, {}, unsupported).empty());
        UNIT_ASSERT(State(unsupported) == EDatabaseState::Unsupported);

        const auto replies = TakeSettled(EMetadataState::Pending);
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Cookie, 3);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Status, Ydb::StatusIds::SUCCESS);
    }

    // Database info fetch hangs. The tracker:
    // - keeps the database Pending before the limit,
    // - releases subscribers with retryable UNAVAILABLE past the limit and forgets the database,
    // - does not start a second fetch while the first one is in flight,
    // - accepts a late fetch result and becomes Ready.
    Y_UNIT_TEST_F(TestPendingTimesOut, TDatabaseTrackerFixture) {
        Subscribe(1);

        UNIT_ASSERT(Tracker.TimeOutPending(T0 + TIMEOUT / 2).empty());
        UNIT_ASSERT(Tracker.HasPending());

        const auto replies = Tracker.TimeOutPending(T0 + TIMEOUT * 2);
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT(!Tracker.HasPending());
        UNIT_ASSERT(!Contains(DATABASE));

        UNIT_ASSERT(!Subscribe(2, DATABASE, T0 + TIMEOUT * 2));
        UNIT_ASSERT(!Tracker.OnWarmup(DATABASE));

        FetchSucceeded(DATABASE, DATABASE, false, T0 + TIMEOUT * 3);
        UNIT_ASSERT(State() == EDatabaseState::Ready);

        const auto ready = TakeSettled();
        UNIT_ASSERT_VALUES_EQUAL(ready.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ready[0].Cookie, 2);
        UNIT_ASSERT_VALUES_EQUAL(ready[0].Status, Ydb::StatusIds::SUCCESS);
    }

    // Database info fetch failed. The error is not cached:
    // - the next warmup starts a new fetch at once,
    // - the next subscriber waits for that fetch and gets SUCCESS once the database is Ready.
    Y_UNIT_TEST_F(TestFailureNotCached, TDatabaseTrackerFixture) {
        Subscribe(1);
        FetchFailed(Ydb::StatusIds::NOT_FOUND, "fetch failed");

        const auto fetch = Tracker.OnWarmup(DATABASE);
        UNIT_ASSERT(fetch);
        UNIT_ASSERT_VALUES_EQUAL(*fetch, DATABASE);
        UNIT_ASSERT(!Tracker.OnWarmup(DATABASE));

        UNIT_ASSERT(!Subscribe(2));
        UNIT_ASSERT(TakeSettled().empty());

        FetchSucceeded();
        UNIT_ASSERT(State() == EDatabaseState::Ready);

        const auto replies = TakeSettled();
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Cookie, 2);
        UNIT_ASSERT_VALUES_EQUAL(replies[0].Status, Ydb::StatusIds::SUCCESS);
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

        const auto fetch = Tracker.OnWarmup(DATABASE);
        UNIT_ASSERT(fetch);
        UNIT_ASSERT_VALUES_EQUAL(*fetch, DATABASE);
    }
}

}
