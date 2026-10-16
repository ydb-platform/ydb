#include <ydb/services/workload_manager/actors/resource_pool_tracker.h>
#include <ydb/services/workload_manager/common/helpers.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager::NPrivate {

namespace {

const TString DATABASE = "/Root/db";
const TString POOL = "pool";

struct TResourcePoolTrackerFixture : public NUnitTest::TBaseFixture {
    TResourcePoolTracker Tracker;

    bool Subscribe(const TString& poolId = POOL) {
        return Tracker.TrySubscribe(DATABASE, poolId);
    }

    bool Update(i32 concurrentQueryLimit, const TString& poolId = POOL) {
        NResourcePool::TPoolSettings config;
        config.ConcurrentQueryLimit = concurrentQueryLimit;
        return Tracker.OnPoolInfo(DATABASE, poolId, config, std::nullopt);
    }

    bool Delete(const TString& poolId = POOL) {
        return Tracker.OnPoolInfo(DATABASE, poolId, std::nullopt, std::nullopt);
    }

    bool Contains(const TString& poolId = POOL) const {
        return Tracker.BuildSnapshot()->contains(GetPoolKey(DATABASE, poolId));
    }

    i32 ConcurrentQueryLimit(const TString& poolId = POOL) const {
        return Tracker.BuildSnapshot()->at(GetPoolKey(DATABASE, poolId)).Config.ConcurrentQueryLimit;
    }
};

}

Y_UNIT_TEST_SUITE(ResourcePoolTracker) {
    // Pool subscription. The tracker:
    // - asks to subscribe once, not again while the fetch is in flight,
    // - publishes the pool config on update,
    // - does not resubscribe a cached pool.
    Y_UNIT_TEST_F(TestSubscribeAndUpdate, TResourcePoolTrackerFixture) {
        UNIT_ASSERT(Subscribe());
        UNIT_ASSERT(!Subscribe());
        UNIT_ASSERT(!Contains());

        UNIT_ASSERT(!Update(10));
        UNIT_ASSERT(Contains());
        UNIT_ASSERT_VALUES_EQUAL(ConcurrentQueryLimit(), 10);
        UNIT_ASSERT(!Subscribe());
    }

    // Pool dropped. The tracker:
    // - on the first nullopt hides the pool and asks for a recheck,
    // - does not resubscribe while the recheck is in flight,
    // - on the second nullopt erases the pool and allows a new subscription.
    Y_UNIT_TEST_F(TestDeletionNeedsTwoNullopts, TResourcePoolTrackerFixture) {
        Subscribe();
        Update(10);

        UNIT_ASSERT(Delete());
        UNIT_ASSERT(!Contains());
        UNIT_ASSERT(!Subscribe());

        UNIT_ASSERT(!Delete());
        UNIT_ASSERT(!Contains());
        UNIT_ASSERT(Subscribe());
    }

    // Pool expired, then updated. The tracker:
    // - publishes the pool again with the new config,
    // - releases the recheck, so the next drop asks for a recheck again.
    Y_UNIT_TEST_F(TestUpdateRevivesExpired, TResourcePoolTrackerFixture) {
        Subscribe();
        Update(10);
        UNIT_ASSERT(Delete());

        UNIT_ASSERT(!Update(20));
        UNIT_ASSERT(Contains());
        UNIT_ASSERT_VALUES_EQUAL(ConcurrentQueryLimit(), 20);
        UNIT_ASSERT(!Subscribe());

        UNIT_ASSERT(Delete());
    }

    // Subscription answered with nullopt for an unknown pool. The tracker:
    // - asks for no recheck,
    // - releases the in-flight fetch so a later subscription goes through.
    Y_UNIT_TEST_F(TestNulloptUnknownReleasesLock, TResourcePoolTrackerFixture) {
        UNIT_ASSERT(Subscribe());

        UNIT_ASSERT(!Delete());
        UNIT_ASSERT(!Contains());
        UNIT_ASSERT(Subscribe());
    }
}

}
