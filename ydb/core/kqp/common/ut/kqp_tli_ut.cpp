#include <ydb/core/kqp/common/kqp_tli.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NDataIntegrity {

Y_UNIT_TEST_SUITE(TNodeQueryTextCacheTest) {

    Y_UNIT_TEST(AddAndGet) {
        TNodeQueryTextCache cache;
        cache.Add(1, "SELECT 1");
        UNIT_ASSERT_EQUAL(cache.Get(1), "SELECT 1");
    }

    Y_UNIT_TEST(GetMissingReturnsEmpty) {
        TNodeQueryTextCache cache;
        UNIT_ASSERT_EQUAL(cache.Get(42), "");
    }

    Y_UNIT_TEST(IgnoresZeroSpanIdAndEmptyQueryText) {
        TNodeQueryTextCache cache;
        cache.Add(0, "SELECT 1");
        UNIT_ASSERT_EQUAL(cache.Get(0), "");
        cache.Add(1, "");
        UNIT_ASSERT_EQUAL(cache.Get(1), "");
    }

    Y_UNIT_TEST(DeduplicatesConsecutiveIdenticalPairs) {
        TNodeQueryTextCache cache;
        cache.Add(1, "SELECT 1");
        cache.Add(1, "SELECT 1");
        // Second add is a no-op; single entry still retrievable
        UNIT_ASSERT_EQUAL(cache.Get(1), "SELECT 1");
    }

    Y_UNIT_TEST(SameSpanIdDifferentTextUpdatesIndex) {
        TNodeQueryTextCache cache;
        cache.Add(1, "SELECT 1");
        cache.Add(1, "SELECT 2");
        // Index should point to the latest entry for this span id
        UNIT_ASSERT_EQUAL(cache.Get(1), "SELECT 2");
    }

    Y_UNIT_TEST(MultipleDistinctEntries) {
        TNodeQueryTextCache cache;
        cache.Add(1, "SELECT 1");
        cache.Add(2, "SELECT 2");
        cache.Add(3, "SELECT 3");
        UNIT_ASSERT_EQUAL(cache.Get(1), "SELECT 1");
        UNIT_ASSERT_EQUAL(cache.Get(2), "SELECT 2");
        UNIT_ASSERT_EQUAL(cache.Get(3), "SELECT 3");
    }

    Y_UNIT_TEST(EvictsOldestEntryWhenFull) {
        // Fill exactly MaxSpanIdEntries entries: ids 1 .. MaxSpanIdEntries
        TNodeQueryTextCache cache;
        constexpr ui64 N = TNodeQueryTextCache::MaxSpanIdEntries;
        for (ui64 i = 1; i <= N; ++i) {
            cache.Add(i, "query_" + ToString(i));
        }

        // All entries present before eviction
        UNIT_ASSERT_EQUAL(cache.Get(1), "query_1");
        UNIT_ASSERT_EQUAL(cache.Get(N), "query_" + ToString(N));

        // Adding one more entry should evict the oldest (id=1)
        cache.Add(N + 1, "query_evict");
        UNIT_ASSERT_EQUAL(cache.Get(1), "");
        UNIT_ASSERT_EQUAL(cache.Get(N + 1), "query_evict");
    }

} // Y_UNIT_TEST_SUITE

} // namespace NDataIntegrity
} // namespace NKikimr
