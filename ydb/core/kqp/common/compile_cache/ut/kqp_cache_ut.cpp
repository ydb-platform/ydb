#include <ydb/core/kqp/common/compile_cache/kqp_cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>

using namespace NKikimr::NKqp;

namespace {

// A value with an accounted size and a token whose lifetime shows who still holds the value.
struct TSized {
    ui64 Bytes = 0;   // 0 only for a miss
    std::shared_ptr<int> Token;
};

TSized Sized(ui64 bytes) {
    return TSized{bytes, std::make_shared<int>(0)};
}

struct TBytes {
    size_t operator()(const TSized& value) const { return value.Bytes; }
};

template <typename TPolicy = TUniformSizeProvider<TSized>>
struct TTestCache : TKqpCache<TTestCache<TPolicy>, int, TSized, TPolicy> {
    using TBase = TKqpCache<TTestCache<TPolicy>, int, TSized, TPolicy>;
    using TBase::TBase;
    using TEvicted = typename TBase::TEvicted;

    void OnDropped(const TEvicted&) { ++Dropped; }
    void OnCleared() { ++Cleared; }
    static TInstant Now() { return Clock; }

    bool Has(int key) {
        return this->Find(key, false).Bytes != 0;
    }

    size_t Dropped = 0;
    size_t Cleared = 0;
    static inline TInstant Clock = TInstant::Seconds(1000);
};

} // namespace

Y_UNIT_TEST_SUITE(KqpCache) {

    Y_UNIT_TEST(WeightedEviction) {
        TTestCache<TBytes> cache(10, TDuration::Zero());
        for (int key : {1, 2, 3}) {
            cache.Insert(key, Sized(3));
        }
        TSized held = cache.Find(1, true);
        UNIT_ASSERT_VALUES_EQUAL(cache.Insert(4, Sized(6)), 2);
        UNIT_ASSERT(cache.Has(1));
        UNIT_ASSERT(!cache.Has(2) && !cache.Has(3));
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalWeight(), 9);

        const std::weak_ptr<int> token = held.Token;
        UNIT_ASSERT_VALUES_EQUAL(cache.Insert(4, Sized(11)), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dropped, 4);
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalWeight(), 11);
        UNIT_ASSERT(!token.expired());
        held = {};
        UNIT_ASSERT(token.expired());

        cache.Clear();
        UNIT_ASSERT_VALUES_EQUAL(cache.Cleared, 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalWeight(), 0);
        cache.Insert(1, Sized(6));
        UNIT_ASSERT_VALUES_EQUAL(cache.Insert(2, Sized(6)), 1);
    }

    Y_UNIT_TEST(CountLimitAndLazyExpiry) {
        using TCache = TTestCache<>;
        TCache cache(2, TDuration::Seconds(10));
        cache.Insert(1, Sized(100));
        cache.Insert(2, Sized(100));
        UNIT_ASSERT_VALUES_EQUAL(cache.Insert(3, Sized(100)), 1);
        UNIT_ASSERT(!cache.Has(1));
        TCache::Clock += TDuration::Seconds(10);
        UNIT_ASSERT(!cache.Has(2));
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpired(), 2);
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpired(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dropped, 3);
    }

    Y_UNIT_TEST(ExpiryWithOutOfOrderTimestamps) {
        using TCache = TTestCache<>;
        TCache cache(10, TDuration::Seconds(10));
        const auto start = TCache::Clock;

        cache.Insert(1, Sized(1));
        TCache::Clock = start + TDuration::Seconds(5);
        cache.Insert(2, Sized(1));
        TCache::Clock = start + TDuration::Seconds(2);
        UNIT_ASSERT(cache.Find(1, /*promote=*/true).Bytes); // after insert(2), expires at +15s

        TCache::Clock = start + TDuration::Seconds(7);
        UNIT_ASSERT(cache.Find(2, /*promote=*/true).Bytes);
        TCache::Clock = start + TDuration::Seconds(4);
        cache.Insert(3, Sized(1)); // after promote(2), expires at +17s

        TCache::Clock = start + TDuration::Seconds(14);
        UNIT_ASSERT(cache.Has(1));
        TCache::Clock = start + TDuration::Seconds(15);
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpired(), 1);
        UNIT_ASSERT(cache.Has(3));
        TCache::Clock = start + TDuration::Seconds(17);
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpired(), 2);
    }
}
