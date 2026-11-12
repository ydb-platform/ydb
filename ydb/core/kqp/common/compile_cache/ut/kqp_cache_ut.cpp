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

// Compile-time policy: which limits are enforced.
template <bool Entries, bool Bytes, bool EntryBytes>
struct TSizedPolicy {
    static constexpr bool LimitEntries = Entries;
    static constexpr bool LimitBytes = Bytes;
    static constexpr bool LimitEntryBytes = EntryBytes;
    static ui64 Size(const TSized& value) { return value.Bytes; }
};

// Minimal owner: the hooks are mandatory, so even a test cache spells them out. It counts the hook
// calls and reads a clock the test moves.
template <typename TPolicy>
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

    // Mutates an entry in place and reaccounts it, the way an owner does under its lock.
    template <typename F>
    bool Mutate(int key, F&& mutate) {
        typename TBase::TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(this->Lock);
        auto* value = this->FindEntryLocked(key, false, Now(), graveyard);
        if (!value) {
            return false;
        }
        mutate(*value);
        return this->ReaccountLocked(key, Now(), graveyard);
    }

    size_t Dropped = 0;
    size_t Cleared = 0;
    static inline TInstant Clock = TInstant::Seconds(1000);
};

} // namespace

Y_UNIT_TEST_SUITE(KqpCache) {

    // The whole TKqpCache contract on one cache that enforces every limit and has a TTL.
    Y_UNIT_TEST(Contract) {
        using TCache = TTestCache<TSizedPolicy<true, true, true>>;
        TCache cache(TCacheLimits{.MaxEntries = 3, .MaxBytes = 100, .MaxEntryBytes = 60}, TDuration::Seconds(10));

        // The entry limit evicts the least recently used: the promoted 1 survives, 2 goes.
        for (int key : {1, 2, 3}) {
            cache.Insert(key, Sized(30));
        }
        UNIT_ASSERT(cache.Find(1, /*promote=*/true).Bytes);
        UNIT_ASSERT_VALUES_EQUAL(cache.Insert(4, Sized(30)).Dropped, 1);
        UNIT_ASSERT(!cache.Has(2));
        UNIT_ASSERT(cache.Has(1));

        // The byte limit evicts as many of the oldest as one big value needs. A lookup result owns
        // its value and outlives the eviction of the entry.
        TSized held = cache.Find(3, false);
        const std::weak_ptr<int> heldToken = held.Token;
        UNIT_ASSERT_VALUES_EQUAL(cache.Insert(5, Sized(60)).Dropped, 2);
        UNIT_ASSERT(!cache.Has(3) && !cache.Has(1));
        UNIT_ASSERT(cache.Has(4) && cache.Has(5));
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalBytes(), 90);
        UNIT_ASSERT(!heldToken.expired());
        held = {};
        UNIT_ASSERT(heldToken.expired());

        // A value above the per-entry limit is not admitted, and the entry it supersedes goes away.
        const auto rejected = cache.Insert(4, Sized(61));
        UNIT_ASSERT(!rejected.Admitted);
        UNIT_ASSERT_VALUES_EQUAL(rejected.Dropped, 1);
        UNIT_ASSERT(!cache.Has(4));
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalBytes(), 60);

        // Growing an entry in place is reaccounted as a use: the byte limit pushes the oldest other
        // one out. Growing past the per-entry limit drops the entry itself.
        cache.Insert(6, Sized(30));
        UNIT_ASSERT(cache.Mutate(6, [](TSized& value) { value.Bytes = 45; }));
        UNIT_ASSERT(!cache.Has(5));
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalBytes(), 45);
        UNIT_ASSERT(!cache.Mutate(6, [](TSized& value) { value.Bytes = 61; }));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalBytes(), 0);

        // TTL: a lookup past it misses and retires the entry; a promote restarts it; the sweep drops
        // the expired oldest entries up to the first live one and reports lazy expiries exactly once.
        cache.Insert(7, Sized(10));                       // expires at +10s
        TCache::Clock += TDuration::Seconds(5);
        cache.Insert(8, Sized(10));                       // +15s
        cache.Insert(9, Sized(10));                       // +15s
        TCache::Clock += TDuration::Seconds(6);          // +11s
        UNIT_ASSERT(!cache.Has(7));
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 2);
        UNIT_ASSERT(cache.Find(8, /*promote=*/true).Bytes);   // now expires at +21s
        TCache::Clock += TDuration::Seconds(5);          // +16s
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpired(), 2);    // 9 by the sweep, 7 retired earlier
        UNIT_ASSERT_VALUES_EQUAL(cache.EraseExpired(), 0);
        UNIT_ASSERT(cache.Has(8));

        // An in-place update is a use too: it restarts the TTL along with the LRU position, so the
        // list stays ordered by expiry.
        UNIT_ASSERT(cache.Mutate(8, [](TSized&) {}));         // now expires at +26s
        TCache::Clock += TDuration::Seconds(6);               // +22s, past the TTL it had before
        UNIT_ASSERT(cache.Has(8));

        // Every entry that left one at a time went through OnDropped: 2, 3, 1, 4, 5, 6, 7, 9.
        UNIT_ASSERT_VALUES_EQUAL(cache.Dropped, 8);

        // Clear releases every value and calls OnCleared once instead of OnDropped per entry.
        const std::weak_ptr<int> lastToken = cache.Find(8, false).Token;
        cache.Clear();
        UNIT_ASSERT_VALUES_EQUAL(cache.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalBytes(), 0);
        UNIT_ASSERT(lastToken.expired());
        UNIT_ASSERT_VALUES_EQUAL(cache.Cleared, 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dropped, 8);
    }

    // A limit the policy turns off is not enforced even when a value for it is passed; the bytes are
    // still tracked. This is the query cache setup: entries are limited, bytes only counted.
    Y_UNIT_TEST(PolicyDisablesLimits) {
        using TCache = TTestCache<TSizedPolicy<true, false, false>>;
        TCache cache(TCacheLimits{.MaxEntries = 2, .MaxBytes = 10, .MaxEntryBytes = 5}, TDuration::Zero());

        UNIT_ASSERT(cache.Insert(1, Sized(100)).Admitted);
        UNIT_ASSERT_VALUES_EQUAL(cache.Insert(2, Sized(100)).Dropped, 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalBytes(), 200);
        UNIT_ASSERT_VALUES_EQUAL(cache.Insert(3, Sized(1)).Dropped, 1);
        UNIT_ASSERT(!cache.Has(1));
        UNIT_ASSERT_VALUES_EQUAL(cache.TotalBytes(), 101);
    }
}
