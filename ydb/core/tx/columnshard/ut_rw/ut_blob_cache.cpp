#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/common/blob.h>

#include <ydb/library/actors/core/events.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBlobCache {

using namespace NActors;
using NOlap::TBlobRange;
using NOlap::TUnifiedBlobId;

namespace {

TBlobRange MakeRange(const ui32 cookie, const ui32 blobSize, const ui32 offset = 0, const ui32 rangeSize = 0) {
    const ui32 size = rangeSize ? rangeSize : (blobSize - offset);
    TLogoBlobID logo(1, 1, 1, 1, blobSize, cookie);
    return TBlobRange(TUnifiedBlobId(0, logo), offset, size);
}

TString MakeData(const ui32 size, const char fill) {
    return TString(size, fill);
}

class TBlobCacheFixture {
public:
    TBlobCacheFixture(const ui64 maxBytes, const ui64 writeProtectDurationMs = 3600000) {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());

        TBlobCacheSettings settings = TBlobCacheSettings::FromProto(NKikimrConfig::TBlobCacheConfig());
        settings.MaxCacheDataSize = maxBytes;
        settings.WriteProtectDurationMs = writeProtectDurationMs;
        Counters = new ::NMonitoring::TDynamicCounters();
        ActorId = Runtime.Register(CreateBlobCache(settings, Counters));
        Runtime.EnableScheduleForActor(ActorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(options);

        Sender = Runtime.AllocateEdgeActor();
    }

    void CacheRange(const TBlobRange& range, const TString& data, const bool sticky = true) {
        Runtime.Send(new IEventHandle(ActorId, Sender, new TEvBlobCache::TEvCacheBlobRange(range, data, sticky)), 0, true);
        Runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
    }

    TEvBlobCache::TEvReadBlobRangeResult::TPtr ReadRange(const TBlobRange& range, const bool cacheAfterRead = true) {
        auto ev = new TEvBlobCache::TEvReadBlobRange(range, TReadBlobRangeOptions{ .CacheAfterRead = cacheAfterRead, .IsBackgroud = false });
        Runtime.Send(new IEventHandle(ActorId, Sender, ev), 0, true);
        return Runtime.GrabEdgeEvent<TEvBlobCache::TEvReadBlobRangeResult>(Sender, TDuration::Seconds(5));
    }

    // A miss is counted synchronously in the actor before the BS read is issued; there is no BS proxy in this fixture,
    // so the read itself never completes. Use only as the last step(s) of a test: a pending miss adds in-flight bytes.
    void ExpectMiss(const TBlobRange& range) {
        const i64 missesBefore = Counter("Misses", true);
        const i64 hitsBefore = Counter("Hits", true);
        auto ev = new TEvBlobCache::TEvReadBlobRange(range, TReadBlobRangeOptions{ .CacheAfterRead = false, .IsBackgroud = false });
        Runtime.Send(new IEventHandle(ActorId, Sender, ev), 0, true);
        Runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL_C(Counter("Misses", true), missesBefore + 1, range.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(Counter("Hits", true), hitsBefore, range.ToString());
    }

    void ExpectHit(const TBlobRange& range, const TString& expectedData) {
        auto result = ReadRange(range);
        UNIT_ASSERT_C(result, range.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Status, NKikimrProto::OK, range.ToString());
        UNIT_ASSERT_C(result->Get()->FromCache, range.ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Data, expectedData, range.ToString());
    }

    void Forget(const TUnifiedBlobId& blobId) {
        Runtime.Send(new IEventHandle(ActorId, Sender, new TEvBlobCache::TEvForgetBlob(blobId)), 0, true);
        Runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
    }

    void Wakeup() {
        Runtime.Send(new IEventHandle(ActorId, Sender, new TEvents::TEvWakeup()), 0, true);
        Runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
    }

    i64 Counter(const char* name, const bool derivative = false) const {
        return Counters->GetCounter(name, derivative)->Val();
    }

    TTestActorRuntime Runtime;
    TActorId ActorId;
    TActorId Sender;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
};

}   // namespace

Y_UNIT_TEST_SUITE(TBlobCache) {
    Y_UNIT_TEST(ExactStickyHit) {
        TBlobCacheFixture fixture(1024);
        auto range = MakeRange(1, 64);
        auto data = MakeData(64, 'a');
        fixture.CacheRange(range, data, true);

        auto result = fixture.ReadRange(range);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT(result->Get()->FromCache);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Data, data);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Hits", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyHits", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyHitsBytes", true), 64);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("HitsBytes", true), 64);
    }

    Y_UNIT_TEST(CoveringHit) {
        TBlobCacheFixture fixture(1024);
        auto full = MakeRange(1, 100);
        auto data = MakeData(100, 'b');
        fixture.CacheRange(full, data, true);

        auto slice = MakeRange(1, 100, 10, 20);
        auto result = fixture.ReadRange(slice);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT(result->Get()->FromCache);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Data, data.substr(10, 20));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Hits", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyHitsBytes", true), 20);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("HitsBytes", true), 20);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 1);
    }

    Y_UNIT_TEST(UnprotectedHitDoesNotCountSticky) {
        TBlobCacheFixture fixture(1024);
        auto range = MakeRange(1, 32);
        auto data = MakeData(32, 'c');
        fixture.CacheRange(range, data, false);

        auto result = fixture.ReadRange(range);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Get()->FromCache);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Data, data);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Hits", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyHits", true), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 0);
    }

    Y_UNIT_TEST(EvictUnprotectedBeforeSticky) {
        TBlobCacheFixture fixture(100);
        auto stickyRange = MakeRange(1, 60);
        auto uRange = MakeRange(2, 50);
        fixture.CacheRange(stickyRange, MakeData(60, 's'), true);
        fixture.CacheRange(uRange, MakeData(50, 'u'), false);

        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 1);
        UNIT_ASSERT(fixture.Counter("Evictions", true) >= 1);

        auto stickyHit = fixture.ReadRange(stickyRange);
        UNIT_ASSERT(stickyHit);
        UNIT_ASSERT(stickyHit->Get()->FromCache);
        UNIT_ASSERT_VALUES_EQUAL(stickyHit->Get()->Data, MakeData(60, 's'));
    }

    Y_UNIT_TEST(EvictOldestStickyDespiteLaterRead) {
        TBlobCacheFixture fixture(100);
        auto oldest = MakeRange(1, 60);
        auto middle = MakeRange(2, 40);
        auto newest = MakeRange(3, 60);
        fixture.CacheRange(oldest, MakeData(60, 'a'), true);
        fixture.CacheRange(middle, MakeData(40, 'b'), true);
        fixture.ExpectHit(oldest, MakeData(60, 'a'));
        fixture.CacheRange(newest, MakeData(60, 'c'), true);

        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyEvictions", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBytes"), 100);
        fixture.ExpectHit(middle, MakeData(40, 'b'));
        fixture.ExpectHit(newest, MakeData(60, 'c'));
        fixture.ExpectMiss(oldest);
    }

    Y_UNIT_TEST(EvictStickyWhenUnprotectedEmpty) {
        TBlobCacheFixture fixture(100);
        auto first = MakeRange(1, 60);
        auto second = MakeRange(2, 60);
        fixture.CacheRange(first, MakeData(60, '1'), true);
        fixture.CacheRange(second, MakeData(60, '2'), true);

        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyEvictions", true), 1);

        auto secondHit = fixture.ReadRange(second);
        UNIT_ASSERT(secondHit);
        UNIT_ASSERT(secondHit->Get()->FromCache);
        UNIT_ASSERT_VALUES_EQUAL(secondHit->Get()->Data, MakeData(60, '2'));
    }

    Y_UNIT_TEST(ForgetDropsCovering) {
        TBlobCacheFixture fixture(1024);
        auto full = MakeRange(1, 80);
        auto other = MakeRange(2, 30);
        fixture.CacheRange(full, MakeData(80, 'f'), true);
        fixture.CacheRange(other, MakeData(30, 'o'), true);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBytes"), 110);

        // A slice is served from the full blob before Forget...
        fixture.ExpectHit(MakeRange(1, 80, 10, 20), MakeData(20, 'f'));

        fixture.Forget(full.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Forgets", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("ForgetBytes", true), 80);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBytes"), 30);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBytes"), 30);

        // ...and the unrelated blob is untouched, while both the exact and the covering read now miss.
        fixture.ExpectHit(other, MakeData(30, 'o'));
        fixture.ExpectMiss(full);
        fixture.ExpectMiss(MakeRange(1, 80, 10, 20));
    }

    Y_UNIT_TEST(ForgetDropsAllRangesOfBlob) {
        TBlobCacheFixture fixture(1024);
        auto head = MakeRange(1, 100, 0, 50);
        auto tail = MakeRange(1, 100, 50, 50);
        fixture.CacheRange(head, MakeData(50, 'h'), true);
        fixture.CacheRange(tail, MakeData(50, 't'), false);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 1);

        fixture.Forget(head.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Forgets", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("ForgetBytes", true), 100);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBytes"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBytes"), 0);
        fixture.ExpectMiss(head);
        fixture.ExpectMiss(tail);
    }

    Y_UNIT_TEST(GraduateExpiredBeforeStillSticky) {
        TBlobCacheFixture fixture(100, 1000);
        auto expired = MakeRange(1, 40);
        auto stillSticky = MakeRange(2, 40);
        auto extra = MakeRange(3, 40);
        fixture.CacheRange(expired, MakeData(40, 'e'), true);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 1);

        // Protection window elapses: the entry is graduated to unprotected but stays cached.
        fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(2));
        fixture.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBytes"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Evictions", true), 0);

        fixture.CacheRange(stillSticky, MakeData(40, 's'), true);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 2);
        // 120 > 100: exactly one eviction, and the victim is the graduated (now unprotected) entry, not the sticky one.
        fixture.CacheRange(extra, MakeData(40, 'u'), false);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Evictions", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyEvictions", true), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 1);

        fixture.ExpectHit(stillSticky, MakeData(40, 's'));
        fixture.ExpectHit(extra, MakeData(40, 'u'));
        fixture.ExpectMiss(expired);
    }

    Y_UNIT_TEST(StickyRefreshExtendsProtection) {
        TBlobCacheFixture fixture(1024, 1000);
        auto range = MakeRange(1, 40);
        fixture.CacheRange(range, MakeData(40, 'a'), true);

        // Re-insert at t=700ms moves expiry to t=1700ms.
        fixture.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(700));
        fixture.CacheRange(range, MakeData(40, 'a'), true);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Adds", true), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 1);

        // t=1400ms: past the original window, still inside the refreshed one.
        fixture.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(700));
        fixture.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBytes"), 40);

        // t=2000ms: refreshed window elapsed too.
        fixture.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(600));
        fixture.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 1);
        fixture.ExpectHit(range, MakeData(40, 'a'));
    }

    Y_UNIT_TEST(StickyRefreshPromotesInLru) {
        TBlobCacheFixture fixture(100);
        auto first = MakeRange(1, 40);
        auto second = MakeRange(2, 40);
        auto third = MakeRange(3, 40);
        fixture.CacheRange(first, MakeData(40, '1'), true);
        fixture.CacheRange(second, MakeData(40, '2'), true);
        // Re-writing `first` makes it MRU; `second` is now the oldest sticky entry.
        fixture.CacheRange(first, MakeData(40, '1'), true);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 2);

        // No unprotected entries: the emergency path evicts the oldest sticky one, which must be `second`.
        fixture.CacheRange(third, MakeData(40, '3'), true);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyEvictions", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 2);
        fixture.ExpectHit(first, MakeData(40, '1'));
        fixture.ExpectHit(third, MakeData(40, '3'));
        fixture.ExpectMiss(second);
    }

    Y_UNIT_TEST(UnprotectedInsertPromotedToSticky) {
        TBlobCacheFixture fixture(100);
        auto target = MakeRange(1, 40);
        auto filler1 = MakeRange(2, 40);
        auto filler2 = MakeRange(3, 40);
        fixture.CacheRange(target, MakeData(40, 't'), false);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 0);

        // Same range arrives as a write: promoted to sticky in place, no duplicate entry.
        fixture.CacheRange(target, MakeData(40, 't'), true);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Adds", true), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBytes"), 40);

        // Overflow with unprotected fillers: the promoted entry is no longer an unprotected victim.
        fixture.CacheRange(filler1, MakeData(40, 'f'), false);
        fixture.CacheRange(filler2, MakeData(40, 'g'), false);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Evictions", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyEvictions", true), 0);
        fixture.ExpectHit(target, MakeData(40, 't'));
        fixture.ExpectHit(filler2, MakeData(40, 'g'));
        fixture.ExpectMiss(filler1);
    }

    Y_UNIT_TEST(UnprotectedInsertOverStickyIsNoop) {
        TBlobCacheFixture fixture(1024);
        auto range = MakeRange(1, 40);
        fixture.CacheRange(range, MakeData(40, 's'), true);
        fixture.CacheRange(range, MakeData(40, 'u'), false);

        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Adds", true), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBytes"), 40);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBytes"), 40);
        // The original sticky payload is kept.
        fixture.ExpectHit(range, MakeData(40, 's'));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyHits", true), 1);
    }

    Y_UNIT_TEST(CoveringHitPromotesUnprotected) {
        TBlobCacheFixture fixture(100);
        auto first = MakeRange(1, 40);
        auto second = MakeRange(2, 40);
        auto third = MakeRange(3, 40);
        fixture.CacheRange(first, MakeData(40, '1'), false);
        fixture.CacheRange(second, MakeData(40, '2'), false);

        // A covering (sub-range) hit on `first` must refresh it in the unprotected LRU.
        fixture.ExpectHit(MakeRange(1, 40, 5, 10), MakeData(10, '1'));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Hits", true), 1);

        // Overflow: the oldest unprotected entry is now `second`.
        fixture.CacheRange(third, MakeData(40, '3'), false);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Evictions", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 2);
        fixture.ExpectHit(first, MakeData(40, '1'));
        fixture.ExpectHit(third, MakeData(40, '3'));
        fixture.ExpectMiss(second);
    }
}

}   // namespace NKikimr::NBlobCache
