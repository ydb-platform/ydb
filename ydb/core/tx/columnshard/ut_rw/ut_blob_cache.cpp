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

        TBlobCacheSettings settings;
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
        fixture.CacheRange(full, MakeData(80, 'f'), true);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 1);

        fixture.Forget(full.BlobId);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("SizeBlobs"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Forgets", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("StickyBlobs"), 0);
    }

    Y_UNIT_TEST(GraduateExpiredBeforeStillSticky) {
        TBlobCacheFixture fixture(100, 1000);
        auto expired = MakeRange(1, 40);
        auto stillSticky = MakeRange(2, 40);
        fixture.CacheRange(expired, MakeData(40, 'e'), true);

        fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(2));
        fixture.Wakeup();

        fixture.CacheRange(stillSticky, MakeData(40, 's'), true);
        auto extra = MakeRange(3, 40);
        fixture.CacheRange(extra, MakeData(40, 'u'), false);

        auto stillHit = fixture.ReadRange(stillSticky);
        UNIT_ASSERT(stillHit);
        UNIT_ASSERT(stillHit->Get()->FromCache);
        UNIT_ASSERT_VALUES_EQUAL(stillHit->Get()->Data, MakeData(40, 's'));
        UNIT_ASSERT(fixture.Counter("Evictions", true) >= 1);
    }
}

}   // namespace NKikimr::NBlobCache
