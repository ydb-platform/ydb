#include <ydb/core/persqueue/pqtablet/cache/pq_l2_cache.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NPQ;

namespace {

constexpr ui64 TabletId = 42;
const TPartitionId Partition(1);

struct TCacheProbe {
    TTestBasicRuntime Runtime{1, false};
    TActorId Edge;
    TActorId Cache;
    NMonitoring::TDynamicCounters::TCounterPtr SizeBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SizeBlobs;

    TCacheProbe() {
        TAppPrepare app;
        app.FeatureFlags.SetEnableTabletRestartOnUnhandledExceptions(true);
        Runtime.Initialize(app.Unwrap());
        Edge = Runtime.AllocateEdgeActor();

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        SizeBytes = counters->GetCounter("NodeCacheSizeBytes", false);
        SizeBlobs = counters->GetCounter("NodeCacheSizeBlobs", false);

        TCacheL2Parameters params;
        params.MaxSizeMB = 32;
        params.KeepTime = TDuration::Seconds(10);
        Cache = Runtime.Register(CreateNodePersQueueL2Cache(params, counters));
        Runtime.EnableScheduleForActor(Cache);
    }

    TCacheBlobL2 Blob(ui64 offset, ui64 size) const {
        TKey key = TKey::ForBody(TKeyPrefix::TypeData, Partition, offset, 0, 1, 0);
        TString data(size, 'x');
        auto value = std::make_shared<TCacheValue>(key, data, Edge, TInstant::Seconds(1));
        return {Partition, offset, 0, 1, 0, Nothing(), std::move(value)};
    }

    void Send(THolder<TCacheL2Request> request) {
        Runtime.Send(new IEventHandle(Cache, Edge, new TEvPqCache::TEvCacheL2Request(request.Release())), 0, true);
    }

    void Add(TCacheBlobL2 blob) {
        auto request = MakeHolder<TCacheL2Request>(TabletId);
        request->StoredBlobs.push_back(std::move(blob));
        Send(std::move(request));
    }

    void Rename(TCacheBlobL2 from, TCacheBlobL2 to) {
        auto request = MakeHolder<TCacheL2Request>(TabletId);
        request->RenamedBlobs.emplace_back(std::move(from), std::move(to));
        Send(std::move(request));
    }

    void Sync() {
        Runtime.Send(new IEventHandle(Cache, Edge, new TEvPqCache::TEvCacheKeysRequest()), 0, true);
        auto response = Runtime.GrabEdgeEvent<TEvPqCache::TEvCacheKeysResponse>(Edge, TDuration::Seconds(5));
        UNIT_ASSERT_C(response, "L2 cache actor did not answer");
    }
};

} // namespace

Y_UNIT_TEST_SUITE(PQCacheL2) {

Y_UNIT_TEST(RenameOntoExistingKeyKeepsByteCount) {
    // Keeping the source bytes in CurrentSize and the 1-byte destination made
    // the next insert abort on CurrentSize <= Count * MAX_BLOB_SIZE.
    // The cache must keep the renamed source and drop the destination size.
    TCacheProbe probe;
    probe.Add(probe.Blob(1, MAX_BLOB_SIZE));
    probe.Add(probe.Blob(2, 1));
    probe.Sync();
    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBlobs->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBytes->Val(), MAX_BLOB_SIZE + 1);

    probe.Rename(probe.Blob(1, 1), probe.Blob(2, 1));
    probe.Sync();
    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBlobs->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBytes->Val(), MAX_BLOB_SIZE);

    probe.Add(probe.Blob(3, 1));
    probe.Sync();
    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBlobs->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBytes->Val(), MAX_BLOB_SIZE + 1);
}

Y_UNIT_TEST(RenameOntoLargerKeyKeepsSourceBytes) {
    TCacheProbe probe;
    probe.Add(probe.Blob(1, 10));
    probe.Add(probe.Blob(2, 30));
    probe.Rename(probe.Blob(1, 10), probe.Blob(2, 30));
    probe.Sync();

    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBlobs->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBytes->Val(), 10);
}

Y_UNIT_TEST(DuplicateInsertDoesNotGrowSize) {
    TCacheProbe probe;
    probe.Add(probe.Blob(1, 100));
    probe.Add(probe.Blob(1, 100));
    probe.Sync();

    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBlobs->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBytes->Val(), 100);
}

Y_UNIT_TEST(ReinsertReplacesByteCount) {
    TCacheProbe probe;
    probe.Add(probe.Blob(1, 10));
    probe.Add(probe.Blob(2, 7));
    probe.Add(probe.Blob(1, 30));
    probe.Sync();

    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBlobs->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBytes->Val(), 37);

    probe.Add(probe.Blob(1, 4));
    probe.Sync();

    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBlobs->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBytes->Val(), 11);
}

Y_UNIT_TEST(RenameToAbsentKeyPreservesSize) {
    TCacheProbe probe;
    probe.Add(probe.Blob(1, 10));
    probe.Rename(probe.Blob(1, 10), probe.Blob(2, 10));
    probe.Add(probe.Blob(3, 7));
    probe.Sync();

    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBlobs->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(probe.SizeBytes->Val(), 17);
}

} // Y_UNIT_TEST_SUITE(PQCacheL2)
