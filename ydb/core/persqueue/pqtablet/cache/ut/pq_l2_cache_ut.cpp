#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/pqtablet/cache/pq_l2_service.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NPQ;

const TPartitionId Partition = TPartitionId(0);

TCacheBlobL2 MakeBlob(ui64 offset, size_t size, const TActorId& owner) {
    TKey key = TKey::ForBody(TKeyPrefix::TypeData, Partition, offset, 0, 1, 1);
    TCacheValue::TPtr value = std::make_shared<TCacheValue>(key, TString(size, 'x'), owner, TAppData::TimeProvider->Now());
    TCacheBlobL2 blob;
    blob.Partition = Partition;
    blob.Offset = offset;
    blob.PartNo = 0;
    blob.Count = 1;
    blob.InternalPartsCount = 1;
    blob.Suffix = key.GetSuffix();
    blob.Value = std::move(value);
    return blob;
}

TCacheBlobL2 MakeKeyBlob(ui64 offset) {
    TCacheBlobL2 blob;
    blob.Partition = Partition;
    blob.Offset = offset;
    blob.PartNo = 0;
    blob.Count = 1;
    blob.InternalPartsCount = 1;
    return blob;
}

struct TTestEnv {
    TTestBasicRuntime Runtime;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TActorId Edge;
    TActorId CacheId;

    TTestEnv()
        : Runtime(1, false)
    {
        TAppPrepare app;
        Runtime.Initialize(app.Unwrap());
        Runtime.DisableBreakOnStopCondition();

        Edge = Runtime.AllocateEdgeActor();

        TCacheL2Parameters params;
        params.MaxSizeMB = 32;
        params.KeepTime = TDuration::Seconds(60);

        CacheId = Runtime.Register(CreateNodePersQueueL2Cache(params, Counters));
        Dispatch();
    }

    void Dispatch() {
        TDispatchOptions options;
        options.CustomFinalCondition = [] { return true; };
        Runtime.DispatchEvents(options);
    }

    void Send(TCacheL2Request&& request) {
        auto data = MakeHolder<TCacheL2Request>(std::move(request));
        Runtime.Send(new IEventHandle(CacheId, Edge, new TEvPqCache::TEvCacheL2Request(data.Release())));
        Dispatch();
    }

    ui64 TotalSize() const {
        return Counters->GetCounter("NodeCacheSizeBytes", false)->Val();
    }

    ui64 TotalCount() const {
        return Counters->GetCounter("NodeCacheSizeBlobs", false)->Val();
    }
};

Y_UNIT_TEST_SUITE(PersQueueL2Cache) {

Y_UNIT_TEST(RenameToExistingKeyKeepsCurrentSize) {
    TTestEnv env;
    const ui64 tabletId = 1;

    TCacheL2Request add(tabletId);
    add.StoredBlobs.push_back(MakeBlob(100, 1000, env.Edge));
    add.StoredBlobs.push_back(MakeBlob(200, 2000, env.Edge));
    env.Send(std::move(add));

    UNIT_ASSERT_VALUES_EQUAL(env.TotalSize(), 1000u + 2000u);
    UNIT_ASSERT_VALUES_EQUAL(env.TotalCount(), 2u);

    TCacheL2Request rename(tabletId);
    rename.RenamedBlobs.emplace_back(MakeKeyBlob(100), MakeKeyBlob(200));
    env.Send(std::move(rename));

    TCacheL2Request add2(tabletId);
    add2.StoredBlobs.push_back(MakeBlob(300, 500, env.Edge));
    env.Send(std::move(add2));

    UNIT_ASSERT_VALUES_EQUAL(env.TotalCount(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(env.TotalSize(), 2000u + 500u);
}

Y_UNIT_TEST(RepeatedRenamesToSameExistingKey) {
    TTestEnv env;
    const ui64 tabletId = 1;

    TCacheL2Request add(tabletId);
    add.StoredBlobs.push_back(MakeBlob(5, 1000, env.Edge));
    add.StoredBlobs.push_back(MakeBlob(10, 100, env.Edge));
    add.StoredBlobs.push_back(MakeBlob(20, 200, env.Edge));
    add.StoredBlobs.push_back(MakeBlob(30, 300, env.Edge));
    env.Send(std::move(add));

    TCacheL2Request rename(tabletId);
    rename.RenamedBlobs.emplace_back(MakeKeyBlob(10), MakeKeyBlob(5));
    rename.RenamedBlobs.emplace_back(MakeKeyBlob(20), MakeKeyBlob(5));
    rename.RenamedBlobs.emplace_back(MakeKeyBlob(30), MakeKeyBlob(5));
    env.Send(std::move(rename));

    TCacheL2Request add2(tabletId);
    add2.StoredBlobs.push_back(MakeBlob(40, 50, env.Edge));
    env.Send(std::move(add2));

    UNIT_ASSERT_VALUES_EQUAL(env.TotalCount(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(env.TotalSize(), 1000u + 50u);
}

Y_UNIT_TEST(RenameToNewKeyKeepsValue) {
    TTestEnv env;
    const ui64 tabletId = 1;

    TCacheL2Request add(tabletId);
    add.StoredBlobs.push_back(MakeBlob(100, 1000, env.Edge));
    env.Send(std::move(add));

    TCacheL2Request rename(tabletId);
    rename.RenamedBlobs.emplace_back(MakeKeyBlob(100), MakeKeyBlob(150));
    env.Send(std::move(rename));

    TCacheL2Request add2(tabletId);
    add2.StoredBlobs.push_back(MakeBlob(300, 500, env.Edge));
    env.Send(std::move(add2));

    UNIT_ASSERT_VALUES_EQUAL(env.TotalCount(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(env.TotalSize(), 1000u + 500u);

    TCacheL2Request remove(tabletId);
    remove.RemovedBlobs.push_back(MakeKeyBlob(150));
    env.Send(std::move(remove));

    UNIT_ASSERT_VALUES_EQUAL(env.TotalCount(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(env.TotalSize(), 500u);
}

} // Y_UNIT_TEST_SUITE(PersQueueL2Cache)

} // namespace
