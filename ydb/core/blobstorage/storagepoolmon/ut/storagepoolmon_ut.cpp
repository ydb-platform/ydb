#include <ydb/core/blobstorage/storagepoolmon/storagepool_counters.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NBlobStorageStoragePoolMonTest {

namespace {

bool ScheduledFilterFunc(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event,
        TDuration delay, TInstant& deadline) {
    if (runtime.IsScheduleForActorEnabled(event->GetRecipientRewrite())) {
        deadline = runtime.GetTimeProvider()->Now() + delay;
        return false;
    }
    return true;
}

void SimulateSleep(TTestBasicRuntime& runtime, TDuration duration) {
    runtime.AdvanceCurrentTime(duration);
    runtime.SimulateSleep(TDuration::MilliSeconds(1));
}

} // namespace

Y_UNIT_TEST_SUITE(TBlobStorageStoragePoolMonTest) {

Y_UNIT_TEST(SizeClassCalcTest) {
    const size_t COUNT = 12;
    ui32 expected[COUNT] = {0, 0, 0,  0,   0,   1,   1,    2,     3,       4,         5,         5};
    ui32 input[COUNT] =    {0, 5, 15, 255, 256, 257, 1562, 15969, 300'000, 2'000'000, 5'000'000, 20'000'000};
    for (ui32 i = 0; i < COUNT; ++i) {
        ui32 sizeClass = TStoragePoolCounters::SizeClassFromSizeBytes(input[i]);
        UNIT_ASSERT_C(sizeClass == expected[i],
                "input# " << input[i]
                << " expected# " << expected[i]
                << " sizeClass# " << sizeClass);
    }
}

Y_UNIT_TEST(ReducedSizeClassCalcTest) {
    const size_t COUNT = 7;
    ui32 expected[COUNT] = {0,    0,        1,       1,         2,         2,            2};
    ui32 input[COUNT] =    {1000, 256*1024, 500'000, 1024*1024, 4'000'000, 16*1024*1024, 20'000'000};
    for (ui32 i = 0; i < COUNT; ++i) {
        ui32 sizeClass = TStoragePoolCounters::ReducedSizeClassFromSizeBytes(input[i]);
        UNIT_ASSERT_C(sizeClass == expected[i],
                "input# " << input[i]
                << " expected# " << expected[i]
                << " sizeClass# " << sizeClass);
    }
}

Y_UNIT_TEST(DsProxyInFlightLatencyAggregatorPublishesFullSnapshots) {
    TTestBasicRuntime runtime(1, false);
    TAppPrepare app;
    app.ClearDomainsAndHive();
    runtime.SetScheduledEventFilter(&ScheduledFilterFunc);
    runtime.Initialize(app.Unwrap());

    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    TIntrusivePtr<TStoragePoolCounters> poolCounters =
        MakeIntrusive<TStoragePoolCounters>(counters, "pool_name", NPDisk::DEVICE_TYPE_SSD);

    const auto handleClass = TStoragePoolCounters::EHandleClass::HcPutUserData;
    const ui32 sizeClassIdx = TStoragePoolCounters::SizeClassIndex(handleClass, 1024);
    TRequestMonItem& requestMonItem = poolCounters->GetItemBySizeClass(handleClass, sizeClassIdx);

    TActorId aggregator = runtime.Register(CreateDsProxyInFlightLatencyAggregator());
    runtime.EnableScheduleForActor(aggregator, true);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
    TActorId source = runtime.AllocateEdgeActor(0);

    TVector<TDsProxyInFlightLatencyBucket> buckets{
        TDsProxyInFlightLatencyBucket{
            .Key = TDsProxyInFlightLatencyBucketKey{
                .PoolCounters = poolCounters,
                .HandleClass = static_cast<ui32>(handleClass),
                .SizeClassIdx = sizeClassIdx,
            },
            .Stats = TDsProxyInFlightLatencyStats{
                .InFlightLatencyUsSum = 3000,
                .InFlightCount = 2,
                .InFlightLatencyUsMax = 2000,
            },
        },
    };

    runtime.Send(new IEventHandle(
        aggregator, source, new TEvDsProxyInFlightLatencySnapshot(std::move(buckets))));

    SimulateSleep(runtime, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.InFlightResponseTimeUsSum->Val(), 3000);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.InFlightCount->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.InFlightResponseTimeUsMax->Val(), 2000);

    runtime.Send(new IEventHandle(aggregator, source, new TEvDsProxyInFlightLatencySnapshot()));

    SimulateSleep(runtime, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.InFlightResponseTimeUsSum->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.InFlightCount->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(requestMonItem.InFlightResponseTimeUsMax->Val(), 0);
}

} // Y_UNIT_TEST_SUITE TBlobStorageStoragePoolMonTest
} // namespace NBlobStorageStoragePoolMonTest
} // namespace NKikimr
