#include <ydb/core/blobstorage/storagepoolmon/storagepool_counters.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NBlobStorageStoragePoolMonTest {

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

Y_UNIT_TEST(RequestLatencyCountersIncludeCompletedAndInFlight) {
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TRequestMonItem requestMonItem;
    requestMonItem.Init(counters, NPDisk::DEVICE_TYPE_ROT);

    requestMonItem.Register(42, 2, 84, 0.123);
    TRequestMonItem::TInFlightLatencyGuard firstInFlight(
        &requestMonItem, 1, TMonotonic::MilliSeconds(1'000));
    TRequestMonItem::TInFlightLatencyGuard secondInFlight(
        &requestMonItem, 2, TMonotonic::MilliSeconds(1'500));
    requestMonItem.Update(TMonotonic::MilliSeconds(2'500));

    auto completedSum = counters->FindCounter("responseTimeUsCompletedSum");
    auto completedCount = counters->FindCounter("responseTimeCompletedCount");
    auto inFlightSum = counters->FindCounter("inFlightResponseTimeUsSum");
    auto inFlightCount = counters->FindCounter("inFlightCount");
    auto maxLatency = counters->FindCounter("responseTimeMsMax");
    auto inFlightMaxLatency = counters->FindCounter("inFlightResponseTimeMsMax");

    UNIT_ASSERT(completedSum);
    UNIT_ASSERT(completedCount);
    UNIT_ASSERT(inFlightSum);
    UNIT_ASSERT(inFlightCount);
    UNIT_ASSERT(maxLatency);
    UNIT_ASSERT(inFlightMaxLatency);

    UNIT_ASSERT(completedSum->ForDerivative());
    UNIT_ASSERT(completedCount->ForDerivative());
    UNIT_ASSERT(!inFlightSum->ForDerivative());
    UNIT_ASSERT(!inFlightCount->ForDerivative());
    UNIT_ASSERT(!maxLatency->ForDerivative());
    UNIT_ASSERT(!inFlightMaxLatency->ForDerivative());

    UNIT_ASSERT_VALUES_EQUAL(completedSum->Val(), 123'000);
    UNIT_ASSERT_VALUES_EQUAL(completedCount->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(inFlightSum->Val(), 2'500'000);
    UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 2);
    UNIT_ASSERT_VALUES_EQUAL(maxLatency->Val(), 123);
    UNIT_ASSERT_VALUES_EQUAL(inFlightMaxLatency->Val(), 1'500);

    firstInFlight.Reset();
    requestMonItem.Update(TMonotonic::MilliSeconds(3'000));

    UNIT_ASSERT_VALUES_EQUAL(completedSum->Val(), 123'000);
    UNIT_ASSERT_VALUES_EQUAL(completedCount->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(inFlightSum->Val(), 1'500'000);
    UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(maxLatency->Val(), 123);
    UNIT_ASSERT_VALUES_EQUAL(inFlightMaxLatency->Val(), 1'500);
}

Y_UNIT_TEST(RequestLatencyCountersAggregateManyInFlightRequests) {
    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TRequestMonItem requestMonItem;
    requestMonItem.Init(counters, NPDisk::DEVICE_TYPE_ROT);

    constexpr ui64 requestCount = 1024;
    ui64 startTimeUsSum = 0;
    TVector<TRequestMonItem::TInFlightLatencyGuard> inFlight;
    inFlight.reserve(requestCount);
    for (ui64 requestId = 1; requestId <= requestCount; ++requestId) {
        const ui64 startTimeUs = requestId;
        startTimeUsSum += startTimeUs;
        inFlight.emplace_back(&requestMonItem, requestId, TMonotonic::MicroSeconds(startTimeUs));
    }

    auto inFlightSum = counters->FindCounter("inFlightResponseTimeUsSum");
    auto inFlightCount = counters->FindCounter("inFlightCount");
    auto inFlightMaxLatency = counters->FindCounter("inFlightResponseTimeMsMax");

    UNIT_ASSERT(inFlightSum);
    UNIT_ASSERT(inFlightCount);
    UNIT_ASSERT(inFlightMaxLatency);

    const ui64 firstUpdateUs = 10'000;
    requestMonItem.Update(TMonotonic::MicroSeconds(firstUpdateUs));

    UNIT_ASSERT_VALUES_EQUAL(inFlightSum->Val(), requestCount * firstUpdateUs - startTimeUsSum);
    UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), requestCount);
    UNIT_ASSERT_VALUES_EQUAL(inFlightMaxLatency->Val(), 9);

    for (ui64 requestId = 1; requestId <= requestCount; requestId += 2) {
        inFlight[requestId - 1].Reset();
        startTimeUsSum -= requestId;
    }

    const ui64 secondUpdateUs = 20'000;
    requestMonItem.Update(TMonotonic::MicroSeconds(secondUpdateUs));

    UNIT_ASSERT_VALUES_EQUAL(inFlightSum->Val(), requestCount / 2 * secondUpdateUs - startTimeUsSum);
    UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), requestCount / 2);
    UNIT_ASSERT_VALUES_EQUAL(inFlightMaxLatency->Val(), 19);
}

} // Y_UNIT_TEST_SUITE TBlobStorageStoragePoolMonTest
} // namespace NBlobStorageStoragePoolMonTest
} // namespace NKikimr
