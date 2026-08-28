#include "vdisk_histogram_latency.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NVDiskMon {

    Y_UNIT_TEST_SUITE(TVDiskLatencyCounters) {

        Y_UNIT_TEST(CompletedAndInFlightLatencyCountersAreReportedSeparately) {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            TLtcHisto histo(counters, "handleclass", "GetFast", NPDisk::DEVICE_TYPE_ROT);

            histo.Collect(TDuration::MicroSeconds(123'456), 42);
            histo.AddInFlightRequest(1, TInstant::MilliSeconds(1'000));
            histo.AddInFlightRequest(2, TInstant::MilliSeconds(1'500));
            histo.UpdateCounters(TInstant::MilliSeconds(2'500));

            auto handleClassGroup = counters->FindSubgroup("handleclass", "GetFast");
            UNIT_ASSERT(handleClassGroup);
            auto latencyGroup = handleClassGroup->FindSubgroup("subsystem", "latency_histo");
            UNIT_ASSERT(latencyGroup);

            auto completedSum = latencyGroup->FindCounter("LatencyUsCompletedSum");
            auto completedCount = latencyGroup->FindCounter("LatencyCompletedCount");
            auto inFlightSum = latencyGroup->FindCounter("InFlightLatencyUsSum");
            auto inFlightCount = latencyGroup->FindCounter("InFlightCount");
            auto maxLatency = latencyGroup->FindCounter("LatencyUsMax");

            UNIT_ASSERT(completedSum->ForDerivative());
            UNIT_ASSERT(completedCount->ForDerivative());
            UNIT_ASSERT(!inFlightSum->ForDerivative());
            UNIT_ASSERT(!inFlightCount->ForDerivative());
            UNIT_ASSERT(!maxLatency->ForDerivative());

            UNIT_ASSERT_VALUES_EQUAL(completedSum->Val(), 123'456);
            UNIT_ASSERT_VALUES_EQUAL(completedCount->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(inFlightSum->Val(), 2'500'000);
            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 2);
            UNIT_ASSERT_VALUES_EQUAL(maxLatency->Val(), 1'500'000);

            histo.RemoveInFlightRequest(1);
            histo.UpdateCounters(TInstant::MilliSeconds(3'000));

            UNIT_ASSERT_VALUES_EQUAL(completedSum->Val(), 123'456);
            UNIT_ASSERT_VALUES_EQUAL(completedCount->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(inFlightSum->Val(), 1'500'000);
            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 1);
        }

        Y_UNIT_TEST(InFlightLatencyGuardRemovesRequestOnDestruction) {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto histo = std::make_shared<TLtcHisto>(counters, "handleclass", "GetFast", NPDisk::DEVICE_TYPE_ROT);

            auto handleClassGroup = counters->FindSubgroup("handleclass", "GetFast");
            UNIT_ASSERT(handleClassGroup);
            auto latencyGroup = handleClassGroup->FindSubgroup("subsystem", "latency_histo");
            UNIT_ASSERT(latencyGroup);

            auto inFlightSum = latencyGroup->FindCounter("InFlightLatencyUsSum");
            auto inFlightCount = latencyGroup->FindCounter("InFlightCount");
            UNIT_ASSERT(inFlightSum);
            UNIT_ASSERT(inFlightCount);

            {
                TInFlightLatencyGuard guard(histo, 42, TInstant::MilliSeconds(1'000));
                UNIT_ASSERT_VALUES_EQUAL(guard.GetRequestId(), 42);

                histo->UpdateCounters(TInstant::MilliSeconds(2'500));

                UNIT_ASSERT_VALUES_EQUAL(inFlightSum->Val(), 1'500'000);
                UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 1);
            }

            histo->UpdateCounters(TInstant::MilliSeconds(3'000));
            UNIT_ASSERT_VALUES_EQUAL(inFlightSum->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 0);
        }

        Y_UNIT_TEST(InFlightLatencyGuardMoveTransfersOwnership) {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto histo = std::make_shared<TLtcHisto>(counters, "handleclass", "GetFast", NPDisk::DEVICE_TYPE_ROT);

            auto handleClassGroup = counters->FindSubgroup("handleclass", "GetFast");
            UNIT_ASSERT(handleClassGroup);
            auto latencyGroup = handleClassGroup->FindSubgroup("subsystem", "latency_histo");
            UNIT_ASSERT(latencyGroup);

            auto inFlightSum = latencyGroup->FindCounter("InFlightLatencyUsSum");
            auto inFlightCount = latencyGroup->FindCounter("InFlightCount");
            UNIT_ASSERT(inFlightSum);
            UNIT_ASSERT(inFlightCount);

            {
                TInFlightLatencyGuard movedGuard;
                {
                    TInFlightLatencyGuard guard(histo, 42, TInstant::MilliSeconds(1'000));
                    movedGuard = std::move(guard);
                    UNIT_ASSERT_VALUES_EQUAL(movedGuard.GetRequestId(), 42);
                }

                histo->UpdateCounters(TInstant::MilliSeconds(2'500));

                UNIT_ASSERT_VALUES_EQUAL(inFlightSum->Val(), 1'500'000);
                UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 1);
            }

            histo->UpdateCounters(TInstant::MilliSeconds(3'000));
            UNIT_ASSERT_VALUES_EQUAL(inFlightSum->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 0);
        }

    } // Y_UNIT_TEST_SUITE(TVDiskLatencyCounters)

} // namespace NKikimr::NVDiskMon
