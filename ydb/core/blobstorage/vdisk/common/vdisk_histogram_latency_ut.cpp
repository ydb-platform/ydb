#include "vdisk_histogram_latency.h"
#include "vdisk_histograms.h"

#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NVDiskMon {

    Y_UNIT_TEST_SUITE(TVDiskLatencyCounters) {

        Y_UNIT_TEST(AsyncClassesUseSeparateCountersAndCoarseBounds) {
            for (auto type : {NPDisk::DEVICE_TYPE_UNKNOWN, NPDisk::DEVICE_TYPE_ROT,
                    NPDisk::DEVICE_TYPE_SSD, NPDisk::DEVICE_TYPE_NVME}) {
                auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
                auto asyncCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
                THistograms histograms(counters, asyncCounters, type);
                const NMonitoring::TBucketBounds asyncBounds = {1, 8, 32, 128, 1'024, 65'536};
                const auto foregroundBounds = GetCommonLatencyHistBounds(type);

                auto check = [&](const TLtcHistoPtr& histogram, const TString& handleClass, bool async) {
                    const auto& expected = async ? asyncCounters : counters;
                    const auto& other = async ? counters : asyncCounters;
                    auto group = expected->FindSubgroup("handleclass", handleClass);
                    UNIT_ASSERT_C(group, handleClass);
                    UNIT_ASSERT(!other->FindSubgroup("handleclass", handleClass));
                    auto latency = group->FindSubgroup("subsystem", "latency_histo");
                    UNIT_ASSERT(latency);
                    auto latencyHistogram = latency->FindHistogram("LatencyMs");
                    UNIT_ASSERT(latencyHistogram);
                    auto snapshot = latencyHistogram->Snapshot();
                    const auto& bounds = async ? asyncBounds : foregroundBounds;
                    UNIT_ASSERT_VALUES_EQUAL(snapshot->Count(), bounds.size() + 1);
                    for (size_t i = 0; i < bounds.size(); ++i) {
                        UNIT_ASSERT_VALUES_EQUAL(snapshot->UpperBound(i), bounds[i]);
                        histogram->Collect(TDuration::MicroSeconds(bounds[i] * 1'000), 42);
                    }
                    histogram->Collect(TDuration::Seconds(120), 42);
                    histogram->AddInFlightRequest(1, TInstant::Seconds(1));
                    // Publishing gauges must not reset accumulated histogram buckets or counters.
                    for (ui32 i = 1; i <= 120; ++i) {
                        histograms.UpdateCounters(TInstant::Seconds(i + 1));
                    }
                    snapshot = latencyHistogram->Snapshot();
                    for (size_t i = 0; i < snapshot->Count(); ++i) {
                        UNIT_ASSERT_VALUES_EQUAL(snapshot->Value(i), 1);
                    }
                    UNIT_ASSERT_VALUES_EQUAL(snapshot->UpperBound(bounds.size()), Max<double>());
                    UNIT_ASSERT_VALUES_EQUAL(group->FindCounter("requestBytes")->Val(), 42 * snapshot->Count());
                    UNIT_ASSERT_VALUES_EQUAL(latency->FindCounter("LatencyCompletedCount")->Val(), snapshot->Count());
                    ui64 completedSumUs = 120'000'000;
                    for (double bound : bounds) {
                        completedSumUs += TDuration::MicroSeconds(bound * 1'000).MicroSeconds();
                    }
                    UNIT_ASSERT_VALUES_EQUAL(latency->FindCounter("LatencyUsCompletedSum")->Val(), completedSumUs);
                    UNIT_ASSERT_VALUES_EQUAL(latency->FindCounter("InFlightCount")->Val(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(latency->FindCounter("InFlightLatencyUsSum")->Val(), 120'000'000);
                    UNIT_ASSERT_VALUES_EQUAL(latency->FindCounter("LatencyUsMax")->Val(), 120'000'000);
                    histogram->RemoveInFlightRequest(1);
                };

                check(histograms.GetHistogram(NKikimrBlobStorage::AsyncRead), "GetAsync", true);
                check(histograms.GetHistogram(NKikimrBlobStorage::Discover), "GetDiscover", true);
                check(histograms.GetHistogram(NKikimrBlobStorage::LowRead), "GetLow", true);
                check(histograms.GetHistogram(NKikimrBlobStorage::AsyncBlob), "PutAsyncBlob", true);
                check(histograms.GetHistogram(NKikimrBlobStorage::FastRead), "GetFast", false);
                check(histograms.GetHistogram(NKikimrBlobStorage::TabletLog), "PutTabletLog", false);
                check(histograms.GetHistogram(NKikimrBlobStorage::UserData), "PutUserData", false);
            }
        }

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
                const ui64 requestId = 42;
                TInFlightLatencyGuard guard(histo, requestId, TInstant::MilliSeconds(1'000));

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
                    const ui64 requestId = 42;
                    TInFlightLatencyGuard guard(histo, requestId, TInstant::MilliSeconds(1'000));
                    movedGuard = std::move(guard);
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
