#include "vdisk_histogram_latency.h"
#include "vdisk_histograms.h"
#include "vdisk_context.h"

#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NVDiskMon {

    Y_UNIT_TEST_SUITE(TVDiskLatencyCounters) {

        Y_UNIT_TEST(AsyncCountersRemainVisibleWithoutSeparateRoot) {
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block);
            auto context = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters,
                TVDiskID(0, 1, 0, 0, 0), nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
            UNIT_ASSERT(context->VDiskAsyncCounters == counters);
            for (const auto* handleClass : {"GetAsync", "GetDiscover", "GetLow", "PutAsyncBlob"}) {
                auto group = counters->FindSubgroup("handleclass", handleClass);
                UNIT_ASSERT_C(group, handleClass);
                UNIT_ASSERT(group->FindSubgroup("subsystem", "latency_histo")->FindHistogram("LatencyMs"));
            }
        }

        Y_UNIT_TEST(AsyncClassesUseSeparateCountersAndCoarseBounds) {
            for (auto type : {NPDisk::DEVICE_TYPE_UNKNOWN, NPDisk::DEVICE_TYPE_ROT,
                    NPDisk::DEVICE_TYPE_SSD, NPDisk::DEVICE_TYPE_NVME}) {
                auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
                auto asyncCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
                THistograms histograms(counters, asyncCounters, type);
                const auto asyncBounds = THistograms::GetAsyncLatencyHistBounds();
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
                    snapshot = latencyHistogram->Snapshot();
                    for (size_t i = 0; i < snapshot->Count(); ++i) {
                        UNIT_ASSERT_VALUES_EQUAL(snapshot->Value(i), 1);
                    }
                    UNIT_ASSERT_VALUES_EQUAL(snapshot->UpperBound(bounds.size()), Max<double>());
                    UNIT_ASSERT_VALUES_EQUAL(group->FindCounter("requestBytes")->Val(), 42 * snapshot->Count());
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

    } // Y_UNIT_TEST_SUITE(TVDiskLatencyCounters)

} // namespace NKikimr::NVDiskMon
