#include "blobstorage_cost_tracker.h"
#include "vdisk_context.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TBlobStorageCostTrackerTest) {
    Y_UNIT_TEST(ContextRegistersOnlyAdvancedCost) {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone);
        auto context = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters,
            TVDiskID(0, 1, 0, 0, 0), nullptr, NPDisk::DEVICE_TYPE_ROT);
        context->CostTracker = std::make_unique<TBsCostTracker>(info->Type, NPDisk::DEVICE_TYPE_ROT,
            counters, TCostMetricsParameters{});

        NPDisk::TEvChunkRead read(1, 1, 1, 0, 4096, 0, nullptr);
        const ui64 cost = context->CostTracker->GetCost(read);
        UNIT_ASSERT_GT(cost, 0);
        context->CountDefragCost(read);
        context->CountScrubCost(read);
        context->CountCompactionCost(read);
        context->CostTracker->UpdatePDiskParameters(2, 4);

        UNIT_ASSERT(!counters->FindSubgroup("subsystem", "cost"));
        auto advancedCost = counters->FindSubgroup("subsystem", "advancedCost");
        UNIT_ASSERT(advancedCost);
        auto readCounters = advancedCost->FindSubgroup("operation", "read");
        UNIT_ASSERT(readCounters);
        for (const TString& name : {"DefragDiskCost", "ScrubDiskCost", "CompactionDiskCost"}) {
            UNIT_ASSERT_VALUES_EQUAL(readCounters->FindCounter(name)->Val(), cost);
        }
        UNIT_ASSERT_GT(advancedCost->FindCounter("DiskTimeAvailableCtr")->Val(), 0);
        UNIT_ASSERT_GT(advancedCost->FindCounter("DiskTimeFairShareNs")->Val(), 0);
    }

    Y_UNIT_TEST(DiskCostOperationLabels) {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TBsCostTracker tracker(TBlobStorageGroupType::ErasureNone, NPDisk::DEVICE_TYPE_ROT, counters, {});

        tracker.CountUserCost<TEvBlobStorage::TEvVGet>(11);
        tracker.CountInternalCost<TEvBlobStorage::TEvVPut>(17);

        auto advancedCost = counters->FindSubgroup("subsystem", "advancedCost");
        UNIT_ASSERT(advancedCost);

        auto read = advancedCost->FindSubgroup("operation", "read");
        auto write = advancedCost->FindSubgroup("operation", "write");
        UNIT_ASSERT(read);
        UNIT_ASSERT(write);

        UNIT_ASSERT_VALUES_EQUAL(read->FindCounter("UserDiskCost")->Val(), 11);
        UNIT_ASSERT_VALUES_EQUAL(read->FindCounter("InternalDiskCost")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(write->FindCounter("UserDiskCost")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(write->FindCounter("InternalDiskCost")->Val(), 17);

        for (const TStringBuf name : {
                "UserDiskCost",
                "CompactionDiskCost",
                "ScrubDiskCost",
                "DefragDiskCost",
                "InternalDiskCost",
            })
        {
            UNIT_ASSERT_C(read->FindCounter(TString(name)), name);
            UNIT_ASSERT_C(write->FindCounter(TString(name)), name);
            UNIT_ASSERT_C(!advancedCost->FindCounter(TString(name)), name);
        }

        UNIT_ASSERT(advancedCost->FindCounter("DiskTimeAvailableCtr"));
        UNIT_ASSERT(advancedCost->FindCounter("DiskTimeFairShareNs"));
    }
}

} // NKikimr
