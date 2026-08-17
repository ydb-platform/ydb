#include "blobstorage_cost_tracker.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TBlobStorageCostTrackerTest) {
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
