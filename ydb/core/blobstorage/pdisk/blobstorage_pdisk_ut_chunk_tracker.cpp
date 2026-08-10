#include "blobstorage_pdisk_chunk_tracker.h"
#include "blobstorage_pdisk_color_limits.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

#define UNIT_ASSERT_EQUAL_X(A, B) do {\
    auto value = (A); \
    UNIT_ASSERT_EQUAL_C(A, B, value); \
} while (false)


Y_UNIT_TEST_SUITE(TChunkTrackerTest) {

    Y_UNIT_TEST(SharedQuotaFailureReleasesOwnerQuota) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 10,
            .ExpectedOwnerCount = 1,
        };

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 10);
        chunkTracker.AddOwner(101, TVDiskID());

        // The owner quota is force-allocated before the shared quota check.
        // When the shared quota rejects the request, the owner quota accounting
        // must be rolled back instead of keeping the never-used allocation.
        UNIT_ASSERT(!chunkTracker.TryAllocate(101, 11, errorReason));
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(101), 0);

        // the rolled-back quota must be allocatable again
        UNIT_ASSERT_C(chunkTracker.TryAllocate(101, 5, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(101), 5);
    }

    Y_UNIT_TEST(ExpectedOwnerSizeOverridesCountBasedQuota) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
            .ExpectedOwnerSize = 30,
        };

        TString errorReason;
        bool ok;

        ok = chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason);
        UNIT_ASSERT_C(ok, errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 100);

        chunkTracker.AddOwner(101, TVDiskID());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);

        chunkTracker.AddOwner(102, TVDiskID());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 30);

        // clearing ExpectedOwnerSize reverts to the count-based equal split
        chunkTracker.SetExpectedOwnerSize(0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 25);
    }

    Y_UNIT_TEST(ExpectedOwnerSizeUsesColorBorderForEnforcement) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
            .ExpectedOwnerSize = 30,
        };

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);
        chunkTracker.AddOwner(101, TVDiskID());

        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);

        // Allocating 28 chunks puts the owner into BLACK according to its
        // personal quota. The default GREEN border hides the personal color,
        // so the allocation must remain possible.
        double occupancy;
        UNIT_ASSERT_EQUAL_X(
            chunkTracker.EstimateSpaceColor(101, 28, &occupancy),
            TColor::GREEN);
        UNIT_ASSERT_C(chunkTracker.TryAllocate(101, 28, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(101), 28);

        // TPDisk checks this color before allocating. Raising the border to
        // BLACK therefore makes the personal quota hard.
        chunkTracker.SetColorBorder(TColor::BLACK);
        UNIT_ASSERT_EQUAL_X(
            chunkTracker.EstimateSpaceColor(101, 1, &occupancy),
            TColor::BLACK);
    }

    Y_UNIT_TEST(ExpectedOwnerSizeRuntimeUpdateKeepsOwnerQuotaSoft) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
        };

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);
        chunkTracker.AddOwner(101, TVDiskID());
        chunkTracker.SetExpectedOwnerSettings(4, 30);

        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);
        UNIT_ASSERT_C(chunkTracker.TryAllocate(101, 28, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(101), 28);
    }
}

} // namespace NKikimr
