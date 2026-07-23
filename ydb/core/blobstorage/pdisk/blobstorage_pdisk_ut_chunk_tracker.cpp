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

        // in the count-based mode the owner quota is force-allocated before the shared quota
        // check; when the shared quota rejects the request, the owner quota accounting must
        // be rolled back instead of keeping the never-used allocation
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

    Y_UNIT_TEST(ExpectedOwnerSizeLimitsOwnerAllocation) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
            .ExpectedOwnerSize = 30,
        };

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);
        chunkTracker.AddOwner(101, TVDiskID());

        UNIT_ASSERT_C(chunkTracker.TryAllocate(101, 27, errorReason), errorReason);
        UNIT_ASSERT(!chunkTracker.TryAllocate(101, 1, errorReason));
        UNIT_ASSERT_C(errorReason.Contains("Owner# 101"), errorReason);
    }

    Y_UNIT_TEST(ExpectedOwnerSizeRuntimeUpdateLimitsOwnerAllocation) {
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

        UNIT_ASSERT_C(chunkTracker.TryAllocate(101, 27, errorReason), errorReason);
        UNIT_ASSERT(!chunkTracker.TryAllocate(101, 1, errorReason));
        UNIT_ASSERT_C(errorReason.Contains("Owner# 101"), errorReason);
    }
}

} // namespace NKikimr
