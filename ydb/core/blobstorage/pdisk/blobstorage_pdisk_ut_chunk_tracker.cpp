#include "blobstorage_pdisk_chunk_tracker.h"
#include "blobstorage_pdisk_color_limits.h"

#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_actions.h"
#include "blobstorage_pdisk_ut_helpers.h"
#include "blobstorage_pdisk_ut_run.h"

#include <ydb/core/testlib/actors/test_runtime.h>

namespace NKikimr {

#define UNIT_ASSERT_EQUAL_X(A, B) do {\
    auto value = (A); \
    UNIT_ASSERT_EQUAL_C(A, B, value); \
} while (false)


Y_UNIT_TEST_SUITE(TChunkTrackerTest) {

    Y_UNIT_TEST(AddRemove) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 265,
            .ExpectedOwnerCount = 2,
        };

        TString errorReason;
        bool ok;

        ok = chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason);
        UNIT_ASSERT_C(ok, errorReason);

        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalUsed(), 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(OwnerSystem), 200);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(OwnerSystemReserve), 5);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 60);

        chunkTracker.AddOwner(101, TVDiskID());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);

        chunkTracker.AddOwner(102, TVDiskID());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 30);

        chunkTracker.AddOwner(103, TVDiskID());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 20);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 20);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(103), 20);

        chunkTracker.RemoveOwner(101);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 30);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(103), 30);

        chunkTracker.RemoveOwner(102);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 0);

        chunkTracker.RemoveOwner(103);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(103), 0);
    }

    Y_UNIT_TEST(TwoOwnersInterference) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 305,
            .ExpectedOwnerCount = 0,
            .SpaceColorBorder = TColor::YELLOW
        };

        TString errorReason;
        bool ok;
        double occupancy;

        ok = chunkTracker.Reset(params, TColorLimits::MakeChunkLimits(params.ChunkBaseLimit), errorReason);
        UNIT_ASSERT_C(ok, errorReason);

        TOwner owner1 = NPDisk::EOwner::OwnerBeginUser + 1;
        chunkTracker.AddOwner(owner1, TVDiskID());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 100);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(owner1), 100);

        auto light_yellow = chunkTracker.ColorFlagLimit(owner1, TColor::LIGHT_YELLOW);
        UNIT_ASSERT_EQUAL_X(light_yellow, 83);

        UNIT_ASSERT_C(chunkTracker.TryAllocate(owner1, light_yellow-1, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(owner1), light_yellow-1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetSpaceColor(owner1, &occupancy), TColor::CYAN);
        UNIT_ASSERT_EQUAL_X(chunkTracker.EstimateSpaceColor(owner1, 1, &occupancy), TColor::LIGHT_YELLOW);

        TOwner owner2 = NPDisk::EOwner::OwnerBeginUser + 2;
        chunkTracker.AddOwner(owner2, TVDiskID());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(owner1), 50);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(owner2), 50);

        UNIT_ASSERT_EQUAL_X(chunkTracker.GetSpaceColor(owner1, &occupancy), TColor::YELLOW);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetSpaceColor(owner2, &occupancy), TColor::CYAN);

        UNIT_ASSERT_C(chunkTracker.TryAllocate(owner2, 1, errorReason), errorReason);

        UNIT_ASSERT_EQUAL_X(chunkTracker.GetSpaceColor(owner1, &occupancy), TColor::YELLOW);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetSpaceColor(owner2, &occupancy), TColor::LIGHT_YELLOW);
    }

    Y_UNIT_TEST(AddOwnerWithWeight) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 80,
            .ExpectedOwnerCount = 4,
        };

        TString errorReason;
        bool ok;

        ok = chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason);
        UNIT_ASSERT_C(ok, errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 80);

        chunkTracker.AddOwner(101, TVDiskID(), 1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 20);

        chunkTracker.AddOwner(102, TVDiskID(), 2);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 20);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 40);

        chunkTracker.AddOwner(103, TVDiskID(), 5);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 10);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 20);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(103), 50);
    }

    Y_UNIT_TEST(ZeroWeight) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 50,
            .ExpectedOwnerCount = 0,
        };

        TString errorReason;
        bool ok;

        ok = chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason);
        UNIT_ASSERT_C(ok, errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 50);

        chunkTracker.AddOwner(101, TVDiskID(), 1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetNumActiveSlots(), 1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 50);

        // Weigh can't be zero (0 is treated as 1)
        chunkTracker.SetOwnerWeight(101, 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetNumActiveSlots(), 1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 50);

        chunkTracker.AddOwner(102, TVDiskID(), 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetNumActiveSlots(), 2);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 25);
    }
}

#undef UNIT_ASSERT_EQUAL_X
} // namespace NKikimr
