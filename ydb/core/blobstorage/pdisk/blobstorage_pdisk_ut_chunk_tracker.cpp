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

    static TVDiskID MakeVDiskId(EGroupConfigurationType type, ui32 groupLocalId) {
        return TVDiskID(TGroupID(type, 1, groupLocalId).GetRaw(), 1, TVDiskIdShort(0, 0, 0));
    }

    static TVDiskID DynamicVDiskId(ui32 groupLocalId = 1) {
        return MakeVDiskId(EGroupConfigurationType::Dynamic, groupLocalId);
    }

    static TVDiskID StaticVDiskId(ui32 groupLocalId = 1) {
        return MakeVDiskId(EGroupConfigurationType::Static, groupLocalId);
    }

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

        chunkTracker.AddOwner(101, DynamicVDiskId());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);

        chunkTracker.AddOwner(102, DynamicVDiskId());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 30);

        chunkTracker.AddOwner(103, DynamicVDiskId());
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
        chunkTracker.AddOwner(owner1, DynamicVDiskId());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 100);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(owner1), 100);

        auto light_yellow = chunkTracker.ColorFlagLimit(owner1, TColor::LIGHT_YELLOW);
        UNIT_ASSERT_EQUAL_X(light_yellow, 83);

        UNIT_ASSERT_C(chunkTracker.TryAllocate(owner1, light_yellow-1, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(owner1), light_yellow-1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetSpaceColor(owner1, &occupancy), TColor::CYAN);
        UNIT_ASSERT_EQUAL_X(chunkTracker.EstimateSpaceColor(owner1, 1, &occupancy), TColor::LIGHT_YELLOW);

        TOwner owner2 = NPDisk::EOwner::OwnerBeginUser + 2;
        chunkTracker.AddOwner(owner2, DynamicVDiskId());
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

        chunkTracker.AddOwner(101, DynamicVDiskId(), 1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 20);

        chunkTracker.AddOwner(102, DynamicVDiskId(), 2);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 20);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 40);

        chunkTracker.AddOwner(103, DynamicVDiskId(), 5);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 10);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 20);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(103), 50);
    }

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
        chunkTracker.AddOwner(101, DynamicVDiskId(), 1);

        // The owner quota is force-allocated before the shared quota check.
        // When the shared quota rejects the request, the owner quota accounting
        // must be rolled back instead of keeping the never-used allocation.
        UNIT_ASSERT(!chunkTracker.TryAllocate(101, 11, errorReason));
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(101), 0);

        // the rolled-back quota must be allocatable again
        UNIT_ASSERT_C(chunkTracker.TryAllocate(101, 5, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(101), 5);
    }

    Y_UNIT_TEST(ExpectedOwnerSizeForcesOwnerWeightToOne) {
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

        chunkTracker.AddOwner(101, DynamicVDiskId(), 1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);

        chunkTracker.AddOwner(102, DynamicVDiskId(), 2);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 30);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerWeight(102), 1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetNumActiveSlots(), 2);

        chunkTracker.SetExpectedOwnerSize(0);
        chunkTracker.SetOwnerWeight(102, 2);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 50);
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
        chunkTracker.AddOwner(101, DynamicVDiskId(), 1);

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
        chunkTracker.AddOwner(101, DynamicVDiskId(), 1);
        chunkTracker.SetExpectedOwnerSettings(4, 30);

        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);
        UNIT_ASSERT_C(chunkTracker.TryAllocate(101, 28, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(101), 28);
    }

    Y_UNIT_TEST(StaticGroupReserveThrottlesTheNeighbours) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
        };
        params.StaticGroupChunkReservePerMille = 500;

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);

        TOwner staticOwner = 101;
        TOwner dynamicOwner = 102;
        chunkTracker.AddOwner(staticOwner, StaticVDiskId());
        chunkTracker.AddOwner(dynamicOwner, DynamicVDiskId());

        // The reserve is the personal quota of the static group owner. Nothing is taken out of the shared quota for
        // it, and the personal quota of the neighbours is not affected either.
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(dynamicOwner), 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(dynamicOwner), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 100);

        // The neighbour from the dynamic group takes user writes until it is told to stop
        double occupancy;
        while (chunkTracker.GetSpaceColor(dynamicOwner, &occupancy) < TColor::YELLOW) {
            UNIT_ASSERT_C(chunkTracker.TryAllocate(dynamicOwner, 1, errorReason), errorReason);
        }
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(dynamicOwner), 61);

        // It gives up while the reserve is still there, and only the static group owner sees that space as free
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(dynamicOwner, false), 14);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(staticOwner, false), 39);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetSpaceColor(staticOwner, &occupancy), TColor::GREEN);

        // The static group owner takes the whole reserve, and that does not push the neighbour any further
        UNIT_ASSERT_C(chunkTracker.TryAllocate(staticOwner, 25, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserveFree(staticOwner), 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(dynamicOwner, false), 14);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetSpaceColor(dynamicOwner, &occupancy), TColor::YELLOW);
    }

    Y_UNIT_TEST(StaticGroupReserveNeverBlocksAnAllocation) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
        };
        params.StaticGroupChunkReservePerMille = 500;

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);

        TOwner staticOwner = 101;
        TOwner dynamicOwner = 102;
        chunkTracker.AddOwner(staticOwner, StaticVDiskId());
        chunkTracker.AddOwner(dynamicOwner, DynamicVDiskId());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 25);

        // The reserve is enforced through the space colors only, it never refuses an allocation: an owner of a full
        // disk has to compact and to let the log be cut, and that takes chunks as well
        double occupancy;
        while (chunkTracker.TryAllocate(dynamicOwner, 1, errorReason)) {
            // Whatever is held back, a neighbour is never told that the disk is completely full because of it
            UNIT_ASSERT(chunkTracker.GetSpaceColor(dynamicOwner, &occupancy) < TColor::BLACK);
        }
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(dynamicOwner), 97);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetSpaceColor(dynamicOwner, &occupancy), TColor::RED);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 25);
    }

    Y_UNIT_TEST(StaticGroupReserveIsHeldBackWhileItIsNotUsed) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
        };
        params.StaticGroupChunkReservePerMille = 500;

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);

        TOwner staticOwner = 101;
        TOwner dynamicOwner = 102;
        chunkTracker.AddOwner(staticOwner, StaticVDiskId());
        chunkTracker.AddOwner(dynamicOwner, DynamicVDiskId());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserveFree(staticOwner), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(dynamicOwner, false), 75);

        // Only the part of the reserve the owner does not use yet is held back from its neighbours
        UNIT_ASSERT_C(chunkTracker.TryAllocate(staticOwner, 10, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserveFree(staticOwner), 15);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(dynamicOwner, false), 75);

        // The reserve is held back again as soon as the chunks are released
        chunkTracker.Release(staticOwner, 10);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserveFree(staticOwner), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(dynamicOwner, false), 75);

        // An owner that uses more than its reserve holds nothing back
        UNIT_ASSERT_C(chunkTracker.TryAllocate(staticOwner, 30, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserveFree(staticOwner), 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(dynamicOwner, false), 70);
    }

    Y_UNIT_TEST(StaticGroupReserveFollowsPersonalQuota) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
        };
        params.StaticGroupChunkReservePerMille = 500;

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);

        TOwner staticOwner = 101;
        chunkTracker.AddOwner(staticOwner, StaticVDiskId());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 25);

        chunkTracker.SetOwnerWeight(staticOwner, 2);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 50);

        chunkTracker.SetExpectedOwnerSettings(2, 0);
        chunkTracker.SetOwnerWeight(staticOwner, 1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 50);

        chunkTracker.SetExpectedOwnerSettings(4, 10);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 10);

        // Nothing is held back for the owner when it is gone
        chunkTracker.RemoveOwner(staticOwner);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 100);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(staticOwner, false), 100);
    }

    Y_UNIT_TEST(StaticGroupReserveSurvivesOverusedDisk) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TOwner staticOwner = 101;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
        };
        params.StaticGroupChunkReservePerMille = 500;
        params.OwnersInfo[staticOwner] = {
            .ChunksOwned = 95,
            .VDiskId = StaticVDiskId(),
            .Weight = 1,
        };

        // An overused disk must start up. The reserve is the personal quota as usual, and nothing is held back for
        // an owner that is over it anyway.
        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserveFree(staticOwner), 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 100);

        chunkTracker.Release(staticOwner, 20);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserveFree(staticOwner), 0);

        // The protection is back as soon as the owner drops below its reserve
        chunkTracker.Release(staticOwner, 60);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(staticOwner), 15);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserveFree(staticOwner), 10);
    }

    Y_UNIT_TEST(StaticGroupReservesAreRebalancedWhenSharedQuotaIsFull) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
        };
        params.StaticGroupChunkReservePerMille = 500;

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);

        TOwner firstStatic = 101;
        TOwner secondStatic = 102;
        TOwner dynamicOwner = 103;
        chunkTracker.AddOwner(firstStatic, StaticVDiskId(1));
        chunkTracker.AddOwner(secondStatic, StaticVDiskId(2));
        chunkTracker.AddOwner(dynamicOwner, DynamicVDiskId(3));
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(firstStatic), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(secondStatic), 25);

        // Leave the dynamic owner as little free space as it is willing to leave itself
        double occupancy;
        while (chunkTracker.GetSpaceColor(dynamicOwner, &occupancy) < TColor::YELLOW) {
            UNIT_ASSERT_C(chunkTracker.TryAllocate(dynamicOwner, 1, errorReason), errorReason);
        }
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerUsed(dynamicOwner), 36);

        // The first owner needs a bigger reserve while the second one has a surplus. The new reserves must take
        // effect right away, not as the chunks of the shared quota happen to be released.
        chunkTracker.SetOwnerWeight(firstStatic, 3);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(firstStatic), 60);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(secondStatic), 20);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(firstStatic), 37);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(secondStatic), 12);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 100);

        // Both new reserves are hidden from the dynamic owner, and neither of them from its own owner
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(dynamicOwner, false), 15);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(firstStatic, false), 52);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(secondStatic, false), 27);

        // Both reserves are usable while the dynamic owner is still holding the rest of the shared quota
        UNIT_ASSERT_C(chunkTracker.TryAllocate(firstStatic, 37, errorReason), errorReason);
        UNIT_ASSERT_C(chunkTracker.TryAllocate(secondStatic, 12, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserveFree(firstStatic), 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(dynamicOwner, false), 15);
    }

    Y_UNIT_TEST(StaticGroupReserveIsSplitBetweenOwners) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 0,
        };
        params.StaticGroupChunkReservePerMille = 500;

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);

        TOwner firstStatic = 101;
        TOwner secondStatic = 102;
        chunkTracker.AddOwner(firstStatic, StaticVDiskId(1), 3);
        chunkTracker.AddOwner(secondStatic, StaticVDiskId(2), 1);

        // The two personal quotas do not fit into the cap, both reserves are scaled down proportionally
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(firstStatic), 75);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(secondStatic), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(firstStatic), 37);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(secondStatic), 12);

        // Neither of the two sees the reserve of the other one as free space
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(firstStatic, false), 88);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(secondStatic, false), 63);

        // So the reserve of the second owner is still there once the first one has taken its share
        UNIT_ASSERT_C(chunkTracker.TryAllocate(firstStatic, 85, errorReason), errorReason);
        UNIT_ASSERT_C(chunkTracker.TryAllocate(secondStatic, 12, errorReason), errorReason);
    }

    Y_UNIT_TEST(SharedCommonLogIgnoresStaticGroupReserve) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        TOwner staticOwner = 101;
        TOwner dynamicOwner = 102;
        TKeeperParams params {
            .TotalChunks = 1 /*syslog*/ + 5 /*system reserve*/ + 200,
            .ExpectedOwnerCount = 4,
            .SysLogSize = 1,
            .MaxCommonLogChunks = 40,
        };
        // The common log of such a disk has no pool of its own, it allocates from the very same shared quota
        params.SeparateCommonLog = false;
        params.StaticGroupChunkReservePerMille = 500;

        TString errorReason;
        TChunkTracker chunkTracker;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);
        chunkTracker.AddOwner(staticOwner, StaticVDiskId());
        chunkTracker.AddOwner(dynamicOwner, DynamicVDiskId());

        // Nothing is taken out of the chunk pool for the reserve, so the log budget is not affected by it
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 50);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 200);

        double occupancy;
        while (chunkTracker.GetSpaceColor(dynamicOwner, &occupancy) < TColor::YELLOW) {
            UNIT_ASSERT_C(chunkTracker.TryAllocate(dynamicOwner, 1, errorReason), errorReason);
        }
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerFree(dynamicOwner, false), 22);

        // The reserve holds the neighbours of the static group owner back, but never the common log: a PDisk that
        // can not write its log is way worse than a static group that has to share its reserve
        UNIT_ASSERT_C(chunkTracker.TryAllocate(OwnerSystem, 40, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 50);
    }

    Y_UNIT_TEST(StaticGroupReserveIsCappedAndCanBeDisabled) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 0,
        };
        params.StaticGroupChunkReservePerMille = 500;

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);

        // The only owner of the disk has the whole pool as its personal quota, the cap keeps the shared quota alive
        TOwner staticOwner = 101;
        chunkTracker.AddOwner(staticOwner, StaticVDiskId());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(staticOwner), 100);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 50);

        chunkTracker.SetStaticGroupChunkReservePerMille(100);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 10);

        chunkTracker.SetStaticGroupChunkReservePerMille(0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 100);
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

        chunkTracker.AddOwner(101, DynamicVDiskId(), 1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetNumActiveSlots(), 1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 50);

        // Weigh can't be zero (0 is treated as 1)
        chunkTracker.SetOwnerWeight(101, 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetNumActiveSlots(), 1);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 50);

        chunkTracker.AddOwner(102, DynamicVDiskId(), 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetNumActiveSlots(), 2);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 25);
    }
}

#undef UNIT_ASSERT_EQUAL_X
} // namespace NKikimr
