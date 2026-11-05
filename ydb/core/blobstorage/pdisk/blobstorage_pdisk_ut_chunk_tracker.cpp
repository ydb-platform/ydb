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

    // The chunk reserve tests below run on pools of a hundred chunks or so: give the static group owners no log pool,
    // which would take a large part of such a small pool, and a cap that fits their personal quotas
    static void SetupStaticGroupParams(NPDisk::TKeeperParams &params) {
        params.CommonStaticLogChunks = 0;
        params.StaticGroupChunkReservePerMille = 500;
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

    Y_UNIT_TEST(ExpectedOwnerSizeScalesByGroupSizeInUnits) {
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

        chunkTracker.AddOwner(102, DynamicVDiskId(), 2, 2);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 60);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerWeight(102), 2);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetNumActiveSlots(), 3);

        chunkTracker.SetOwnerWeight(102, 7);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 60);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerWeight(102), 7);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetNumActiveSlots(), 8);

        chunkTracker.SetOwnerWeight(102, 9);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerWeight(102), 9);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 60);
        chunkTracker.SetOwnerSettings(102, 9, 3);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerWeight(102), 9);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 30);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 90);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetNumActiveSlots(), 10);

        chunkTracker.SetOwnerSettings(102, 9, 0);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 30);
        chunkTracker.SetOwnerSettings(102, 9, 3);

        chunkTracker.SetExpectedOwnerSize(0);
        chunkTracker.SetOwnerWeight(102, 2);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(101), 25);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerHardLimit(102), 50);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerWeight(102), 2);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetNumActiveSlots(), 3);
    }

    Y_UNIT_TEST(ExpectedOwnerSizeCappedByTotal) {
        using namespace NPDisk;

        TPerOwnerQuotaTracker tracker;
        tracker.Reset(100, TColorLimits::MakeLogLimits());
        tracker.SetExpectedOwnerSize(30);
        tracker.AddOwner(101, DynamicVDiskId(), 1, 4);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 100);
        tracker.InitialAllocate(101, 20);

        tracker.SetTotal(80);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 80);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetFree(101), 60);
        tracker.SetTotal(200);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 120);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetUsed(101), 20);

        tracker.SetExpectedOwnerSize(50);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 200);
        tracker.SetExpectedOwnerSize(60);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 200);
        tracker.SetOwnerSettings(101, 1, 2);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 120);
        tracker.SetOwnerSettings(101, 1, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 60);

        tracker.SetTotal(0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetUsed(101), 20);
    }

    Y_UNIT_TEST(ExpectedOwnerSizeMultiplicationDoesNotOverflow) {
        using namespace NPDisk;

        TPerOwnerQuotaTracker tracker;
        tracker.Reset(100, TColorLimits::MakeLogLimits());
        tracker.SetExpectedOwnerSize(i64{1} << 32);
        tracker.AddOwner(101, DynamicVDiskId(), 1, Max<ui32>());
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 100);
        tracker.InitialAllocate(101, 20);

        tracker.SetExpectedOwnerSize(Max<i64>());
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 100);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetFree(101), 80);
        tracker.SetOwnerSettings(101, 1, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 100);
        tracker.SetExpectedOwnerSize(30);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 30);
        tracker.SetOwnerSettings(101, 1, Max<ui32>());
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 100);
        tracker.SetTotal(0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetHardLimit(101), 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetUsed(101), 20);
    }

    Y_UNIT_TEST(ExpectedOwnerSizeIndependentOfCapacityAndSlotCounts) {
        using namespace NPDisk;

        const std::pair<ui32, i64> groupSizesAndQuotas[] = {{0, 1000}, {1, 1000}, {4, 4000}};
        // Grow from one to nine owners to cross both slot and capacity budgets, including equality.
        // Only the first group varies; the others consume one slot each.
        for (ui32 diskSizeInSlots : {2, 4, 8}) {
            for (ui32 maxSlots : {2, 4, 8}) {
                for (const auto& [groupSizeInUnits, expectedQuota] : groupSizesAndQuotas) {
                    TKeeperParams params{
                        .TotalChunks = diskSizeInSlots * 1000,
                        .ExpectedOwnerCount = Min(diskSizeInSlots, maxSlots),
                        .ExpectedOwnerSize = 1000,
                    };
                    TChunkTracker tracker;
                    TString error;
                    UNIT_ASSERT_C(tracker.Reset(params, TColorLimits::MakeLogLimits(), error), error);
                    for (ui32 numOwners = 1; numOwners <= 9; ++numOwners) {
                        const TString context = TStringBuilder() << "DiskSizeInSlots# " << diskSizeInSlots
                            << " MaxSlots# " << maxSlots << " GroupSizeInUnits# " << groupSizeInUnits
                            << " NumOwners# " << numOwners;
                        const TOwner owner = OwnerBeginUser + numOwners - 1;
                        const ui32 ownerGroupSizeInUnits = numOwners == 1 ? groupSizeInUnits : 1;
                        tracker.AddOwner(owner, DynamicVDiskId(numOwners), ownerGroupSizeInUnits, ownerGroupSizeInUnits);
                        UNIT_ASSERT_C(tracker.TryAllocate(owner, 10, error), error);
                        UNIT_ASSERT_VALUES_EQUAL_C(tracker.GetNumActiveSlots(), numOwners - 1 + Max(1u, groupSizeInUnits), context);
                        for (TOwner id = OwnerBeginUser; id <= owner; ++id) {
                            const i64 quota = Min(tracker.GetTotalHardLimit(), id == OwnerBeginUser ? expectedQuota : 1000);
                            UNIT_ASSERT_VALUES_EQUAL_C(tracker.GetOwnerHardLimit(id), quota, context);
                            UNIT_ASSERT_VALUES_EQUAL_C(tracker.GetOwnerWeight(id), id == OwnerBeginUser ? Max(1u, groupSizeInUnits) : 1, context);
                            UNIT_ASSERT_VALUES_EQUAL_C(tracker.GetOwnerUsed(id), 10, context);
                            UNIT_ASSERT_VALUES_EQUAL_C(tracker.GetOwnerFree(id, true), quota - 10, context);
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(ExpectedOwnerSizeOvercommitPreservesNeighbours) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        TChunkTracker tracker;
        TKeeperParams params{
            .TotalChunks = 205 + 1000,
            .ExpectedOwnerCount = 2,
            .ExpectedOwnerSize = 100,
            .SpaceColorBorder = TColor::YELLOW,
        };
        TString error;
        UNIT_ASSERT_C(tracker.Reset(params, TColorLimits::MakeLogLimits(), error), error);
        tracker.AddOwner(101, DynamicVDiskId(1), 1, 1);
        tracker.AddOwner(102, DynamicVDiskId(2), 1, 1);
        UNIT_ASSERT_C(tracker.TryAllocate(101, 60, error), error);
        double originalOccupancy;
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetSpaceColor(101, &originalOccupancy), TColor::GREEN);
        auto checkNeighbour = [&]() {
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetOwnerHardLimit(101), 100);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetOwnerFree(101, true), 40);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetOwnerUsed(101), 60);
            double occupancy;
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetSpaceColor(101, &occupancy), TColor::GREEN);
            UNIT_ASSERT_VALUES_EQUAL(occupancy, originalOccupancy);
        };

        // Resizing must leave the neighbour's quota and color unchanged.
        tracker.SetOwnerSettings(102, 1, 1000);
        checkNeighbour();
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetOwnerHardLimit(102), 1000);

        // Eleven owners exceed both the two-slot budget and the pool's ten slots.
        for (TOwner owner = 103; owner <= 111; ++owner) {
            tracker.AddOwner(owner, DynamicVDiskId(owner), 1, 1);
            checkNeighbour();
        }
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetNumActiveSlots(), 11);
        tracker.SetExpectedOwnerCount(1);
        checkNeighbour();
        tracker.RemoveOwner(111);
        checkNeighbour();

        // Actual consumption still changes the shared color and is constrained by the physical pool.
        UNIT_ASSERT_C(tracker.TryAllocate(102, 850, error), error);
        double occupancy;
        UNIT_ASSERT(tracker.GetSpaceColor(101, &occupancy) >= TColor::LIGHT_YELLOW);
        UNIT_ASSERT(!tracker.TryAllocate(102, 1000, error));
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetOwnerHardLimit(101), 100);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetOwnerUsed(101), 60);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetTotalUsed(), 910);
    }

    Y_UNIT_TEST(ExpectedOwnerSizeShrinkWithSemiStrictIsolation) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        TChunkTracker tracker;
        TKeeperParams params{
            .TotalChunks = 205 + 1000,
            .ExpectedOwnerCount = 2,
            .SpaceColorBorder = TColor::LIGHT_YELLOW,
        };
        TString error;
        UNIT_ASSERT_C(tracker.Reset(params, TColorLimits::MakeLogLimits(), error), error);
        tracker.AddOwner(101, DynamicVDiskId(), 1);
        tracker.AddOwner(102, DynamicVDiskId(), 2, 2);
        UNIT_ASSERT_C(tracker.TryAllocate(101, 150, error), error);
        double occupancy;
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetSpaceColor(101, &occupancy), TColor::GREEN);

        tracker.SetExpectedOwnerSettings(2, 100);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetOwnerHardLimit(101), 100);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetOwnerHardLimit(102), 200);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetOwnerUsed(101), 150);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetNumActiveSlots(), 3);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetSpaceColor(101, &occupancy), TColor::LIGHT_YELLOW);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetSpaceColor(102, &occupancy), TColor::GREEN);

        tracker.SetExpectedOwnerSize(0);
        tracker.SetOwnerWeight(102, 2);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetOwnerWeight(102), 2);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetOwnerUsed(101), 150);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetNumActiveSlots(), 3);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetSpaceColor(101, &occupancy), TColor::GREEN);
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
        SetupStaticGroupParams(params);

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
        SetupStaticGroupParams(params);

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

    // The reserve is enforced through the color an owner is told about, which is what stops
    // its user writes. It must not also stop the compaction output that is the only way that
    // owner can give space back, or a disk that reaches this point never comes out of it.
    Y_UNIT_TEST(StaticGroupReserveDoesNotBlockHousekeeping) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
            // As the disks in question are configured: the personal quota is reported no
            // worse than green, so what an owner is judged by is the shared quota alone.
            .SpaceColorBorder = TColor::GREEN,
        };
        SetupStaticGroupParams(params);

        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);

        const TOwner staticOwner = 101;
        const TOwner dynamicOwner = 102;
        chunkTracker.AddOwner(staticOwner, StaticVDiskId());
        chunkTracker.AddOwner(dynamicOwner, DynamicVDiskId());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetOwnerStaticReserve(staticOwner), 25);

        // Fill the neighbour until an ordinary chunk reservation would be refused. It gets
        // there while the shared quota itself still has the whole reserve free.
        double occupancy;
        for (ui32 i = 0; i < 1000 && chunkTracker.EstimateSpaceColor(dynamicOwner, 1, &occupancy) < TColor::BLACK; ++i) {
            UNIT_ASSERT_C(chunkTracker.TryAllocate(dynamicOwner, 1, errorReason), errorReason);
        }
        UNIT_ASSERT_EQUAL_X(chunkTracker.EstimateSpaceColor(dynamicOwner, 1, &occupancy), TColor::BLACK);

        // What AllocateChunkForOwner() sees for a write of newly accepted data: no room.
        UNIT_ASSERT_EQUAL_X(chunkTracker.EstimateAllocationColor(dynamicOwner, 1, false, &occupancy), TColor::BLACK);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetSpaceHeadroom(dynamicOwner).ToBlack, 0);

        // ... and what it sees for compaction output: the reserve is not held against it.
        UNIT_ASSERT(chunkTracker.EstimateAllocationColor(dynamicOwner, 1, true, &occupancy) < TColor::BLACK);
        const ui64 room = chunkTracker.GetSpaceHeadroom(dynamicOwner).AllocatableToBlack;
        UNIT_ASSERT(room > 0);

        // The compaction budget is that room exactly: spending all of it stays out of black,
        // so housekeeping cannot run the shared pool dry either.
        UNIT_ASSERT(chunkTracker.EstimateAllocationColor(dynamicOwner, room, true, &occupancy) < TColor::BLACK);
        UNIT_ASSERT_EQUAL_X(chunkTracker.EstimateAllocationColor(dynamicOwner, room + 1, true, &occupancy),
            TColor::BLACK);

        // A non-user owner has no housekeeping exception to make.
        UNIT_ASSERT_EQUAL_X(chunkTracker.EstimateAllocationColor(OwnerSystem, 1, true, &occupancy),
            chunkTracker.EstimateSpaceColor(OwnerSystem, 1, &occupancy));
    }

    Y_UNIT_TEST(AllocationReservesProtectSystemAcrossOwnersAndReturnOnRelease) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        using EPurpose = EAllocationPurpose;
        TChunkTracker tracker;
        TKeeperParams params {
            .TotalChunks = 205 + 1000,
            .ExpectedOwnerCount = 4,
            .SpaceColorBorder = TColor::GREEN,
        };
        SetupStaticGroupParams(params);
        TString error;
        UNIT_ASSERT_C(tracker.Reset(params, TColorLimits::MakeLogLimits(), error), error);
        tracker.AddOwner(101, StaticVDiskId());
        tracker.AddOwner(102, DynamicVDiskId());
        tracker.AddOwner(103, DynamicVDiskId(2));
        tracker.SetAllocationReserves(10, 20);

        // USER leaves both reserves alone; the two dynamic owners share the pool, so one of them using up its
        // USER room uses up the other's too.
        const ui64 userRoom = tracker.GetAllocationHeadroom(102, EPurpose::User);
        UNIT_ASSERT_C(tracker.TryAllocate(102, userRoom, error), error);
        for (TOwner owner : {102, 103}) {
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetAllocationHeadroom(owner, EPurpose::User), 0);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetAllocationHeadroom(owner, EPurpose::Recovery), 20);
        }
        // Recovery leaves the system reserve alone.
        UNIT_ASSERT_C(tracker.TryAllocate(103, 12, error), error);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetAllocationHeadroom(102, EPurpose::Recovery), 8);
        UNIT_ASSERT_C(tracker.TryAllocate(102, 8, error), error);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetAllocationHeadroom(102, EPurpose::Recovery), 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetWorstAllocationHeadroom(EPurpose::Recovery), 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetSpaceHeadroom(102).ToRed, 10);
        double occupancy;
        UNIT_ASSERT(tracker.EstimateAllocationColor(102, 10, false, &occupancy) < TColor::RED);
        // SYSTEM may spend the reserve, and so may maintenance: it is what gives space back.
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetAllocationHeadroom(102, EPurpose::System), Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetAllocationHeadroom(102, EPurpose::Maintenance), Max<ui64>());
        UNIT_ASSERT(tracker.GetSpaceHeadroom(102).AllocatableToBlack > 10);

        // Completion/forecast alone returns nothing. Releasing actual output or
        // input chunks restores the shared workspace for every owner.
        tracker.Release(103, 12);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetAllocationHeadroom(102, EPurpose::Recovery), 12);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetAllocationHeadroom(102, EPurpose::User), 0);
        tracker.Release(102, 9);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetAllocationHeadroom(103, EPurpose::User), 1);
        tracker.SetAllocationReserves(0, 20);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetAllocationHeadroom(102, EPurpose::Recovery), Max<ui64>());
    }

    Y_UNIT_TEST(AllocationHeadroomIsPerOwner) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        using EPurpose = EAllocationPurpose;
        TChunkTracker tracker;
        // A RED color border enforces the personal quotas, so an owner can run out of room on its own.
        TKeeperParams params {
            .TotalChunks = 205 + 1000,
            .ExpectedOwnerCount = 4,
            .SpaceColorBorder = TColor::RED,
        };
        TString error;
        UNIT_ASSERT_C(tracker.Reset(params, TColorLimits::MakeLogLimits(), error), error);
        tracker.AddOwner(101, DynamicVDiskId());
        tracker.AddOwner(102, DynamicVDiskId(2));
        tracker.SetAllocationReserves(10, 20);

        const ui64 room = tracker.GetAllocationHeadroom(101, EPurpose::User);
        UNIT_ASSERT(room > 0);
        UNIT_ASSERT_C(tracker.TryAllocate(101, room, error), error);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetAllocationHeadroom(101, EPurpose::User), 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetWorstAllocationHeadroom(EPurpose::User), 0);
        // The neighbour keeps the room of its own quota.
        UNIT_ASSERT(tracker.GetAllocationHeadroom(102, EPurpose::User) > 0);
        UNIT_ASSERT_C(tracker.TryAllocate(102, 1, error), error);
    }

    Y_UNIT_TEST(CompactionPressureUsesEffectiveDynamicOwnerColor) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        TChunkTracker tracker;
        TKeeperParams params {
            .TotalChunks = 205 + 1000,
            .ExpectedOwnerCount = 4,
            .SpaceColorBorder = TColor::GREEN,
        };
        SetupStaticGroupParams(params);
        TString error;
        UNIT_ASSERT_C(tracker.Reset(params, TColorLimits::MakeLogLimits(), error), error);
        const TOwner staticOwner = 101;
        const TOwner dynamicOwner = 102;
        tracker.AddOwner(staticOwner, StaticVDiskId());
        tracker.AddOwner(dynamicOwner, DynamicVDiskId());
        double occupancy;
        while (tracker.GetSpaceColor(dynamicOwner, &occupancy) < TColor::PRE_ORANGE) {
            UNIT_ASSERT_C(tracker.TryAllocate(dynamicOwner, 1, error), error);
        }
        UNIT_ASSERT(tracker.GetSharedPoolColor() < TColor::YELLOW);
        UNIT_ASSERT_EQUAL(tracker.GetCompactionPressureColor(), TColor::PRE_ORANGE);
        UNIT_ASSERT_EQUAL(tracker.GetSpaceHeadroom(dynamicOwner).ToPreOrange, 0);

        // Changing the static reserve changes pressure without allocating or
        // freeing a physical chunk. Removing an owner must remove its color too.
        const i64 used = tracker.GetTotalUsed();
        tracker.RemoveOwner(staticOwner);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetTotalUsed(), used);
        UNIT_ASSERT_EQUAL(tracker.GetCompactionPressureColor(), tracker.GetSharedPoolColor());
        tracker.Release(dynamicOwner, tracker.GetOwnerUsed(dynamicOwner));
        tracker.RemoveOwner(dynamicOwner);
        UNIT_ASSERT_EQUAL(tracker.GetCompactionPressureColor(), TColor::GREEN);

        // Reset must forget the old owner list before reconstructing it.
        UNIT_ASSERT_C(tracker.Reset(params, TColorLimits::MakeLogLimits(), error), error);
        UNIT_ASSERT_EQUAL(tracker.GetCompactionPressureColor(), TColor::GREEN);
    }

    Y_UNIT_TEST(CompactionPressureUsesPersonalQuota) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        TChunkTracker tracker;
        TKeeperParams params {
            .TotalChunks = 205 + 1000,
            .ExpectedOwnerCount = 4,
            .SpaceColorBorder = TColor::YELLOW,
        };
        TString error;
        UNIT_ASSERT_C(tracker.Reset(params, TColorLimits::MakeLogLimits(), error), error);
        tracker.AddOwner(101, DynamicVDiskId());
        UNIT_ASSERT_C(tracker.TryAllocate(101, 240, error), error);
        UNIT_ASSERT_EQUAL(tracker.GetSharedPoolColor(), TColor::GREEN);
        UNIT_ASSERT_EQUAL(tracker.GetCompactionPressureColor(), TColor::YELLOW);
        tracker.SetOwnerWeight(101, 2);
        UNIT_ASSERT_EQUAL(tracker.GetCompactionPressureColor(), TColor::GREEN);
    }

    Y_UNIT_TEST(StaticGroupReserveIsHeldBackWhileItIsNotUsed) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 4,
        };
        SetupStaticGroupParams(params);

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
        SetupStaticGroupParams(params);

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
        SetupStaticGroupParams(params);
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
        SetupStaticGroupParams(params);

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
        SetupStaticGroupParams(params);

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
        SetupStaticGroupParams(params);

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

    Y_UNIT_TEST(CommonStaticLogFollowsStaticGroupOwners) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        TOwner staticOwner = 101;
        TOwner dynamicOwner = 102;
        TKeeperParams params {
            .TotalChunks = 1 /*syslog*/ + 5 /*system reserve*/ + 400,
            .ExpectedOwnerCount = 4,
            .SysLogSize = 1,
            .MaxCommonLogChunks = 40,
            .CommonStaticLogChunks = 20,
        };
        // The common log of such a disk has no pool of its own, it allocates from the very same shared quota
        params.SeparateCommonLog = false;

        TString errorReason;
        TChunkTracker chunkTracker;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);

        // A disk with no static groups keeps the whole chunk pool for its owners
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 400);
        chunkTracker.AddOwner(dynamicOwner, DynamicVDiskId());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 400);

        // The log gets a pool of its own as soon as a VDisk of a static group shows up, without waiting for the PDisk
        // to be restarted
        chunkTracker.AddOwner(staticOwner, StaticVDiskId());
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 380);

        // And that pool is what keeps the static group VDisks writing their logs on an exhausted disk
        while (chunkTracker.TryAllocate(dynamicOwner, 1, errorReason)) {
        }
        UNIT_ASSERT(!chunkTracker.TryAllocate(OwnerSystem, 1, errorReason));
        UNIT_ASSERT_C(chunkTracker.TryAllocate(OwnerCommonStaticLog, 10, errorReason), errorReason);

        // They are still told how the common log is really doing, so that they keep cutting it like everybody else
        double occupancy;
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetSpaceColor(OwnerCommonStaticLog, &occupancy), TColor::RED);

        // A slain static group leaves its log pool behind, and the chunks go back to the owners as the log is cut
        chunkTracker.RemoveOwner(staticOwner);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 390);
        chunkTracker.Release(OwnerCommonStaticLog, 10);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 400);
    }

    Y_UNIT_TEST(CommonStaticLogNeverKeepsAnOverfullDiskFromStarting) {
        using namespace NPDisk;

        TOwner staticOwner = 101;
        TOwner dynamicOwner = 102;
        TKeeperParams params {
            .TotalChunks = 1 /*syslog*/ + 5 /*system reserve*/ + 400,
            .ExpectedOwnerCount = 4,
            .SysLogSize = 1,
            .CommonLogSize = 10,
            .MaxCommonLogChunks = 40,
            .CommonStaticLogChunks = 20,
        };
        params.SeparateCommonLog = false;
        params.OwnersInfo[staticOwner] = {
            .ChunksOwned = 0,
            .VDiskId = StaticVDiskId(),
            .Weight = 1,
        };
        params.OwnersInfo[dynamicOwner] = {
            .ChunksOwned = 390,
            .VDiskId = DynamicVDiskId(),
            .Weight = 1,
        };

        // A disk filled to the brim must start up: the log pool takes what is free and nothing of what is used
        TString errorReason;
        TChunkTracker chunkTracker;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 400);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalUsed(), 400);

        // The log gets its pool once the owners have something to spare
        chunkTracker.Release(dynamicOwner, 30);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalHardLimit(), 380);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetTotalUsed(), 370);
    }

    Y_UNIT_TEST(StaticGroupReserveIsCappedAndCanBeDisabled) {
        using namespace NPDisk;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 0,
        };
        SetupStaticGroupParams(params);

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

    Y_UNIT_TEST(SpaceHeadroomMatchesEstimatedColor) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 1,
            .SpaceColorBorder = TColor::BLACK,
        };
        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeChunkLimits(params.ChunkBaseLimit), errorReason),
            errorReason);
        const TOwner owner = 101;
        chunkTracker.AddOwner(owner, DynamicVDiskId());

        // The headroom is exactly the largest allocation that still estimates better
        // than the boundary, so the two views of the disk cannot disagree.
        double occupancy = 0;
        for (TColor::E color : {TColor::PRE_ORANGE, TColor::ORANGE, TColor::RED, TColor::BLACK}) {
            const i64 headroom = chunkTracker.GetHeadroomBelow(owner, color);
            UNIT_ASSERT(headroom > 0);
            UNIT_ASSERT(chunkTracker.EstimateSpaceColor(owner, headroom, &occupancy) < color);
            UNIT_ASSERT(chunkTracker.EstimateSpaceColor(owner, headroom + 1, &occupancy) >= color);
        }

        const TSpaceHeadroom headroom = chunkTracker.GetSpaceHeadroom(owner);
        UNIT_ASSERT(headroom.Valid);
        UNIT_ASSERT(headroom.ToPreOrange < headroom.ToOrange);
        UNIT_ASSERT(headroom.ToOrange < headroom.ToRed);
        UNIT_ASSERT(headroom.ToRed < headroom.ToBlack);
    }

    Y_UNIT_TEST(SpaceHeadroomShrinksAsTheDiskFills) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 205 /*system*/ + 100,
            .ExpectedOwnerCount = 1,
            .SpaceColorBorder = TColor::BLACK,
        };
        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeChunkLimits(params.ChunkBaseLimit), errorReason),
            errorReason);
        const TOwner owner = 101;
        chunkTracker.AddOwner(owner, DynamicVDiskId());

        const i64 toPreOrange = chunkTracker.GetHeadroomBelow(owner, TColor::PRE_ORANGE);
        UNIT_ASSERT_C(chunkTracker.TryAllocate(owner, toPreOrange, errorReason), errorReason);
        UNIT_ASSERT_EQUAL_X(chunkTracker.GetHeadroomBelow(owner, TColor::PRE_ORANGE), 0);

        double occupancy = 0;
        UNIT_ASSERT(chunkTracker.GetSpaceColor(owner, &occupancy) < TColor::PRE_ORANGE);
        UNIT_ASSERT(chunkTracker.EstimateSpaceColor(owner, 1, &occupancy) >= TColor::PRE_ORANGE);
    }

    Y_UNIT_TEST(TightSpaceColorFloorsHonorIcbCyanPermille) {
        using namespace NPDisk;
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        TChunkTracker chunkTracker;
        TKeeperParams params {
            .TotalChunks = 10'205,
            .ExpectedOwnerCount = 1,
            .SysLogSize = 0,
            .CommonLogSize = 0,
            .MaxCommonLogChunks = 0,
            .SeparateCommonLog = true,
            .ChunkBaseLimit = 50,
            .TightSpaceColorFloors = true,
        };
        TString errorReason;
        UNIT_ASSERT_C(chunkTracker.Reset(params, TColorLimits::MakeLogLimits(), errorReason), errorReason);

        const TOwner owner = 101;
        chunkTracker.AddOwner(owner, DynamicVDiskId());

        const i64 hard = chunkTracker.GetTotalHardLimit();
        UNIT_ASSERT_VALUES_EQUAL(hard, 10'200);
        const i64 cyanQuota = TColorLimits::MakeChunkLimits(params.ChunkBaseLimit, true)
            .GetQuotaForColor(TColor::CYAN, hard);
        UNIT_ASSERT_VALUES_EQUAL(cyanQuota, 510);

        double occupancy = 0;
        UNIT_ASSERT_VALUES_EQUAL(
            chunkTracker.EstimateSpaceColor(owner, hard - cyanQuota, &occupancy), TColor::CYAN);
        UNIT_ASSERT_VALUES_EQUAL(
            chunkTracker.EstimateSpaceColor(owner, hard - cyanQuota - 1, &occupancy), TColor::GREEN);
    }

}

#undef UNIT_ASSERT_EQUAL_X
} // namespace NKikimr
