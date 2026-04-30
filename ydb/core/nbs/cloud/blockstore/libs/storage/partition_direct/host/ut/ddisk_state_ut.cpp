#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/ddisk_state.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

constexpr ui32 BlockSize = 4096;
constexpr ui64 TotalBlocks = 100;
constexpr ui64 TotalBytes = TotalBlocks * BlockSize;

}   // namespace

Y_UNIT_TEST_SUITE(TDDiskStateListTest)
{
    Y_UNIT_TEST(DefaultIsOperational)
    {
        TDDiskStateList list(3, BlockSize, TotalBlocks);

        UNIT_ASSERT_VALUES_EQUAL(3u, list.HostCount());
        for (THostIndex h = 0; h < 3; ++h) {
            UNIT_ASSERT(!list.GetFreshWatermark(h).has_value());
            UNIT_ASSERT(list.CanReadFromDDisk(
                h,
                TBlockRange64::WithLength(0, TotalBlocks)));
            UNIT_ASSERT(list.NeedFlushToDDisk(
                h,
                TBlockRange64::WithLength(0, TotalBlocks)));
        }
    }

    Y_UNIT_TEST(MarkFreshSetsBothWatermarks)
    {
        TDDiskStateList list(2, BlockSize, TotalBlocks);

        const ui64 watermarkBytes = 10 * BlockSize;
        list.MarkFresh(/*h=*/1, watermarkBytes);

        // Host 0 untouched.
        UNIT_ASSERT(!list.GetFreshWatermark(0).has_value());

        // Host 1 went Fresh.
        auto wm = list.GetFreshWatermark(1);
        UNIT_ASSERT(wm.has_value());
        UNIT_ASSERT_VALUES_EQUAL(watermarkBytes, *wm);

        // Below watermark — readable and needs flush.
        UNIT_ASSERT(list.CanReadFromDDisk(1, TBlockRange64::WithLength(0, 5)));
        UNIT_ASSERT(list.NeedFlushToDDisk(1, TBlockRange64::WithLength(0, 5)));
    }

    Y_UNIT_TEST(CanReadOnlyBelowReadWatermark)
    {
        TDDiskStateList list(1, BlockSize, TotalBlocks);
        list.MarkFresh(0, 10 * BlockSize);

        // Range fully below watermark (last block index 9 < 10).
        UNIT_ASSERT(
            list.CanReadFromDDisk(0, TBlockRange64::MakeClosedInterval(0, 9)));

        // Range crossing the watermark.
        UNIT_ASSERT(!list.CanReadFromDDisk(
            0,
            TBlockRange64::MakeClosedInterval(5, 15)));

        // Range above watermark.
        UNIT_ASSERT(!list.CanReadFromDDisk(
            0,
            TBlockRange64::MakeClosedInterval(10, 20)));
    }

    Y_UNIT_TEST(NeedFlushOnlyBelowFlushWatermark)
    {
        TDDiskStateList list(1, BlockSize, TotalBlocks);
        list.MarkFresh(0, 10 * BlockSize);

        // Start below watermark — needs flush.
        UNIT_ASSERT(
            list.NeedFlushToDDisk(0, TBlockRange64::MakeClosedInterval(0, 0)));
        UNIT_ASSERT(
            list.NeedFlushToDDisk(0, TBlockRange64::MakeClosedInterval(9, 20)));

        // Start at/above watermark — does not need flush.
        UNIT_ASSERT(!list.NeedFlushToDDisk(
            0,
            TBlockRange64::MakeClosedInterval(10, 15)));
    }

    Y_UNIT_TEST(SetWatermarksAtTotalGoesOperational)
    {
        TDDiskStateList list(1, BlockSize, TotalBlocks);
        list.MarkFresh(0, 10 * BlockSize);
        UNIT_ASSERT(list.GetFreshWatermark(0).has_value());

        list.SetReadWatermark(0, TotalBytes);
        // Flush watermark still at 10 -> still Fresh.
        UNIT_ASSERT(list.GetFreshWatermark(0).has_value());

        list.SetFlushWatermark(0, TotalBytes);
        // Both at total -> Operational.
        UNIT_ASSERT(!list.GetFreshWatermark(0).has_value());
        UNIT_ASSERT(list.CanReadFromDDisk(
            0,
            TBlockRange64::WithLength(0, TotalBlocks)));
        UNIT_ASSERT(list.NeedFlushToDDisk(
            0,
            TBlockRange64::WithLength(0, TotalBlocks)));
    }

    Y_UNIT_TEST(FilterReadable)
    {
        TDDiskStateList list(3, BlockSize, TotalBlocks);
        // h0 - operational; h1 - fresh @ 10 blocks; h2 - fresh @ 20 blocks.
        list.MarkFresh(1, 10 * BlockSize);
        list.MarkFresh(2, 20 * BlockSize);

        THostMask all;
        all.Set(0);
        all.Set(1);
        all.Set(2);

        // Range fully below all watermarks - all hosts readable.
        const auto lowRange = TBlockRange64::MakeClosedInterval(0, 5);
        const auto lowMask = list.FilterReadable(all, lowRange);
        UNIT_ASSERT(lowMask.Test(0));
        UNIT_ASSERT(lowMask.Test(1));
        UNIT_ASSERT(lowMask.Test(2));

        // Range above h1's watermark but below h2's.
        const auto midRange = TBlockRange64::MakeClosedInterval(15, 18);
        const auto midMask = list.FilterReadable(all, midRange);
        UNIT_ASSERT(midMask.Test(0));
        UNIT_ASSERT(!midMask.Test(1));
        UNIT_ASSERT(midMask.Test(2));

        // Range above all fresh watermarks - only h0 readable.
        const auto highRange = TBlockRange64::MakeClosedInterval(50, 55);
        const auto highMask = list.FilterReadable(all, highRange);
        UNIT_ASSERT(highMask.Test(0));
        UNIT_ASSERT(!highMask.Test(1));
        UNIT_ASSERT(!highMask.Test(2));
    }

    Y_UNIT_TEST(DebugPrintFormat)
    {
        TDDiskStateList list(2, BlockSize, TotalBlocks);
        list.MarkFresh(1, 10 * BlockSize);

        const auto s = list.DebugPrint();
        UNIT_ASSERT_VALUES_EQUAL(
            TString("H0{Operational,100,100};H1{Fresh,10,10};"),
            s);
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
