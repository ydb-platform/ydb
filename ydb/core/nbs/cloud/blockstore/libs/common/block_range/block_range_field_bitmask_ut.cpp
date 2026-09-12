#include "block_range_field_bitmask.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cstdlib>
#include <memory>

namespace NYdb::NBS::NBlockStore {

namespace {

class TTrackingAllocator final: public IArenaAllocator
{
public:
    void* Allocate(size_t size) override
    {
        ++AllocatedBlocksCount;
        return std::malloc(size);
    }

    void DeAllocate(void* ptr) override
    {
        UNIT_ASSERT(ptr);
        UNIT_ASSERT(AllocatedBlocksCount);
        --AllocatedBlocksCount;
        std::free(ptr);
    }

    size_t AllocatedBlocks() const override
    {
        return AllocatedBlocksCount;
    }

    size_t AllocatedSize() const override
    {
        return 0;
    }

    TVector<TArenaAllocatorStats> GetStats() const override
    {
        return {};
    }

private:
    size_t AllocatedBlocksCount = 0;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeFieldBitMaskTest)
{
    Y_UNIT_TEST(ShouldCreateEmpty)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        UNIT_ASSERT(field.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0, field.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(
            IBlockRangeFieldImpl::EBackend::Bitmask,
            field.GetBackend());
    }

    Y_UNIT_TEST(ShouldMeasureFixedMaskSize)
    {
        constexpr size_t MaskSize = 256;
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, MaskSize * 8);

        UNIT_ASSERT_VALUES_EQUAL(MaskSize, field.GetAllocatedSize());
        UNIT_ASSERT_VALUES_EQUAL(MaskSize, field.GetUsedSize());

        bool changed = false;
        UNIT_ASSERT(field.TryAdd(TBlockRange16::MakeOneBlock(3), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(MaskSize, field.GetAllocatedSize());
        UNIT_ASSERT_VALUES_EQUAL(MaskSize, field.GetUsedSize());

        field.Clear();
        UNIT_ASSERT_VALUES_EQUAL(MaskSize, field.GetAllocatedSize());
        UNIT_ASSERT_VALUES_EQUAL(MaskSize, field.GetUsedSize());
    }

    Y_UNIT_TEST(ShouldAddSingleBlock)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(field.TryAdd(TBlockRange16::MakeOneBlock(3), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(!field.Empty());
        UNIT_ASSERT_VALUES_EQUAL(1, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldAddContinuousRange)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(2, 5), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(4, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldAddMultipleRanges)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(0, 2), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(3, field.GetBlockCount());

        changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(5, 7), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(6, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldMergeAdjacentRanges)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(0, 3), &changed));
        UNIT_ASSERT(changed);

        changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(4, 7), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(8, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldDetectNoChangeOnDuplicateAdd)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(1, 3), &changed));
        UNIT_ASSERT(changed);

        changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(1, 3), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT_VALUES_EQUAL(3, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldDetectNoChangeOnPartialAdd)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(1, 3), &changed));
        UNIT_ASSERT(changed);

        changed = false;
        UNIT_ASSERT(field.TryAdd(TBlockRange16::MakeOneBlock(2), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT_VALUES_EQUAL(3, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldRemoveSingleBlock)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(0, 7), &changed));

        changed = false;
        UNIT_ASSERT(field.TryRemove(TBlockRange16::MakeOneBlock(4), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(7, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldRemoveContinuousRange)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(0, 7), &changed));
        UNIT_ASSERT_VALUES_EQUAL(8, field.GetBlockCount());

        changed = false;
        UNIT_ASSERT(
            field.TryRemove(TBlockRange16::MakeClosedInterval(2, 5), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(4, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldDetectNoChangeOnRemoveFromEmpty)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryRemove(TBlockRange16::MakeClosedInterval(0, 3), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT(field.Empty());
    }

    Y_UNIT_TEST(ShouldDetectNoChangeOnRemoveNonExistent)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(0, 3), &changed));

        changed = false;
        UNIT_ASSERT(
            field.TryRemove(TBlockRange16::MakeClosedInterval(4, 7), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT_VALUES_EQUAL(4, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldPartialRemove)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(0, 7), &changed));

        changed = false;
        UNIT_ASSERT(
            field.TryRemove(TBlockRange16::MakeClosedInterval(2, 5), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(4, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldClearAll)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(0, 7), &changed));
        UNIT_ASSERT_VALUES_EQUAL(8, field.GetBlockCount());

        field.Clear();
        UNIT_ASSERT(field.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldOverlapsCorrectly)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        UNIT_ASSERT(!field.Overlaps(TBlockRange16::MakeClosedInterval(0, 3)));

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(2, 5), &changed));

        UNIT_ASSERT(field.Overlaps(TBlockRange16::MakeClosedInterval(0, 3)));
        UNIT_ASSERT(field.Overlaps(TBlockRange16::MakeClosedInterval(3, 4)));
        UNIT_ASSERT(field.Overlaps(TBlockRange16::MakeClosedInterval(5, 7)));
        UNIT_ASSERT(!field.Overlaps(TBlockRange16::MakeOneBlock(0)));
        UNIT_ASSERT(!field.Overlaps(TBlockRange16::MakeOneBlock(7)));
    }

    Y_UNIT_TEST(ShouldOverlapsEmptyReturnsFalse)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        UNIT_ASSERT(!field.Overlaps(TBlockRange16::MakeClosedInterval(0, 7)));
    }

    Y_UNIT_TEST(ShouldHandleRangeAtBoundary)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(0, 7), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(8, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldHandleSingleBlockRange)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(field.TryAdd(TBlockRange16::MakeOneBlock(0), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(1, field.GetBlockCount());

        changed = false;
        UNIT_ASSERT(field.TryAdd(TBlockRange16::MakeOneBlock(7), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(2, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldInterleaveAddAndRemove)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(0, 3), &changed));
        UNIT_ASSERT_VALUES_EQUAL(4, field.GetBlockCount());

        UNIT_ASSERT(
            field.TryRemove(TBlockRange16::MakeClosedInterval(1, 2), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(2, field.GetBlockCount());

        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(5, 7), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(5, field.GetBlockCount());

        UNIT_ASSERT(
            field.TryRemove(TBlockRange16::MakeClosedInterval(0, 7), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(field.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0, field.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldReturnNulloptForEmptyOnGetFirstRange)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        UNIT_ASSERT(!field.GetFirstRange().has_value());
    }

    Y_UNIT_TEST(ShouldReturnSingleBlockOnGetFirstRange)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(field.TryAdd(TBlockRange16::MakeOneBlock(3), &changed));

        const auto range = field.GetFirstRange();
        UNIT_ASSERT(range.has_value());
        UNIT_ASSERT_VALUES_EQUAL(3, range->Start);
        UNIT_ASSERT_VALUES_EQUAL(3, range->End);
    }

    Y_UNIT_TEST(ShouldReturnContinuousRangeOnGetFirstRange)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(2, 5), &changed));

        const auto range = field.GetFirstRange();
        UNIT_ASSERT(range.has_value());
        UNIT_ASSERT_VALUES_EQUAL(2, range->Start);
        UNIT_ASSERT_VALUES_EQUAL(5, range->End);
    }

    Y_UNIT_TEST(ShouldReturnFirstRangeWhenMultipleRanges)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(10, 12), &changed));
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(20, 25), &changed));
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(30, 31), &changed));

        const auto range = field.GetFirstRange();
        UNIT_ASSERT(range.has_value());
        UNIT_ASSERT_VALUES_EQUAL(10, range->Start);
        UNIT_ASSERT_VALUES_EQUAL(12, range->End);
    }

    Y_UNIT_TEST(ShouldMergeAdjacentRangesInGetFirstRange)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(2, 5), &changed));
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(6, 9), &changed));

        const auto range = field.GetFirstRange();
        UNIT_ASSERT(range.has_value());
        UNIT_ASSERT_VALUES_EQUAL(2, range->Start);
        UNIT_ASSERT_VALUES_EQUAL(9, range->End);
    }

    Y_UNIT_TEST(ShouldReturnFirstRangeAfterRemovingFirst)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(1, 4), &changed));
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(8, 10), &changed));

        UNIT_ASSERT(
            field.TryRemove(TBlockRange16::MakeClosedInterval(1, 4), &changed));

        const auto range = field.GetFirstRange();
        UNIT_ASSERT(range.has_value());
        UNIT_ASSERT_VALUES_EQUAL(8, range->Start);
        UNIT_ASSERT_VALUES_EQUAL(10, range->End);
    }

    Y_UNIT_TEST(ShouldHandleGetFirstRangeAcrossChunks)
    {
        // 64-bit chunks: range crossing a chunk boundary.
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(62, 65), &changed));

        const auto range = field.GetFirstRange();
        UNIT_ASSERT(range.has_value());
        UNIT_ASSERT_VALUES_EQUAL(62, range->Start);
        UNIT_ASSERT_VALUES_EQUAL(65, range->End);
    }

    Y_UNIT_TEST(ShouldHandleGetFirstRangeAtChunkStart)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(64, 70), &changed));

        const auto range = field.GetFirstRange();
        UNIT_ASSERT(range.has_value());
        UNIT_ASSERT_VALUES_EQUAL(64, range->Start);
        UNIT_ASSERT_VALUES_EQUAL(70, range->End);
    }

    Y_UNIT_TEST(ShouldHandleGetFirstRangeFullChunks)
    {
        // Fully set chunks followed by a partially set chunk.
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(0, 63), &changed));
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(64, 127), &changed));
        UNIT_ASSERT(field.TryAdd(
            TBlockRange16::MakeClosedInterval(128, 130),
            &changed));

        const auto range = field.GetFirstRange();
        UNIT_ASSERT(range.has_value());
        UNIT_ASSERT_VALUES_EQUAL(0, range->Start);
        UNIT_ASSERT_VALUES_EQUAL(130, range->End);
    }

    Y_UNIT_TEST(ShouldHandleGetFirstRangeAtMaxBoundary)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(field.TryAdd(
            TBlockRange16::MakeClosedInterval(2040, 2047),
            &changed));

        const auto range = field.GetFirstRange();
        UNIT_ASSERT(range.has_value());
        UNIT_ASSERT_VALUES_EQUAL(2040, range->Start);
        UNIT_ASSERT_VALUES_EQUAL(2047, range->End);
    }

    Y_UNIT_TEST(ShouldHandleGetFirstRangeAfterDeserialize)
    {
        auto allocator = CreateArenaAllocator();
        TBlockRangeFieldBitMask field(allocator, 256 * 8);

        bool changed = false;
        UNIT_ASSERT(
            field.TryAdd(TBlockRange16::MakeClosedInterval(5, 9), &changed));

        TBlockRangeFieldBitMask restored(allocator, 256 * 8);
        restored.DeserializeFromBitmap(field.Save());

        const auto range = restored.GetFirstRange();
        UNIT_ASSERT(range.has_value());
        UNIT_ASSERT_VALUES_EQUAL(5, range->Start);
        UNIT_ASSERT_VALUES_EQUAL(9, range->End);
    }

    Y_UNIT_TEST(ShouldReleaseMaskMemoryOnDestruction)
    {
        auto allocator = std::make_shared<TTrackingAllocator>();

        {
            TBlockRangeFieldBitMask field(allocator, 256 * 8);
            UNIT_ASSERT_VALUES_EQUAL(1, allocator->AllocatedBlocks());

            bool changed = false;
            UNIT_ASSERT(field.TryAdd(
                TBlockRange16::MakeClosedInterval(0, 127),
                &changed));
            UNIT_ASSERT(changed);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedBlocks());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
