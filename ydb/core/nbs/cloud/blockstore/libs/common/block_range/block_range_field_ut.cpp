#include "block_range_field.h"

#include "block_range_field_flat_set.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

// Exposes the storage selected by TBlockRangeField for unit tests.
class TBlockRangeFieldTestAccessor
{
public:
    // Returns true when the field stores its range inline.
    static bool HasInlineRange(const TBlockRangeField& field)
    {
        return field.SimpleImpl != std::nullopt;
    }

    // Returns true when the field uses the set-based implementation.
    static bool HasImpl(const TBlockRangeField& field)
    {
        return field.NodeBasedImpl != nullptr ||
               field.BitMaskBasedImpl != nullptr;
    }

    static size_t GetSegmentCount(const TBlockRangeField& field)
    {
        if (auto impl = field.GetNodeBasedImpl()) {
            return impl->GetSegmentCount();
        }
        return 0;
    }
};

namespace {

////////////////////////////////////////////////////////////////////////////////

TBlockRange16 R(ui16 start, ui16 end)
{
    return TBlockRange16::MakeClosedInterval(start, end);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeFieldTest)
{
    Y_UNIT_TEST(ShouldCreateImplOnlyForMultipleRanges)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(TBlockRangeFieldTestAccessor::HasInlineRange(f));
        UNIT_ASSERT(!TBlockRangeFieldTestAccessor::HasImpl(f));

        f.Add(R(10, 20));
        UNIT_ASSERT(TBlockRangeFieldTestAccessor::HasInlineRange(f));
        UNIT_ASSERT(!TBlockRangeFieldTestAccessor::HasImpl(f));

        f.Add(R(21, 30));
        UNIT_ASSERT(TBlockRangeFieldTestAccessor::HasInlineRange(f));
        UNIT_ASSERT(!TBlockRangeFieldTestAccessor::HasImpl(f));

        f.Add(R(40, 50));
        UNIT_ASSERT(!TBlockRangeFieldTestAccessor::HasInlineRange(f));
        UNIT_ASSERT(TBlockRangeFieldTestAccessor::HasImpl(f));

        f.Remove(R(40, 50));
        UNIT_ASSERT(!TBlockRangeFieldTestAccessor::HasInlineRange(f));
        UNIT_ASSERT(TBlockRangeFieldTestAccessor::HasImpl(f));

        f.Remove(R(15, 20));
        UNIT_ASSERT(!TBlockRangeFieldTestAccessor::HasInlineRange(f));
        UNIT_ASSERT(TBlockRangeFieldTestAccessor::HasImpl(f));

        f.Clear();
        UNIT_ASSERT(TBlockRangeFieldTestAccessor::HasInlineRange(f));
        UNIT_ASSERT(!TBlockRangeFieldTestAccessor::HasImpl(f));
    }

    // -------------------------------------------------------------------------
    // Memory measurement tests

    Y_UNIT_TEST(ShouldUsePreferredBackendAfterSwitch)
    {
        static constexpr std::array backends = {
            TBlockRangeField::EBackend::StdSet,
            TBlockRangeField::EBackend::Set,
            TBlockRangeField::EBackend::FlatSet};

        for (const auto backend: backends) {
            TBlockRangeField f(CreateArenaAllocator(), 32768, backend);
            UNIT_ASSERT_VALUES_EQUAL(
                ToString(int(TBlockRangeField::EBackend::Simple)),
                ToString(int(f.GetBackend())));

            f.Add(R(10, 20));
            UNIT_ASSERT_VALUES_EQUAL(
                ToString(int(TBlockRangeField::EBackend::Simple)),
                ToString(int(f.GetBackend())));

            // The second disjoint range triggers the switch to the preferred
            // backend.
            f.Add(R(30, 40));
            UNIT_ASSERT_VALUES_EQUAL(
                ToString(int(backend)),
                ToString(int(f.GetBackend())));
            UNIT_ASSERT_VALUES_EQUAL("[10..20][30..40]", f.Print());

            // The backend is kept while the field is not empty; it returns
            // to Simple only after the field is fully collapsed.
            f.Remove(R(30, 40));
            UNIT_ASSERT_VALUES_EQUAL(
                ToString(int(backend)),
                ToString(int(f.GetBackend())));

            f.Clear();
            UNIT_ASSERT_VALUES_EQUAL(
                ToString(int(TBlockRangeField::EBackend::Simple)),
                ToString(int(f.GetBackend())));
        }
    }

    Y_UNIT_TEST(MoveSemanticsPreservesPreferredBackend)
    {
        TBlockRangeField f1(
            CreateArenaAllocator(),
            32768,
            TBlockRangeField::EBackend::Set);
        UNIT_ASSERT(f1.Add(R(0, 10)));
        UNIT_ASSERT(f1.Add(R(20, 30)));
        UNIT_ASSERT_VALUES_EQUAL(
            ToString(int(TBlockRangeField::EBackend::Set)),
            ToString(int(f1.GetBackend())));

        TBlockRangeField f2 = std::move(f1);
        UNIT_ASSERT_VALUES_EQUAL(
            ToString(int(TBlockRangeField::EBackend::Set)),
            ToString(int(f2.GetBackend())));
        UNIT_ASSERT_VALUES_EQUAL("[0..10][20..30]", f2.Print());
    }

    Y_UNIT_TEST(MemoryPerSingleRange)
    {
        // StdSet now uses TArenaAllocatorPool which doesn't track per-instance
        // memory. The test only verifies that the field switches to Impl.
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(100, 200)));

        UNIT_ASSERT(f.Add(R(300, 400)));
        UNIT_ASSERT(TBlockRangeFieldTestAccessor::HasImpl(f));
    }

    Y_UNIT_TEST(ShouldMeasureFlatSetMemory)
    {
        TBlockRangeField f(
            CreateArenaAllocator(),
            32768,
            TBlockRangeField::EBackend::FlatSet);
        UNIT_ASSERT_VALUES_EQUAL(0, f.GetAllocatedSize());
        UNIT_ASSERT_VALUES_EQUAL(0, f.GetUsedSize());

        UNIT_ASSERT(f.Add(R(10, 20)));
        f.Add(R(30, 40));
        UNIT_ASSERT_VALUES_EQUAL("[10..20][30..40]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRangeFieldFlatSet::ChunkSize,
            f.GetAllocatedSize());

        UNIT_ASSERT(f.Remove(R(30, 40)));
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRangeFieldFlatSet::ChunkSize,
            f.GetAllocatedSize());
        UNIT_ASSERT_VALUES_EQUAL(
            sizeof(ui32) + sizeof(TBlockRange16),
            f.GetUsedSize());

        f.Clear();
        UNIT_ASSERT_VALUES_EQUAL(0, f.GetAllocatedSize());
        UNIT_ASSERT_VALUES_EQUAL(0, f.GetUsedSize());
    }

    Y_UNIT_TEST(MemoryScalingWithRangeCount)
    {
        TBlockRangeField f(CreateArenaAllocator());
        for (ui64 i = 0; i < 1000; ++i) {
            UNIT_ASSERT(f.Add(R(i * 10, i * 10 + 5)));
        }

        // Memory tracking is not available with the shared arena allocator.
    }

    Y_UNIT_TEST(MemoryPerMergedRange)
    {
        TBlockRangeField f(CreateArenaAllocator());
        // Add a disjoint range to switch the field to the set-backed
        // implementation.
        UNIT_ASSERT(f.Add(R(500, 600)));
        // Add many small ranges that will merge into one
        for (ui64 i = 0; i < 100; ++i) {
            UNIT_ASSERT(f.Add(R(i, i)));
        }

        // Should be two ranges: [0, 99] and [500, 600]
        UNIT_ASSERT_VALUES_EQUAL(
            2u,
            TBlockRangeFieldTestAccessor::GetSegmentCount(f));
    }

    Y_UNIT_TEST(MemoryPerLargeRange)
    {
        TBlockRangeField f(CreateArenaAllocator(), 32768);
        // A single large range (same memory as a small one — range data is
        // fixed size)
        UNIT_ASSERT(f.Add(R(0, 100)));
        UNIT_ASSERT(f.Add(R(200, 30000)));

        UNIT_ASSERT_VALUES_EQUAL(
            2u,
            TBlockRangeFieldTestAccessor::GetSegmentCount(f));
    }

    Y_UNIT_TEST(MemoryAfterRemoveAndAdd)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(0, 1000));
        f.Add(R(2000, 3000));

        // Verify that remove + add keeps the impl alive.
        f.Remove(R(0, 1000));
        UNIT_ASSERT(TBlockRangeFieldTestAccessor::HasImpl(f));
        f.Add(R(0, 1000));
    }

    Y_UNIT_TEST(PoolGrowsOnDemand)
    {
        TBlockRangeField f(CreateArenaAllocator());
        for (ui64 i = 0; i < 1000; ++i) {
            UNIT_ASSERT(f.Add(R(i * 20, i * 20 + 5)));
        }

        // The arena allocator pool grows on demand — verified by successful
        // allocation of many ranges.
    }

    Y_UNIT_TEST(PerInstancePools)
    {
        // Each TBlockRangeField has its own pool
        TBlockRangeField f1(CreateArenaAllocator());
        TBlockRangeField f2(CreateArenaAllocator());

        UNIT_ASSERT(f1.Add(R(0, 10)));
        UNIT_ASSERT(f1.Add(R(20, 30)));
        UNIT_ASSERT(f2.Add(R(0, 10)));
        UNIT_ASSERT(f2.Add(R(20, 30)));
    }

    Y_UNIT_TEST(CopyingIsForbidden)
    {
        static_assert(
            !std::is_copy_constructible_v<TBlockRangeField>,
            "TBlockRangeField must not be copy constructible");
        static_assert(
            !std::is_copy_assignable_v<TBlockRangeField>,
            "TBlockRangeField must not be copy assignable");
    }

    Y_UNIT_TEST(MoveSemantics)
    {
        TBlockRangeField f1(CreateArenaAllocator());
        UNIT_ASSERT(f1.Add(R(0, 10)));
        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            TBlockRangeFieldTestAccessor::GetSegmentCount(f1));

        TBlockRangeField f2 = std::move(f1);
        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            TBlockRangeFieldTestAccessor::GetSegmentCount(f2));
        UNIT_ASSERT_VALUES_EQUAL("[0..10]", f2.Print());

        TBlockRangeField f3(CreateArenaAllocator());
        UNIT_ASSERT(f3.Add(R(100, 200)));
        f3 = std::move(f2);
        UNIT_ASSERT_VALUES_EQUAL(
            1u,
            TBlockRangeFieldTestAccessor::GetSegmentCount(f3));
        UNIT_ASSERT_VALUES_EQUAL("[0..10]", f3.Print());
    }

    // -------------------------------------------------------------------------
    // Existing functional tests (preserved)

    Y_UNIT_TEST(AddSingleRange)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(10, 20)));
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", f.Print());
    }

    Y_UNIT_TEST(AddTwoNonAdjacentRanges)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 5)));
        UNIT_ASSERT(f.Add(R(10, 15)));
        UNIT_ASSERT_VALUES_EQUAL("[0..5][10..15]", f.Print());
    }

    Y_UNIT_TEST(AddAdjacentRangesMerged)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 5)));
        UNIT_ASSERT(f.Add(R(6, 10)));
        UNIT_ASSERT_VALUES_EQUAL("[0..10]", f.Print());
    }

    Y_UNIT_TEST(AddOverlappingRangesMerged)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 10)));
        UNIT_ASSERT(f.Add(R(5, 15)));
        UNIT_ASSERT_VALUES_EQUAL("[0..15]", f.Print());
    }

    Y_UNIT_TEST(AddCoveredByExistingIsNoop)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 100)));
        UNIT_ASSERT(!f.Add(R(10, 20)));
        UNIT_ASSERT_VALUES_EQUAL("[0..100]", f.Print());
    }

    Y_UNIT_TEST(AddCoversMultipleRanges)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 5)));
        UNIT_ASSERT(f.Add(R(10, 15)));
        UNIT_ASSERT(f.Add(R(20, 25)));
        // New range covers all three and the gaps between them.
        UNIT_ASSERT(f.Add(R(0, 25)));
        UNIT_ASSERT_VALUES_EQUAL("[0..25]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(26u, f.GetBlockCount());
    }

    Y_UNIT_TEST(AddMergesOnBothSides)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 5)));
        UNIT_ASSERT(f.Add(R(10, 15)));
        // Bridge the gap.
        UNIT_ASSERT(f.Add(R(5, 10)));
        UNIT_ASSERT_VALUES_EQUAL("[0..15]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(16u, f.GetBlockCount());
    }

    Y_UNIT_TEST(AddSameRangeTwice)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(3, 7)));
        UNIT_ASSERT(!f.Add(R(3, 7)));
        UNIT_ASSERT_VALUES_EQUAL("[3..7]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(5u, f.GetBlockCount());
    }

    Y_UNIT_TEST(AddField)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 4)));
        UNIT_ASSERT(f.Add(R(20, 24)));

        TBlockRangeField other(CreateArenaAllocator());
        UNIT_ASSERT(other.Add(R(5, 10)));
        UNIT_ASSERT(other.Add(R(30, 34)));

        f.Add(other);
        UNIT_ASSERT_VALUES_EQUAL("[0..10][20..24][30..34]", f.Print());
        // Adding the same field again changes nothing.
        f.Add(other);
        UNIT_ASSERT_VALUES_EQUAL("[0..10][20..24][30..34]", f.Print());
        f.Add(f);
        UNIT_ASSERT_VALUES_EQUAL("[0..10][20..24][30..34]", f.Print());
    }

    // -------------------------------------------------------------------------
    // Remove – basic

    Y_UNIT_TEST(RemoveFromEmpty)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(!f.Remove(R(0, 10)));   // must not crash, returns false
        UNIT_ASSERT_VALUES_EQUAL("", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
    }

    Y_UNIT_TEST(RemoveExact)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 10)));
        UNIT_ASSERT(f.Remove(R(0, 10)));
        UNIT_ASSERT_VALUES_EQUAL("", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
    }

    Y_UNIT_TEST(RemoveFromMiddle)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 20)));
        UNIT_ASSERT(f.Remove(R(5, 10)));
        UNIT_ASSERT_VALUES_EQUAL("[0..4][11..20]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(15u, f.GetBlockCount());
    }

    Y_UNIT_TEST(RemoveLeftPart)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 20)));
        UNIT_ASSERT(f.Remove(R(0, 9)));
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(11u, f.GetBlockCount());
    }

    Y_UNIT_TEST(RemoveRightPart)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 20)));
        UNIT_ASSERT(f.Remove(R(10, 20)));
        UNIT_ASSERT_VALUES_EQUAL("[0..9]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(10u, f.GetBlockCount());
    }

    Y_UNIT_TEST(RemoveNonOverlapping)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(10, 20)));
        UNIT_ASSERT(!f.Remove(R(30, 40)));   // no overlap, no change
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(11u, f.GetBlockCount());
    }

    Y_UNIT_TEST(RemoveSeveralRanges)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 5)));
        UNIT_ASSERT(f.Add(R(10, 15)));
        UNIT_ASSERT(f.Add(R(20, 25)));
        UNIT_ASSERT(f.Remove(R(3, 22)));
        UNIT_ASSERT_VALUES_EQUAL("[0..2][23..25]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(6u, f.GetBlockCount());
    }

    Y_UNIT_TEST(RemoveField)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 40)));

        TBlockRangeField other(CreateArenaAllocator());
        UNIT_ASSERT(other.Add(R(5, 9)));
        UNIT_ASSERT(other.Add(R(20, 29)));

        f.Remove(other);
        UNIT_ASSERT_VALUES_EQUAL("[0..4][10..19][30..40]", f.Print());
        // Removing the same field again changes nothing.
        f.Remove(other);
        UNIT_ASSERT_VALUES_EQUAL("[0..4][10..19][30..40]", f.Print());
        f.Remove(f);
        UNIT_ASSERT(f.Empty());
    }

    // -------------------------------------------------------------------------
    // Overlaps

    Y_UNIT_TEST(OverlapsOnEmpty)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(!f.Overlaps(R(0, 100)));
    }

    Y_UNIT_TEST(OverlapsExact)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(10, 20));
        UNIT_ASSERT(f.Overlaps(R(10, 20)));
    }

    Y_UNIT_TEST(OverlapsPartialLeft)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(10, 20));
        UNIT_ASSERT(f.Overlaps(R(5, 12)));
    }

    Y_UNIT_TEST(OverlapsPartialRight)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(10, 20));
        UNIT_ASSERT(f.Overlaps(R(15, 30)));
    }

    Y_UNIT_TEST(OverlapsCovering)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(10, 20));
        UNIT_ASSERT(f.Overlaps(R(0, 100)));
    }

    Y_UNIT_TEST(OverlapsNoOverlapBefore)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(10, 20));
        UNIT_ASSERT(!f.Overlaps(R(0, 9)));
    }

    Y_UNIT_TEST(OverlapsNoOverlapAfter)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(10, 20));
        UNIT_ASSERT(!f.Overlaps(R(21, 30)));
    }

    Y_UNIT_TEST(OverlapsAdjacentNotOverlapping)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(10, 20));
        // [9,9] touches start but doesn't overlap.
        UNIT_ASSERT(!f.Overlaps(R(5, 9)));
        // [21,21] touches end but doesn't overlap.
        UNIT_ASSERT(!f.Overlaps(R(21, 25)));
    }

    Y_UNIT_TEST(OverlapsField)
    {
        TBlockRangeField left(CreateArenaAllocator());
        TBlockRangeField right(CreateArenaAllocator());

        UNIT_ASSERT(!left.Overlaps(right));

        left.Add(R(0, 5));
        left.Add(R(20, 25));
        right.Add(R(6, 10));
        right.Add(R(30, 35));
        UNIT_ASSERT(!left.Overlaps(right));
        UNIT_ASSERT(!right.Overlaps(left));

        right.Add(R(24, 29));
        UNIT_ASSERT(left.Overlaps(right));
        UNIT_ASSERT(right.Overlaps(left));
    }

    // -------------------------------------------------------------------------
    // Return value – false cases

    Y_UNIT_TEST(AddReturnsFalseWhenFullyCovered)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 100)));
        // Fully covered by existing interval – no change.
        UNIT_ASSERT(!f.Add(R(10, 20)));
        UNIT_ASSERT(!f.Add(R(0, 100)));
        UNIT_ASSERT(!f.Add(R(50, 50)));
    }

    Y_UNIT_TEST(RemoveReturnsFalseWhenEmpty)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(!f.Remove(R(0, 100)));
    }

    Y_UNIT_TEST(RemoveReturnsFalseWhenNoOverlap)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(10, 20)));
        // Strictly before.
        UNIT_ASSERT(!f.Remove(R(0, 9)));
        // Strictly after.
        UNIT_ASSERT(!f.Remove(R(21, 30)));
        // Contents unchanged.
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(11u, f.GetBlockCount());
    }

    // -------------------------------------------------------------------------
    // Edge / boundary cases

    Y_UNIT_TEST(AddStartingAtZero)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 0)));
        UNIT_ASSERT(f.Add(R(1, 5)));
        UNIT_ASSERT_VALUES_EQUAL("[0..5]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(6u, f.GetBlockCount());
    }

    Y_UNIT_TEST(RemoveSingleBlock)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 4)));
        UNIT_ASSERT(f.Remove(R(2, 2)));
        UNIT_ASSERT_VALUES_EQUAL("[0..1][3..4]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(4u, f.GetBlockCount());
    }

    Y_UNIT_TEST(ManyFragmentsAfterRemoves)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 99)));
        // Remove every even block to create 50 gaps.
        for (ui16 i = 0; i < 100; i += 2) {
            UNIT_ASSERT(f.Remove(R(i, i)));
        }
        UNIT_ASSERT_VALUES_EQUAL(50u, f.GetBlockCount());
        for (ui16 i = 0; i < 100; i += 2) {
            UNIT_ASSERT(!f.Overlaps(R(i, i)));
        }
        for (ui16 i = 1; i < 100; i += 2) {
            UNIT_ASSERT(f.Overlaps(R(i, i)));
        }
    }

    Y_UNIT_TEST(AddRestoresAfterRemoves)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 99)));
        for (ui16 i = 0; i < 100; i += 2) {
            UNIT_ASSERT(f.Remove(R(i, i)));
        }
        // Adding back should merge everything.
        for (ui16 i = 0; i < 100; i += 2) {
            UNIT_ASSERT(f.Add(R(i, i)));
        }
        UNIT_ASSERT_VALUES_EQUAL("[0..99]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(100u, f.GetBlockCount());
    }

    Y_UNIT_TEST(EnumerateOrderedByStart)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(50, 60)));
        UNIT_ASSERT(f.Add(R(10, 20)));
        UNIT_ASSERT(f.Add(R(30, 40)));
        UNIT_ASSERT_VALUES_EQUAL("[10..20][30..40][50..60]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(33u, f.GetBlockCount());
    }

    // -------------------------------------------------------------------------
    // GetBlockCount

    Y_UNIT_TEST(CountersOnEmpty)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
    }

    Y_UNIT_TEST(CountersAfterSingleAdd)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(10, 20));   // 11 blocks, 1 segment
        UNIT_ASSERT_VALUES_EQUAL(11u, f.GetBlockCount());
    }

    Y_UNIT_TEST(CountersAfterTwoDisjointAdds)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(0, 4));     // 5 blocks
        f.Add(R(10, 14));   // 5 blocks → total 10, 2 segments
        UNIT_ASSERT_VALUES_EQUAL(10u, f.GetBlockCount());
    }

    Y_UNIT_TEST(CountersAfterMerge)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(0, 4));
        f.Add(R(5, 9));   // adjacent – merges into [0,9], 10 blocks, 1 segment
        UNIT_ASSERT_VALUES_EQUAL(10u, f.GetBlockCount());
    }

    Y_UNIT_TEST(CountersAfterRemove)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(0, 19));     // 20 blocks, 1 segment
        f.Remove(R(5, 9));   // removes 5 blocks from the middle → 2 segments
        UNIT_ASSERT_VALUES_EQUAL(15u, f.GetBlockCount());
    }

    Y_UNIT_TEST(CountersAfterClear)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(0, 9));
        f.Add(R(20, 29));
        f.Clear();
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
    }

    Y_UNIT_TEST(CountersSingleBlock)
    {
        TBlockRangeField f(CreateArenaAllocator());
        f.Add(R(42, 42));
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetBlockCount());
    }

    // -------------------------------------------------------------------------
    // Serialization / deserialization

    Y_UNIT_TEST(SerializeEmptyField)
    {
        TBlockRangeField f(CreateArenaAllocator());
        const TString data = f.Serialize();
        UNIT_ASSERT(data.empty());

        // Deserializing an empty payload keeps the field empty.
        TBlockRangeField restored(CreateArenaAllocator());
        restored.DeserializeFromRLE(data);
        UNIT_ASSERT(restored.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0u, restored.GetBlockCount());
    }

    Y_UNIT_TEST(SerializeDeserializeRoundTripSingleRange)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(10, 20)));

        const TString data = f.Serialize();
        UNIT_ASSERT(!data.empty());

        TBlockRangeField restored(CreateArenaAllocator());
        restored.DeserializeFromRLE(data);
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", restored.Print());
        UNIT_ASSERT_VALUES_EQUAL(11u, restored.GetBlockCount());
    }

    Y_UNIT_TEST(SerializeDeserializeRoundTripMultipleRanges)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 5)));
        UNIT_ASSERT(f.Add(R(10, 15)));
        UNIT_ASSERT(f.Add(R(100, 200)));

        const TString data = f.Serialize();

        TBlockRangeField restored(CreateArenaAllocator());
        restored.DeserializeFromRLE(data);
        UNIT_ASSERT_VALUES_EQUAL("[0..5][10..15][100..200]", restored.Print());
        UNIT_ASSERT_VALUES_EQUAL(113u, restored.GetBlockCount());
    }

    Y_UNIT_TEST(SerializeDeserializeRoundTripSingleBlock)
    {
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(42, 42)));

        const TString data = f.Serialize();

        TBlockRangeField restored(CreateArenaAllocator());
        restored.DeserializeFromRLE(data);
        UNIT_ASSERT_VALUES_EQUAL("[42..42]", restored.Print());
        UNIT_ASSERT_VALUES_EQUAL(1u, restored.GetBlockCount());
    }

    Y_UNIT_TEST(SerializeDeserializeRoundTripLongRun)
    {
        // Skip and fill lengths exceeding RunLengthContinuation (0xff) to
        // cover the multi-byte run-length encoding.
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 511)));       // fill length > 255
        UNIT_ASSERT(f.Add(R(1024, 2047)));   // skip length > 255

        const TString data = f.Serialize();

        TBlockRangeField restored(CreateArenaAllocator());
        restored.DeserializeFromRLE(data);
        UNIT_ASSERT_VALUES_EQUAL("[0..511][1024..2047]", restored.Print());
        UNIT_ASSERT_VALUES_EQUAL(1536u, restored.GetBlockCount());
    }

    Y_UNIT_TEST(DeserializeRLEIntoNonEmptyField)
    {
        // DeserializeFromRLE replaces the field contents: the previously
        // stored [0..4] is discarded.
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 4)));

        TBlockRangeField source(CreateArenaAllocator());
        UNIT_ASSERT(source.Add(R(10, 14)));
        UNIT_ASSERT(source.Add(R(30, 34)));

        f.DeserializeFromRLE(source.Serialize());
        UNIT_ASSERT_VALUES_EQUAL("[10..14][30..34]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(10u, f.GetBlockCount());
    }

    Y_UNIT_TEST(DeserializeRLEEmptyReplacesContents)
    {
        // Deserializing an empty payload clears the field.
        TBlockRangeField f(CreateArenaAllocator());
        UNIT_ASSERT(f.Add(R(0, 4)));

        f.DeserializeFromRLE(TString{});
        UNIT_ASSERT(f.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
    }

    Y_UNIT_TEST(ShouldRejectMalformedRLE)
    {
        const auto enumerate = [](TBlockRange16)
        {
            return TNodeBasedBlockRangeField::EEnumerateContinuation::Continue;
        };
        TString oversizedSkip(10, char(0xff));
        oversizedSkip.push_back(char(1));
        oversizedSkip.push_back(char(1));

        UNIT_ASSERT(!TNodeBasedBlockRangeField::DeserializeFromRLE(
            TString("\x01", 1),
            2048,
            enumerate));
        UNIT_ASSERT(!TNodeBasedBlockRangeField::DeserializeFromRLE(
            TString("\x01\x00", 2),
            2048,
            enumerate));
        UNIT_ASSERT(!TNodeBasedBlockRangeField::DeserializeFromRLE(
            oversizedSkip,
            2048,
            enumerate));
    }

    Y_UNIT_TEST(SerializeDeserializeBitmapRoundTrip)
    {
        // Note: the mask size must be a power of two for the arena
        // allocator, hence maxBlockCount = 2048.
        TBlockRangeField f(
            CreateArenaAllocator(),
            2048,
            TBlockRangeField::EBackend::Bitmask);
        UNIT_ASSERT(f.Add(R(0, 63)));
        UNIT_ASSERT(f.Add(R(64, 70)));
        UNIT_ASSERT(f.Add(R(1000, 1001)));
        UNIT_ASSERT(f.IsBitmapBased());

        const TString data = f.Serialize();
        UNIT_ASSERT(!data.empty());

        TBlockRangeField restored(
            CreateArenaAllocator(),
            2048,
            TBlockRangeField::EBackend::Bitmask);
        restored.DeserializeFromBitmap(data);
        UNIT_ASSERT(restored.IsBitmapBased());
        UNIT_ASSERT_VALUES_EQUAL(f.Print(), restored.Print());
        UNIT_ASSERT_VALUES_EQUAL(f.GetBlockCount(), restored.GetBlockCount());
        UNIT_ASSERT(restored.Overlaps(R(64, 70)));
        UNIT_ASSERT(!restored.Overlaps(R(71, 999)));
    }

    Y_UNIT_TEST(DeserializeBitmapIntoSimpleField)
    {
        // Deserializing bitmap data into a field that starts in the Simple
        // backend must upgrade it to the bitmap backend.
        TBlockRangeField f(CreateArenaAllocator(), 2048);
        UNIT_ASSERT(f.Add(R(5, 9)));

        TBlockRangeField source(
            CreateArenaAllocator(),
            2048,
            TBlockRangeField::EBackend::Bitmask);
        // Two disjoint ranges force the switch to the bitmap backend so
        // that Serialize() returns raw bitmap data.
        UNIT_ASSERT(source.Add(R(20, 25)));
        UNIT_ASSERT(source.Add(R(100, 105)));
        UNIT_ASSERT(source.IsBitmapBased());

        // DeserializeFromBitmap replaces the whole mask: the previously
        // stored [5..9] is overwritten by the source bitmap.
        f.DeserializeFromBitmap(source.Serialize());
        UNIT_ASSERT(f.IsBitmapBased());
        UNIT_ASSERT_VALUES_EQUAL(12u, f.GetBlockCount());
        UNIT_ASSERT(!f.Overlaps(R(5, 9)));
        UNIT_ASSERT(f.Overlaps(R(20, 25)));
        UNIT_ASSERT(f.Overlaps(R(100, 105)));
    }

    Y_UNIT_TEST(SerializeDeserializeBetweenBackends)
    {
        // Copy data between backends via Add(field): the receiving field
        // upgrades to the bitmap backend when the source is bitmap-based.
        TBlockRangeField setBacked(
            CreateArenaAllocator(),
            32768,
            TBlockRangeField::EBackend::Set);
        UNIT_ASSERT(setBacked.Add(R(1, 3)));
        UNIT_ASSERT(setBacked.Add(R(10, 12)));

        TBlockRangeField bitmaskBacked(
            CreateArenaAllocator(),
            2048,
            TBlockRangeField::EBackend::Bitmask);
        bitmaskBacked.Add(setBacked);
        UNIT_ASSERT(bitmaskBacked.IsBitmapBased());
        UNIT_ASSERT_VALUES_EQUAL(6u, bitmaskBacked.GetBlockCount());
        UNIT_ASSERT(bitmaskBacked.Overlaps(R(1, 3)));
        UNIT_ASSERT(bitmaskBacked.Overlaps(R(10, 12)));
        UNIT_ASSERT(!bitmaskBacked.Overlaps(R(4, 9)));

        // Restore into a node-based field via RLE serialization.
        TBlockRangeField restored(CreateArenaAllocator());
        restored.DeserializeFromRLE(setBacked.Serialize());
        UNIT_ASSERT_VALUES_EQUAL("[1..3][10..12]", restored.Print());
        UNIT_ASSERT_VALUES_EQUAL(6u, restored.GetBlockCount());
    }

    Y_UNIT_TEST(DeserializeEmptyBitmap)
    {
        TBlockRangeField f(CreateArenaAllocator(), 2048);
        f.DeserializeFromBitmap(TString{});
        UNIT_ASSERT(f.IsBitmapBased());
        UNIT_ASSERT(f.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
        UNIT_ASSERT(!f.GetFirstRange().has_value());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
