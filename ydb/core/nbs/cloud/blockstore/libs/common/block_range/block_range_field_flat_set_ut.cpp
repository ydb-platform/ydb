#include "block_range_field_flat_set.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TBlockRangeFieldFlatSet::TRange R(ui16 start, ui16 end)
{
    return TBlockRangeFieldFlatSet::TRange::MakeClosedInterval(start, end);
}

std::shared_ptr<IArenaAllocator> MakeAllocator()
{
    return CreateArenaAllocator();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeFieldFlatSetTest)
{
    // -------------------------------------------------------------------------
    // Basic Add

    Y_UNIT_TEST(AddSingleRange)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", f.Print());
    }

    Y_UNIT_TEST(AddTwoNonAdjacentRanges)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 5), &changed));
        UNIT_ASSERT(f.TryAdd(R(10, 15), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..5][10..15]", f.Print());
    }

    Y_UNIT_TEST(AddAdjacentRangesMerged)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 5), &changed));
        UNIT_ASSERT(f.TryAdd(R(6, 10), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..10]", f.Print());
    }

    Y_UNIT_TEST(AddOverlappingRangesMerged)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 10), &changed));
        UNIT_ASSERT(f.TryAdd(R(5, 15), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..15]", f.Print());
    }

    Y_UNIT_TEST(AddCoveredByExistingIsNoop)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 100), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..100]", f.Print());
        UNIT_ASSERT(!f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..100]", f.Print());
    }

    Y_UNIT_TEST(AddCoversMultipleRanges)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 5), &changed));
        UNIT_ASSERT(f.TryAdd(R(10, 15), &changed));
        UNIT_ASSERT(f.TryAdd(R(20, 25), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..5][10..15][20..25]", f.Print());
        UNIT_ASSERT(f.TryAdd(R(0, 25), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..25]", f.Print());
    }

    Y_UNIT_TEST(AddMergesOnBothSides)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 5), &changed));
        UNIT_ASSERT(f.TryAdd(R(10, 15), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..5][10..15]", f.Print());
        UNIT_ASSERT(f.TryAdd(R(5, 10), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..15]", f.Print());
    }

    Y_UNIT_TEST(AddSameRangeTwice)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(3, 7), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[3..7]", f.Print());
        UNIT_ASSERT(!f.TryAdd(R(3, 7), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT_VALUES_EQUAL("[3..7]", f.Print());
    }

    Y_UNIT_TEST(AddField)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 4), &changed));
        UNIT_ASSERT(f.TryAdd(R(20, 24), &changed));

        TBlockRangeFieldFlatSet other{CreateArenaAllocator()};
        bool otherChanged = false;
        UNIT_ASSERT(other.TryAdd(R(5, 10), &otherChanged));
        UNIT_ASSERT(other.TryAdd(R(30, 34), &otherChanged));

        // Merge ranges from other into f via enumerate
        other.Enumerate(
            [&](TBlockRangeFieldFlatSet::TRange r)
            {
                f.TryAdd(r, &changed);
                return TBlockRangeFieldFlatSet::EEnumerateContinuation::
                    Continue;
            });
        UNIT_ASSERT_VALUES_EQUAL("[0..4][5..10][20..24][30..34]", f.Print());
    }

    // -------------------------------------------------------------------------
    // Remove

    Y_UNIT_TEST(RemoveFromEmpty)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        UNIT_ASSERT(!f.TryRemove(R(0, 10), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT(f.Print().empty());
    }

    Y_UNIT_TEST(RemoveExact)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 10), &changed);
        UNIT_ASSERT(f.TryRemove(R(0, 10), &changed));
        UNIT_ASSERT(f.Print().empty());
    }

    Y_UNIT_TEST(RemoveFromMiddle)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 20), &changed);
        UNIT_ASSERT(f.TryRemove(R(5, 10), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..4][11..20]", f.Print());
    }

    Y_UNIT_TEST(RemoveLeftPart)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 20), &changed);
        UNIT_ASSERT(f.TryRemove(R(0, 9), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", f.Print());
    }

    Y_UNIT_TEST(RemoveRightPart)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 20), &changed);
        UNIT_ASSERT(f.TryRemove(R(10, 20), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..9]", f.Print());
    }

    Y_UNIT_TEST(RemoveNonOverlapping)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        UNIT_ASSERT(!f.TryRemove(R(30, 40), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", f.Print());
    }

    Y_UNIT_TEST(RemoveSeveralRanges)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 5), &changed);
        f.TryAdd(R(10, 15), &changed);
        f.TryAdd(R(20, 25), &changed);
        UNIT_ASSERT(f.TryRemove(R(3, 22), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..2][23..25]", f.Print());
    }

    // -------------------------------------------------------------------------
    // Overlaps

    Y_UNIT_TEST(OverlapsOnEmpty)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        UNIT_ASSERT(!f.Overlaps(R(0, 100)));
    }

    Y_UNIT_TEST(OverlapsExact)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        UNIT_ASSERT(f.Overlaps(R(10, 20)));
    }

    Y_UNIT_TEST(OverlapsPartialLeft)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        UNIT_ASSERT(f.Overlaps(R(5, 12)));
    }

    Y_UNIT_TEST(OverlapsPartialRight)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        UNIT_ASSERT(f.Overlaps(R(15, 30)));
    }

    Y_UNIT_TEST(OverlapsCovering)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        UNIT_ASSERT(f.Overlaps(R(0, 100)));
    }

    Y_UNIT_TEST(OverlapsNoOverlapBefore)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        UNIT_ASSERT(!f.Overlaps(R(0, 9)));
    }

    Y_UNIT_TEST(OverlapsNoOverlapAfter)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        UNIT_ASSERT(!f.Overlaps(R(21, 30)));
    }

    Y_UNIT_TEST(OverlapsAdjacentNotOverlapping)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        UNIT_ASSERT(!f.Overlaps(R(5, 9)));
        UNIT_ASSERT(!f.Overlaps(R(21, 25)));
    }

    // -------------------------------------------------------------------------
    // Return value semantics

    Y_UNIT_TEST(AddReturnsFalseWhenFullyCovered)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 100), &changed);
        UNIT_ASSERT(!f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT(!f.TryAdd(R(0, 100), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT(!f.TryAdd(R(50, 50), &changed));
        UNIT_ASSERT(!changed);
    }

    Y_UNIT_TEST(RemoveReturnsFalseWhenEmpty)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        UNIT_ASSERT(!f.TryRemove(R(0, 100), &changed));
        UNIT_ASSERT(!changed);
    }

    Y_UNIT_TEST(RemoveReturnsFalseWhenNoOverlap)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        UNIT_ASSERT(!f.TryRemove(R(0, 9), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT(!f.TryRemove(R(21, 30), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", f.Print());
    }

    // -------------------------------------------------------------------------
    // Edge / boundary cases

    Y_UNIT_TEST(AddStartingAtZero)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 0), &changed);
        f.TryAdd(R(1, 5), &changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..5]", f.Print());
    }

    Y_UNIT_TEST(RemoveSingleBlock)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 4), &changed);
        UNIT_ASSERT(f.TryRemove(R(2, 2), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..1][3..4]", f.Print());
    }

    Y_UNIT_TEST(ManyFragmentsAfterRemoves)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 99), &changed);
        for (ui16 i = 0; i < 100; i += 2) {
            f.TryRemove(R(i, i), &changed);
        }
        TStringBuilder expected;
        for (ui16 i = 1; i < 100; i += 2) {
            expected << "[" << i << ".." << i << "]";
        }
        UNIT_ASSERT_VALUES_EQUAL(expected, f.Print());
    }

    Y_UNIT_TEST(AddRestoresAfterRemoves)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 99), &changed);
        for (ui16 i = 0; i < 100; i += 2) {
            f.TryRemove(R(i, i), &changed);
        }
        for (ui16 i = 0; i < 100; i += 2) {
            f.TryAdd(R(i, i), &changed);
        }
        UNIT_ASSERT_VALUES_EQUAL("[0..99]", f.Print());
    }

    Y_UNIT_TEST(EnumerateOrderedByStart)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(50, 60), &changed);
        f.TryAdd(R(10, 20), &changed);
        f.TryAdd(R(30, 40), &changed);
        UNIT_ASSERT_VALUES_EQUAL("[10..20][30..40][50..60]", f.Print());
    }

    Y_UNIT_TEST(EnumerateStopsAtCondition)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 5), &changed);
        f.TryAdd(R(10, 15), &changed);
        f.TryAdd(R(20, 25), &changed);

        TVector<TBlockRangeFieldFlatSet::TRange> seen;
        f.Enumerate(
            [&](TBlockRangeFieldFlatSet::TRange r)
            {
                seen.push_back(r);
                return r.Start >= 10 ? TBlockRangeFieldFlatSet::
                                           EEnumerateContinuation::Stop
                                     : TBlockRangeFieldFlatSet::
                                           EEnumerateContinuation::Continue;
            });

        UNIT_ASSERT_VALUES_EQUAL(2u, seen.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 5), seen[0]);
        UNIT_ASSERT_VALUES_EQUAL(R(10, 15), seen[1]);
    }

    // -------------------------------------------------------------------------
    // Counters

    Y_UNIT_TEST(CountersOnEmpty)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersAfterSingleAdd)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        UNIT_ASSERT_VALUES_EQUAL(11u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersAfterTwoDisjointAdds)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 4), &changed);
        f.TryAdd(R(10, 14), &changed);
        UNIT_ASSERT_VALUES_EQUAL(10u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(2u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersAfterMerge)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 4), &changed);
        f.TryAdd(R(5, 9), &changed);
        UNIT_ASSERT_VALUES_EQUAL(10u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersAfterRemove)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 19), &changed);
        f.TryRemove(R(5, 9), &changed);
        UNIT_ASSERT_VALUES_EQUAL(15u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(2u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersAfterClear)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 9), &changed);
        f.TryAdd(R(20, 29), &changed);
        f.Clear();
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersSingleBlock)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(42, 42), &changed);
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersManyFragments)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 99), &changed);
        for (ui16 i = 0; i < 100; i += 2) {
            f.TryRemove(R(i, i), &changed);
        }
        UNIT_ASSERT_VALUES_EQUAL(50u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(50u, f.GetSegmentCount());
    }

    // -------------------------------------------------------------------------
    // Capacity exhaustion & reuse

    Y_UNIT_TEST(CapacityExhaustion)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        // Add 64 disjoint ranges (max capacity = 64 segments)
        for (ui16 i = 0; i < 64; ++i) {
            UNIT_ASSERT(f.TryAdd(R(i * 10, i * 10 + 5), &changed));
        }
        UNIT_ASSERT_VALUES_EQUAL(64u, f.GetSegmentCount());
        // 65th range should fail
        UNIT_ASSERT(!f.TryAdd(R(700, 705), &changed));
    }

    Y_UNIT_TEST(ReuseAfterClear)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        // Fill capacity
        for (ui16 i = 0; i < 64; ++i) {
            UNIT_ASSERT(f.TryAdd(R(i * 10, i * 10 + 5), &changed));
        }
        UNIT_ASSERT_VALUES_EQUAL(64u, f.GetSegmentCount());

        f.Clear();
        UNIT_ASSERT(f.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetSegmentCount());

        // Should be able to add again
        for (ui16 i = 0; i < 64; ++i) {
            UNIT_ASSERT(f.TryAdd(R(i * 10, i * 10 + 5), &changed));
        }
        UNIT_ASSERT_VALUES_EQUAL(64u, f.GetSegmentCount());
    }

    // -------------------------------------------------------------------------
    // Add range that extends predecessor only

    Y_UNIT_TEST(AddExtendsPredecessor)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        f.TryAdd(R(5, 15), &changed);
        UNIT_ASSERT_VALUES_EQUAL("[5..20]", f.Print());
    }

    // -------------------------------------------------------------------------
    // Add range that extends successor only

    Y_UNIT_TEST(AddExtendsSuccessor)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(10, 20), &changed);
        f.TryAdd(R(15, 30), &changed);
        UNIT_ASSERT_VALUES_EQUAL("[10..30]", f.Print());
    }

    // -------------------------------------------------------------------------
    // Insert in the middle without overlap

    Y_UNIT_TEST(AddNonAdjacentInMiddle)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 5), &changed);
        f.TryAdd(R(20, 25), &changed);
        f.TryAdd(R(10, 15), &changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..5][10..15][20..25]", f.Print());
    }

    // -------------------------------------------------------------------------
    // Remove that splits a range into two parts

    Y_UNIT_TEST(RemoveSplitsRange)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 100), &changed);
        UNIT_ASSERT(f.TryRemove(R(30, 60), &changed));
        UNIT_ASSERT_VALUES_EQUAL("[0..29][61..100]", f.Print());
    }

    // -------------------------------------------------------------------------
    // Multiple remove operations in sequence

    Y_UNIT_TEST(MultipleSequentialRemoves)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        f.TryAdd(R(0, 99), &changed);
        f.TryRemove(R(0, 9), &changed);
        f.TryRemove(R(20, 29), &changed);
        f.TryRemove(R(50, 59), &changed);
        f.TryRemove(R(80, 89), &changed);
        UNIT_ASSERT_VALUES_EQUAL("[10..19][30..49][60..79][90..99]", f.Print());
    }

    // -------------------------------------------------------------------------
    // Print on empty

    Y_UNIT_TEST(PrintEmpty)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        UNIT_ASSERT(f.Print().empty());
    }

    // -------------------------------------------------------------------------
    // Clear resets all state

    Y_UNIT_TEST(ClearResetsState)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldFlatSet f{allocator};
        bool changed = false;
        for (ui16 i = 0; i < 64; ++i) {
            f.TryAdd(R(i * 10, i * 10 + 5), &changed);
        }
        f.Clear();
        UNIT_ASSERT(f.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetSegmentCount());
    }

}   // Y_UNIT_TEST_SUITE

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
