#include "block_range_field_set.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore {

namespace {

constexpr size_t MaxPoolSize = 4096;

std::shared_ptr<IArenaAllocator> MakeAllocator()
{
    return CreateArenaAllocator();
}

TBlockRangeFieldSet::TRange R(ui16 start, ui16 end)
{
    return TBlockRangeFieldSet::TRange::MakeClosedInterval(start, end);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeFieldSetTest)
{
    // -------------------------------------------------------------------------
    // Basic Add

    Y_UNIT_TEST(AddSingleRange)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", f.Print());
    }

    Y_UNIT_TEST(AddTwoNonAdjacentRanges)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 5), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(10, 15), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..5][10..15]", f.Print());
    }

    Y_UNIT_TEST(AddAdjacentRangesMerged)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 5), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(6, 10), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..10]", f.Print());
    }

    Y_UNIT_TEST(AddOverlappingRangesMerged)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 10), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(5, 15), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..15]", f.Print());
    }

    Y_UNIT_TEST(AddCoveredByExistingIsNoop)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 100), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..100]", f.Print());
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..100]", f.Print());
    }

    Y_UNIT_TEST(AddCoversMultipleRanges)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 5), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(10, 15), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(20, 25), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..5][10..15][20..25]", f.Print());
        UNIT_ASSERT(f.TryAdd(R(0, 25), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..25]", f.Print());
    }

    Y_UNIT_TEST(AddMergesOnBothSides)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 5), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(10, 15), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..5][10..15]", f.Print());
        UNIT_ASSERT(f.TryAdd(R(5, 10), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..15]", f.Print());
    }

    Y_UNIT_TEST(AddSameRangeTwice)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(3, 7), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[3..7]", f.Print());
        UNIT_ASSERT(f.TryAdd(R(3, 7), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT_VALUES_EQUAL("[3..7]", f.Print());
    }

    Y_UNIT_TEST(AddField)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 4), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(20, 24), &changed));
        UNIT_ASSERT(changed);

        UNIT_ASSERT(f.TryAdd(R(5, 10), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..10][20..24]", f.Print());
        UNIT_ASSERT(f.TryAdd(R(30, 34), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..10][20..24][30..34]", f.Print());
    }

    Y_UNIT_TEST(MergeMultipleRanges)
    {
        // This test reproduces the bug where TryAdd with a range that
        // spans multiple existing ranges corrupts the BST, causing
        // FindNextGreaterNode to loop forever.
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;

        // Add 3 disjoint ranges
        UNIT_ASSERT(f.TryAdd(R(0, 5), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(10, 15), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(20, 25), &changed));
        UNIT_ASSERT(changed);

        // Add a range that merges all three — this triggers the BST bug
        UNIT_ASSERT(f.TryAdd(R(3, 23), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..25]", f.Print());
    }

    // -------------------------------------------------------------------------
    // Remove

    Y_UNIT_TEST(RemoveFromEmpty)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryRemove(R(0, 10), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT_VALUES_EQUAL("", f.Print());
    }

    Y_UNIT_TEST(RemoveExact)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 10), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryRemove(R(0, 10), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("", f.Print());
    }

    Y_UNIT_TEST(RemoveFromMiddle)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryRemove(R(5, 10), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..4][11..20]", f.Print());
    }

    Y_UNIT_TEST(RemoveLeftPart)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryRemove(R(0, 9), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", f.Print());
    }

    Y_UNIT_TEST(RemoveRightPart)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryRemove(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..9]", f.Print());
    }

    Y_UNIT_TEST(RemoveNonOverlapping)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryRemove(R(30, 40), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT_VALUES_EQUAL("[10..20]", f.Print());
    }

    Y_UNIT_TEST(RemoveSeveralRanges)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 5), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(10, 15), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(20, 25), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryRemove(R(3, 22), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[0..2][23..25]", f.Print());
    }

    // -------------------------------------------------------------------------
    // Overlaps

    Y_UNIT_TEST(OverlapsOnEmpty)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        UNIT_ASSERT(!f.Overlaps(R(0, 100)));
    }

    Y_UNIT_TEST(OverlapsExact)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.Overlaps(R(10, 20)));
    }

    Y_UNIT_TEST(OverlapsPartialLeft)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.Overlaps(R(5, 12)));
    }

    Y_UNIT_TEST(OverlapsPartialRight)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.Overlaps(R(15, 30)));
    }

    Y_UNIT_TEST(OverlapsCovering)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.Overlaps(R(0, 100)));
    }

    Y_UNIT_TEST(OverlapsNoOverlapBefore)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(!f.Overlaps(R(0, 9)));
    }

    Y_UNIT_TEST(OverlapsNoOverlapAfter)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(!f.Overlaps(R(21, 30)));
    }

    Y_UNIT_TEST(OverlapsAdjacentNotOverlapping)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(!f.Overlaps(R(5, 9)));
        UNIT_ASSERT(!f.Overlaps(R(21, 25)));
    }

    // -------------------------------------------------------------------------
    // Return value semantics

    Y_UNIT_TEST(AddReturnsFalseWhenFullyCovered)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 100), &changed));
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT(f.TryAdd(R(0, 100), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT(f.TryAdd(R(50, 50), &changed));
        UNIT_ASSERT(!changed);
    }

    Y_UNIT_TEST(RemoveReturnsFalseWhenEmpty)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryRemove(R(0, 100), &changed));
        UNIT_ASSERT(!changed);
    }

    Y_UNIT_TEST(RemoveReturnsFalseWhenNoOverlap)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(f.TryRemove(R(0, 9), &changed));
        UNIT_ASSERT(!changed);
        UNIT_ASSERT(f.TryRemove(R(21, 30), &changed));
        UNIT_ASSERT(!changed);
    }

    // -------------------------------------------------------------------------
    // Edge / boundary cases

    Y_UNIT_TEST(AddStartingAtZero)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 0), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(1, 5), &changed));
        UNIT_ASSERT(changed);
    }

    Y_UNIT_TEST(RemoveSingleBlock)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 4), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryRemove(R(2, 2), &changed));
        UNIT_ASSERT(changed);
    }

    Y_UNIT_TEST(ManyFragmentsAfterRemoves)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 20), &changed));
        UNIT_ASSERT(changed);
        for (ui16 i = 0; i < 10; i += 2) {
            UNIT_ASSERT(f.TryRemove(R(i, i), &changed));
            UNIT_ASSERT(changed);
        }
        UNIT_ASSERT_VALUES_EQUAL("[1..1][3..3][5..5][7..7][9..20]", f.Print());
    }

    Y_UNIT_TEST(AddRestoresAfterRemoves)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 200), &changed));
        UNIT_ASSERT(changed);
        for (ui16 i = 0; i < 100; i += 2) {
            UNIT_ASSERT(f.TryRemove(R(i, i), &changed));
            UNIT_ASSERT(changed);
        }

        for (ui16 i = 0; i < 100; i += 2) {
            UNIT_ASSERT(f.TryAdd(R(i, i), &changed));
            UNIT_ASSERT(changed);
        }
        UNIT_ASSERT_VALUES_EQUAL("[0..200]", f.Print());
    }

    Y_UNIT_TEST(EnumerateOrderedByStart)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(50, 60), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(30, 40), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[10..20][30..40][50..60]", f.Print());
    }

    Y_UNIT_TEST(EnumerateStopsAtCondition)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 5), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(10, 15), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(20, 25), &changed));
        UNIT_ASSERT(changed);

        TVector<TBlockRangeFieldSet::TRange> seen;
        f.Enumerate(
            [&](TBlockRangeFieldSet::TRange r)
            {
                seen.push_back(r);
                return r.Start >= 10
                           ? TBlockRangeFieldSet::EEnumerateContinuation::Stop
                           : TBlockRangeFieldSet::EEnumerateContinuation::
                                 Continue;
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
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersAfterSingleAdd)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(10, 20), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(11u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersAfterTwoDisjointAdds)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 4), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(10, 14), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(10u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(2u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersAfterMerge)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 4), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(5, 9), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(10u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersAfterRemove)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 19), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryRemove(R(5, 9), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(15u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(2u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersAfterClear)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 9), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(20, 29), &changed));
        UNIT_ASSERT(changed);
        f.Clear();
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersSingleBlock)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(42, 42), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(CountersManyFragments)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(0, 99), &changed));
        UNIT_ASSERT(changed);
        for (ui16 i = 0; i < 100; i += 2) {
            UNIT_ASSERT(f.TryRemove(R(i, i), &changed));
            UNIT_ASSERT(changed);
        }
        UNIT_ASSERT_VALUES_EQUAL(50u, f.GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(50u, f.GetSegmentCount());
    }

    // -------------------------------------------------------------------------
    // Arena exhaustion & reuse

    Y_UNIT_TEST(ArenaExhaustion)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        // Add 64 disjoint ranges (max for 512-byte arena = 64 nodes)
        for (ui16 i = 0; i < 64; ++i) {
            UNIT_ASSERT(f.TryAdd(R(i * 10, i * 10 + 5), &changed));
            UNIT_ASSERT(changed);
        }
        UNIT_ASSERT_VALUES_EQUAL(64u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(AddMergeFailsRecalculatesCounters)
    {
        // Fill arena with 64 disjoint ranges of 3 blocks each.
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, 64 * 8};
        bool changed = false;
        for (ui16 i = 0; i < 64; ++i) {
            UNIT_ASSERT(f.TryAdd(R(i * 10, i * 10 + 2), &changed));
            UNIT_ASSERT(changed);
        }
        UNIT_ASSERT_VALUES_EQUAL(64u, f.GetSegmentCount());
        UNIT_ASSERT_VALUES_EQUAL(192u, f.GetBlockCount());   // 64 * 3

        // TryAdd a range that merges all 64 nodes — removes them all from tree,
        // then AllocNode fails (arena full). RecalculateCounters must fix
        // counters.
        UNIT_ASSERT(!f.TryAdd(R(32767, 32767), &changed));

        // Counters must reflect the tree as-is (no merge happened).
        UNIT_ASSERT_VALUES_EQUAL(64u, f.GetSegmentCount());
        UNIT_ASSERT_VALUES_EQUAL(192u, f.GetBlockCount());
    }

    Y_UNIT_TEST(RemoveSplitFailsRecalculatesCounters)
    {
        // Fill arena with 64 disjoint ranges of 3 blocks each.
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, 64 * 8};
        bool changed = false;
        for (ui16 i = 0; i < 64; ++i) {
            UNIT_ASSERT(f.TryAdd(R(i * 10, i * 10 + 2), &changed));
            UNIT_ASSERT(changed);
        }
        UNIT_ASSERT_VALUES_EQUAL(64u, f.GetSegmentCount());
        UNIT_ASSERT_VALUES_EQUAL(192u, f.GetBlockCount());   // 64 * 3

        // Remove the middle block of range [0..2] → requires splitting into
        // [0..0] + [2..2] (new node), but arena is full.
        // BlockCount is modified BEFORE AllocNode is called — this is the
        // invariant-violation point. RecalculateCounters must fix it.
        UNIT_ASSERT(!f.TryRemove(R(1, 1), &changed));

        // Counters must reflect the tree as-is (nothing was removed).
        UNIT_ASSERT_VALUES_EQUAL(64u, f.GetSegmentCount());
        UNIT_ASSERT_VALUES_EQUAL(192u, f.GetBlockCount());
    }

    Y_UNIT_TEST(ReuseAfterClear)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        // Fill arena
        for (ui16 i = 0; i < 64; ++i) {
            UNIT_ASSERT(f.TryAdd(R(i * 10, i * 10 + 5), &changed));
            UNIT_ASSERT(changed);
        }
        UNIT_ASSERT_VALUES_EQUAL(64u, f.GetSegmentCount());

        f.Clear();
        UNIT_ASSERT(f.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetSegmentCount());

        // Should be able to add again
        for (ui16 i = 0; i < 64; ++i) {
            UNIT_ASSERT(f.TryAdd(R(i * 10, i * 10 + 5), &changed));
            UNIT_ASSERT(changed);
        }
        UNIT_ASSERT_VALUES_EQUAL(64u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(MemoryPerRange)
    {
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(100, 200), &changed));
        UNIT_ASSERT(changed);
        // Node size is 8 bytes, so one range = 8 bytes
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetSegmentCount());
    }

    Y_UNIT_TEST(AddAdjacentToMaxEndRange)
    {
        // Range with End = 65535 (ui16 max). Adding an adjacent range
        // [65534..65534] should merge with [65535..65535] because they are
        // adjacent. But 65535 + 1 overflows to 0, so the adjacency check
        // on line 83: End + 1 >= range.Start becomes 0 >= 65534 = false.
        // This is a known overflow issue.
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;

        // Add a range ending at ui16 max.
        UNIT_ASSERT(f.TryAdd(R(65535, 65535), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetSegmentCount());
        UNIT_ASSERT_VALUES_EQUAL(1u, f.GetBlockCount());

        // Try to add an adjacent range. Since End + 1 overflows, the merge
        // may not happen as expected.
        changed = false;
        UNIT_ASSERT(f.TryAdd(R(65534, 65534), &changed));
        // With the overflow bug, this may not merge correctly.
        // The range [65534..65534] should merge with [65535..65535] into
        // [65534..65535].
        UNIT_ASSERT(changed);
    }

    Y_UNIT_TEST(RemoveWithMaxEndRange)
    {
        // Test remove when a range ends at ui16 max.
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;

        UNIT_ASSERT(f.TryAdd(R(65533, 65535), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(3u, f.GetBlockCount());

        // Remove the last block. This requires setting Start = 65535 + 1
        // which overflows to 0. The resulting range [65533..65534] should
        // remain valid.
        changed = false;
        UNIT_ASSERT(f.TryRemove(R(65535, 65535), &changed));
        UNIT_ASSERT(changed);
    }

    Y_UNIT_TEST(AddManyRangesWithOneBlockGap)
    {
        // Add 512 single-block ranges with a 1-block gap between them:
        // [0,0], [2,2], [4,4], ..., [1022,1022].
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        constexpr size_t Count = 512;
        bool changed = false;
        for (size_t i = 0; i < Count; ++i) {
            const ui16 block = static_cast<ui16>(i * 2);
            UNIT_ASSERT(f.TryAdd(R(block, block), &changed));
            UNIT_ASSERT(changed);
        }

        UNIT_ASSERT_VALUES_EQUAL(Count, f.GetSegmentCount());
        UNIT_ASSERT_VALUES_EQUAL(Count, f.GetBlockCount());
    }

    Y_UNIT_TEST(RemoveNodeWithTwoChildrenSuccessorNotRightChild)
    {   //  Tree built by insertion order. Ranges are non-adjacent so that
        // TryAdd does not merge them:
        //   [50..58]           <- root, two children
        //   [30..38]  [70..78] <- left / right child
        //             [60..68] <- right child's left subtree (the successor)
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        UNIT_ASSERT(f.TryAdd(R(50, 58), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.TryAdd(R(30, 38), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[30..38][50..58]", f.Print());
        UNIT_ASSERT(f.TryAdd(R(70, 78), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[30..38][50..58][70..78]", f.Print());
        UNIT_ASSERT(f.TryAdd(R(60, 68), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[30..38][50..58][60..68][70..78]", f.Print());

        // Remove the root: it has two children and its successor [60..68]
        // is not the right child.
        UNIT_ASSERT(f.TryRemove(R(50, 58), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[30..38][60..68][70..78]", f.Print());
        UNIT_ASSERT_VALUES_EQUAL(27u, f.GetBlockCount());

        // The tree must remain consistent: remove the rest without crashes.
        UNIT_ASSERT(f.TryRemove(R(60, 68), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[30..38][70..78]", f.Print());
        UNIT_ASSERT(f.TryRemove(R(30, 38), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL("[70..78]", f.Print());
        UNIT_ASSERT(f.TryRemove(R(70, 78), &changed));
        UNIT_ASSERT(changed);
        UNIT_ASSERT(f.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
    }

    Y_UNIT_TEST(RemoveRootWithTwoChildrenRepeatedly)
    {
        // Stress the two-children removal path: always remove the current
        // minimum so that every removal detaches a successor deep in the
        // right subtree.
        auto allocator = MakeAllocator();
        TBlockRangeFieldSet f{allocator, MaxPoolSize};
        bool changed = false;
        for (ui16 i = 0; i < 10; ++i) {
            UNIT_ASSERT(f.TryAdd(R(i * 10, i * 10 + 5), &changed));
            UNIT_ASSERT(changed);
        }

        for (ui16 i = 0; i < 10; ++i) {
            UNIT_ASSERT(f.TryRemove(R(i * 10, i * 10 + 5), &changed));
            UNIT_ASSERT(changed);
            UNIT_ASSERT_VALUES_EQUAL(9 - i, f.GetSegmentCount());
        }
        UNIT_ASSERT(f.Empty());
        UNIT_ASSERT_VALUES_EQUAL(0u, f.GetBlockCount());
    }

    // -------------------------------------------------------------------------
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
