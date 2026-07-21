#include "block_range_field.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<TBlockRange64> Collect(const TBlockRangeField& field)
{
    TVector<TBlockRange64> result;
    field.Enumerate([&](TBlockRange64 r) { result.push_back(r); });
    return result;
}

TBlockRange64 R(ui64 start, ui64 end)
{
    return TBlockRange64::MakeClosedInterval(start, end);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeFieldTest)
{
    // -------------------------------------------------------------------------
    // Add – basic

    Y_UNIT_TEST(AddSingleRange)
    {
        TBlockRangeField f;
        f.Add(R(10, 20));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(10, 20), v[0]);
    }

    Y_UNIT_TEST(AddTwoNonAdjacentRanges)
    {
        TBlockRangeField f;
        f.Add(R(0, 5));
        f.Add(R(10, 15));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(2u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 5), v[0]);
        UNIT_ASSERT_VALUES_EQUAL(R(10, 15), v[1]);
    }

    Y_UNIT_TEST(AddAdjacentRangesMerged)
    {
        TBlockRangeField f;
        f.Add(R(0, 5));
        f.Add(R(6, 10));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 10), v[0]);
    }

    Y_UNIT_TEST(AddOverlappingRangesMerged)
    {
        TBlockRangeField f;
        f.Add(R(0, 10));
        f.Add(R(5, 15));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 15), v[0]);
    }

    Y_UNIT_TEST(AddCoveredByExistingIsNoop)
    {
        TBlockRangeField f;
        f.Add(R(0, 100));
        f.Add(R(10, 20));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 100), v[0]);
    }

    Y_UNIT_TEST(AddCoversMultipleRanges)
    {
        TBlockRangeField f;
        f.Add(R(0, 5));
        f.Add(R(10, 15));
        f.Add(R(20, 25));
        // New range covers all three and the gaps between them.
        f.Add(R(0, 25));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 25), v[0]);
    }

    Y_UNIT_TEST(AddMergesOnBothSides)
    {
        TBlockRangeField f;
        f.Add(R(0, 5));
        f.Add(R(10, 15));
        // Bridge the gap.
        f.Add(R(5, 10));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 15), v[0]);
    }

    Y_UNIT_TEST(AddSameRangeTwice)
    {
        TBlockRangeField f;
        f.Add(R(3, 7));
        f.Add(R(3, 7));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(3, 7), v[0]);
    }

    // -------------------------------------------------------------------------
    // Remove – basic

    Y_UNIT_TEST(RemoveFromEmpty)
    {
        TBlockRangeField f;
        f.Remove(R(0, 10));   // must not crash
        UNIT_ASSERT(Collect(f).empty());
    }

    Y_UNIT_TEST(RemoveExact)
    {
        TBlockRangeField f;
        f.Add(R(0, 10));
        f.Remove(R(0, 10));
        UNIT_ASSERT(Collect(f).empty());
    }

    Y_UNIT_TEST(RemoveFromMiddle)
    {
        TBlockRangeField f;
        f.Add(R(0, 20));
        f.Remove(R(5, 10));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(2u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 4), v[0]);
        UNIT_ASSERT_VALUES_EQUAL(R(11, 20), v[1]);
    }

    Y_UNIT_TEST(RemoveLeftPart)
    {
        TBlockRangeField f;
        f.Add(R(0, 20));
        f.Remove(R(0, 9));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(10, 20), v[0]);
    }

    Y_UNIT_TEST(RemoveRightPart)
    {
        TBlockRangeField f;
        f.Add(R(0, 20));
        f.Remove(R(10, 20));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 9), v[0]);
    }

    Y_UNIT_TEST(RemoveNonOverlapping)
    {
        TBlockRangeField f;
        f.Add(R(10, 20));
        f.Remove(R(30, 40));   // no overlap, no change
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(10, 20), v[0]);
    }

    Y_UNIT_TEST(RemoveSeveralRanges)
    {
        TBlockRangeField f;
        f.Add(R(0, 5));
        f.Add(R(10, 15));
        f.Add(R(20, 25));
        f.Remove(R(3, 22));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(2u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 2), v[0]);
        UNIT_ASSERT_VALUES_EQUAL(R(23, 25), v[1]);
    }

    // -------------------------------------------------------------------------
    // Overlaps

    Y_UNIT_TEST(OverlapsOnEmpty)
    {
        TBlockRangeField f;
        UNIT_ASSERT(!f.Overlaps(R(0, 100)));
    }

    Y_UNIT_TEST(OverlapsExact)
    {
        TBlockRangeField f;
        f.Add(R(10, 20));
        UNIT_ASSERT(f.Overlaps(R(10, 20)));
    }

    Y_UNIT_TEST(OverlapsPartialLeft)
    {
        TBlockRangeField f;
        f.Add(R(10, 20));
        UNIT_ASSERT(f.Overlaps(R(5, 12)));
    }

    Y_UNIT_TEST(OverlapsPartialRight)
    {
        TBlockRangeField f;
        f.Add(R(10, 20));
        UNIT_ASSERT(f.Overlaps(R(15, 30)));
    }

    Y_UNIT_TEST(OverlapsCovering)
    {
        TBlockRangeField f;
        f.Add(R(10, 20));
        UNIT_ASSERT(f.Overlaps(R(0, 100)));
    }

    Y_UNIT_TEST(OverlapsNoOverlapBefore)
    {
        TBlockRangeField f;
        f.Add(R(10, 20));
        UNIT_ASSERT(!f.Overlaps(R(0, 9)));
    }

    Y_UNIT_TEST(OverlapsNoOverlapAfter)
    {
        TBlockRangeField f;
        f.Add(R(10, 20));
        UNIT_ASSERT(!f.Overlaps(R(21, 30)));
    }

    Y_UNIT_TEST(OverlapsAdjacentNotOverlapping)
    {
        TBlockRangeField f;
        f.Add(R(10, 20));
        // [9,9] touches start but doesn't overlap.
        UNIT_ASSERT(!f.Overlaps(R(5, 9)));
        // [21,21] touches end but doesn't overlap.
        UNIT_ASSERT(!f.Overlaps(R(21, 25)));
    }

    // -------------------------------------------------------------------------
    // Edge / boundary cases

    Y_UNIT_TEST(AddStartingAtZero)
    {
        TBlockRangeField f;
        f.Add(R(0, 0));
        f.Add(R(1, 5));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 5), v[0]);
    }

    Y_UNIT_TEST(RemoveSingleBlock)
    {
        TBlockRangeField f;
        f.Add(R(0, 4));
        f.Remove(R(2, 2));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(2u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 1), v[0]);
        UNIT_ASSERT_VALUES_EQUAL(R(3, 4), v[1]);
    }

    Y_UNIT_TEST(ManyFragmentsAfterRemoves)
    {
        TBlockRangeField f;
        f.Add(R(0, 99));
        // Remove every even block to create 50 gaps.
        for (ui64 i = 0; i < 100; i += 2) {
            f.Remove(R(i, i));
        }
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(50u, v.size());
        for (ui64 i = 0; i < 50; ++i) {
            ui64 odd = i * 2 + 1;
            UNIT_ASSERT_VALUES_EQUAL(R(odd, odd), v[i]);
        }
    }

    Y_UNIT_TEST(AddRestoresAfterRemoves)
    {
        TBlockRangeField f;
        f.Add(R(0, 99));
        for (ui64 i = 0; i < 100; i += 2) {
            f.Remove(R(i, i));
        }
        // Adding back should merge everything.
        for (ui64 i = 0; i < 100; i += 2) {
            f.Add(R(i, i));
        }
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(1u, v.size());
        UNIT_ASSERT_VALUES_EQUAL(R(0, 99), v[0]);
    }

    Y_UNIT_TEST(EnumerateOrderedByStart)
    {
        TBlockRangeField f;
        f.Add(R(50, 60));
        f.Add(R(10, 20));
        f.Add(R(30, 40));
        auto v = Collect(f);
        UNIT_ASSERT_VALUES_EQUAL(3u, v.size());
        UNIT_ASSERT(v[0].Start < v[1].Start);
        UNIT_ASSERT(v[1].Start < v[2].Start);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
