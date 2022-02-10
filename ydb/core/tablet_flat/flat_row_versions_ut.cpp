#include "flat_row_versions.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(TRowVersionRangesTest) {

    Y_UNIT_TEST(SimpleInserts) {
        TRowVersionRanges ranges;
        UNIT_ASSERT(ranges.Add(TRowVersion(1, 10), TRowVersion(2, 20)));
        UNIT_ASSERT(ranges.Add(TRowVersion(5, 50), TRowVersion(6, 60)));
        UNIT_ASSERT(ranges.Add(TRowVersion(3, 30), TRowVersion(4, 40)));
        UNIT_ASSERT(ranges.Add(TRowVersion(0, 1), TRowVersion(0, 5)));
        UNIT_ASSERT(!ranges.Add(TRowVersion(7, 70), TRowVersion(5, 50)));
        UNIT_ASSERT(!ranges.Add(TRowVersion(7, 70), TRowVersion(7, 70)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v0/1, v0/5), [v1/10, v2/20), [v3/30, v4/40), [v5/50, v6/60) }");
    }

    /**
     * Constructs common ranges for various merge tests
     */
    TRowVersionRanges CommonRanges() {
        TRowVersionRanges ranges;
        ranges.Add(TRowVersion(1, 10), TRowVersion(2, 20));
        ranges.Add(TRowVersion(3, 30), TRowVersion(4, 40));
        ranges.Add(TRowVersion(5, 50), TRowVersion(6, 60));
        ranges.Add(TRowVersion(7, 70), TRowVersion(8, 80));
        return ranges;
    }

    Y_UNIT_TEST(MergeFailLeft) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(!ranges.Add(TRowVersion(3, 30), TRowVersion(4, 30)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/30, v4/40), [v5/50, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeFailRight) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(!ranges.Add(TRowVersion(3, 40), TRowVersion(4, 40)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/30, v4/40), [v5/50, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeFailOuter) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(!ranges.Add(TRowVersion(3, 30), TRowVersion(4, 40)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/30, v4/40), [v5/50, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeFailInner) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(!ranges.Add(TRowVersion(3, 40), TRowVersion(4, 30)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/30, v4/40), [v5/50, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeExtendLeft) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(3, 10), TRowVersion(3, 30)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/10, v4/40), [v5/50, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeExtendLeftInner) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(3, 10), TRowVersion(4, 10)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/10, v4/40), [v5/50, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeExtendLeftComplete) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(3, 10), TRowVersion(4, 40)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/10, v4/40), [v5/50, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeExtendRight) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(4, 40), TRowVersion(4, 60)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/30, v4/60), [v5/50, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeExtendRightInner) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(4, 10), TRowVersion(4, 60)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/30, v4/60), [v5/50, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeExtendRightComplete) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(3, 30), TRowVersion(4, 60)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/30, v4/60), [v5/50, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeExtendBoth) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(3, 10), TRowVersion(4, 60)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/10, v4/60), [v5/50, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeHoleExact) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(4, 40), TRowVersion(5, 50)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/30, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeHoleInner) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(4, 10), TRowVersion(5, 70)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/30, v6/60), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeHoleOuter) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(3, 10), TRowVersion(6, 80)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v2/20), [v3/10, v6/80), [v7/70, v8/80) }");
    }

    Y_UNIT_TEST(MergeAllOuter) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(0, 1), TRowVersion(9, 90)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v0/1, v9/90) }");
    }

    Y_UNIT_TEST(MergeAllInner) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(1, 20), TRowVersion(8, 70)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v8/80) }");
    }

    Y_UNIT_TEST(MergeAllEdges) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Add(TRowVersion(2, 20), TRowVersion(7, 70)));
        UNIT_ASSERT_VALUES_EQUAL(ranges.ToString(),
            "TRowVersionRanges{ [v1/10, v8/80) }");
    }

    Y_UNIT_TEST(ContainsEmpty) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Contains(TRowVersion(3, 30), TRowVersion(3, 30)));
        UNIT_ASSERT(ranges.Contains(TRowVersion(3, 50), TRowVersion(3, 50)));
        UNIT_ASSERT(ranges.Contains(TRowVersion(4, 40), TRowVersion(4, 40)));
        UNIT_ASSERT(ranges.Contains(TRowVersion(8, 80), TRowVersion(8, 80)));
        UNIT_ASSERT(!ranges.Contains(TRowVersion(4, 80), TRowVersion(4, 80)));
        UNIT_ASSERT(!ranges.Contains(TRowVersion(9, 90), TRowVersion(9, 90)));
    }

    Y_UNIT_TEST(ContainsNonEmpty) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(ranges.Contains(TRowVersion(3, 30), TRowVersion(4, 40)));
        UNIT_ASSERT(ranges.Contains(TRowVersion(3, 30), TRowVersion(4, 20)));
        UNIT_ASSERT(ranges.Contains(TRowVersion(3, 50), TRowVersion(4, 40)));
        UNIT_ASSERT(ranges.Contains(TRowVersion(3, 50), TRowVersion(4, 20)));
        UNIT_ASSERT(!ranges.Contains(TRowVersion(3, 10), TRowVersion(4, 40)));
        UNIT_ASSERT(!ranges.Contains(TRowVersion(3, 30), TRowVersion(4, 60)));
        UNIT_ASSERT(!ranges.Contains(TRowVersion(3, 10), TRowVersion(4, 60)));
        UNIT_ASSERT(!ranges.Contains(TRowVersion(8, 80), TRowVersion(8, 90)));
        UNIT_ASSERT(!ranges.Contains(TRowVersion(8, 90), TRowVersion(8, 91)));
    }

    Y_UNIT_TEST(ContainsInvalid) {
        auto ranges = CommonRanges();
        UNIT_ASSERT(!ranges.Contains(TRowVersion(4, 40), TRowVersion(3, 30)));
    }

    Y_UNIT_TEST(AdjustDown) {
        auto ranges = CommonRanges();
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(0, 1)), TRowVersion(0, 1));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(1, 10)), TRowVersion(1, 10));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(1, 20)), TRowVersion(1, 10));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(2, 10)), TRowVersion(1, 10));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(2, 20)), TRowVersion(1, 10));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(2, 30)), TRowVersion(2, 30));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(7, 70)), TRowVersion(7, 70));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(8, 80)), TRowVersion(7, 70));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(8, 81)), TRowVersion(8, 81));
    }

    Y_UNIT_TEST(AdjustDownSnapshot) {
        auto ranges = CommonRanges().Snapshot();
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(0, 1)), TRowVersion(0, 1));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(1, 10)), TRowVersion(1, 10));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(1, 20)), TRowVersion(1, 10));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(2, 10)), TRowVersion(1, 10));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(2, 20)), TRowVersion(1, 10));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(2, 30)), TRowVersion(2, 30));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(7, 70)), TRowVersion(7, 70));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(8, 80)), TRowVersion(7, 70));
        UNIT_ASSERT_VALUES_EQUAL(ranges.AdjustDown(TRowVersion(8, 81)), TRowVersion(8, 81));
    }

    using TSteppedCookieAllocator = TRowVersionRanges::TSteppedCookieAllocator;

    TString DumpSteppedCookieAllocatorOrder(size_t size) {
        TSteppedCookieAllocator steppedCookieAllocator(size);
        TStringBuilder builder;
        bool first = true;
        while (steppedCookieAllocator) {
            if (first) {
                first = false;
            } else {
                builder << ", ";
            }
            builder << steppedCookieAllocator.Current();
            steppedCookieAllocator.MoveNext();
        }
        return builder;
    }

    Y_UNIT_TEST(SteppedCookieAllocatorOrder) {
        TVector<TString> expected{{
            "",
            "0",
            "0, 1",
            "0, 1, 2",
            "0, 1, 2, 3",
            "4, 0, 1, 2, 3",
            "4, 5, 0, 1, 2, 3",
            "4, 5, 6, 0, 1, 2, 3",
            "4, 5, 6, 7, 0, 1, 2, 3",
            "4, 5, 6, 7, 0, 8, 1, 2, 3",
            "4, 5, 6, 7, 0, 8, 9, 1, 2, 3",
            "4, 5, 6, 7, 0, 8, 9, 10, 1, 2, 3",
            "4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 2, 3",
            "4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 2, 3",
            "4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 2, 3",
            "4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 2, 3",
            "4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 3",
            "4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 3",
            "4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 3",
            "4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 3",
            "4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 29, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 29, 30, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 29, 30, 31, 6, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 29, 30, 31, 6, 32, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 29, 30, 31, 6, 32, 33, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 29, 30, 31, 6, 32, 33, 34, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 29, 30, 31, 6, 32, 33, 34, 35, 7, 0, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 29, 30, 31, 6, 32, 33, 34, 35, 7, 0, 36, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 29, 30, 31, 6, 32, 33, 34, 35, 7, 0, 36, 37, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 29, 30, 31, 6, 32, 33, 34, 35, 7, 0, 36, 37, 38, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            "20, 21, 22, 23, 4, 24, 25, 26, 27, 5, 28, 29, 30, 31, 6, 32, 33, 34, 35, 7, 0, 36, 37, 38, 39, 8, 9, 10, 11, 1, 12, 13, 14, 15, 2, 16, 17, 18, 19, 3",
            // N.B. full 3-level tree would have 84 elements
        }};

        for (size_t size = 0; size < expected.size(); ++size) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                DumpSteppedCookieAllocatorOrder(size),
                expected[size],
                "DumpSteppedCookieAllocator(" << size << ")");
        }
    }

    Y_UNIT_TEST(SteppedCookieAllocatorLowerBound) {
        TVector<size_t> sizes{{ 0, 5, 21, 37, 84 }};
        for (size_t size : sizes) {
            TVector<size_t> tree;

            {
                // Build a tree of values 5, 15, 25, ...
                tree.resize(size);
                size_t value = 0;
                TSteppedCookieAllocator steppedCookieAllocator(size);
                while (steppedCookieAllocator) {
                    tree[steppedCookieAllocator.Current()] = (value++) * 10 + 5;
                    steppedCookieAllocator.MoveNext();
                }
                UNIT_ASSERT_VALUES_EQUAL(value, size);
            }

            for (size_t index = 0; index < size; ++index) {
                // Search for every needle must succeed
                size_t needle = index * 10;
                size_t found = TSteppedCookieAllocator::LowerBound(tree, needle);
                UNIT_ASSERT_C(
                    found != tree.size(),
                    "While looking for " << needle << " in a tree of size " << size);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    tree[found], needle + 5,
                    "While looking for " << needle << " in a tree of size " << size);
            }

            {
                // Search for needle above the maximum must fail
                size_t needle = size * 10;
                size_t found = TSteppedCookieAllocator::LowerBound(tree, needle);
                UNIT_ASSERT_C(
                    found == tree.size(),
                    "While looking for " << needle << " in a tree of size" << size);
            }
        }
    }

}

}
}
