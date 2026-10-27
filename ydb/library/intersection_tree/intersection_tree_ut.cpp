#include "intersection_tree.h"

#include <library/cpp/testing/unittest/registar.h>
#include <limits>
#include <vector>
#include <span>

namespace NKikimr {

Y_UNIT_TEST_SUITE(IntersectionTree) {

    struct TExampleRange {
        ui64 MinKey;
        ui64 MaxKey;
    };

    using TExampleTree = TIntersectionTree<ui64, size_t>;

    std::vector<TExampleRange> GenerateRanges(size_t count, ui64 maxValue) {
        std::vector<TExampleRange> ranges;
        ranges.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            ui64 minKey = RandomNumber<ui64>() % maxValue;
            ui64 maxKey = minKey + RandomNumber<ui64>() % (maxValue - minKey);
            ranges.push_back({ minKey, maxKey });
        }
        return ranges;
    }

    struct TDebugRange {
        TExampleTree::TPartitionKey Left;
        TExampleTree::TPartitionKey Right;
        i32 Count;
    };

    TString ToString(const std::vector<TDebugRange>& ranges) {
        TStringBuilder b;
        for (const auto& range : ranges) {
            b << (range.Left.EqualGoesLeft ? '(' : '[') << range.Left.Key
                << ", " << range.Right.Key << (range.Right.EqualGoesLeft ? ']' : ')')
                << " = " << range.Count << Endl;
        }
        return b;
    }

    std::vector<TDebugRange> TreeToRanges(const TExampleTree& tree) {
        std::vector<TDebugRange> ranges;
        tree.ForEachRange([&](const auto& range) {
            ranges.push_back({ range.GetLeftPartitionKey(), range.GetRightPartitionKey(), range.GetCount() });
            return true;
        });
        return ranges;
    }

    std::vector<TDebugRange> TreeToRangesBackward(const TExampleTree& tree) {
        TExampleTree::TFoundRange last;
        tree.ForEachRange([&](const auto& range) {
            last = range;
            return true;
        });
        std::vector<TDebugRange> ranges;
        for (auto range = last; range; range = range.Prev()) {
            ranges.push_back({ range.GetLeftPartitionKey(), range.GetRightPartitionKey(), range.GetCount() });
        }
        std::reverse(ranges.begin(), ranges.end());
        return ranges;
    }

    std::vector<TDebugRange> SpanToRanges(const std::span<TExampleRange>& srcRanges) {
        std::map<TExampleTree::TPartitionKey, i32> deltas;
        for (const auto& range : srcRanges) {
            deltas[{ range.MinKey, false}] += 1;
            deltas[{ range.MaxKey, true}] -= 1;
        }
        std::vector<TDebugRange> ranges;
        std::optional<TExampleTree::TPartitionKey> lastKey;
        i32 count = 0;
        for (const auto& [key, delta] : deltas) {
            if (lastKey) {
                ranges.push_back({ *lastKey, key, count });
            }
            lastKey = key;
            count += delta;
        }
        return ranges;
    }

    void CheckMaxRange(const TExampleTree& tree) {
        auto maxCount = tree.GetMaxCount();
        const auto maxRange = tree.GetMaxRange();
        UNIT_ASSERT_VALUES_EQUAL(maxRange.GetCount(), maxCount);
        if (maxCount > 0) {
            UNIT_ASSERT(maxRange.HasLeftKey() && maxRange.HasRightKey());
            i32 count = 0;
            bool unique = true;
            THashSet<ui64> visited;
            maxRange.ForEachValue([&](size_t value) {
                ++count;
                if (!visited.insert(value).second) {
                    unique = false;
                }
                return true;
            });
            UNIT_ASSERT_VALUES_EQUAL(count, maxRange.GetCount());
            UNIT_ASSERT(unique);
        }
    }

    Y_UNIT_TEST(EmptyRangeCount) {
        TExampleTree::TFoundRange range;
        UNIT_ASSERT_VALUES_EQUAL(range.GetCount(), 0);

        TExampleTree tree;
        UNIT_ASSERT_VALUES_EQUAL(tree.FirstRange().GetCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tree.FindRange(15).GetCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tree.GetMaxRange().GetCount(), 0);

        tree.Add(0, 10, 20);
        tree.Remove(0);
        UNIT_ASSERT_VALUES_EQUAL(tree.FirstRange().GetCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tree.FindRange(15).GetCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tree.GetMaxRange().GetCount(), 0);
    }

    Y_UNIT_TEST(OutOfBoundsRangeCount) {
        TExampleTree tree;
        tree.Add(0, 10, 20);
        auto range = tree.FirstRange();
        UNIT_ASSERT_VALUES_EQUAL(range.GetCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tree.FindRange(9).GetCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tree.FindRange(21).GetCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.Prev().GetCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.Next().GetCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.Prev().Prev().GetCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(range.Next().Next().GetCount(), 0);
    }

    Y_UNIT_TEST(RejectReversedEndpoints) {
        TExampleTree tree;
        tree.Add(0, 10, 20);
        const auto before = ToString(TreeToRanges(tree));

        UNIT_ASSERT_EXCEPTION(tree.Add(1, 20, 10), yexception);
        UNIT_ASSERT_VALUES_EQUAL(ToString(TreeToRanges(tree)), before);
        CheckMaxRange(tree);

        // Rejection must leave the value available, and equal endpoints are valid.
        tree.Add(1, 15, 15);
        UNIT_ASSERT_VALUES_EQUAL(tree.FindRange(15).GetCount(), 2);
        tree.Remove(1);
        UNIT_ASSERT_VALUES_EQUAL(ToString(TreeToRanges(tree)), before);
    }

    Y_UNIT_TEST(RejectUnorderedEndpoints) {
        TIntersectionTree<double, size_t> tree;
        const double nan = std::numeric_limits<double>::quiet_NaN();
        const std::pair<double, double> endpoints[] = {{nan, 20.0}, {10.0, nan}, {nan, nan}};
        for (const auto& [left, right] : endpoints) {
            UNIT_ASSERT_EXCEPTION(tree.Add(0, left, right), yexception);
            UNIT_ASSERT(!tree.FirstRange());
            UNIT_ASSERT_VALUES_EQUAL(tree.GetMaxCount(), 0);

            tree.Add(0, 10.0, 20.0);
            UNIT_ASSERT_VALUES_EQUAL(tree.FindRange(15.0).GetCount(), 1);
            tree.Remove(0);
            UNIT_ASSERT(!tree.FirstRange());
        }
    }

    Y_UNIT_TEST(Randomized) {
        SetRandomSeed(42);
        auto rangesVec = GenerateRanges(100, 1000000);
        std::span<TExampleRange> ranges(rangesVec.data(), rangesVec.size());
        TExampleTree tree;
        size_t nextPosition = 0;
        for (const auto& range : ranges) {
            tree.Add(nextPosition++, range.MinKey, range.MaxKey);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            ToString(TreeToRanges(tree)),
            ToString(SpanToRanges(ranges))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            ToString(TreeToRangesBackward(tree)),
            ToString(SpanToRanges(ranges))
        );
        CheckMaxRange(tree);
        nextPosition = 0;
        while (!ranges.empty()) {
            tree.Remove(nextPosition++);
            ranges = ranges.subspan(1);
            UNIT_ASSERT_VALUES_EQUAL(
                ToString(TreeToRanges(tree)),
                ToString(SpanToRanges(ranges))
            );
            UNIT_ASSERT_VALUES_EQUAL_C(
                ToString(TreeToRangesBackward(tree)),
                ToString(SpanToRanges(ranges)),
                "after " << nextPosition << " removals"
            );
            CheckMaxRange(tree);
        }
    }

    // True when point p lies to the right of partition key pk
    bool IsRightOf(ui64 p, const TExampleTree::TPartitionKey& pk) {
        return p > pk.Key || (p == pk.Key && !pk.EqualGoesLeft);
    }

    struct TRandomTree {
        std::vector<TExampleRange> RangesVec;
        TExampleTree Tree;
        std::vector<TDebugRange> Segments;
        std::vector<ui64> Probes;
    };

    std::unique_ptr<TRandomTree> BuildRandomTree() {
        // Reproduce both the generated ranges and the tree's node priorities.
        SetRandomSeed(42);
        auto b = std::make_unique<TRandomTree>();
        b->RangesVec = GenerateRanges(100, 1000000);
        size_t next = 0;
        for (const auto& r : b->RangesVec) {
            b->Tree.Add(next++, r.MinKey, r.MaxKey);
        }
        b->Segments = SpanToRanges(std::span<TExampleRange>(b->RangesVec.data(), b->RangesVec.size()));
        for (const auto& r : b->RangesVec) {
            b->Probes.push_back(r.MinKey);
            b->Probes.push_back(r.MaxKey);
            if (r.MinKey) b->Probes.push_back(r.MinKey - 1);
            b->Probes.push_back(r.MaxKey + 1);
        }
        for (int i = 0; i < 1000; ++i) {
            b->Probes.push_back(RandomNumber<ui64>() % 1000000);
        }
        return b;
    }

    Y_UNIT_TEST(FindRangeMatchesBruteForce) {
        auto b = BuildRandomTree();
        for (ui64 p : b->Probes) {
            i32 expected = 0;
            for (const auto& r : b->RangesVec) {
                if (r.MinKey <= p && p <= r.MaxKey) ++expected;
            }
            const TDebugRange* seg = nullptr;
            for (const auto& s : b->Segments) {
                if (IsRightOf(p, s.Left) && !IsRightOf(p, s.Right)) { seg = &s; break; }
            }
            const auto found = b->Tree.FindRange(p);
            if (!seg) {
                UNIT_ASSERT_C(!found, "probe " << p);
                UNIT_ASSERT_VALUES_EQUAL(expected, 0);
                continue;
            }
            UNIT_ASSERT_C(found, "probe " << p);
            UNIT_ASSERT_VALUES_EQUAL_C(found.GetCount(), expected, "probe " << p);
            UNIT_ASSERT_VALUES_EQUAL_C(found.GetLeftKey(), seg->Left.Key, "probe " << p);
            UNIT_ASSERT_VALUES_EQUAL_C(found.GetRightKey(), seg->Right.Key, "probe " << p);
            i32 cnt = 0;
            THashSet<size_t> seen;
            found.ForEachValue([&](size_t v) {
                ++cnt;
                UNIT_ASSERT(seen.insert(v).second);
                const auto& r = b->RangesVec[v];
                UNIT_ASSERT_C(r.MinKey <= p && p <= r.MaxKey, "probe " << p << " value " << v);
                return true;
            });
            UNIT_ASSERT_VALUES_EQUAL_C(cnt, expected, "probe " << p);
        }
    }

    Y_UNIT_TEST(InclusiveFlags) {
        auto b = BuildRandomTree();
        size_t checked = 0;
        b->Tree.ForEachRange([&](const auto& r) {
            // Left border {k, EqualGoesLeft=false} means k itself belongs to this segment
            UNIT_ASSERT_VALUES_EQUAL_C(r.IsLeftInclusive(), !r.GetLeftPartitionKey().EqualGoesLeft,
                "left key " << r.GetLeftPartitionKey());
            UNIT_ASSERT_VALUES_EQUAL_C(r.IsRightInclusive(), r.GetRightPartitionKey().EqualGoesLeft,
                "right key " << r.GetRightPartitionKey());
            ++checked;
            return true;
        });
        UNIT_ASSERT(checked > 0);
    }

    Y_UNIT_TEST(InclusiveFlagsForKnownRanges) {
        struct TExpectedRange {
            ui64 LeftKey;
            ui64 RightKey;
            bool LeftInclusive;
            bool RightInclusive;
            i32 Count;
        };
        struct TTestCase {
            std::vector<TExampleRange> Ranges;
            std::vector<TExpectedRange> Expected;
        };
        const TTestCase testCases[] = {
            {
                {{10, 20}, {15, 25}},
                {{10, 15, true, false, 1}, {15, 20, true, true, 2}, {20, 25, false, true, 1}},
            },
            {
                {{10, 20}, {20, 30}},
                {{10, 20, true, false, 1}, {20, 20, true, true, 2}, {20, 30, false, true, 1}},
            },
        };
        for (const auto& testCase : testCases) {
            SetRandomSeed(42);
            TExampleTree tree;
            size_t next = 0;
            for (const auto& range : testCase.Ranges) {
                tree.Add(next++, range.MinKey, range.MaxKey);
            }
            auto range = tree.FirstRange();
            for (const auto& expected : testCase.Expected) {
                UNIT_ASSERT_C(range, "expected range " << expected.LeftKey << ", " << expected.RightKey);
                UNIT_ASSERT_VALUES_EQUAL(range.GetLeftKey(), expected.LeftKey);
                UNIT_ASSERT_VALUES_EQUAL(range.GetRightKey(), expected.RightKey);
                UNIT_ASSERT_VALUES_EQUAL(range.IsLeftInclusive(), expected.LeftInclusive);
                UNIT_ASSERT_VALUES_EQUAL(range.IsRightInclusive(), expected.RightInclusive);
                UNIT_ASSERT_VALUES_EQUAL(range.GetCount(), expected.Count);
                range = range.Next();
            }
            UNIT_ASSERT(!range);
        }
    }

    Y_UNIT_TEST(PrevMatchesNext) {
        auto b = BuildRandomTree();
        UNIT_ASSERT_VALUES_EQUAL(ToString(TreeToRangesBackward(b->Tree)), ToString(TreeToRanges(b->Tree)));
    }

    Y_UNIT_TEST(NextPrevRoundTrip) {
        auto b = BuildRandomTree();
        size_t checked = 0;
        for (auto r = b->Tree.FirstRange(); r; r = r.Next()) {
            auto n = r.Next();
            if (!n) break;
            auto back = n.Prev();
            UNIT_ASSERT_C(back, "at " << r.GetLeftPartitionKey());
            UNIT_ASSERT_VALUES_EQUAL_C(back.GetLeftKey(), r.GetLeftKey(), "at " << r.GetLeftPartitionKey());
            UNIT_ASSERT_VALUES_EQUAL_C(back.GetRightKey(), r.GetRightKey(), "at " << r.GetLeftPartitionKey());
            UNIT_ASSERT_VALUES_EQUAL_C(back.GetCount(), r.GetCount(), "at " << r.GetLeftPartitionKey());
            ++checked;
        }
        UNIT_ASSERT(checked > 0);
    }

} // Y_UNIT_TEST_SUITE(IntersectionTree)

} // namespace NKikimr
