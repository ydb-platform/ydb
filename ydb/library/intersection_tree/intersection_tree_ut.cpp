#include "intersection_tree.h"

#include <library/cpp/testing/unittest/registar.h>
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
        auto maxRange = tree.GetMaxRange();
        if (maxCount > 0) {
            UNIT_ASSERT(maxRange.HasLeftKey() && maxRange.HasRightKey());
            UNIT_ASSERT_VALUES_EQUAL(maxRange.GetCount(), maxCount);
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

    Y_UNIT_TEST(Randomized) {
        size_t removeCount = 99;
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
        CheckMaxRange(tree);
        nextPosition = 0;
        for (size_t i = 0; i < removeCount && !ranges.empty(); ++i) {
            tree.Remove(nextPosition++);
            ranges = ranges.subspan(1);
            UNIT_ASSERT_VALUES_EQUAL(
                ToString(TreeToRanges(tree)),
                ToString(SpanToRanges(ranges))
            );
            CheckMaxRange(tree);
        }
    }

} // Y_UNIT_TEST_SUITE(IntersectionTree)

} // namespace NKikimr
