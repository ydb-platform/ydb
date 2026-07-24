#include <util/generic/hash.h>

#include <ydb/library/intersection_tree/intersection_tree.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/system/yassert.h>

#include <algorithm>
#include <map>
#include <optional>
#include <set>
#include <tuple>
#include <utility>
#include <vector>

namespace {

constexpr size_t MaxOps = 192;
constexpr size_t MaxRanges = 32;

using TIntersectionTree = NKikimr::TIntersectionTree<ui32, ui32>;

struct TPartitionKeyLess {
    bool operator()(const TIntersectionTree::TPartitionKey& lhs, const TIntersectionTree::TPartitionKey& rhs) const {
        if (lhs.Key != rhs.Key) {
            return lhs.Key < rhs.Key;
        }
        return lhs.EqualGoesLeft < rhs.EqualGoesLeft;
    }
};

struct TRangeInfo {
    ui32 Left = 0;
    ui32 Right = 0;
};

using TRangeModel = std::map<ui32, TRangeInfo>;

std::vector<std::tuple<TIntersectionTree::TPartitionKey, TIntersectionTree::TPartitionKey, i32>> ModelRanges(const TRangeModel& model) {
    std::map<TIntersectionTree::TPartitionKey, i32, TPartitionKeyLess> deltas;
    for (const auto& [_, range] : model) {
        deltas[{range.Left, false}] += 1;
        deltas[{range.Right, true}] -= 1;
    }

    std::vector<std::tuple<TIntersectionTree::TPartitionKey, TIntersectionTree::TPartitionKey, i32>> ranges;
    std::optional<TIntersectionTree::TPartitionKey> lastKey;
    i32 count = 0;
    for (const auto& [key, delta] : deltas) {
        if (lastKey) {
            ranges.emplace_back(*lastKey, key, count);
        }
        lastKey = key;
        count += delta;
    }
    return ranges;
}

std::vector<std::tuple<TIntersectionTree::TPartitionKey, TIntersectionTree::TPartitionKey, i32>> TreeRanges(const TIntersectionTree& tree) {
    std::vector<std::tuple<TIntersectionTree::TPartitionKey, TIntersectionTree::TPartitionKey, i32>> ranges;
    tree.ForEachRange([&](const auto& range) {
        ranges.emplace_back(range.GetLeftPartitionKey(), range.GetRightPartitionKey(), range.GetCount());
        return true;
    });
    return ranges;
}

bool SamePartitionKey(const TIntersectionTree::TPartitionKey& lhs, const TIntersectionTree::TPartitionKey& rhs) {
    return lhs.Key == rhs.Key && lhs.EqualGoesLeft == rhs.EqualGoesLeft;
}

void CheckIntersectionTree(const TIntersectionTree& tree, const TRangeModel& model, ui32 probeKey) {
    const auto treeRanges = TreeRanges(tree);
    const auto modelRanges = ModelRanges(model);
    Y_ABORT_UNLESS(treeRanges.size() == modelRanges.size());
    i32 maxCount = 0;
    for (size_t i = 0; i < treeRanges.size(); ++i) {
        const auto& [tl, tr, tc] = treeRanges[i];
        const auto& [ml, mr, mc] = modelRanges[i];
        Y_ABORT_UNLESS(SamePartitionKey(tl, ml));
        Y_ABORT_UNLESS(SamePartitionKey(tr, mr));
        Y_ABORT_UNLESS(tc == mc);
        maxCount = std::max(maxCount, mc);
    }
    Y_ABORT_UNLESS(tree.GetMaxCount() == maxCount);

    const auto found = tree.FindRange(probeKey);
    i32 expectedAtProbe = 0;
    for (const auto& [_, range] : model) {
        if (range.Left <= probeKey && probeKey <= range.Right) {
            ++expectedAtProbe;
        }
    }
    Y_ABORT_UNLESS((found ? found.GetCount() : 0) == expectedAtProbe);

    auto maxRange = tree.GetMaxRange();
    if (maxCount > 0) {
        Y_ABORT_UNLESS(maxRange.HasLeftKey());
        Y_ABORT_UNLESS(maxRange.HasRightKey());
        Y_ABORT_UNLESS(maxRange.GetCount() == maxCount);
        std::set<ui32> values;
        i32 valueCount = 0;
        maxRange.ForEachValue([&](ui32 value) {
            ++valueCount;
            Y_ABORT_UNLESS(values.insert(value).second);
            Y_ABORT_UNLESS(model.contains(value));
            return true;
        });
        Y_ABORT_UNLESS(valueCount == maxRange.GetCount());
    }
}

void ExerciseIntersectionTree(FuzzedDataProvider& fdp) {
    TIntersectionTree tree;
    TRangeModel model;

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        const ui32 value = fdp.ConsumeIntegralInRange<ui32>(0, MaxRanges - 1);
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 3)) {
            case 0:
                if (!model.contains(value)) {
                    ui32 left = fdp.ConsumeIntegralInRange<ui32>(0, 64);
                    ui32 right = fdp.ConsumeIntegralInRange<ui32>(0, 64);
                    if (right < left) {
                        std::swap(left, right);
                    }
                    tree.Add(value, left, right);
                    model.emplace(value, TRangeInfo{left, right});
                }
                break;
            case 1:
                tree.Remove(value);
                model.erase(value);
                break;
            default:
                CheckIntersectionTree(tree, model, fdp.ConsumeIntegralInRange<ui32>(0, 64));
                break;
        }
        CheckIntersectionTree(tree, model, fdp.ConsumeIntegralInRange<ui32>(0, 64));
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);
    ExerciseIntersectionTree(fdp);

    return 0;
}
