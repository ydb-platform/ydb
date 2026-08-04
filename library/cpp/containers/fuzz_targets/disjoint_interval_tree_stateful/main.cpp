#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <util/system/types.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <vector>

namespace {

constexpr ui32 Domain = 256;

class TInput {
public:
    TInput(const ui8* data, size_t size)
        : Data_(data)
        , Size_(size)
    {
    }

    bool Empty() const {
        return Pos_ >= Size_;
    }

    ui8 Byte() {
        if (Empty()) {
            return 0;
        }
        return Data_[Pos_++];
    }

    ui32 Int() {
        ui32 value = 0;
        for (int i = 0; i < 4; ++i) {
            value = (value << 8) | Byte();
        }
        return value;
    }

private:
    const ui8* Data_;
    size_t Size_;
    size_t Pos_ = 0;
};

ui32 BoundaryValue(TInput& input) {
    static constexpr std::array<ui32, 16> Boundaries = {
        0, 1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, Domain - 2, Domain - 1, Domain
    };
    const ui32 raw = input.Int();
    if ((raw & 3) == 0) {
        return Boundaries[(raw >> 2) % Boundaries.size()];
    }
    return raw % (Domain + 1);
}

using TModel = std::array<bool, Domain>;

bool AnySet(const TModel& model, ui32 begin, ui32 end) {
    for (ui32 value = begin; value < end; ++value) {
        if (model[value]) {
            return true;
        }
    }
    return false;
}

size_t CountSet(const TModel& model) {
    return std::count(model.begin(), model.end(), true);
}

size_t EraseModel(TModel& model, ui32 begin, ui32 end) {
    size_t removed = 0;
    for (ui32 value = begin; value < end; ++value) {
        if (model[value]) {
            model[value] = false;
            ++removed;
        }
    }
    return removed;
}

bool IntersectsModel(const TModel& model, ui32 begin, ui32 end) {
    for (ui32 value = begin; value < end; ++value) {
        if (model[value]) {
            return true;
        }
    }
    return false;
}

std::vector<std::pair<ui32, ui32>> ModelIntervals(const TModel& model) {
    std::vector<std::pair<ui32, ui32>> intervals;
    for (ui32 value = 0; value < Domain;) {
        if (!model[value]) {
            ++value;
            continue;
        }
        const ui32 begin = value;
        while (value < Domain && model[value]) {
            ++value;
        }
        intervals.push_back({begin, value});
    }
    return intervals;
}

void CheckTree(const TDisjointIntervalTree<ui32>& tree, const TModel& model) {
    const auto intervals = ModelIntervals(model);
    Y_ABORT_UNLESS(tree.GetNumElements() == CountSet(model));
    Y_ABORT_UNLESS(tree.GetNumIntervals() == intervals.size());
    Y_ABORT_UNLESS(tree.Empty() == intervals.empty());

    auto it = tree.begin();
    for (const auto& [begin, end] : intervals) {
        Y_ABORT_UNLESS(it != tree.end());
        Y_ABORT_UNLESS(it->first == begin);
        Y_ABORT_UNLESS(it->second == end);
        ++it;
    }
    Y_ABORT_UNLESS(it == tree.end());

    for (ui32 value = 0; value < Domain; ++value) {
        Y_ABORT_UNLESS(tree.Has(value) == model[value]);
        const auto containing = tree.FindContaining(value);
        Y_ABORT_UNLESS((containing != tree.end()) == model[value]);
        if (model[value]) {
            Y_ABORT_UNLESS(containing->first <= value);
            Y_ABORT_UNLESS(value < containing->second);
        }
    }

    if (!intervals.empty()) {
        Y_ABORT_UNLESS(tree.Min() == intervals.front().first);
        Y_ABORT_UNLESS(tree.Max() == intervals.back().second);
    }

    for (ui32 begin = 0; begin <= Domain; begin += 17) {
        for (ui32 width : {1u, 2u, 7u, 31u}) {
            const ui32 end = std::min(Domain, begin + width);
            if (begin < end) {
                Y_ABORT_UNLESS(const_cast<TDisjointIntervalTree<ui32>&>(tree).Intersects(begin, end) == IntersectsModel(model, begin, end));
            }
        }
    }
}

void InsertModel(TModel& model, ui32 begin, ui32 end) {
    for (ui32 value = begin; value < end; ++value) {
        Y_ABORT_UNLESS(!model[value]);
        model[value] = true;
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TInput input(data, size);
    if (input.Empty()) {
        return 0;
    }

    TDisjointIntervalTree<ui32> trees[2];
    TModel models[2] = {};

    const size_t steps = std::min<size_t>(input.Byte() + 1, 224);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const size_t slot = input.Byte() & 1;
        auto& tree = trees[slot];
        auto& model = models[slot];
        const ui8 op = input.Byte();
        ui32 begin = BoundaryValue(input);
        ui32 end = BoundaryValue(input);
        if (begin > end) {
            std::swap(begin, end);
        }
        if (begin == end && begin < Domain) {
            ++end;
        }
        end = std::min(end, Domain);

        switch (op % 12) {
            case 0:
                if (begin < Domain && !model[begin]) {
                    tree.Insert(begin);
                    model[begin] = true;
                }
                break;
            case 1:
            case 2:
                if (begin < end && !AnySet(model, begin, end)) {
                    tree.InsertInterval(begin, end);
                    InsertModel(model, begin, end);
                }
                break;
            case 3:
                if (begin < Domain) {
                    const bool actual = tree.Erase(begin);
                    const bool expected = model[begin];
                    model[begin] = false;
                    Y_ABORT_UNLESS(actual == expected);
                }
                break;
            case 4:
            case 5:
                if (begin < end) {
                    const size_t expected = EraseModel(model, begin, end);
                    Y_ABORT_UNLESS(tree.EraseInterval(begin, end) == expected);
                }
                break;
            case 6:
                if (begin < end) {
                    Y_ABORT_UNLESS(tree.Intersects(begin, end) == IntersectsModel(model, begin, end));
                }
                break;
            case 7:
                if (begin < Domain) {
                    Y_ABORT_UNLESS(tree.Has(begin) == model[begin]);
                    const auto& constTree = tree;
                    Y_ABORT_UNLESS((constTree.FindContaining(begin) != constTree.end()) == model[begin]);
                }
                break;
            case 8:
                tree.Clear();
                model = {};
                break;
            case 9:
                trees[0].Swap(trees[1]);
                models[0].swap(models[1]);
                break;
            case 10: {
                TDisjointIntervalTree<ui32> copy(tree);
                CheckTree(copy, model);
                break;
            }
            case 11: {
                TDisjointIntervalTree<ui32> assigned;
                assigned = tree;
                CheckTree(assigned, model);
                break;
            }
        }

        CheckTree(trees[0], models[0]);
        CheckTree(trees[1], models[1]);
    }

    return 0;
}
