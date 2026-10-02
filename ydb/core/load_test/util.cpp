#include "util.h"

#include <util/random/random.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <limits>

namespace NKikimr {

ui32 TWeightedIndices::AddWeight(ui64 weight) {
    Y_ABORT_UNLESS(weight, "Weight must be non-zero");
    Y_ABORT_UNLESS(Entries.size() < std::numeric_limits<ui32>::max(), "Too many weighted entries");

    const ui32 index = static_cast<ui32>(Entries.size());
    Entries.push_back({index, weight});
    IsPrepared = false;
    return index;
}

void TWeightedIndices::Clear() {
    Entries.clear();
    AccumWeights.clear();
    TotalWeight = 0;
    IsPrepared = false;
}

bool TWeightedIndices::Empty() const {
    return Entries.empty();
}

ui32 TWeightedIndices::Size() const {
    return static_cast<ui32>(Entries.size());
}

void TWeightedIndices::PrepareIfNeeded() const {
    if (IsPrepared) {
        return;
    }

    std::stable_sort(Entries.begin(), Entries.end(),
        [](const TEntry& lhs, const TEntry& rhs) {
            return lhs.Weight > rhs.Weight;
        });

    AccumWeights.clear();
    AccumWeights.reserve(Entries.size());
    TotalWeight = 0;
    for (const TEntry& entry : Entries) {
        Y_ABORT_UNLESS(
            TotalWeight <= std::numeric_limits<ui64>::max() - entry.Weight,
            "Total weight overflow");
        TotalWeight += entry.Weight;
        AccumWeights.push_back(TotalWeight);
    }

    IsPrepared = true;
}

ui32 TWeightedIndices::GetRandomIndex() const {
    PrepareIfNeeded();
    Y_ABORT_UNLESS(TotalWeight, "No entries to choose from");

    const ui64 randomValue = RandomNumber<ui64>();
    const ui64 weight = randomValue % TotalWeight;
    const auto it = std::upper_bound(AccumWeights.begin(), AccumWeights.end(), weight);
    Y_ABORT_UNLESS(it != AccumWeights.end(), "Invalid weighted index lookup");
    return Entries[it - AccumWeights.begin()].Index;
}

} // namespace NKikimr
