#pragma once

#include "block_range.h"

#include <util/system/types.h>

#include <span>

namespace NYdb::NBS::NBlockStore {

struct TWeightedRange
{
    ui64 Key{};
    TBlockRange64 Range;

    bool operator<(const TWeightedRange& other) const
    {
        return Key < other.Key;
    }
};

// The function splits overlapping ranges into non-overlapping ranges
//   and returns their in a container.
// Result is continuous range's sequence, where original 'holes' are
//   fullfilled with ranges with a key == 0.
TVector<TWeightedRange> SplitOnNonOverlappingContinuousRanges(
    TBlockRange64 fullRange,
    const std::span<TWeightedRange> overlappingRanges);

}   // namespace NYdb::NBS::NBlockStore
