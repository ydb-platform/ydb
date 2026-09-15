#pragma once

#include "block_range.h"

#include <util/system/types.h>

#include <span>

namespace NYdb::NBS::NBlockStore {

struct TWeightedRange
{
<<<<<<< HEAD
    ui64 Key{};
    TBlockRange64 Range;
=======
    TPBufferKey Key{};
    TBlockRange16 Range;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

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
    TBlockRange16 fullRange,
    std::span<const TWeightedRange> overlappingRanges);

}   // namespace NYdb::NBS::NBlockStore
