#pragma once

#include "block_range.h"
#include "pbuffer_key.h"

#include <span>

namespace NYdb::NBS::NBlockStore {

// A block range tagged with a PBuffer record id.
// SplitOnNonOverlappingContinuousRanges keeps the greatest key on overlaps. A
// default-constructed key marks a hole (no overlapping record; callers treat it
// as DDisk).
struct TWeightedRange
{
    TPBufferKey Key{};
    TBlockRange64 Range;

    bool operator<(const TWeightedRange& other) const
    {
        return Key < other.Key;
    }
};

// Splits overlapping ranges into a continuous sequence of non-overlapping
// ranges covering fullRange. Holes are filled with a default-constructed key.
TVector<TWeightedRange> SplitOnNonOverlappingContinuousRanges(
    TBlockRange64 fullRange,
    std::span<const TWeightedRange> overlappingRanges);

}   // namespace NYdb::NBS::NBlockStore
