#include "block_range_algorithms.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/algorithm.h>
#include <util/generic/set.h>

namespace NYdb::NBS::NBlockStore {

namespace {

struct TBoundary
{
    ui64 Offset{};
    ui64 Key{};
    bool Open{};

    bool operator<(const TBoundary& rhs) const;
};

bool TBoundary::operator<(const TBoundary& rhs) const
{
    return std::make_tuple(Offset, Open, Key) <
           std::make_tuple(rhs.Offset, rhs.Open, rhs.Key);
}

}   // namespace

// Algorithm's short description:
// - create vector with boundaries (also lsn and open/close sign) of source
// ranges
// - sort boundaries by offset
// - iterate over boundaries and keep active keys for all current overlapping
// ranges
// - add range into the result on the end of the current range with an active
// lsn

TVector<TWeightedRange> SplitOnNonOverlappingContinuousRanges(
    TBlockRange64 fullRange,
    std::span<const TWeightedRange> overlappingRanges)
{
    TVector<TWeightedRange> result;
    if (overlappingRanges.empty()) {
        result.push_back({.Key = 0, .Range = fullRange});
        return result;
    }

    // prepare boundaries
    TStackVec<TBoundary> boundaries;
    boundaries.push_back({.Offset = fullRange.Start, .Key = 0, .Open = true});
    boundaries.push_back(
        {.Offset = fullRange.End + 1, .Key = 0, .Open = false});

    for (const auto& item: overlappingRanges) {
        auto intersect = fullRange.Intersect(item.Range);
        boundaries.push_back(
            {.Offset = intersect.Start, .Key = item.Key, .Open = true});
        boundaries.push_back(
            {.Offset = intersect.End + 1, .Key = item.Key, .Open = false});
    }
    Sort(boundaries.begin(), boundaries.end());

    // main algorithm's part
    TSet<ui64, std::greater<>> activeKeys;
    activeKeys.insert(boundaries[0].Key);
    ui64 segmentStart = boundaries[0].Offset;
    ui64 currentBestKey = *activeKeys.begin();

    for (ui64 i = 1; i < boundaries.size(); ++i) {
        if (boundaries[i].Open) {
            activeKeys.insert(boundaries[i].Key);
        } else {
            activeKeys.erase(boundaries[i].Key);
        }

        ui64 newBestKey = activeKeys.empty() ? 0 : *activeKeys.begin();
        bool isLast = activeKeys.empty();
        if (isLast) {
            Y_ABORT_IF(i != boundaries.size() - 1);
        }
        if (newBestKey != currentBestKey || isLast) {
            if (boundaries[i].Offset > segmentStart) {
                ui64 segmentEnd = boundaries[i].Offset - 1;
                result.push_back(
                    {.Key = currentBestKey,
                     .Range = TBlockRange64::MakeClosedInterval(
                         segmentStart,
                         segmentEnd)});
            }
            segmentStart = boundaries[i].Offset;
            currentBestKey = newBestKey;
        }
    }

    return result;
}

}   // namespace NYdb::NBS::NBlockStore
