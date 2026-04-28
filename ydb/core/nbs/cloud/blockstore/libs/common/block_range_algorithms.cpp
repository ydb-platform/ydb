#include "block_range_algorithms.h"

#include <util/generic/algorithm.h>
#include <util/generic/set.h>

namespace NYdb::NBS::NBlockStore::NBlockRangeAlgorithmsInternal {

bool TBoundary::operator<(const TBoundary& rhs) const
{
    return std::make_tuple(Offset, Open, Key) <
           std::make_tuple(rhs.Offset, rhs.Open, rhs.Key);
}

// The function with the main algorithm.
// Algorithm's short description:
// - sort boundaries by offset
// - iterate over boundaries and keep active keys for all current overlapping
// ranges
// - call callback function on the end of the current range with an active lsn

void SplitExecOnNonOverlappingRanges(TVector<TBoundary> boundaries, Cb cb)
{
    Sort(boundaries.begin(), boundaries.end());

    TSet<ui64, std::greater<ui64>> activeKeys;
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
                if (!cb(currentBestKey,
                        TBlockRange64::MakeClosedInterval(
                            segmentStart,
                            segmentEnd)))
                {
                    return;
                }
            }
            segmentStart = boundaries[i].Offset;
            currentBestKey = newBestKey;
        }
    }
}

}   // namespace NYdb::NBS::NBlockStore::NBlockRangeAlgorithmsInternal
