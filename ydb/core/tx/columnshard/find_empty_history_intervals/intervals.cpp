#include "intervals.h"

#include <utility>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {

TIntervalFilter::TIntervalFilter(ui64 tabletId, TIntervals intervals)
    : TabletId(tabletId)
    , Intervals(std::move(intervals))
{
}

std::optional<ui32> TIntervalFilter::Exclude(const TLogoBlobID& blobId) {
    // Shared blobs belong to their source tablet and must not exclude our history.
    if (blobId.TabletID() != TabletId) {
        return {};
    }
    auto it = Intervals.upper_bound({ blobId.Channel(), blobId.Generation() });
    if (it == Intervals.begin()) {
        return {};
    }
    --it;
    if (it->first.Channel != blobId.Channel() || blobId.Generation() >= it->second.To) {
        return {};
    }
    const ui32 group = it->second.Group;
    Intervals.erase(it);
    return group;
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
