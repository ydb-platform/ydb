#include "intervals.h"

#include <utility>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {

THistoryIntervals::THistoryIntervals(ui64 tabletId, TIntervals intervals)
    : TabletId(tabletId)
    , Intervals(std::move(intervals))
{
}

void THistoryIntervals::Exclude(const TLogoBlobID& blobId, std::optional<ui32> actualGroup, TFunctionRef<void()> onGroupMismatch) {
    // Shared blobs belong to their source tablet and must not exclude our history.
    if (blobId.TabletID() != TabletId) {
        return;
    }
    auto it = Intervals.upper_bound({ blobId.Channel(), blobId.Generation() });
    if (it == Intervals.begin()) {
        return;
    }
    --it;
    const auto& [key, interval] = *it;
    if (key.Channel != blobId.Channel() || blobId.Generation() >= interval.To) {
        return;
    }
    const bool mismatch = actualGroup && *actualGroup != interval.Group;
    Intervals.erase(it);
    if (mismatch) {
        onGroupMismatch();
    }
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
