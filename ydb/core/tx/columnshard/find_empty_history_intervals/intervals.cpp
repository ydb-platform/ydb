#include "intervals.h"

#include <utility>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {

THistoryIntervals::THistoryIntervals(ui64 tabletId, TIntervals intervals)
    : TabletId(tabletId)
    , Intervals(std::move(intervals))
{
}

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
