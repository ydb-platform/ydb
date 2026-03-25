#include "watermark_tracker.h"

#include <util/system/yassert.h>

namespace NKikimr::NMiniKQL {

TWatermarkTracker::TWatermarkTracker(
    ui64 delay,
    ui64 granularity)
    : Delay_(delay)
    , Granularity_(granularity)
{
    Y_ABORT_UNLESS(granularity > 0);
}

std::optional<ui64> TWatermarkTracker::HandleNextEventTime(ui64 ts) {
    if (Y_UNLIKELY(ts >= NextEventWithWatermark_)) {
        NextEventWithWatermark_ = CalcNextEventWithWatermark(ts);
        return CalcLastWatermark();
    }

    return std::nullopt;
}

ui64 TWatermarkTracker::CalcNextEventWithWatermark(ui64 ts) {
    return ts + Granularity_ - (ts - Delay_) % Granularity_;
}

std::optional<ui64> TWatermarkTracker::CalcLastWatermark() {
    if (Y_UNLIKELY(Delay_ + Granularity_ > NextEventWithWatermark_)) {
        // Protect from negative values
        return std::nullopt;
    }
    return NextEventWithWatermark_ - Delay_ - Granularity_;
}

} // namespace NKikimr::NMiniKQL
