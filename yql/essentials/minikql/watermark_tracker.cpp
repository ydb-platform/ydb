#include "watermark_tracker.h"

#include<util/system/yassert.h>

namespace NKikimr {
namespace NMiniKQL {

TWatermarkTracker::TWatermarkTracker(
    ui64 delay,
    ui64 granularity)
    : Delay(delay)
    , Granularity(granularity)
{
    Y_ABORT_UNLESS(granularity > 0);
}

std::optional<ui64> TWatermarkTracker::HandleNextEventTime(ui64 ts) {
    if (Y_UNLIKELY(ts >= NextEventWithWatermark)) {
        NextEventWithWatermark = CalcNextEventWithWatermark(ts);
        return CalcLastWatermark();
    }

    return std::nullopt;
}

ui64 TWatermarkTracker::CalcNextEventWithWatermark(ui64 ts) {
    return ts + Granularity - (ts - Delay) % Granularity;
}

std::optional<ui64> TWatermarkTracker::CalcLastWatermark() {
    if (Y_UNLIKELY(Delay + Granularity > NextEventWithWatermark)) {
        // Protect from negative values
        return std::nullopt;
    }
    return NextEventWithWatermark - Delay - Granularity;
}

} // NMiniKQL
} // NKikimr
