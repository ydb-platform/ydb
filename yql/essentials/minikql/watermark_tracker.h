#pragma once

#include <optional>
#include <util/system/types.h>

namespace NKikimr {
namespace NMiniKQL {

class TWatermarkTracker {
public:
    TWatermarkTracker(ui64 lag, ui64 granularity);
    std::optional<ui64> HandleNextEventTime(ui64 ts);

private:
    ui64 CalcNextEventWithWatermark(ui64 ts);
    std::optional<ui64> CalcLastWatermark();

private:
    ui64 NextEventWithWatermark = 0;
    const ui64 Delay;
    const ui64 Granularity;
};

} // NMiniKQL
} // NKikimr
