#include "helpers.h"

#include <util/generic/size_literals.h>

#include <cmath>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

ui64 CalculateThrottlerC1(double maxIops, double maxBandwidth)
{
    if (maxBandwidth == 0) {
        return maxIops;
    }

    const auto denominator = Max(1_KB / maxIops - 4_MB / maxBandwidth, 0.);

    if (abs(denominator) < 1e-5) {
        // fallback for "special" params
        return maxIops;
    }

    return Max<ui64>((1_KB - 1) / denominator, 1);
}

ui64 CalculateThrottlerC2(double maxIops, double maxBandwidth)
{
    if (maxBandwidth == 0) {
        return 0;
    }

    const auto denominator = Max(4_MB / maxBandwidth - 1 / maxIops, 0.);

    if (abs(denominator) < 1e-5) {
        // fallback for "special" params
        return Max<ui64>();
    }

    return Max<ui64>(Min<ui64>((4_MB - 4_KB) / denominator, Max<ui64>()), 1);
}

TDuration SecondsToDuration(double seconds)
{
    return TDuration::MicroSeconds(ceil(1e6 * seconds));
}

TDuration CostPerIO(ui64 maxIops, ui64 maxBandwidth, ui64 byteCount)
{
    Y_DEBUG_ABORT_UNLESS(maxIops);

    double cost = 1.0 / static_cast<double>(maxIops);

    // 0 is special value which disables throttling by byteCount
    if (maxBandwidth) {
        cost +=
            static_cast<double>(byteCount) / static_cast<double>(maxBandwidth);
    }

    return SecondsToDuration(cost);
}

}   // namespace NYdb::NBS
