#include "vdisk_performance_params.h"

namespace NKikimr {

TCostMetricsParameters::TCostMetricsParameters(ui64 defaultBurstThresholdMs)
    : BurstThresholdNs(defaultBurstThresholdMs * 1'000'000, 1, 1'000'000'000'000)
    , DiskTimeAvailableScale(1'000, 1, 1'000'000)
{}

} // namespace NKikimr
