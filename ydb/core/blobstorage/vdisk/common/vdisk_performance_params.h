#pragma once

#include <unordered_map>

#include "defs.h"

#include <ydb/library/pdisk_io/device_type.h>
#include <ydb/core/control/immediate_control_board_wrapper.h>

namespace NKikimr {

struct TVDiskPerformanceParams {
    const TDuration ReplMaxTimeToMakeProgress;
};

extern const std::unordered_map<NPDisk::EDeviceType, TVDiskPerformanceParams> VDiskPerformance;

struct TCostMetricsParameters {
    TCostMetricsParameters(ui64 defaultBurstThresholdMs = 100);
    TControlWrapper BurstThresholdNs;
    TControlWrapper DiskTimeAvailableScale;
};

using TCostMetricsParametersByMedia = TStackVec<TCostMetricsParameters, 3>;

} // namespace NKikimr
