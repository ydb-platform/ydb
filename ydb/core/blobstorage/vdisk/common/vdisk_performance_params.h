#pragma once

#include <unordered_map>

#include "defs.h"

#include <ydb/library/pdisk_io/device_type.h>

namespace NKikimr {

struct TVDiskPerformanceParams {
    const TDuration ReplMaxTimeToMakeProgress;
};

extern const std::unordered_map<NPDisk::EDeviceType, TVDiskPerformanceParams> VDiskPerformance;

} // namespace NKikimr
