#include "vdisk_performance_params.h"

namespace NKikimr {

extern const std::unordered_map<NPDisk::EDeviceType, TVDiskPerformanceParams> VDiskPerformance = {
    { NPDisk::DEVICE_TYPE_UNKNOWN, TVDiskPerformanceParams{
        .ReplMaxTimeToMakeProgress =    TDuration::Minutes(180),
    } },
    { NPDisk::DEVICE_TYPE_ROT, TVDiskPerformanceParams{
        .ReplMaxTimeToMakeProgress =    TDuration::Minutes(180),
    } },
    { NPDisk::DEVICE_TYPE_SSD, TVDiskPerformanceParams{
        .ReplMaxTimeToMakeProgress =    TDuration::Minutes(30),
    } },
    { NPDisk::DEVICE_TYPE_NVME, TVDiskPerformanceParams{
        .ReplMaxTimeToMakeProgress =    TDuration::Minutes(30),
    } },
};

} // namespace NKikimr
