#include "params.h"

namespace NKikimr {

extern const std::unordered_map<NPDisk::EDeviceType, TVDiskPerformanceParams> DevicePerformance= {
    { DEVICE_TYPE_UNKNOWN, TVDiskPerformanceParams{
        .ReplMaxTimeToMakeProgress =    TDuration::Minutes(180),
    } },
    { DEVICE_TYPE_ROT, TVDiskPerformanceParams{
        .ReplMaxTimeToMakeProgress =    TDuration::Minutes(180),
    } },
    { DEVICE_TYPE_SSD, TVDiskPerformanceParams{
        .ReplMaxTimeToMakeProgress =    TDuration::Minutes(30),
    } },
    { DEVICE_TYPE_NVME, TVDiskPerformanceParams{
        .ReplMaxTimeToMakeProgress =    TDuration::Minutes(30),
    } },
};

} // namespace NKikimr
