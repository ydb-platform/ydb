#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <unordered_map>

namespace NKikimr::NPDisk {
    enum EDeviceType : ui8 {
        DEVICE_TYPE_ROT = 0,
        DEVICE_TYPE_SSD = 1,
        DEVICE_TYPE_NVME = 2,
        DEVICE_TYPE_UNKNOWN = 255,
    };

    struct TDevicePerformanceParams {
        ui64 SeekTimeNs;
        ui64 FirstSectorReadBytesPerSec;
        ui64 LastSectorReadBytesPerSec;
        ui64 FirstSectorWriteBytesPerSec;
        ui64 LastSectorWriteBytesPerSec;
        ui64 BurstThresholdNs;
    };

    const static std::unordered_map<EDeviceType, TDevicePerformanceParams> DevicePerformance = {
        { DEVICE_TYPE_UNKNOWN, TDevicePerformanceParams{
            .SeekTimeNs = 0,
            .FirstSectorReadBytesPerSec = 0,
            .LastSectorReadBytesPerSec = 0,
            .FirstSectorWriteBytesPerSec = 0,
            .LastSectorWriteBytesPerSec = 0,
            .BurstThresholdNs = 1'000'000'000,
        } },
        { DEVICE_TYPE_ROT, TDevicePerformanceParams{
            .SeekTimeNs = 8000000,
            .FirstSectorReadBytesPerSec = 200ull * 1024 * 1024,
            .LastSectorReadBytesPerSec = 66ull * 1024 * 1024,
            .FirstSectorWriteBytesPerSec = 200ull * 1024 * 1024,
            .LastSectorWriteBytesPerSec = 66ull * 1024 * 1024,
            .BurstThresholdNs = 200'000'000,
        } },
        { DEVICE_TYPE_SSD, TDevicePerformanceParams{
            .SeekTimeNs = 40000,
            .FirstSectorReadBytesPerSec = 500ull * 1024 * 1024,
            .LastSectorReadBytesPerSec = 500ull * 1024 * 1024,
            .FirstSectorWriteBytesPerSec = 500ull * 1024 * 1024,
            .LastSectorWriteBytesPerSec = 500ull * 1024 * 1024,
            .BurstThresholdNs = 50'000'000,
        } },
        { DEVICE_TYPE_NVME, TDevicePerformanceParams{
            .SeekTimeNs = 40000,
            .FirstSectorReadBytesPerSec = 1000ull * 1024 * 1024,
            .LastSectorReadBytesPerSec = 1000ull * 1024 * 1024,
            .FirstSectorWriteBytesPerSec = 1000ull * 1024 * 1024,
            .LastSectorWriteBytesPerSec = 1000ull * 1024 * 1024,
            .BurstThresholdNs = 32'000'000,
        } },
    };

    TString DeviceTypeStr(const EDeviceType type, bool isShort);
    EDeviceType DeviceTypeFromStr(const TString &typeName);

} // NKikimr::NPDisk
