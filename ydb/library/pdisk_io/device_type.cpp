#include "device_type.h"
#include <util/string/printf.h>


namespace NKikimr::NPDisk {

namespace {

constexpr TDevicePerformanceParams DevicePerformance[] = {
    // DEVICE_TYPE_ROT = 0,
    TDevicePerformanceParams{
        .SeekTimeNs = 8000000,
        .FirstSectorReadBytesPerSec = 200ull * 1024 * 1024,
        .LastSectorReadBytesPerSec = 66ull * 1024 * 1024,
        .FirstSectorWriteBytesPerSec = 200ull * 1024 * 1024,
        .LastSectorWriteBytesPerSec = 66ull * 1024 * 1024,
        .BurstThresholdNs = 200'000'000,
    },
    // DEVICE_TYPE_SSD = 1,
    TDevicePerformanceParams{
        .SeekTimeNs = 40000,
        .FirstSectorReadBytesPerSec = 500ull * 1024 * 1024,
        .LastSectorReadBytesPerSec = 500ull * 1024 * 1024,
        .FirstSectorWriteBytesPerSec = 500ull * 1024 * 1024,
        .LastSectorWriteBytesPerSec = 500ull * 1024 * 1024,
        .BurstThresholdNs = 50'000'000,
    },
    // DEVICE_TYPE_NVME = 2,
    TDevicePerformanceParams{
        .SeekTimeNs = 40000,
        .FirstSectorReadBytesPerSec = 1000ull * 1024 * 1024,
        .LastSectorReadBytesPerSec = 1000ull * 1024 * 1024,
        .FirstSectorWriteBytesPerSec = 1000ull * 1024 * 1024,
        .LastSectorWriteBytesPerSec = 1000ull * 1024 * 1024,
        .BurstThresholdNs = 32'000'000,
    },
    // DEVICE_TYPE_UNKNOWN = 255,
    TDevicePerformanceParams{
        .SeekTimeNs = 0,
        .FirstSectorReadBytesPerSec = 0,
        .LastSectorReadBytesPerSec = 0,
        .FirstSectorWriteBytesPerSec = 0,
        .LastSectorWriteBytesPerSec = 0,
        .BurstThresholdNs = 1'000'000'000,
    },
};

} // namespace

    // static
    const TDevicePerformanceParams& TDevicePerformanceParams::Get(EDeviceType deviceType) {
        switch (deviceType) {
            case DEVICE_TYPE_ROT:
            case DEVICE_TYPE_SSD:
            case DEVICE_TYPE_NVME:
                return DevicePerformance[deviceType];
            default:
                return DevicePerformance[3];
        }
    }

    EDeviceType DeviceTypeFromStr(const TString &typeName) {
        if (typeName == "ROT" || typeName == "DEVICE_TYPE_ROT") {
            return DEVICE_TYPE_ROT;
        } else if (typeName == "SSD" || typeName == "DEVICE_TYPE_SSD") {
            return DEVICE_TYPE_SSD;
        } else if (typeName == "NVME" || typeName == "DEVICE_TYPE_NVME") {
            return DEVICE_TYPE_NVME;
        }
        return DEVICE_TYPE_UNKNOWN;
    }

    TString DeviceTypeStr(const EDeviceType type, bool isShort) {
        switch(type) {
            case DEVICE_TYPE_ROT:
                return isShort ? "ROT" : "DEVICE_TYPE_ROT";
            case DEVICE_TYPE_SSD:
                return isShort ? "SSD" : "DEVICE_TYPE_SSD";
            case DEVICE_TYPE_NVME:
                return isShort ? "NVME" : "DEVICE_TYPE_NVME";
            default:
                return Sprintf("DEVICE_TYPE_UNKNOWN(%" PRIu64 ")", (ui64)type);
        }
    }

} // NKikimr::NPDisk
