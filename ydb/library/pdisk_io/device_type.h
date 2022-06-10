#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NPDisk {
    enum EDeviceType : ui8 {
        DEVICE_TYPE_ROT = 0,
        DEVICE_TYPE_SSD = 1,
        DEVICE_TYPE_NVME = 2,
        DEVICE_TYPE_UNKNOWN = 255,
    };

    TString DeviceTypeStr(const EDeviceType type, bool isShort);
    EDeviceType DeviceTypeFromStr(const TString &typeName);

} // NKikimr::NPDisk
