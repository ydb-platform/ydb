#pragma once

#include "device_type.h"
#include <util/generic/string.h>

namespace NKikimr {
namespace NPDisk {

struct TDriveData {
    TString Path;
    bool IsWriteCacheValid = false;
    bool IsWriteCacheEnabled = false;
    TString SerialNumber;
    TString FirmwareRevision;
    TString ModelNumber;
    EDeviceType DeviceType = DEVICE_TYPE_UNKNOWN;
    ui64 Size = 0;
    bool IsMock = false;

    TDriveData() = default;

    TString ToString(bool isMultiline) const;
};

bool operator==(const TDriveData& lhs, const TDriveData& rhs);

} // NPDisk
} // NKikimr
