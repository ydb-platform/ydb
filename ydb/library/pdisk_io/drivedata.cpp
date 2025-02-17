#include "drivedata.h"
#include "device_type.h"

#include <util/stream/str.h>

namespace NKikimr {
namespace NPDisk {

TString TDriveData::ToString(bool isMultiline) const {
    const char *x = isMultiline ? "\n" : "";
    TStringStream str;
    str << "{Path# " << Path.Quote() << x
        << " IsWriteCacheValid# " << IsWriteCacheValid << x
        << " IsWriteCacheEnabled# " << IsWriteCacheEnabled << x
        << " ModelNumber# " << ModelNumber.Quote() << x
        << " SerialNumber# " << SerialNumber.Quote() << x
        << " FirmwareRevision# " << FirmwareRevision.Quote() << x
        << " DeviceType# " << NPDisk::DeviceTypeStr(DeviceType, true) << x
        << " Size# " << Size << x
        << "}" << x;
    return str.Str();
}

bool operator==(const NPDisk::TDriveData& lhs, const NPDisk::TDriveData& rhs) {
    return
        lhs.Path == rhs.Path
        // TODO(cthulhu): Use a map for the list of drives with path+model+serial as the key, not a vector
        // so that operator== can actually check for equality.
        // Consider serializing write cache state and processing it at the controller (system tables for example)
        // && lhs.IsWriteCacheValid == rhs.IsWriteCacheValid
        // && lhs.IsWriteCacheEnabled == rhs.IsWriteCacheEnabled
        && lhs.SerialNumber == rhs.SerialNumber
        && lhs.FirmwareRevision == rhs.FirmwareRevision
        && lhs.ModelNumber == rhs.ModelNumber
        && lhs.DeviceType == rhs.DeviceType
        && lhs.Size == rhs.Size
        && lhs.IsMock == rhs.IsMock;
}


} // NPDisk
} // NKikimr
