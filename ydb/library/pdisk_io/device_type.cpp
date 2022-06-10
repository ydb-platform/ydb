#include "device_type.h"
#include <util/string/printf.h>


namespace NKikimr::NPDisk {
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
