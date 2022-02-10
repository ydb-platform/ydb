#pragma once 
 
#include <util/generic/string.h> 
#include <util/stream/str.h> 
#include <util/string/printf.h> 
#include <util/system/types.h> 
 
namespace NKikimr { 
 
 
class TPDiskCategory { 
    union { 
        struct { 
            ui64 IsSolidState : 1; 
            ui64 Kind : 55; 
            ui64 TypeExt : 8; 
        } N; 
 
        ui64 X; 
    } Raw; 
 
    // For compatibility TypeExt not used for old types (ROT, SSD), so followed scheme is used: 
    // ROT  -> IsSolidState# 0, TypeExt# 0 
    // SSD  -> IsSolidState# 1, TypeExt# 0 
    // NVME -> IsSolidState# 1, TypeExt# 2 
 
public: 
    enum EDeviceType : ui8 { 
        DEVICE_TYPE_ROT = 0, 
        DEVICE_TYPE_SSD = 1, 
        DEVICE_TYPE_NVME = 2, 
        DEVICE_TYPE_UNKNOWN = 255, 
    }; 
 
    static TPDiskCategory::EDeviceType DeviceTypeFromStr(const TString &typeName) { 
        if (typeName == "ROT" || typeName == "DEVICE_TYPE_ROT") { 
            return DEVICE_TYPE_ROT; 
        } else if (typeName == "SSD" || typeName == "DEVICE_TYPE_SSD") { 
            return DEVICE_TYPE_SSD; 
        } else if (typeName == "NVME" || typeName == "DEVICE_TYPE_NVME") { 
            return DEVICE_TYPE_NVME; 
        } 
        return DEVICE_TYPE_UNKNOWN; 
    } 
 
    static TString DeviceTypeStr(const TPDiskCategory::EDeviceType type, bool isShort) { 
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
 
    TPDiskCategory() = default; 
 
    TPDiskCategory(ui64 raw) { 
        Raw.X = raw; 
    } 
 
    TPDiskCategory(EDeviceType type, ui64 kind) { 
        Raw.N.TypeExt = 0; 
        if (type == DEVICE_TYPE_NVME) { 
            Raw.N.TypeExt = type; 
            Y_VERIFY(Raw.N.TypeExt == type, "type# %" PRIu64 " is out of range!", (ui64)type); 
        } 
        Raw.N.IsSolidState = (type == DEVICE_TYPE_SSD || type == DEVICE_TYPE_NVME); 
        Raw.N.Kind = kind; 
        Y_VERIFY(Raw.N.Kind == kind, "kind# %" PRIu64 " is out of range!", (ui64)kind); 
    } 
 
    ui64 GetRaw() const { 
        return Raw.X; 
    } 
 
    operator ui64() const { 
        return Raw.X; 
    } 
 
    bool IsSolidState() const { 
        return Raw.N.IsSolidState || Raw.N.TypeExt == DEVICE_TYPE_SSD || Raw.N.TypeExt == DEVICE_TYPE_NVME; 
    } 
 
    EDeviceType Type() const { 
        if (Raw.N.TypeExt == 0) { 
            if (Raw.N.IsSolidState) { 
                return DEVICE_TYPE_SSD; 
            } else { 
                return DEVICE_TYPE_ROT; 
            } 
        } 
        return static_cast<EDeviceType>(Raw.N.TypeExt); 
    } 
 
    TString TypeStrLong() const { 
        return DeviceTypeStr(Type(), false); 
    } 
 
    TString TypeStrShort() const { 
        return DeviceTypeStr(Type(), true); 
    } 
 
    ui64 Kind() const { 
        return Raw.N.Kind; 
    } 
 
    TString ToString() const { 
        TStringStream str; 
        str << "{Type# " << DeviceTypeStr(Type(), false); 
        str << " Kind# " << Raw.N.Kind; 
        str << "}"; 
        return str.Str(); 
    } 
}; 
 
static_assert(sizeof(TPDiskCategory) == sizeof(ui64), "sizeof(TPDiskCategory) must be 8 bytes!"); 
 
bool operator==(const TPDiskCategory x, const TPDiskCategory y); 
bool operator!=(const TPDiskCategory x, const TPDiskCategory y); 
 
bool operator<(const TPDiskCategory x, const TPDiskCategory y); 
 
} 
