#pragma once

#include <ydb/library/pdisk_io/device_type.h>
#include <util/generic/string.h>
#include <util/stream/str.h>
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

    TPDiskCategory() = default;

    TPDiskCategory(ui64 raw) {
        Raw.X = raw;
    }

    TPDiskCategory(NPDisk::EDeviceType type, ui64 kind) {
        Raw.N.TypeExt = 0;
        if (type == NPDisk::DEVICE_TYPE_NVME) {
            Raw.N.TypeExt = type;
            Y_ABORT_UNLESS(Raw.N.TypeExt == type, "type# %" PRIu64 " is out of range!", (ui64)type);
        }
        Raw.N.IsSolidState = (type == NPDisk::DEVICE_TYPE_SSD || type == NPDisk::DEVICE_TYPE_NVME);
        Raw.N.Kind = kind;
        Y_ABORT_UNLESS(Raw.N.Kind == kind, "kind# %" PRIu64 " is out of range!", (ui64)kind);
    }

    ui64 GetRaw() const {
        return Raw.X;
    }

    operator ui64() const {
        return Raw.X;
    }

    bool IsSolidState() const {
        return Raw.N.IsSolidState || Raw.N.TypeExt == NPDisk::DEVICE_TYPE_SSD || Raw.N.TypeExt == NPDisk::DEVICE_TYPE_NVME;
    }

    NPDisk::EDeviceType Type() const {
        if (Raw.N.TypeExt == 0) {
            if (Raw.N.IsSolidState) {
                return NPDisk::DEVICE_TYPE_SSD;
            } else {
                return NPDisk::DEVICE_TYPE_ROT;
            }
        }
        return static_cast<NPDisk::EDeviceType>(Raw.N.TypeExt);
    }

    TString TypeStrLong() const {
        return NPDisk::DeviceTypeStr(Type(), false);
    }

    TString TypeStrShort() const {
        return NPDisk::DeviceTypeStr(Type(), true);
    }

    ui64 Kind() const {
        return Raw.N.Kind;
    }

    TString ToString() const {
        TStringStream str;
        str << "{Type# " << NPDisk::DeviceTypeStr(Type(), false);
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
