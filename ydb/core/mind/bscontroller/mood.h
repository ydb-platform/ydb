#pragma once
#include "defs.h"

namespace NKikimr {
namespace NBsController {

struct TMood {
    enum EValue {
        Normal = 0,
        Wipe = 1,
        Delete = 2,
        Donor = 3,
        ReadOnly = 4,
        ReadOnlyDonor = 5,
    };

    static bool IsDonor(const ui64 value) {
        return IsDonor(static_cast<TMood::EValue>(value));
    }

    static bool IsDonor(const EValue value) {
        return value == Donor || value == ReadOnlyDonor;
    }

    static bool IsReadOnly(const ui64 value) {
        return IsReadOnly(static_cast<TMood::EValue>(value));
    }

    static bool IsReadOnly(const EValue value) {
        return value == ReadOnly || value == ReadOnlyDonor;
    }

    static TString Name(const EValue value) {
        switch (value) {
            case Normal:
                return "Normal";
            case Wipe:
                return "Wipe";
            case Delete:
                return "Delete";
            case Donor:
                return "Donor";
            case ReadOnly:
                return "ReadOnly";
            case ReadOnlyDonor:
                return "ReadOnlyDonor";
        }
        return Sprintf("Unknown%" PRIu64, (ui64)value);
    }
};

struct TPDiskMood {
    enum EValue : ui8 {
        Normal = 0,
        Restarting = 1
    };

    static TString Name(const EValue value) {
        switch (value) {
            case Normal:
                return "Normal";
            case Restarting:
                return "Restarting";
        }
        return Sprintf("Unknown%" PRIu64, (ui64)value);
    }
};

}  // NBsController
}  // NKikimr
