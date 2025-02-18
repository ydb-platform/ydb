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
    };

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
        }
        return Sprintf("Unknown%" PRIu64, (ui64)value);
    }
};

struct TPDiskMood {
    enum EValue : ui8 {
        Normal = 0,
        Restarting = 1,
        ReadOnly = 2,
        Stop = 3
    };

    static TString Name(const EValue value) {
        switch (value) {
            case Normal:
                return "Normal";
            case Restarting:
                return "Restarting";
            case ReadOnly:
                return "ReadOnly";
            case Stop:
                return "Stop";
        }
        return Sprintf("Unknown%" PRIu64, (ui64)value);
    }
};

}  // NBsController
}  // NKikimr
