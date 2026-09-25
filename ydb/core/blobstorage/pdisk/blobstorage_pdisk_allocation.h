#pragma once

#include "defs.h"

namespace NKikimr::NPDisk {

// This describes the work, not the VDisk owner: a dynamic VDisk stores both
// USER and SYSTEM data. Recovery never silently inherits SYSTEM permission.
enum class EAllocationPurpose : ui8 {
    User,
    System,
    Maintenance,
    Recovery,
    Count,
};

inline const char* AllocationPurposeName(EAllocationPurpose purpose) {
    switch (purpose) {
        case EAllocationPurpose::User: return "USER";
        case EAllocationPurpose::System: return "SYSTEM";
        case EAllocationPurpose::Maintenance: return "MAINTENANCE";
        case EAllocationPurpose::Recovery: return "RECOVERY";
        case EAllocationPurpose::Count: break;
    }
    Y_ABORT("invalid allocation purpose");
}

} // NKikimr::NPDisk
