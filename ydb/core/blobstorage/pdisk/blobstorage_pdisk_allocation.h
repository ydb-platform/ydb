#pragma once

#include "defs.h"

namespace NKikimr::NPDisk {

// This describes the work, not the VDisk owner: a dynamic VDisk stores both
// USER and SYSTEM data. An allocation that does not say otherwise is Recovery
// and never silently inherits SYSTEM permission; VDisk metadata a SYSTEM write
// depends on (sync log, chunk keeper) asks for System explicitly.
//
// With PDiskControls.SystemReserveChunks set, USER leaves the system and the
// maintenance reserve alone, Recovery leaves the system reserve alone, and
// System and Maintenance are held back by neither: SYSTEM data is what the
// reserve is for, and maintenance (compaction, garbage collection) is what
// gives space back, so stopping it would keep the disk full for good.
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

// How much of the reserves an allocation of this purpose has to leave alone: a
// purpose of a lower rank is never refused where one of a higher rank at the
// same colour bound is granted.
inline ui32 AllocationReserveRank(EAllocationPurpose purpose) {
    switch (purpose) {
        case EAllocationPurpose::User: return 2;
        case EAllocationPurpose::Recovery: return 1;
        case EAllocationPurpose::System: return 0;
        case EAllocationPurpose::Maintenance: return 0;
        case EAllocationPurpose::Count: break;
    }
    Y_ABORT("invalid allocation purpose");
}

} // NKikimr::NPDisk
