#pragma once
#include "defs.h"
#include "blobstorage_pdisk_defs.h"
#include "blobstorage_pdisk_color_limits.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk quota record
//
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#define DISK_SPACE_COLORS(XX) \
    XX(Black) \
    XX(Red) \
    XX(Orange) \
    XX(LightOrange) \
    XX(PreOrange) \
    XX(Yellow) \
    XX(LightYellow) \
    XX(Cyan) \
    //

class TQuotaRecord {
    friend class TPerOwnerQuotaTracker;

    TAtomic HardLimit = 0;
    TAtomic Free = 0;

#define DEFINE_DISK_SPACE_COLOR(NAME) TAtomic NAME = 0;
    DISK_SPACE_COLORS(DEFINE_DISK_SPACE_COLOR)
#undef DEFINE_DISK_SPACE_COLOR

    TString Name;
    std::optional<TVDiskID> VDiskId;
public:
    void SetName(const TString& name) {
        Name = name;
    }

    void SetVDiskId(const TVDiskID& v) {
        VDiskId = v;
    }

    i64 GetUsed() const {
        return AtomicGet(HardLimit) - AtomicGet(Free);
    }

    i64 GetHardLimit() const {
        return AtomicGet(HardLimit);
    }

    i64 GetFree() const {
        return AtomicGet(Free);
    }

    TString Print() const {
        TStringStream str;
        Print(str);
        return str.Str();
    }

    void Print(IOutputStream &str) const {
        str << "\nName# \"" << Name << "\"";
        if (VDiskId) {
            str << " VDiskId# " << *VDiskId;
        }
        str << "\n";
        str << " HardLimit# " << HardLimit;
        str << " Free# " << Free;
        str << " Used# " << GetUsed();
        double occupancy;
        str << " CurrentColor# " << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(EstimateSpaceColor(0, &occupancy)) << "\n";
        str << " Occupancy# " << occupancy << "\n";
#define PRINT_DISK_SPACE_COLOR(NAME) str << " " #NAME "# " << NAME;
        DISK_SPACE_COLORS(PRINT_DISK_SPACE_COLOR)
#undef PRINT_DISK_SPACE_COLOR
    }

    // Called only from the main thread
    // Returns number of chunks released (negative for chunks acquired)
    i64 ForceHardLimit(i64 hardLimit, const TColorLimits &limits) {
        i64 oldHardLimit = AtomicGet(HardLimit);
        i64 increment = hardLimit - oldHardLimit;
        AtomicAdd(HardLimit, increment);
        AtomicAdd(Free, increment);

        i64 value = 0;
#define CALCULATE_COLOR(NAME) \
        value = Max(value, limits.NAME.CalculateQuota(hardLimit)); \
        AtomicSet(NAME, value); \
        ++value;
        DISK_SPACE_COLORS(CALCULATE_COLOR)
#undef CALCULATE_COLOR

        return -increment;
    }

    bool ForceAllocate(i64 count) {
        return AtomicSub(Free, count) > AtomicGet(Black);
    }

    // Called only from the main thread
    bool TryAllocate(i64 count, TString &outErrorReason) {
        Y_ABORT_UNLESS(count > 0);
        if (AtomicSub(Free, count) > AtomicGet(Black)) {
            return true;
        }
        AtomicAdd(Free, count);
        outErrorReason = (TStringBuilder() << "Allocation of count# " << count
                << " chunks falls into the black zone, free# " << AtomicGet(Free)
                << " black# " << AtomicGet(Black)
                << " hardLimit# " << AtomicGet(HardLimit)
                << " Name# \"" << Name << "\""
                << " Marker# BPQ10");
        return false;
    }

    bool InitialAllocate(i64 count) {
        Y_ABORT_UNLESS(count >= 0);
        if (AtomicSub(Free, count) >= 0) {
            return true;
        } else {
            AtomicAdd(Free, count);
            return false;
        }
    }

    void Release(i64 count) {
        Y_ABORT_UNLESS(count > 0);
        TAtomicBase newFree = AtomicAdd(Free, count);
        Y_VERIFY_S(newFree <= AtomicGet(HardLimit), Print());
    }

    // Called from any thread
    // TODO(cthulhu): Profile and consider caching
    NKikimrBlobStorage::TPDiskSpaceColor::E EstimateSpaceColor(i64 count, double *occupancy) const {
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        const i64 newFree = AtomicGet(Free) - count;

        *occupancy = HardLimit ? (double)(HardLimit - newFree) / HardLimit : 1.0;

        if (newFree > AtomicGet(Cyan)) {
            return TColor::GREEN;
        } else if (newFree > AtomicGet(LightYellow)) {
            return TColor::CYAN;
        } else if (newFree > AtomicGet(Yellow)) {
            return TColor::LIGHT_YELLOW;
        } else if (newFree > AtomicGet(LightOrange)) {
            return TColor::YELLOW;
        } else if (newFree > AtomicGet(PreOrange)) {
            return TColor::LIGHT_ORANGE;
        } else if (newFree > AtomicGet(Orange)) {
            return TColor::PRE_ORANGE;
        } else if (newFree > AtomicGet(Red)) {
            return TColor::ORANGE;
        } else if (newFree > AtomicGet(Black)) {
            return TColor::RED;
        } else {
            return TColor::BLACK;
        }
    }

    ui32 ColorFlagLimit(NKikimrBlobStorage::TPDiskSpaceColor::E color) {
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

        switch (color) {
        case TColor::CYAN:
            return AtomicGet(HardLimit) - AtomicGet(Cyan);
        case TColor::LIGHT_YELLOW:
            return AtomicGet(HardLimit) - AtomicGet(LightYellow);
        case TColor::YELLOW:
            return AtomicGet(HardLimit) - AtomicGet(Yellow);
        case TColor::LIGHT_ORANGE:
            return AtomicGet(HardLimit) - AtomicGet(LightOrange);
        case TColor::PRE_ORANGE:
            return AtomicGet(HardLimit) - AtomicGet(PreOrange);
        case TColor::ORANGE:
            return AtomicGet(HardLimit) - AtomicGet(Orange);
        case TColor::RED:
            return AtomicGet(HardLimit) - AtomicGet(Red);
        case TColor::BLACK:
            return AtomicGet(HardLimit) - AtomicGet(Black);
        default:
            return 0;
        }
    }
};

} // NPDisk
} // NKikimr
