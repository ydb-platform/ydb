#pragma once
#include "defs.h"
#include "blobstorage_pdisk_defs.h"

#include <ydb/core/protos/blobstorage_disk_color.pb.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Color limits for the Quota Tracker
//
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TDiskColor {
    i64 Multiplier = 0;
    i64 Divisor = 1;
    i64 Addend = 0;

    TString ToString() const {
        return TStringBuilder() << Multiplier << " / " << Divisor << " + " << Addend;
    }

    i64 CalculateQuota(i64 total) const {
        return total * Multiplier / Divisor + Addend;
    }

    double CalculateOccupancy(i64 total) const {
        return (double)CalculateQuota(total) / total;
    }
};

struct TColorLimits {
    TDiskColor Black;
    TDiskColor Red;
    TDiskColor Orange;
    TDiskColor PreOrange;
    TDiskColor LightOrange;
    TDiskColor Yellow;
    TDiskColor LightYellow;
    TDiskColor Cyan;

    void Print(IOutputStream &str) {
        str << "  Black = Total * " << Black.ToString() << "\n";
        str << "  Red = Total * " << Red.ToString() << "\n";
        str << "  Orange = Total * " << Orange.ToString() << "\n";
        str << "  PreOrange = Total * " << PreOrange.ToString() << "\n";
        str << "  LightOrange = Total * " << LightOrange.ToString() << "\n";
        str << "  Yellow = Total * " << Yellow.ToString() << "\n";
        str << "  LightYellow = Total * " << LightYellow.ToString() << "\n";
        str << "  Cyan = Total * " << Cyan.ToString() << "\n";
    }

    static TColorLimits MakeChunkLimits(i64 cyan) {
        cyan = Min<i64>(130, cyan);
        cyan = Max<i64>(13, cyan);

        i64 lightYellow = cyan * 100 / 130;
        i64 yellow = cyan * 80 / 130;
        i64 lightOrange = cyan * 65 / 130;
        i64 preOrange = cyan * 50 / 130;
        i64 orange = cyan * 30 / 130;
        i64 red = cyan * 10 / 130;

        return {
            {1,   1000, 2}, // Black: Leave bare minimum for disaster recovery
            {red,  1000, 3}, // Red
            {orange,  1000, 4}, // Orange
            {preOrange,  1000, 4}, // PreOrange
            {lightOrange,  1000, 5}, // LightOrange
            {yellow,  1000, 6}, // Yellow: Stop serving user writes at 8% (by default) free space
            {lightYellow, 1000, 7}, // LightYellow: Ask tablets to move to another group at 10% (by default) free space
            {cyan, 1000, 8}, // Cyan: 13% (by default) free space or less
        };
    }

    static TColorLimits MakeLogLimits() {
        return {
            {100, 1000}, // Black: Stop early to leave some space for disaster recovery
            {150, 1000}, // Red
            {200, 1000}, // Orange
            {210, 1000}, // PreOrange
            {250, 1000}, // LightOrange
            {350, 1000}, // Yellow
            {400, 1000}, // LightYellow
            {450, 1000}, // Cyan
        };
    }

    static TColorLimits MakeExtendedLogLimits() {
        return {
            {50, 1000}, // Black: Stop early to leave some space for disaster recovery
            {100, 1000}, // Red
            {200, 1000}, // Orange
            {210, 1000}, // PreOrange
            {250, 1000}, // LightOrange
            {350, 1000}, // Yellow
            {400, 1000}, // LightYellow
            {450, 1000}, // Cyan
        };
    }

    double GetOccupancyForColor(NKikimrBlobStorage::TPDiskSpaceColor::E color, i64 total) {
        switch (color) {
            case NKikimrBlobStorage::TPDiskSpaceColor::GREEN:          return Cyan.CalculateOccupancy(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::CYAN:           return LightYellow.CalculateOccupancy(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_YELLOW:   return Yellow.CalculateOccupancy(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::YELLOW:         return LightOrange.CalculateOccupancy(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_ORANGE:   return PreOrange.CalculateOccupancy(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::PRE_ORANGE:     return Orange.CalculateOccupancy(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::ORANGE:         return Red.CalculateOccupancy(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::RED:            return Black.CalculateOccupancy(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::BLACK:          return 1.0;

            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ABORT();
        }
    }

    i64 GetQuotaForColor(NKikimrBlobStorage::TPDiskSpaceColor::E color, i64 total) {
        switch (color) {
            case NKikimrBlobStorage::TPDiskSpaceColor::CYAN:           return Cyan.CalculateQuota(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_YELLOW:   return LightYellow.CalculateQuota(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::YELLOW:         return Yellow.CalculateQuota(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_ORANGE:   return LightOrange.CalculateQuota(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::PRE_ORANGE:     return PreOrange.CalculateQuota(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::ORANGE:         return Orange.CalculateQuota(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::RED:            return Red.CalculateQuota(total);
            case NKikimrBlobStorage::TPDiskSpaceColor::BLACK:          return Black.CalculateQuota(total);

            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MAX_SENTINEL_DO_NOT_USE_:
            default:
                Y_ABORT();
        }
    }
};

} // NPDisk
} // NKikimr

