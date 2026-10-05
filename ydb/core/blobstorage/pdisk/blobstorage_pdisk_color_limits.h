#pragma once
#include "defs.h"
#include "blobstorage_pdisk_defs.h"

#include <ydb/core/protos/blobstorage_disk_color.pb.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

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
    i64 MinChunks = 0;

    TString ToString() const {
        TStringBuilder str;
        str << Multiplier << " / " << Divisor << " + " << Addend;
        if (MinChunks) {
            str << " min " << MinChunks;
        }
        return str;
    }

    i64 CalculateQuota(i64 total) const {
        return Max(total * Multiplier / Divisor + Addend, MinChunks);
    }

    double CalculateOccupancy(i64 total) const {
        return 1 - (double)CalculateQuota(total) / total;
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

    static constexpr i64 DefaultCyanPermille = 130;
    static constexpr i64 TightCyanPermille = 30;

    static constexpr i64 TightMinChunksBlack = 2;
    static constexpr i64 TightMinChunksRed = 4;
    static constexpr i64 TightMinChunksOrange = 8;
    static constexpr i64 TightMinChunksPreOrange = 12;
    static constexpr i64 TightMinChunksLightOrange = 14;
    static constexpr i64 TightMinChunksYellow = 16;
    static constexpr i64 TightMinChunksLightYellow = 20;
    static constexpr i64 TightMinChunksCyan = 24;

    static TColorLimits MakeChunkLimits(i64 cyan, bool tightFloors = false) {
        cyan = Min<i64>(DefaultCyanPermille, cyan);
        cyan = Max<i64>(13, cyan);

        i64 lightYellow = cyan * 100 / DefaultCyanPermille;
        i64 yellow = cyan * 80 / DefaultCyanPermille;
        i64 lightOrange = cyan * 65 / DefaultCyanPermille;
        i64 preOrange = cyan * 50 / DefaultCyanPermille;
        i64 orange = cyan * 30 / DefaultCyanPermille;
        i64 red = cyan * 10 / DefaultCyanPermille;

        if (tightFloors) {
            // 3% cyan (by default) with per-color chunk floors so small disks
            // keep a compaction runway. Addends are 0: Max(percent, MinChunks)
            // is the floor, not percent plus extra chunks.
            return {
                {1, 1000, 0, TightMinChunksBlack},
                {red, 1000, 0, TightMinChunksRed},
                {orange, 1000, 0, TightMinChunksOrange},
                {preOrange, 1000, 0, TightMinChunksPreOrange},
                {lightOrange, 1000, 0, TightMinChunksLightOrange},
                {yellow, 1000, 0, TightMinChunksYellow},
                {lightYellow, 1000, 0, TightMinChunksLightYellow},
                {cyan, 1000, 0, TightMinChunksCyan},
            };
        }

        return {
            {1,   1000, 2}, // Black: Leave bare minimum for disaster recovery
            {red,  1000, 3}, // Red
            {orange,  1000, 4}, // Orange
            {preOrange,  1000, 4}, // PreOrange
            {lightOrange,  1000, 5}, // LightOrange
            {yellow,  1000, 6}, // Yellow: Stop serving user writes at 8% (by default) free space
            {lightYellow, 1000, 7}, // LightYellow: Ask tablets to move to another group at 10% (by default) free space
            {cyan, 1000, 8}, // Cyan: 13% (by default) free space or less; EnableTightPDiskSpaceColors uses 3% plus MinChunks
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

    double GetOccupancyForColor(NKikimrBlobStorage::TPDiskSpaceColor::E color, i64 total) const {
        switch (color) {
                case NKikimrBlobStorage::TPDiskSpaceColor::GREEN:           return 0.0;
                case NKikimrBlobStorage::TPDiskSpaceColor::CYAN:            return Cyan.CalculateOccupancy(total);
                case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_YELLOW:    return LightYellow.CalculateOccupancy(total);
                case NKikimrBlobStorage::TPDiskSpaceColor::YELLOW:          return Yellow.CalculateOccupancy(total);
                case NKikimrBlobStorage::TPDiskSpaceColor::LIGHT_ORANGE:    return LightOrange.CalculateOccupancy(total);
                case NKikimrBlobStorage::TPDiskSpaceColor::PRE_ORANGE:      return PreOrange.CalculateOccupancy(total);
                case NKikimrBlobStorage::TPDiskSpaceColor::ORANGE:          return Orange.CalculateOccupancy(total);
                case NKikimrBlobStorage::TPDiskSpaceColor::RED:             return Red.CalculateOccupancy(total);
                case NKikimrBlobStorage::TPDiskSpaceColor::BLACK:           return Black.CalculateOccupancy(total);

            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ABORT();
        }
    }

    i64 GetQuotaForColor(NKikimrBlobStorage::TPDiskSpaceColor::E color, i64 total) const {
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
