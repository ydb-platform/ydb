#pragma once
#include "defs.h"

#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_disk_color.pb.h>

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_defs.h>

namespace NKikimr {

inline NKikimrBlobStorage::TPDiskSpaceColor::E StatusFlagToSpaceColor(NPDisk::TStatusFlags flags) {
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

    if (flags & NKikimrBlobStorage::StatusDiskSpaceBlack) {
        return TColor::BLACK;
    } else if (flags & NKikimrBlobStorage::StatusDiskSpaceRed) {
        return TColor::RED;
    } else if (flags & NKikimrBlobStorage::StatusDiskSpaceOrange) {
        return TColor::ORANGE;
    } else if (flags & NKikimrBlobStorage::StatusDiskSpaceLightOrange) {
        return TColor::LIGHT_ORANGE;
    } else if (flags & NKikimrBlobStorage::StatusDiskSpacePreOrange) {
        return TColor::PRE_ORANGE;
    } else if (flags & NKikimrBlobStorage::StatusDiskSpaceYellowStop) {
        return TColor::YELLOW;
    } else if (flags & NKikimrBlobStorage::StatusDiskSpaceLightYellowMove) {
        return TColor::LIGHT_YELLOW;
    } else if (flags & NKikimrBlobStorage::StatusDiskSpaceCyan) {
        return TColor::CYAN;
    } else {
        return TColor::GREEN;
    }
}

inline NPDisk::TStatusFlags SpaceColorToStatusFlag(NKikimrBlobStorage::TPDiskSpaceColor::E color) {
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

    NPDisk::TStatusFlags flags = NKikimrBlobStorage::StatusIsValid;
    switch (color) {
        case TColor::BLACK:
            flags |= NKikimrBlobStorage::StatusDiskSpaceBlack;
            [[fallthrough]];
        case TColor::RED:
            flags |= NKikimrBlobStorage::StatusDiskSpaceRed;
            [[fallthrough]];
        case TColor::ORANGE:
            flags |= NKikimrBlobStorage::StatusDiskSpaceOrange;
            [[fallthrough]];
        case TColor::PRE_ORANGE:
            flags |= NKikimrBlobStorage::StatusDiskSpacePreOrange;
            [[fallthrough]];
        case TColor::LIGHT_ORANGE:
            flags |= NKikimrBlobStorage::StatusDiskSpaceLightOrange;
            [[fallthrough]];
        case TColor::YELLOW:
            flags |= NKikimrBlobStorage::StatusDiskSpaceYellowStop;
            [[fallthrough]];
        case TColor::LIGHT_YELLOW:
            flags |= NKikimrBlobStorage::StatusDiskSpaceLightYellowMove;
            [[fallthrough]];
        case TColor::CYAN:
            flags |= NKikimrBlobStorage::StatusDiskSpaceCyan;
            [[fallthrough]];
        case TColor::GREEN:
            [[fallthrough]];
        case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MIN_SENTINEL_DO_NOT_USE_:
            [[fallthrough]];
        case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MAX_SENTINEL_DO_NOT_USE_:
            break;
    }
    return flags;
}

inline NKikimrBlobStorage::TPDiskSpaceColor::E ColorByName(const TString name) {
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

    if (name == "black") {
        return TColor::BLACK;
    } else if (name == "red") {
        return TColor::RED;
    } else if (name == "orange") {
        return TColor::ORANGE;
    } else if (name == "light_orange") {
        return TColor::LIGHT_ORANGE;
    } else if (name == "yellow") {
        return TColor::YELLOW;
    } else if (name == "light_yellow") {
        return TColor::LIGHT_YELLOW;
    } else if (name == "cyan") {
        return TColor::CYAN;
    } else {
        return TColor::GREEN;
    }
}

inline TString TPDiskSpaceColor_Name(const NKikimrBlobStorage::TPDiskSpaceColor::E color) {
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

    switch (color) {
    case TColor::BLACK: return "black";
    case TColor::RED: return "red";
    case TColor::ORANGE: return "orange";
    case TColor::LIGHT_ORANGE: return "light_orange";
    case TColor::YELLOW: return "yellow";
    case TColor::LIGHT_YELLOW: return "light_yellow";
    case TColor::CYAN: return "cyan";
    case TColor::GREEN: return "green";
    default: return "unknown";
    }
}

inline TString TPDiskSpaceColor_HtmlCode(const NKikimrBlobStorage::TPDiskSpaceColor::E color) {
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

    switch (color) {
    case TColor::BLACK: return "black";
    case TColor::RED: return "red";
    case TColor::ORANGE: return "orange";
    case TColor::LIGHT_ORANGE: return "#FFE500";
    case TColor::YELLOW: return "yellow";
    case TColor::LIGHT_YELLOW: return "#EEFF33";
    case TColor::CYAN: return "cyan";
    case TColor::GREEN: return "green";
    default: return "grey";
    }
}
}
