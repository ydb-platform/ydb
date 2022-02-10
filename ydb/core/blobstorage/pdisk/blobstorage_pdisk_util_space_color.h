#pragma once
#include "defs.h"

#include <ydb/core/protos/blobstorage_config.pb.h> 
#include <ydb/core/protos/blobstorage.pb.h> 

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

}
