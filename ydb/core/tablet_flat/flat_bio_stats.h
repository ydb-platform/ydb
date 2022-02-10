#pragma once

#include "flat_bio_events.h"
#include <ydb/core/tablet/tablet_metrics.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBlockIO {

    enum class EDir {
        Read,
        Write,
    };

    struct TEvStat: public TEventLocal<TEvStat, ui32(EEv::Stat)> {
        using TByCnGr = std::unordered_map<std::pair<ui8, ui32>, ui64>;

        TEvStat(EDir dir, EPriority priority, ui32 group, const TLogoBlobID& blobId)
            : Dir(dir)
            , Priority(priority)
            , Bytes(blobId.BlobSize())
            , Ops(1)
        {
            GroupBytes.emplace(std::make_pair(blobId.Channel(), group), Bytes);
            GroupOps.emplace(std::make_pair(blobId.Channel(), group), Ops);
        }

        TEvStat(EDir dir, EPriority priority, ui64 bytes, ui64 ops, TByCnGr groupBytes, TByCnGr groupOps)
            : Dir(dir)
            , Priority(priority)
            , Bytes(bytes)
            , Ops(ops)
            , GroupBytes(std::move(groupBytes))
            , GroupOps(std::move(groupOps))
        {

        }

        const EDir Dir;
        const EPriority Priority;
        const ui64 Bytes;
        const ui64 Ops;
        TByCnGr GroupBytes;
        TByCnGr GroupOps;
    };

}
}
}
