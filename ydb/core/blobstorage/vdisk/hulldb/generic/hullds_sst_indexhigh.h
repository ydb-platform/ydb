#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/memusage.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_rec.h>

namespace NKikimr {

    // The high level shared by the two-level and packed forms: one record per (tablet, channel, generation).
    struct TLogoBlobSstRecHigh {
        ui64 TabletId;
        ui64 ChannelGeneration; // Channel << 32 | Generation, i.e. TLogoBlobID::GetRaw()[1] >> 24
        ui32 LowRangeEndIndex = 0;

        explicit TLogoBlobSstRecHigh(const TLogoBlobID& id)
            : TabletId(id.GetRaw()[0])
            , ChannelGeneration(id.GetRaw()[1] >> 24)
        {}

        bool SameKey(const TLogoBlobSstRecHigh& r) const {
            return TabletId == r.TabletId && ChannelGeneration == r.ChannelGeneration;
        }

        bool operator < (const TLogoBlobSstRecHigh& r) const {
            return TabletId != r.TabletId ? TabletId < r.TabletId : ChannelGeneration < r.ChannelGeneration;
        }
    };

    static_assert(sizeof(TLogoBlobSstRecHigh) == 24, "expect sizeof(TLogoBlobSstRecHigh) == 24");
    static_assert(alignof(TLogoBlobSstRecHigh) == 8, "expect alignof(TLogoBlobSstRecHigh) == 8");

    TTrackableVector<TLogoBlobSstRecHigh> BuildLogoBlobSstIndexHigh(const TMemoryConsumer& consumer,
        const TTrackableVector<TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>>& linearIndex);

} // NKikimr
