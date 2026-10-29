#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/memusage.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_rec.h>

namespace NKikimr {

    // The high level shared by the two-level and packed forms: one record per (tablet, channel, generation).
    struct TLogoBlobSstRecHigh {
        ui64 TabletId;
        ui64 ChannelGeneration; // Channel << 32 | Generation
        ui32 LowRangeEndIndex = 0;

        static constexpr ui32 StepLowBits = 64 - TLogoBlobID::StepLowShift;

        explicit TLogoBlobSstRecHigh(const TLogoBlobID& id)
            : TabletId(id.GetRaw()[0])
            , ChannelGeneration(id.GetRaw()[1] >> TLogoBlobID::GenerationShift)
        {}

        ui64 Raw1(ui64 step) const {
            return ChannelGeneration << TLogoBlobID::GenerationShift | step >> StepLowBits;
        }

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
