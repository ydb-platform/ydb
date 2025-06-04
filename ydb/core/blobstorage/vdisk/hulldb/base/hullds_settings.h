#pragma once

#include "defs.h"
#include "blobstorage_hulldefs.h"

namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////
    // TLevelIndexSettings
    /////////////////////////////////////////////////////////////////////////
    struct TLevelIndexSettings {
        TIntrusivePtr<THullCtx> HullCtx;
    private:
        const ui32 Level0MaxSstsAtOnce;
    public:
        const ui64 BufSize;
        const ui64 CompThreshold;
        const TDuration FreshHistoryWindow;
        const ui64 FreshHistoryBuckets;
        const bool FreshUseDreg;
        const bool Level0UseDreg;
        const ui32 RealLevel0MaxSstsAtOnce;

        TLevelIndexSettings(TIntrusivePtr<THullCtx> hullCtx,
                            ui32 level0MaxSstsAtOnce,
                            ui64 bufSize,
                            ui64 compThreshold,
                            TDuration historyWindow,
                            ui64 historyBuckets,
                            bool freshUseDreg,
                            bool level0UseDreg)
            : HullCtx(std::move(hullCtx))
            , Level0MaxSstsAtOnce(level0MaxSstsAtOnce)
            , BufSize(bufSize)
            , CompThreshold(compThreshold)
            , FreshHistoryWindow(historyWindow)
            , FreshHistoryBuckets(historyBuckets)
            , FreshUseDreg(freshUseDreg)
            , Level0UseDreg(level0UseDreg)
            , RealLevel0MaxSstsAtOnce(level0UseDreg ? (Level0MaxSstsAtOnce * 2) : Level0MaxSstsAtOnce)
        {}

        ui32 GetMaxSstsAtLevel0AtOnce() const {
            return RealLevel0MaxSstsAtOnce;
        }
    };

} // NKikimr

