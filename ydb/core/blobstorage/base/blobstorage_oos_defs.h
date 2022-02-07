#pragma once
#include "defs.h"

namespace NKikimr {

    struct TOutOfSpaceStatus {
        ui32 Flags = 0;
        float ApproximateFreeSpaceShare = 0.0;

        TOutOfSpaceStatus(ui32 flags, float share)
            : Flags(flags)
            , ApproximateFreeSpaceShare(share)
        {}
    };

} // NKikimr

