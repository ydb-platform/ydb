#pragma once

#include "defs.h"
#include "prepare.h"

///////////////////////////////////////////////////////////////////////////
struct TFakeTabletLoadTest {
    const ui64 TabletsNum;
    const ui32 HugeBlobSize;
    const ui32 MinHugeBlobSize;
    const TDuration HugeBlobWaitTime;

    TFakeTabletLoadTest(ui64 tabletsNum, ui32 hugeBlobSize, ui32 minHugeBlobSize, const TDuration &hugeBlobWaitTime)
        : TabletsNum(tabletsNum)
        , HugeBlobSize(hugeBlobSize)
        , MinHugeBlobSize(minHugeBlobSize)
        , HugeBlobWaitTime(hugeBlobWaitTime)
    {}

    void operator ()(TConfiguration *conf);
};
