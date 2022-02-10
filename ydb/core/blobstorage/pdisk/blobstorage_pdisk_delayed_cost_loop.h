#pragma once
#include "defs.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 2-tact work cycle types
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TDelayedCostLoop {
    TVector<ui64> Data;
    ui64 Begin = 0;
    ui64 End = 0;
public:
    TDelayedCostLoop(ui64 loopSize);
    void Clear();
    void Push(ui64 cost);

    // Tries popping the cost specified from the loop. Returns the remaining cost if there was not enough cost in the
    // loop.
    ui64 TryPop(ui64 cost);
};

} // NPDisk
} // NKikimr

