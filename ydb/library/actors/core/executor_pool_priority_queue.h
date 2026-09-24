#pragma once

#include <util/system/types.h>

namespace NActors::NPrivate {

// Poll High once before Normal. A ring queue may temporarily fail Pop despite
// having queued items; allow Normal on that miss instead of adding a shared
// availability counter or retrying High here.
template<class TQueue>
ui32 PopPriorityActivation(TQueue& high, TQueue& normal, ui64 revolvingCounter, bool& isHigh) {
    if (const ui32 hint = high.Pop(revolvingCounter)) {
        isHigh = true;
        return hint;
    }
    isHigh = false;
    return normal.Pop(revolvingCounter);
}

} // namespace NActors::NPrivate
