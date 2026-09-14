#pragma once
#include "sleep.h"

namespace NActors {

    inline auto AsyncYield() {
        return AsyncSleepFor(TDuration::Zero());
    }

} // namespace NActors
