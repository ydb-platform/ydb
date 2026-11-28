#pragma once

#include <util/random/random.h>
#include <util/generic/ylimits.h>
#include <util/system/types.h>

namespace NWilson {

// Opaque process incarnation for local-clock domains. This is not a timestamp
// or an identity that may be compared between different processes.
inline ui64 GetProcessTraceEpoch() {
    static const ui64 epoch = RandomNumber<ui64>(Max<i64>()) + 1;
    return epoch;
}

} // namespace NWilson
