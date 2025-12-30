#pragma once
#include <util/datetime/base.h>

namespace NYql::NHoppingWindow {

enum class EPolicy {       // Note: only effective after watermark set
    Drop /* "drop" */,     // drop events outside limit
    Adjust /* "adjust" */, // reassign event to first possible/last possible window
    Close /* "close" */,   // adjust window so that limit is satisfied; unimplemented for late events
};

struct TSettings {
    EPolicy LatePolicy = EPolicy::Drop;
    EPolicy EarlyPolicy = EPolicy::Close;
    TDuration FarFutureTimeLimit = TDuration::Zero(); // ahead of current watermark (effective only when watermark is set)
    ui64 FarFutureSizeLimit = Max<ui64>();            // number of "far future" hops
    auto operator<=>(const TSettings&) const = default;
};

} // namespace NYql::NHoppingWindow
