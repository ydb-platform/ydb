#pragma once

#include "public.h"

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

IDnsResolverPtr CreateAresDnsResolver(
    int retries,
    TDuration resolveTimeout,
    TDuration maxResolveTimeout,
    TDuration warningTimeout,
    std::optional<double> jitter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns

