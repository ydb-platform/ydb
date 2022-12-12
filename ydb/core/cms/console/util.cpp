#include "util.h"

#include <util/random/random.h>

namespace NKikimr::NConsole {

    NTabletPipe::TClientRetryPolicy FastConnectRetryPolicy() {
        auto policy = NTabletPipe::TClientRetryPolicy::WithRetries();
        policy.MaxRetryTime = TDuration::MilliSeconds(1000 + RandomNumber<ui32>(1000)); // 1-2 seconds
        return policy;
    }

} // namespace NKikimr::NConsole
