#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/logging/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Creates an invoker that emits warning into #logger when callback executes
//! longer than #threshold without interruptions.
IInvokerPtr CreateWatchdogInvoker(
    IInvokerPtr underlyingInvoker,
    const NLogging::TLogger& logger,
    TDuration threshold);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
