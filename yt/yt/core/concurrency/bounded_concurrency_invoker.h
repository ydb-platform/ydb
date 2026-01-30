#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Creates an invoker that executes all callbacks in the
//! context of #underlyingInvoker allowing up to #maxConcurrentInvocations
//! outstanding requests to the latter.
IBoundedConcurrencyInvokerPtr CreateBoundedConcurrencyInvoker(
    IInvokerPtr underlyingInvoker,
    int maxConcurrentInvocations);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
