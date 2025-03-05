#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Creates an invoker that creates a codicil guard with a given string before each
//! callback invocation.
IInvokerPtr CreateCodicilGuardedInvoker(
    IInvokerPtr underlyingInvoker,
    std::string codicil);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
