#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/public.h>
#include <yt/yt/library/profiling/tag.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Creates an invoker that executes all callbacks in the
//! context of #underlyingInvoker (possibly in different threads)
//! but in a serialized fashion (i.e. all queued callbacks are executed
//! in the proper order and no two callbacks are executed in parallel).
//! #invokerName is used as a profiling tag.
//! #registry is needed for testing purposes only.
IInvokerPtr CreateSerializedInvoker(
    IInvokerPtr underlyingInvoker);

IInvokerPtr CreateSerializedInvoker(
    IInvokerPtr underlyingInvoker,
    const std::string& invokerName,
    NProfiling::IRegistryPtr registry = nullptr);

IInvokerPtr CreateSerializedInvoker(
    IInvokerPtr underlyingInvoker,
    const NProfiling::TTagSet& tagSet,
    NProfiling::IRegistryPtr registry = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
