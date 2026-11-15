#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/public.h>
#include <yt/yt/library/profiling/tag.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Creates a wrapper around IInvoker that supports callback reordering.
//! Callbacks with the highest priority are executed first.
//! #invokerName is used as a profiling tag.
//! #registry is needed for testing purposes only.
IPrioritizedInvokerPtr CreatePrioritizedInvoker(
    IInvokerPtr underlyingInvoker);

IPrioritizedInvokerPtr CreatePrioritizedInvoker(
    IInvokerPtr underlyingInvoker,
    const std::string& invokerName,
    NProfiling::IRegistryPtr registry = nullptr);

IPrioritizedInvokerPtr CreatePrioritizedInvoker(
    IInvokerPtr underlyingInvoker,
    const NProfiling::TTagSet& tagSet,
    NProfiling::IRegistryPtr registry = nullptr);

//! Creates a wrapper around IInvoker that implements IPrioritizedInvoker but
//! does not perform any actual reordering. Priorities passed to #IPrioritizedInvoker::Invoke
//! are ignored.
IPrioritizedInvokerPtr CreateFakePrioritizedInvoker(
    IInvokerPtr underlyingInvoker);

//! Creates a wrapper around IPrioritizedInvoker turning it into a regular IInvoker.
//! All callbacks are propagated with a given fixed #priority.
IInvokerPtr CreateFixedPriorityInvoker(
    IPrioritizedInvokerPtr underlyingInvoker,
    i64 priority);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
