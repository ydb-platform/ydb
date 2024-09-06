#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/library/profiling/public.h>
#include <yt/yt/library/profiling/tag.h>

#include <library/cpp/yt/memory/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// XXX(sandello): Facade does not have to be ref-counted.
class TActionQueue
    : public TRefCounted
{
public:
    explicit TActionQueue(TString threadName = "ActionQueue");
    virtual ~TActionQueue();

    void Shutdown(bool graceful = false);

    const IInvokerPtr& GetInvoker();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TActionQueue)

////////////////////////////////////////////////////////////////////////////////

//! Creates an invoker that executes all callbacks in the
//! context of #underlyingInvoker (possibly in different threads)
//! but in a serialized fashion (i.e. all queued callbacks are executed
//! in the proper order and no two callbacks are executed in parallel).
//! #invokerName is used as a profiling tag.
//! #registry is needed for testing purposes only.
IInvokerPtr CreateSerializedInvoker(
    IInvokerPtr underlyingInvoker,
    const TString& invokerName = "default",
    NProfiling::IRegistryImplPtr registry = nullptr);

IInvokerPtr CreateSerializedInvoker(
    IInvokerPtr underlyingInvoker,
    const NProfiling::TTagSet& tagSet,
    NProfiling::IRegistryImplPtr registry = nullptr);

////////////////////////////////////////////////////////////////////////////////

//! Creates a wrapper around IInvoker that supports callback reordering.
//! Callbacks with the highest priority are executed first.
//! #invokerName is used as a profiling tag.
//! #registry is needed for testing purposes only.
IPrioritizedInvokerPtr CreatePrioritizedInvoker(
    IInvokerPtr underlyingInvoker,
    const TString& invokerName = "default",
    NProfiling::IRegistryImplPtr registry = nullptr);

IPrioritizedInvokerPtr CreatePrioritizedInvoker(
    IInvokerPtr underlyingInvoker,
    const NProfiling::TTagSet& tagSet,
    NProfiling::IRegistryImplPtr registry = nullptr);

//! Creates a wrapper around IInvoker that implements IPrioritizedInvoker but
//! does not perform any actual reordering. Priorities passed to #IPrioritizedInvoker::Invoke
//! are ignored.
IPrioritizedInvokerPtr CreateFakePrioritizedInvoker(IInvokerPtr underlyingInvoker);

//! Creates a wrapper around IPrioritizedInvoker turning it into a regular IInvoker.
//! All callbacks are propagated with a given fixed #priority.
IInvokerPtr CreateFixedPriorityInvoker(
    IPrioritizedInvokerPtr underlyingInvoker,
    i64 priority);

////////////////////////////////////////////////////////////////////////////////

//! Creates an invoker that executes all callbacks in the
//! context of #underlyingInvoker allowing up to #maxConcurrentInvocations
//! outstanding requests to the latter.
IInvokerPtr CreateBoundedConcurrencyInvoker(
    IInvokerPtr underlyingInvoker,
    int maxConcurrentInvocations);

////////////////////////////////////////////////////////////////////////////////

ISuspendableInvokerPtr CreateSuspendableInvoker(IInvokerPtr underlyingInvoker);

////////////////////////////////////////////////////////////////////////////////

//! Creates an invoker that creates a codicil guard with a given string before each
//! callback invocation.
IInvokerPtr CreateCodicilGuardedInvoker(
    IInvokerPtr underlyingInvoker,
    TString codicil);

////////////////////////////////////////////////////////////////////////////////

//! Creates an invoker that emits warning into #logger when callback executes
//! longer than #threshold without interruptions.
IInvokerPtr CreateWatchdogInvoker(
    IInvokerPtr underlyingInvoker,
    const NLogging::TLogger& logger,
    TDuration threshold);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
