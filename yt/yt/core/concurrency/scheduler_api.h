#pragma once

#include "public.h"
#include "context_switch.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/tracing/public.h>

#include <optional>

#ifdef _win_
#undef Yield
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// Forward declaration
IInvoker* GetCurrentInvoker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

using TFiberCanceler = TCallback<void(const TError&)>;

TFiberCanceler GetCurrentFiberCanceler();

////////////////////////////////////////////////////////////////////////////////

//! Returns the current fiber id.
TFiberId GetCurrentFiberId();

//! Sets the current fiber id.
void SetCurrentFiberId(TFiberId id);

////////////////////////////////////////////////////////////////////////////////

//! Options governing how a wait is performed.
struct TWaitOptions
{
    //! Yield the fiber (SuspendFiber) or block the thread (BlockThread).
    EWaitForStrategy Strategy = EWaitForStrategy::SuspendFiber;

    //! Invoker the fiber is resumed on (SuspendFiber only); null means the current one.
    IInvokerPtr ResumingInvoker = {};

    //! Whether to yield the fiber even when |future| is already set (SuspendFiber only).
    //! false gives the "fast" behavior — no reschedule when the future is ready.
    bool AlwaysYieldFiber = true;

    //! Absolute deadline; std::nullopt means no timeout.
    /*!
     *  Reaching the deadline does not cancel the awaited future — the wait simply gives up
     *  while the future keeps running. This is the principal difference from
     *  #TFuture::WithTimeout.
     */
    std::optional<TInstant> Deadline = {};

    //! Sets #Deadline to |timeout| from now.
    TWaitOptions WithTimeout(TDuration timeout) &&;
};

//! Blocks the current fiber until |future| is set and returns the resulting value.
//! The fiber is rescheduled to |invoker|.
template <CFuture TFuture>
TErrorOr<typename TFuture::TValueType> WaitFor(
    TFuture future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! Similar to #WaitFor but if |future| is already set then the fiber
//! is not rescheduled. If not, the fiber is rescheduled via
//! the current invoker.
template <CFuture TFuture>
TErrorOr<typename TFuture::TValueType> WaitForFast(
    TFuture future);

//! Waits until |future| is set or #TWaitOptions::Deadline elapses; inspect |future|
//! afterwards (e.g. #TFuture::IsSet / #TFuture::TryGet) for the outcome.
void WaitUntilSet(TFuture<void> future, TWaitOptions options = {});

//! Reschedules the current fiber to the current invoker.
void Yield();

//! Reschedules the current fiber to |invoker|.
void SwitchTo(IInvokerPtr invoker);

//! Returns |true| if there is enough remaining stack space.
bool CheckFreeStackSpace(size_t space);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define SCHEDULER_API_INL_H_
#include "scheduler_api-inl.h"
#undef SCHEDULER_API_INL_H_
