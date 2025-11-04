#pragma once

#include "public.h"
#include "context_switch.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/tracing/public.h>

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

//! Blocks the current fiber until #future is set.
//! The fiber is resceduled to #invoker.
void WaitUntilSet(
    TFuture<void> future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! Blocks the current fiber until #future is set and returns the resulting value.
//! The fiber is rescheduled to #invoker.
template <CFuture TFuture>
TErrorOr<typename TFuture::TValueType> WaitFor(
    TFuture future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! Similar to #WaitFor but if #future is already set then the fiber
//! is not rescheduled. If not, the fiber is rescheduled via
//! the current invoker.
template <CFuture TFuture>
TErrorOr<typename TFuture::TValueType> WaitForFast(
    TFuture future);

//! Similar to #WaitFor but extracts the value from #future via |GetUnique|.
// TODO(babenko): deprecated, see YT-26319
template <class T>
[[nodiscard]] TErrorOr<T> WaitForUnique(
    TFuture<T> future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! From the authors of #WaitForUnique and #WaitForFast.
template <class T>
[[nodiscard]] TErrorOr<T> WaitForUniqueFast(
    TFuture<T> future);

//! A possibly blocking version of #WaitFor.
template <CFuture TFuture>
TErrorOr<typename TFuture::TValueType> WaitForWithStrategy(
    TFuture future,
    EWaitForStrategy strategy);

//! Reschedules the current fiber to the current invoker.
void Yield();

//! Reschedules the current fiber to #invoker.
void SwitchTo(IInvokerPtr invoker);

//! Returns |true| if there is enough remaining stack space.
bool CheckFreeStackSpace(size_t space);

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency

#define SCHEDULER_API_INL_H_
#include "scheduler_api-inl.h"
#undef SCHEDULER_API_INL_H_
