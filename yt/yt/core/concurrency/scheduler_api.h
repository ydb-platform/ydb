#pragma once

#include "public.h"

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

//! Returns |true| if fiber context switch is currently forbidden.
bool IsContextSwitchForbidden();

class TForbidContextSwitchGuard
{
public:
    TForbidContextSwitchGuard();
    TForbidContextSwitchGuard(const TForbidContextSwitchGuard&) = delete;

    ~TForbidContextSwitchGuard();

private:
    const bool OldValue_;
};

////////////////////////////////////////////////////////////////////////////////

// NB: Use function pointer to minimize the overhead.
using TGlobalContextSwitchHandler = void(*)();

void InstallGlobalContextSwitchHandlers(
    TGlobalContextSwitchHandler outHandler,
    TGlobalContextSwitchHandler inHandler);

////////////////////////////////////////////////////////////////////////////////

using TContextSwitchHandler = std::function<void()>;

class TContextSwitchGuard
{
public:
    TContextSwitchGuard(
        TContextSwitchHandler outHandler,
        TContextSwitchHandler inHandler);

    TContextSwitchGuard(const TContextSwitchGuard& other) = delete;
    ~TContextSwitchGuard();
};

class TOneShotContextSwitchGuard
    : public TContextSwitchGuard
{
public:
    explicit TOneShotContextSwitchGuard(TContextSwitchHandler outHandler);

private:
    bool Active_;
};

//! Blocks the current fiber until #future is set.
//! The fiber is resceduled to #invoker.
void WaitUntilSet(
    TFuture<void> future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! Blocks the current fiber until #future is set and returns the resulting value.
//! The fiber is rescheduled to #invoker.
template <class T>
[[nodiscard]] TErrorOr<T> WaitFor(
    TFuture<T> future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! Similar to #WaitFor but if #future is already set then the fiber
//! is not rescheduled. If not, the fiber is rescheduled via
//! the current invoker.
template <class T>
[[nodiscard]] TErrorOr<T> WaitForFast(
    TFuture<T> future);

//! Similar to #WaitFor but extracts the value from #future via |GetUnique|.
template <class T>
[[nodiscard]] TErrorOr<T> WaitForUnique(
    const TFuture<T>& future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! From the authors of #WaitForUnique and #WaitForFast.
template <class T>
[[nodiscard]] TErrorOr<T> WaitForUniqueFast(
    const TFuture<T>& future);

//! A possibly blocking version of #WaitFor.
template <class T>
TErrorOr<T> WaitForWithStrategy(
    TFuture<T> future,
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
