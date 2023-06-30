#ifndef SCHEDULER_API_INL_H_
#error "Direct inclusion of this file is not allowed, include scheduler_api.h"
// For the sake of sane code completion.
#include "scheduler_api.h"
#endif
#undef SCHEDULER_API_INL_H_

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
[[nodiscard]] TErrorOr<T> WaitFor(TFuture<T> future, IInvokerPtr invoker)
{
    YT_ASSERT(future);
    YT_ASSERT(invoker);

    WaitUntilSet(future.AsVoid(), std::move(invoker));

    return future.Get();
}

template <class T>
[[nodiscard]] TErrorOr<T> WaitForFast(TFuture<T> future)
{
    YT_ASSERT(future);

    if (!future.IsSet()) {
        WaitUntilSet(future.AsVoid(), GetCurrentInvoker());
    }

    return future.Get();
}

template <class T>
[[nodiscard]] TErrorOr<T> WaitForUnique(const TFuture<T>& future, IInvokerPtr invoker)
{
    YT_ASSERT(future);
    YT_ASSERT(invoker);

    WaitUntilSet(future.AsVoid(), std::move(invoker));

    return future.GetUnique();
}

template <class T>
[[nodiscard]] TErrorOr<T> WaitForUniqueFast(const TFuture<T>& future)
{
    YT_ASSERT(future);

    if (!future.IsSet()) {
        WaitUntilSet(future.AsVoid(), GetCurrentInvoker());
    }

    return future.GetUnique();
}

template <class T>
TErrorOr<T> WaitForWithStrategy(
    TFuture<T> future,
    EWaitForStrategy strategy)
{
    switch (strategy) {
        case EWaitForStrategy::WaitFor:
            return WaitFor(std::move(future));
        case EWaitForStrategy::Get:
            return future.Get();
        default:
            YT_ABORT();
    }
}

inline void Yield()
{
    WaitUntilSet(VoidFuture);
}

inline void SwitchTo(IInvokerPtr invoker)
{
    WaitUntilSet(VoidFuture, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
