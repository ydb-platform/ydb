#ifndef SCHEDULER_API_INL_H_
#error "Direct inclusion of this file is not allowed, include scheduler_api.h"
// For the sake of sane code completion.
#include "scheduler_api.h"
#endif

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////
// NB: Please refer to YT-18899 before trying to pass future to
// these functions by const-ref.

template <CFuture TFuture>
TErrorOr<typename TFuture::TValueType> WaitFor(TFuture future, IInvokerPtr invoker)
{
    YT_ASSERT(future);
    YT_ASSERT(invoker);

    WaitUntilSet(future.AsVoid(), {.ResumingInvoker = std::move(invoker)});

    return future.GetOrCrash();
}

template <CFuture TFuture>
TErrorOr<typename TFuture::TValueType> WaitForFast(TFuture future)
{
    YT_ASSERT(future);
    YT_ASSERT(!IsContextSwitchForbidden());

    WaitUntilSet(future.AsVoid(), {.AlwaysYieldFiber = false});

    return future.GetOrCrash();
}

inline void Yield()
{
    WaitUntilSet(OKFuture);
}

inline void SwitchTo(IInvokerPtr invoker)
{
    WaitUntilSet(OKFuture, {.ResumingInvoker = std::move(invoker)});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
