#ifndef INVOKER_INL_H_
#error "Direct inclusion of this file is not allowed, include invoker.h"
// For the sake of sane code completion.
#include "invoker.h"
#endif
#undef INVOKER_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class R, class... TArgs>
TExtendedCallback<R(TArgs...)>
TExtendedCallback<R(TArgs...)>::Via(IInvokerPtr invoker) const &
{
    return ViaImpl(*this, std::move(invoker));
}

template <class R, class... TArgs>
TExtendedCallback<R(TArgs...)>
TExtendedCallback<R(TArgs...)>::Via(IInvokerPtr invoker) &&
{
    return ViaImpl(std::move(*this), std::move(invoker));
}

template <class R, class... TArgs>
TExtendedCallback<R(TArgs...)>
TExtendedCallback<R(TArgs...)>::ViaImpl(TExtendedCallback<R(TArgs...)> callback, TIntrusivePtr<IInvoker> invoker)
{
    static_assert(
        std::is_void_v<R>,
        "Via() can only be used with void return type.");
    YT_ASSERT(invoker);

    return BIND_NO_PROPAGATE([callback = std::move(callback), invoker = std::move(invoker)] (TArgs... args) {
        invoker->Invoke(BIND_NO_PROPAGATE(callback, WrapToPassed(std::forward<TArgs>(args))...));
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
