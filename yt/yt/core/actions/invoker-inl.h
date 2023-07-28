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
TExtendedCallback<R(TArgs...)>::Via(IInvokerPtr invoker) const
{
    static_assert(
        std::is_void_v<R>,
        "Via() can only be used with void return type.");
    YT_ASSERT(invoker);

    auto this_ = *this;
    return BIND_NO_PROPAGATE([=, invoker = std::move(invoker)] (TArgs... args) {
        invoker->Invoke(BIND_NO_PROPAGATE(this_, WrapToPassed(std::forward<TArgs>(args))...));
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
