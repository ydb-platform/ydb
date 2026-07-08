#ifndef CANCELABLE_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include cancelable_context.h"
// For the sake of sane code completion.
#include "cancelable_context.h"
#endif

#include <yt/yt/core/concurrency/fls.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TCancelableContext::PropagateTo(const TFuture<T>& future)
{
    PropagateTo(future.AsVoid());
}

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(NConcurrency::TFlsSlot<TCancelableContextPtr>, CurrentCancelableContextSlot);

inline TCancelableContext* TryGetCurrentCancelableContext()
{
    return CurrentCancelableContextSlot()->Get();
}

inline TCancelableContextPtr SwitchCurrentCancelableContext(TCancelableContextPtr newContext)
{
    return std::exchange(*CurrentCancelableContextSlot(), std::move(newContext));
}

////////////////////////////////////////////////////////////////////////////////

inline TCurrentCancelableContextGuard::TCurrentCancelableContextGuard(TCancelableContextPtr context)
    : Active_(true)
    , OldContext_(SwitchCurrentCancelableContext(std::move(context)))
{ }

inline TCurrentCancelableContextGuard::TCurrentCancelableContextGuard(TCurrentCancelableContextGuard&& other) noexcept
    : Active_(other.Active_)
    , OldContext_(std::move(other.OldContext_))
{
    other.Active_ = false;
}

inline TCurrentCancelableContextGuard::~TCurrentCancelableContextGuard()
{
    if (Active_) {
        SwitchCurrentCancelableContext(std::move(OldContext_));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
