#ifndef CANCELABLE_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include cancelable_context.h"
// For the sake of sane code completion.
#include "cancelable_context.h"
#endif
#undef CANCELABLE_CONTEXT_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TCancelableContext::PropagateTo(const TFuture<T>& future)
{
    PropagateTo(future.AsVoid());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
