#ifndef NEW_WITH_OFFLOADED_DTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include new_with_offloaded_dtor.h"
// For the sake of sane code completion.
#include "new_with_offloaded_dtor.h"
#endif

#include "bind.h"
#include "invoker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class... TArgs>
TIntrusivePtr<T> NewWithOffloadedDtor(
    IInvokerPtr dtorInvoker,
    TArgs&&... args)
{
    return NewWithDeleter<T>(
        [dtorInvoker = std::move(dtorInvoker)] (T* obj) {
            dtorInvoker->Invoke(BIND([obj] {
                NYT::NDetail::DestroyRefCountedImpl<T>(obj);
            }));
        },
        std::forward<TArgs>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
