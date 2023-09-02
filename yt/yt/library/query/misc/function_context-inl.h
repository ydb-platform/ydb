#ifndef FUNCTION_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include function_context.h"
// For the sake of sane code completion.
#include "function_context.h"
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class T, class... Args>
T* TFunctionContext::CreateObject(Args&&... args)
{
    auto pointer = new T(std::forward<Args>(args)...);
    auto deleter = [] (void* ptr) {
        static_assert(sizeof(T) > 0, "Cannot delete incomplete type.");
        delete static_cast<T*>(ptr);
    };

    return static_cast<T*>(CreateUntypedObject(pointer, deleter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
