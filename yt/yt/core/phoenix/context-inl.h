#ifndef CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include context.h"
// For the sake of sane code completion.
#include "context.h"
#endif

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TLoadContext::RegisterConstructedObject(T* ptr)
{
    if constexpr(std::derived_from<T, TRefCounted>) {
        Deletors_.push_back([=] { Unref(ptr); });
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2

