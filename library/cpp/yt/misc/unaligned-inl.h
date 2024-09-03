#ifndef UNALIGNED_INL_H_
#error "Direct inclusion of this file is not allowed, include unaligned.h"
// For the sake of sane code completion.
#include "unaligned.h"
#endif

#include <cstring>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires std::is_trivial_v<T>
T UnalignedLoad(const T* ptr)
{
    T value;
    std::memcpy(&value, ptr, sizeof(T));
    return value;
}

template <class T>
    requires std::is_trivial_v<T>
void UnalignedStore(T* ptr, const T& value)
{
    std::memcpy(ptr, &value, sizeof(T));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
