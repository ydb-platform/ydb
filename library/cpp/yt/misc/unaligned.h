#pragma once

#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires std::is_trivial_v<T>
T UnalignedLoad(const T* ptr);

template <class T>
    requires std::is_trivial_v<T>
void UnalignedStore(T* ptr, const T& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define UNALIGNED_INL_H_
#include "unaligned-inl.h"
#undef UNALIGNED_INL_H_
