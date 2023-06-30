#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Instantiates an object whose destruction will be offloaded to a given #invoker.
template <class T, class... TArgs>
TIntrusivePtr<T> NewWithOffloadedDtor(
    IInvokerPtr dtorInvoker,
    TArgs&&... args);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define NEW_WITH_OFFLOADED_DTOR_INL_H_
#include "new_with_offloaded_dtor-inl.h"
#undef NEW_WITH_OFFLOADED_DTOR_PTR_INL_H_
