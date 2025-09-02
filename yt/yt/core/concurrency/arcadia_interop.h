#pragma once

#include <yt/yt/core/actions/future.h>

#include <library/cpp/threading/future/core/future.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
::NThreading::TFuture<T> ToArcadiaFuture(const TFuture<T>& future);

template <class T>
TFuture<T> FromArcadiaFuture(const ::NThreading::TFuture<T>& future);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define ARCADIA_INTEROP_INL_H_
#include "arcadia_interop-inl.h"
#undef ARCADIA_INTEROP_INL_H_
