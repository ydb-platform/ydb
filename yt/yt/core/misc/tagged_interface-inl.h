#ifndef TAGGED_INTERFACE_INL_H_
#error "Direct inclusion of this file is not allowed, include sync_expiring_cache.h"
// For the sake of sane code completion.
#include "tagged_interface.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename T, class TTag>
template <typename U>
TTaggedInterface<T, TTag>::TTaggedInterface(TIntrusivePtr<U>&& ptr)
    : Impl_(std::move(ptr))
{ }

template <typename T, class TTag>
template <typename U>
TTaggedInterface<T, TTag>::TTaggedInterface(const TIntrusivePtr<U>& ptr)
    : Impl_(ptr)
{ }

template <typename T, class TTag>
const T& TTaggedInterface<T, TTag>::operator*() const
{
    // NB(apachee): TIntrusivePtr's implementations asserts that Impl_ is not null.
    return *Impl_;
}

template <typename T, class TTag>
T& TTaggedInterface<T, TTag>::operator*()
{
    // NB(apachee): TIntrusivePtr's implementations asserts that Impl_ is not null.
    return *Impl_;
}

template <typename T, class TTag>
const T* TTaggedInterface<T, TTag>::operator->() const
{
    return Impl_.Get();
}

template <typename T, class TTag>
T* TTaggedInterface<T, TTag>::operator->()
{
    return Impl_.Get();
}

template <typename T, class TTag>
TTaggedInterface<T, TTag>::operator TIntrusivePtr<T>() const &
{
    return Impl_;
}

template <typename T, class TTag>
TTaggedInterface<T, TTag>::operator TIntrusivePtr<T>() &&
{
    // XXX(apachee): Idk why, but TIntrusivePtr<T>(std::move(Impl_)) fails to resolve TIntrusivePtr<T> symbol in concurrency unittests for whatever reason.
    return std::move(Impl_);
}

template <typename T, class TTag>
T* TTaggedInterface<T, TTag>::Get()
{
    return Impl_.Get();
}

template <typename T, class TTag>
const T* TTaggedInterface<T, TTag>::Get() const
{
    return Impl_.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
