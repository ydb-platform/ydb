#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// XXX(apachee): Maybe we should force invariant that static_cast<bool>(Impl_) is true?

//! Wrapper for interfaces to add arbitrary tags.
//!
//! Stores interface of type #T with some tag #TTag.
//! Can be used to tag specific implementations of some interface, e.g.
//! TTaggedInterface<IInvoker, TBoundedConcurrencyInvokerTag> for
//! TBoundedConcurrencyInvoker.
template <typename T, class TTag>
class TTaggedInterface
{
public:
    TTaggedInterface() = default;

    template <typename U>
    explicit TTaggedInterface(TIntrusivePtr<U>&& ptr);
    template <typename U>
    explicit TTaggedInterface(const TIntrusivePtr<U>& ptr);

    const T& operator*() const;
    T& operator*();

    const T* operator->() const;
    T* operator->();

    //! Implicit cast to TIntrusivePtr from const reference that returns a copy of Impl_.
    operator TIntrusivePtr<T>() const &;

    //! Implicit cast to TIntrusivePtr from rvalue reference that returns moved Impl_.
    //! This overload is intended for cases when TTaggedInterface was used to wrap an already existing entity,
    //! e.g. changing create function return type to TTaggedInterface.
    //! This allows to change type without breaking compatability.
    //! Uses TInstrusivePtr's move constructor, thus avoiding atomic operations.
    operator TIntrusivePtr<T>() &&;

    T* Get();
    const T* Get() const;

private:
    const TIntrusivePtr<T> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define TAGGED_INTERFACE_INL_H_
#include "tagged_interface-inl.h"
#undef TAGGED_INTERFACE_INL_H_
