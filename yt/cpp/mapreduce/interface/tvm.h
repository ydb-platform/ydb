#pragma once

#include <yt/yt/library/tvm/tvm_base.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

/// This wrapper is required because NYT::NAuth::IServiceTicketAuthPtr is NYT::TIntrusivePtr,
/// and, if we used this pointer in interfaces of `mapreduce/yt` client, a lot of users of this library
/// could get unexpected build errors that `TIntrusivePtr` is ambiguous
/// (from `::` namespace and from `::NYT::` namespace).
/// So we use this wrapper in our interfaces to avoid such problems for users.
struct IServiceTicketAuthPtrWrapper
{
    //
    /// Construct wrapper from NYT::TIntrusivePtr
    ///
    /// This constructor is implicit so users can transparently pass NYT::TIntrusivePtr to the functions of
    /// mapreduce/yt client.
    template <class T, class = typename std::enable_if_t<std::is_convertible_v<T*, IServiceTicketAuth*>>>
    IServiceTicketAuthPtrWrapper(const TIntrusivePtr<T> ptr)
        : Ptr(ptr)
    {
    }

    /// Wrapped pointer
    NYT::TIntrusivePtr<IServiceTicketAuth> Ptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
