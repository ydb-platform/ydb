#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif
#undef HELPERS_INL_H_

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

template <class T, void(*Dtor)(T*)>
TGrpcObjectPtr<T, Dtor>::TGrpcObjectPtr()
    : Ptr_(nullptr)
{ }

template <class T, void(*Dtor)(T*)>
TGrpcObjectPtr<T, Dtor>::~TGrpcObjectPtr()
{
    Reset();
}

template <class T, void(*Dtor)(T*)>
T* TGrpcObjectPtr<T, Dtor>::Unwrap()
{
    return Ptr_;
}

template <class T, void(*Dtor)(T*)>
const T* TGrpcObjectPtr<T, Dtor>::Unwrap() const
{
    return Ptr_;
}

template <class T, void(*Dtor)(T*)>
TGrpcObjectPtr<T, Dtor>::operator bool() const
{
    return Ptr_ != nullptr;
}

template <class T, void(*Dtor)(T*)>
T** TGrpcObjectPtr<T, Dtor>::GetPtr()
{
    Reset();
    return &Ptr_;
}

template <class T, void(*Dtor)(T*)>
void TGrpcObjectPtr<T, Dtor>::Reset()
{
    if (Ptr_) {
        Dtor(Ptr_);
        Ptr_ = nullptr;
    }
}

template <class T, void(*Dtor)(T*)>
TGrpcObjectPtr<T, Dtor>::TGrpcObjectPtr(T* obj)
    : Ptr_(obj)
{ }

template <class T, void(*Dtor)(T*)>
TGrpcObjectPtr<T, Dtor>::TGrpcObjectPtr(TGrpcObjectPtr&& other)
    : Ptr_(other.Ptr_)
{
    other.Ptr_ = nullptr;
}

template <class T, void(*Dtor)(T*)>
TGrpcObjectPtr<T, Dtor>& TGrpcObjectPtr<T, Dtor>::operator=(TGrpcObjectPtr&& other)
{
    if (this != &other) {
        Reset();
        Ptr_ = other.Ptr_;
        other.Ptr_ = nullptr;
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

template <class T, void(*Ctor)(T*), void(*Dtor)(T*)>
TGrpcObject<T, Ctor, Dtor>::TGrpcObject()
{
    Ctor(&Native_);
}

template <class T, void(*Ctor)(T*), void(*Dtor)(T*)>
TGrpcObject<T, Ctor, Dtor>::~TGrpcObject()
{
    Dtor(&Native_);
}

template <class T, void(*Ctor)(T*), void(*Dtor)(T*)>
T* TGrpcObject<T, Ctor, Dtor>::Unwrap()
{
    return &Native_;
}

template <class T, void(*Ctor)(T*), void(*Dtor)(T*)>
T* TGrpcObject<T, Ctor, Dtor>::operator->()
{
    return &Native_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
