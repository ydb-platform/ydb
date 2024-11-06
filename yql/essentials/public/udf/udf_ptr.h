#pragma once

#include <util/system/yassert.h> // Y_ASSERT


namespace NYql {
namespace NUdf {

namespace NDetails {
struct TDelete {
    template <typename T>
    static void DoDelete(T* ptr) {
        delete ptr;
    }
};
}

///////////////////////////////////////////////////////////////////////////////
// TUniquePtr
///////////////////////////////////////////////////////////////////////////////
template <typename T, typename D = NDetails::TDelete>
class TUniquePtr
{
public:
    inline TUniquePtr(T* ptr = nullptr)
        : Ptr_(ptr)
    {
    }

    inline TUniquePtr(TUniquePtr&& rhs)
        : Ptr_(rhs.Release())
    {
    }

    inline TUniquePtr& operator=(TUniquePtr&& rhs) {
        if (this != &rhs) {
            Reset(rhs.Release());
        }

        return *this;
    }

    TUniquePtr(const TUniquePtr& rhs) = delete;
    TUniquePtr& operator=(const TUniquePtr& rhs) = delete;

    inline ~TUniquePtr() {
        DoDestroy();
    }

    inline T* Release() {
        T* tmp = Ptr_;
        Ptr_ = nullptr;
        return tmp;
    }

    inline void Reset(T* ptr = nullptr) {
        if (Ptr_ != ptr) {
            DoDestroy();
            Ptr_ = ptr;
        }
    }

    inline void Swap(TUniquePtr& rhs) {
        T* tmp = Ptr_;
        Ptr_ = rhs.Ptr_;
        rhs.Ptr_ = tmp;
    }

    inline T* Get() const { return Ptr_; }
    inline T& operator*() const { return *Ptr_; }
    inline T* operator->() const { return Ptr_; }
    inline explicit operator bool() const { return Ptr_ != nullptr; }

private:
    inline void DoDestroy() {
        if (Ptr_)
            D::DoDelete(Ptr_);
    }

private:
    T* Ptr_;
};

///////////////////////////////////////////////////////////////////////////////
// TRefCountedPtr
///////////////////////////////////////////////////////////////////////////////
template <class T>
class TDefaultRefCountedPtrOps
{
public:
    static inline void Ref(T* t) {
        Y_ASSERT(t);
        t->Ref();
    }

    static inline void UnRef(T* t) {
        Y_ASSERT(t);
        t->UnRef();
    }

    static inline ui32 RefCount(const T* t) {
        Y_ASSERT(t);
        return t->RefCount();
    }
};

template <typename T, typename Ops = TDefaultRefCountedPtrOps<T>>
class TRefCountedPtr
{
public:
    enum AddRef { ADD_REF };
    enum StealRef { STEAL_REF };

public:
    inline TRefCountedPtr(T* ptr = nullptr)
        : Ptr_(ptr)
    {
        Ref();
    }

    inline TRefCountedPtr(T* ptr, StealRef)
        : Ptr_(ptr)
    {
        // do not call Ref() on new pointer
    }

    inline TRefCountedPtr(const TRefCountedPtr& rhs)
        : Ptr_(rhs.Ptr_)
    {
        Ref();
    }

    inline TRefCountedPtr(TRefCountedPtr&& rhs)
        : Ptr_(rhs.Ptr_)
    {
        rhs.Ptr_ = nullptr;
    }

    inline TRefCountedPtr& operator=(const TRefCountedPtr& rhs) {
        if (this != &rhs) {
            UnRef();
            Ptr_ = rhs.Ptr_;
            Ref();
        }

        return *this;
    }

    inline TRefCountedPtr& operator=(TRefCountedPtr&& rhs) {
        if (this != &rhs) {
            UnRef();
            Ptr_ = rhs.Ptr_;
            rhs.Ptr_ = nullptr;
        }

        return *this;
    }

    inline ~TRefCountedPtr() {
        UnRef();
    }

    inline void Reset(T* ptr = nullptr) {
        if (Ptr_ != ptr) {
            UnRef();
            Ptr_ = ptr;
            Ref();
        }
    }

    inline void Reset(T* ptr, StealRef) {
        if (Ptr_ != ptr) {
            UnRef();
            Ptr_ = ptr;
        }
    }

    inline void Swap(TRefCountedPtr& rhs) {
        T* tmp = Ptr_;
        Ptr_ = rhs.Ptr_;
        rhs.Ptr_ = tmp;
    }

    inline T* Release() {
        // do not decrement ref counter here. just send ownership to caller
        T* tmp = Ptr_;
        Ptr_ = nullptr;
        return tmp;
    }

    inline T* Get() const { return Ptr_; }
    inline T& operator*() const { return *Ptr_; }
    inline T* operator->() const { return Ptr_; }
    inline explicit operator bool() const { return Ptr_ != nullptr; }

    inline ui32 RefCount() const {
        return Ptr_ ? Ops::RefCount(Ptr_) : 0;
    }

private:
    inline void Ref() {
        if (Ptr_){
            Ops::Ref(Ptr_);
        }
    }

    inline void UnRef() {
        if (Ptr_) {
            Ops::UnRef(Ptr_);
            Ptr_ = nullptr;
        }
    }

private:
    T* Ptr_;
};

} // namspace NUdf
} // namspace NYql
