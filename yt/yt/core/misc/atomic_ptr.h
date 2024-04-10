#pragma once

#include "hazard_ptr.h"

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Holds an atomic pointer to an instance of ref-counted type |T| enabling concurrent
//! read and write access.
template <class T, bool EnableAcquireHazard = false>
class TAtomicPtr
{
public:
    TAtomicPtr() = default;
    TAtomicPtr(std::nullptr_t);
    explicit TAtomicPtr(TIntrusivePtr<T> other);
    TAtomicPtr(TAtomicPtr&& other);

    ~TAtomicPtr();

    TAtomicPtr& operator=(TIntrusivePtr<T> other);
    TAtomicPtr& operator=(std::nullptr_t);

    void Reset();

    //! Acquires a hazard pointer.
    /*!
     *
     *  Returning a hazard pointer avoids contention on ref-counter in read-heavy scenarios.
     *  The user, however, must not keep this hazard pointer alive for longer
     *  than needed. Currently there are limits on the number of HPs that a thread
     *  may concurrently maintain.
     */
    THazardPtr<T> AcquireHazard() const;

    //! Attempts to acquire an intrusive pointer.
    //! May return null in case of a race.
    TIntrusivePtr<T> AcquireWeak() const;

    //! Acquires an intrusive pointer.
    TIntrusivePtr<T> Acquire() const;

    TAtomicPtr<T, EnableAcquireHazard> Exchange(TIntrusivePtr<T> other);
    void Store(TIntrusivePtr<T> other);

    TAtomicPtr<T, EnableAcquireHazard> SwapIfCompare(THazardPtr<T>& compare, TIntrusivePtr<T> target);
    bool SwapIfCompare(T* comparePtr, TIntrusivePtr<T> target);
    bool SwapIfCompare(const TIntrusivePtr<T>& compare, TIntrusivePtr<T> target);
    bool SwapIfCompare(const TIntrusivePtr<T>& compare, TIntrusivePtr<T>* target);

    explicit operator bool() const;

private:
    explicit TAtomicPtr(T* ptr);

    template <class T_, bool EnableAcquireHazard_>
    friend bool operator==(const TAtomicPtr<T_, EnableAcquireHazard_>& lhs, const T_* rhs);

    template <class T_, bool EnableAcquireHazard_>
    friend bool operator==(const T_* lhs, const TAtomicPtr<T_, EnableAcquireHazard_>& rhs);

    std::atomic<T*> Ptr_ = nullptr;

    THazardPtr<T> DoAcquireHazard() const;
    void Drop(T* ptr);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ATOMIC_PTR_INL_H_
#include "atomic_ptr-inl.h"
#undef ATOMIC_PTR_INL_H_
