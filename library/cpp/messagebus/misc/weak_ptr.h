#pragma once

#include <util/generic/ptr.h>
#include <util/system/mutex.h>

template <typename T>
struct TWeakPtr;

template <typename TSelf>
struct TWeakRefCounted {
    template <typename>
    friend struct TWeakPtr;

private:
    struct TRef: public TAtomicRefCount<TRef> {
        TMutex Mutex;
        TSelf* Outer;

        TRef(TSelf* outer)
            : Outer(outer)
        {
        }

        void Release() {
            TGuard<TMutex> g(Mutex);
            Y_ASSERT(!!Outer);
            Outer = nullptr;
        }

        TIntrusivePtr<TSelf> Get() {
            TGuard<TMutex> g(Mutex);
            Y_ASSERT(!Outer || Outer->RefCount() > 0);
            return Outer;
        }
    };

    TAtomicCounter Counter;
    TIntrusivePtr<TRef> RefPtr;

public:
    TWeakRefCounted()
        : RefPtr(new TRef(static_cast<TSelf*>(this)))
    {
    }

    void Ref() {
        Counter.Inc();
    }

    void UnRef() {
        if (Counter.Dec() == 0) {
            RefPtr->Release();

            // drop is to prevent dtor from reading it
            RefPtr.Drop();

            delete static_cast<TSelf*>(this);
        }
    }

    void DecRef() {
        Counter.Dec();
    }

    unsigned RefCount() const {
        return Counter.Val();
    }
};

template <typename T>
struct TWeakPtr {
private:
    typedef TIntrusivePtr<typename T::TRef> TRefPtr;
    TRefPtr RefPtr;

public:
    TWeakPtr() {
    }

    TWeakPtr(T* t) {
        if (!!t) {
            RefPtr = t->RefPtr;
        }
    }

    TWeakPtr(TIntrusivePtr<T> t) {
        if (!!t) {
            RefPtr = t->RefPtr;
        }
    }

    TIntrusivePtr<T> Get() {
        if (!RefPtr) {
            return nullptr;
        } else {
            return RefPtr->Get();
        }
    }
};
