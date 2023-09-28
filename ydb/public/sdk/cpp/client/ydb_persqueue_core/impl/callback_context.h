#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <memory>
#include <shared_mutex>
#include <vector>

namespace NYdb::NPersQueue {

template <typename TGuardedObject>
class TCallbackContext {
public:
    using TMutexPtr = std::shared_ptr<std::shared_mutex>;

    class TBorrowed {
    public:
        explicit TBorrowed(const TCallbackContext& parent) : Mutex(parent.Mutex) {
            Mutex->lock_shared();
            Ptr = parent.GuardedObjectPtr.get();
        }

        ~TBorrowed() {
            Mutex->unlock_shared();
        }

        TGuardedObject* operator->() {
            return Ptr;
        }

        const TGuardedObject* operator->() const {
            return Ptr;
        }

        operator bool() {
            return Ptr;
        }

    private:
        TMutexPtr Mutex;
        TGuardedObject* Ptr = nullptr;
    };

public:
    explicit TCallbackContext(typename std::shared_ptr<TGuardedObject> ptr)
        : Mutex(std::make_shared<std::shared_mutex>())
        , GuardedObjectPtr(std::move(ptr))
        {}

    // may block
    ~TCallbackContext() {
        Cancel();
    }

    TBorrowed LockShared() {
        return TBorrowed(*this);
    }

    void Cancel() {
        std::lock_guard lock(*Mutex);
        GuardedObjectPtr.reset();
    }

private:
    TMutexPtr Mutex;
    typename std::shared_ptr<TGuardedObject> GuardedObjectPtr;
};

}
