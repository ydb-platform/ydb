#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>

namespace NYdb::NTopic {

template<typename T>
class TContextOwner;

template <typename TGuardedObject>
class TCallbackContext {
    friend class TContextOwner<TGuardedObject>;

    // thread_id -> number of LockShared calls from this thread
    using TSharedLockCounter = std::map<std::thread::id, size_t>;
    using TSharedLockCounterPtr = std::shared_ptr<TSharedLockCounter>;
    using TSpinLockPtr = std::shared_ptr<TSpinLock>;

public:
    using TMutexPtr = std::shared_ptr<std::shared_mutex>;

    class TBorrowed {
    public:
        explicit TBorrowed(const TCallbackContext& parent)
            : Mutex(parent.Mutex)
            , SharedLockCounterMutex(parent.SharedLockCounterMutex)
            , SharedLockCounter(parent.SharedLockCounter)
        {
            // "Recursive shared lock".
            //
            // https://en.cppreference.com/w/cpp/thread/shared_mutex/lock_shared says:
            //   If lock_shared is called by a thread that already owns the mutex
            //   in any mode (exclusive or shared), the behavior is UNDEFINED.
            //
            // So if a thread calls LockShared more than once without releasing the lock,
            // we should call lock_shared only on the first call.

            bool takeLock = false;

            with_lock(*SharedLockCounterMutex) {
                auto& counter = SharedLockCounter->emplace(std::this_thread::get_id(), 0).first->second;
                ++counter;
                takeLock = counter == 1;
            }

            if (takeLock) {
                Mutex->lock_shared();
            }

            Ptr = parent.GuardedObjectPtr.get();
        }

        ~TBorrowed() {
            bool releaseLock = false;

            with_lock(*SharedLockCounterMutex) {
                auto it = SharedLockCounter->find(std::this_thread::get_id());
                Y_ABORT_UNLESS(it != SharedLockCounter->end());
                auto& counter = it->second;
                --counter;
                if (counter == 0) {
                    releaseLock = true;
                    SharedLockCounter->erase(it);
                }
            }

            if (releaseLock) {
                Mutex->unlock_shared();
            }
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

        TSpinLockPtr SharedLockCounterMutex;
        TSharedLockCounterPtr SharedLockCounter;
    };

public:
    explicit TCallbackContext(std::shared_ptr<TGuardedObject> ptr)
        : Mutex(std::make_shared<std::shared_mutex>())
        , GuardedObjectPtr(std::move(ptr))
        , SharedLockCounterMutex(std::make_shared<TSpinLock>())
        , SharedLockCounter(std::make_shared<TSharedLockCounter>())
        {}

    TBorrowed LockShared() {
        return TBorrowed(*this);
    }

// TODO change section below to private after removing pqv1 read session implementation
// (relation of 1 owner : n impls)
public:
    void Cancel() {
        std::shared_ptr<TGuardedObject> waste;
        std::lock_guard lock(*Mutex);
        std::swap(waste, GuardedObjectPtr);
    }

    std::shared_ptr<TGuardedObject> TryGet() const {
        if (!GuardedObjectPtr) {
            ythrow yexception() << "TryGet failed, empty GuardedObjectPtr";
        }
        return GuardedObjectPtr;
    }

private:

    TMutexPtr Mutex;
    std::shared_ptr<TGuardedObject> GuardedObjectPtr;

    TSpinLockPtr SharedLockCounterMutex;
    TSharedLockCounterPtr SharedLockCounter;
};

template<typename T>
class TEnableSelfContext {
    template<typename U, typename... Args>
    friend std::shared_ptr<TCallbackContext<U>> MakeWithCallbackContext(Args&&... args);

public:
    TEnableSelfContext() = default;
    ~TEnableSelfContext() = default;

    // non-moveable for simplicity, use only via shared pointers
    TEnableSelfContext(const TEnableSelfContext&) = delete;
    TEnableSelfContext(TEnableSelfContext&&) = delete;
    TEnableSelfContext& operator=(const TEnableSelfContext&) = delete;
    TEnableSelfContext& operator=(TEnableSelfContext&&) = delete;

protected:
    void SetSelfContext(std::shared_ptr<T> ptr) {
        SelfContext = std::make_shared<TCallbackContext<T>>(std::move(ptr));
    }

protected:
    std::shared_ptr<TCallbackContext<T>> SelfContext;
};

template<typename T, typename... Args>
std::shared_ptr<TCallbackContext<T>> MakeWithCallbackContext(Args&&... args) {
    static_assert(std::is_base_of_v<TEnableSelfContext<T>, T>, "Expected object derived from TEnableSelfContext");
    auto pObject = std::make_shared<T>(std::forward<Args>(args)...);
    pObject->SetSelfContext(pObject);
    return pObject->SelfContext;
}

template<typename T>
class TContextOwner {
public:
    template <typename... Args>
    TContextOwner(Args&&... args)
        : ImplContext(MakeWithCallbackContext<T>(std::forward<Args>(args)...)) {
    }

    // may block
    ~TContextOwner() {
        CancelImpl();
    }

    TContextOwner(const TContextOwner&) = delete;
    TContextOwner(TContextOwner&&) = default;
    TContextOwner& operator=(const TContextOwner&) = delete;
    TContextOwner& operator=(TContextOwner&&) = default;

protected:
    std::shared_ptr<T> TryGetImpl() const {
        return ImplContext->TryGet();
    }

    void CancelImpl() {
        if (ImplContext) {
            ImplContext->Cancel();
        }
    }

protected:
    std::shared_ptr<TCallbackContext<T>> ImplContext;
};

}  // namespace NYdb::NTopic
