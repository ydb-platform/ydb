#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <memory>
#include <shared_mutex>
#include <vector>

namespace NYdb::NPersQueue {

template<typename T>
class TContextOwner;

template <typename TGuardedObject>
class TCallbackContext {
    friend class TContextOwner<TGuardedObject>;

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
    explicit TCallbackContext(std::shared_ptr<TGuardedObject> ptr)
        : Mutex(std::make_shared<std::shared_mutex>())
        , GuardedObjectPtr(std::move(ptr))
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

}
