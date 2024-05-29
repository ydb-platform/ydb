#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <memory>
#include <mutex>

namespace NYdb::NTopic {

template<typename T>
class TContextOwner;

template <typename TGuardedObject>
class TCallbackContext {
    friend class TContextOwner<TGuardedObject>;

public:
    class TBorrowed {
    public:
        TBorrowed() {}

        explicit TBorrowed(const TCallbackContext& parent)
            : Mutex(parent.Mutex)
            , Die(parent.Die)
            , AllDied(parent.AllDied)
            , LockCounter(parent.LockCounter)
        {
            ++*LockCounter;
            Ptr = parent.GuardedObjectPtr.get();
        }

        ~TBorrowed() {
            if (!Mutex) return;
            std::lock_guard lock(*Mutex);
            --*LockCounter;
            if (*Die) {
                AllDied->notify_one();
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
        std::shared_ptr<std::mutex> Mutex;
        std::shared_ptr<std::atomic<bool>> Die;
        std::shared_ptr<std::condition_variable> AllDied;
        TGuardedObject* Ptr = nullptr;
        std::shared_ptr<size_t> LockCounter;
    };

public:
    explicit TCallbackContext(std::shared_ptr<TGuardedObject> ptr)
        : Mutex(std::make_shared<std::mutex>())
        , Die(std::make_shared<std::atomic<bool>>(false))
        , AllDied(std::make_shared<std::condition_variable>())
        , GuardedObjectPtr(std::move(ptr))
        , LockCounter(std::make_shared<size_t>())
        {}

    TBorrowed LockShared() {
        std::lock_guard lock(*Mutex);
        if (*Die) {
            return TBorrowed();
        }
        return TBorrowed(*this);
    }

// TODO change section below to private after removing pqv1 read session implementation
// (relation of 1 owner : n impls)
public:
    void Cancel() {
        std::unique_lock lock(*Mutex);
        *Die = true;
        std::shared_ptr<TGuardedObject> waste;
        AllDied->wait(lock, [this] { return *LockCounter == 0; });
        std::swap(waste, GuardedObjectPtr);
    }

    std::shared_ptr<TGuardedObject> TryGet() const {
        if (!GuardedObjectPtr) {
            ythrow yexception() << "TryGet failed, empty GuardedObjectPtr";
        }
        return GuardedObjectPtr;
    }

private:

    std::shared_ptr<std::mutex> Mutex;
    std::shared_ptr<std::atomic<bool>> Die;
    std::shared_ptr<std::condition_variable> AllDied;
    std::shared_ptr<TGuardedObject> GuardedObjectPtr;
    std::shared_ptr<size_t> LockCounter;
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
