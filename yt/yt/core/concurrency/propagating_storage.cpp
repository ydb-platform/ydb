#include "propagating_storage.h"

#include <library/cpp/yt/small_containers/compact_flat_map.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TPropagatingStorageImplBase
{
public:
    using TStorage = TCompactFlatMap<std::type_index, std::any, 16>;

    bool IsEmpty() const
    {
        return Data_.empty();
    }

    const std::any* GetRaw(const std::type_info& typeInfo) const
    {
        auto iter = Data_.find(std::type_index(typeInfo));
        return iter == Data_.end() ? nullptr : &iter->second;
    }

    std::optional<std::any> ExchangeRaw(std::any value)
    {
        std::type_index key(value.type());
        auto iter = Data_.find(key);
        if (iter == Data_.end()) {
            Data_.emplace(key, std::move(value));
            return std::nullopt;
        }
        return std::exchange(iter->second, std::move(value));
    }

    std::optional<std::any> RemoveRaw(const std::type_info& typeInfo)
    {
        auto iter = Data_.find(std::type_index(typeInfo));
        if (iter == Data_.end()) {
            return std::nullopt;
        }
        auto result = std::make_optional<std::any>(iter->second);
        Data_.erase(iter);
        return result;
    }

    DEFINE_SIGNAL_SIMPLE(void(), OnBeforeUninstall);
    DEFINE_SIGNAL_SIMPLE(void(), OnAfterInstall);

private:
    TStorage Data_;
};

////////////////////////////////////////////////////////////////////////////////

class TPropagatingStorage::TImpl
    : public TRefCounted
    , public TPropagatingStorageImplBase
{
public:
    TImpl() = default;

    TIntrusivePtr<TImpl> Clone() const
    {
        return New<TImpl>(static_cast<const TPropagatingStorageImplBase&>(*this));
    }

private:
    DECLARE_NEW_FRIEND()

    explicit TImpl(const TPropagatingStorageImplBase& base)
        : TPropagatingStorageImplBase(base)
    { }
};

////////////////////////////////////////////////////////////////////////////////

TPropagatingStorage::TPropagatingStorage() = default;

TPropagatingStorage::TPropagatingStorage(TIntrusivePtr<TImpl> impl)
    : Impl_(std::move(impl))
{ }

TPropagatingStorage::~TPropagatingStorage() = default;

TPropagatingStorage::TPropagatingStorage(const TPropagatingStorage& other) = default;
TPropagatingStorage::TPropagatingStorage(TPropagatingStorage&& other) = default;

TPropagatingStorage& TPropagatingStorage::operator=(const TPropagatingStorage& other) = default;
TPropagatingStorage& TPropagatingStorage::operator=(TPropagatingStorage&& other) = default;

bool TPropagatingStorage::IsNull() const
{
    return !static_cast<bool>(Impl_);
}

bool TPropagatingStorage::IsEmpty() const
{
    return !Impl_ || Impl_->IsEmpty();
}

const std::any* TPropagatingStorage::FindRaw(const std::type_info& typeInfo) const
{
    if (!Impl_) {
        return nullptr;
    }
    return Impl_->GetRaw(typeInfo);
}

std::optional<std::any> TPropagatingStorage::ExchangeRaw(std::any value)
{
    EnsureUnique();
    return Impl_->ExchangeRaw(std::move(value));
}

std::optional<std::any> TPropagatingStorage::RemoveRaw(const std::type_info& typeInfo)
{
    EnsureUnique();
    return Impl_->RemoveRaw(typeInfo);
}

void TPropagatingStorage::SubscribeOnAfterInstall(const TCallback<void()>& callback)
{
    EnsureUnique();
    Impl_->SubscribeOnAfterInstall(callback);
}

void TPropagatingStorage::UnsubscribeOnAfterInstall(const TCallback<void()>& callback)
{
    EnsureUnique();
    Impl_->UnsubscribeOnAfterInstall(callback);
}

void TPropagatingStorage::SubscribeOnBeforeUninstall(const TCallback<void()>& callback)
{
    EnsureUnique();
    Impl_->SubscribeOnBeforeUninstall(callback);
}

void TPropagatingStorage::UnsubscribeOnBeforeUninstall(const TCallback<void()>& callback)
{
    EnsureUnique();
    Impl_->UnsubscribeOnBeforeUninstall(callback);
}

TPropagatingStorage TPropagatingStorage::Create()
{
    return TPropagatingStorage(New<TImpl>());
}

void TPropagatingStorage::EnsureUnique()
{
    if (!Impl_) {
        Impl_ = New<TImpl>();
        return;
    }

    // NB(gepardo). It can be proved that this code doesn't clone only if there are no references to this storage
    // in other threads, so our copy-on-write mechanism doesn't result in data races.
    //
    // Basically, we need to prove the following:
    //
    // 1) All the previous unrefs happens-before we obtain the reference count. This is true, because GetRefCount()
    // does acquire-load on the reference counter, while Unref() does release-store on it.
    //
    // 2) Modifying the object happens-before taking any new references. This is true, because we are the only owner
    // of the reference, so Ref() can only be done later in this thread, so modifications will be sequenced-before
    // taking new references.
    auto refCount = Impl_->GetRefCount();
    if (refCount == 1) {
        return;
    }
    YT_VERIFY(refCount > 1);
    Impl_ = Impl_->Clone();
}

////////////////////////////////////////////////////////////////////////////////

class TPropagatingStorageManager
{
public:
    static TPropagatingStorageManager* Get()
    {
        return Singleton<TPropagatingStorageManager>();
    }

    TPropagatingStorage& GetCurrentPropagatingStorage()
    {
        return *Slot_;
    }

    const TPropagatingStorage& GetPropagatingStorage(const TFls& fls)
    {
        return *Slot_.Get(fls);
    }

    void InstallGlobalSwitchHandler(TPropagatingStorageGlobalSwitchHandler handler)
    {
        auto guard = Guard(Lock_);
        int index = SwitchHandlerCount_.load();
        YT_VERIFY(index < MaxSwitchHandlerCount);
        SwitchHandlers_[index] = handler;
        ++SwitchHandlerCount_;
    }

    TPropagatingStorage SwitchPropagatingStorage(TPropagatingStorage newStorage)
    {
        auto& storage = *Slot_;
        int count = SwitchHandlerCount_.load(std::memory_order::acquire);
        for (int index = 0; index < count; ++index) {
            SwitchHandlers_[index](storage, newStorage);
        }
        return std::exchange(storage, std::move(newStorage));
    }

private:
    TFlsSlot<TPropagatingStorage> Slot_;

    NThreading::TForkAwareSpinLock Lock_;

    static constexpr int MaxSwitchHandlerCount = 16;
    std::array<TPropagatingStorageGlobalSwitchHandler, MaxSwitchHandlerCount> SwitchHandlers_;
    std::atomic<int> SwitchHandlerCount_ = 0;

    TPropagatingStorageManager() = default;
    Y_DECLARE_SINGLETON_FRIEND()
};

TPropagatingStorage& GetCurrentPropagatingStorage()
{
    return TPropagatingStorageManager::Get()->GetCurrentPropagatingStorage();
}

const TPropagatingStorage& GetPropagatingStorage(const TFls& fls)
{
    return TPropagatingStorageManager::Get()->GetPropagatingStorage(fls);
}

void InstallGlobalPropagatingStorageSwitchHandler(TPropagatingStorageGlobalSwitchHandler handler)
{
    TPropagatingStorageManager::Get()->InstallGlobalSwitchHandler(handler);
}

////////////////////////////////////////////////////////////////////////////////

TPropagatingStorageGuard::TPropagatingStorageGuard(TPropagatingStorage storage)
    : OldStorage_(TPropagatingStorageManager::Get()->SwitchPropagatingStorage(std::move(storage)))
{ }

TPropagatingStorageGuard::~TPropagatingStorageGuard()
{
    TPropagatingStorageManager::Get()->SwitchPropagatingStorage(std::move(OldStorage_));
}

const TPropagatingStorage& TPropagatingStorageGuard::GetOldStorage() const
{
    return OldStorage_;
}

////////////////////////////////////////////////////////////////////////////////

TNullPropagatingStorageGuard::TNullPropagatingStorageGuard()
    : TPropagatingStorageGuard(TPropagatingStorage())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
