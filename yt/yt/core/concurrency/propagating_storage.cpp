#include "propagating_storage.h"

#include <library/cpp/yt/compact_containers/compact_flat_map.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <yt/yt/core/misc/static_ring_queue.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

std::optional<std::any> TPropagatingStorageImpl::ExchangeRaw(std::any value)
{
    std::type_index key(value.type());
    auto iter = Data_.find(key);
    if (iter == Data_.end()) {
        Data_.emplace(key, std::move(value));
        return std::nullopt;
    }
    return std::exchange(iter->second, std::move(value));
}

std::optional<std::any> TPropagatingStorageImpl::RemoveRaw(const std::type_info& typeInfo)
{
    auto iter = Data_.find(std::type_index(typeInfo));
    if (iter == Data_.end()) {
        return std::nullopt;
    }
    auto result = std::make_optional<std::any>(iter->second);
    Data_.erase(iter);
    return result;
}

TIntrusivePtr<TPropagatingStorageImpl> TPropagatingStorageImpl::Clone() const
{
    return New<TPropagatingStorageImpl>(*this);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TPropagatingStorage::TPropagatingStorage(TIntrusivePtr<NDetail::TPropagatingStorageImpl> impl)
    : Impl_(std::move(impl))
{ }

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
    return TPropagatingStorage(New<NDetail::TPropagatingStorageImpl>());
}

void TPropagatingStorage::EnsureUnique()
{
    if (!Impl_) {
        Impl_ = New<NDetail::TPropagatingStorageImpl>();
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

namespace NDetail {

void TPropagatingStorageManager::RunSwitchHandlers(
    const TPropagatingStorage& oldStorage,
    const TPropagatingStorage& newStorage,
    int count)
{
    for (int index = 0; index < count; ++index) {
        SwitchHandlers_[index](oldStorage, newStorage);
    }
}

void TPropagatingStorageManager::InstallSwitchHandler(TPropagatingStorageGlobalSwitchHandler handler)
{
    auto guard = Guard(SwitchHandlerLock_);
    int index = SwitchHandlerCount_.load();
    YT_VERIFY(index < MaxSwitchHandlerCount_);
    SwitchHandlers_[index] = handler;
    ++SwitchHandlerCount_;
}

TPropagatingStorage TPropagatingStorageManager::SwitchPropagatingStorage(TPropagatingStorage newStorage, TFls*& cachedFls)
{
    auto* fls = cachedFls;
    if (!fls) {
        fls = cachedFls = GetCurrentFls();
    }
    auto& slot = PropagatingStorageSlot();
    auto* current = slot.TryGet(*fls);
    if (!current || current->IsNull()) {
        // Nothing meaningful installed currently.
        if (newStorage.IsNull()) {
            return TPropagatingStorage();
        }
        {
            static const TPropagatingStorage Empty;
            RunSwitchHandlers(Empty, newStorage, SwitchHandlerCount_.load(std::memory_order::acquire));
        }
        // Lazily allocates the slot via GetOrCreate (does its own TLS lookup).
        // This branch is only taken once per fiber, so the extra lookup is
        // negligible.
        *slot = std::move(newStorage);
        return TPropagatingStorage();
    }
    RunSwitchHandlers(*current, newStorage, SwitchHandlerCount_.load(std::memory_order::acquire));
    return std::exchange(*current, std::move(newStorage));
}

} // namespace NDetail

const TPropagatingStorage* TryGetPropagatingStorage(const TFls& fls)
{
    return NDetail::PropagatingStorageSlot().TryGet(fls);
}

void InstallGlobalPropagatingStorageSwitchHandler(TPropagatingStorageGlobalSwitchHandler handler)
{
    NDetail::PropagatingStorageManager.InstallSwitchHandler(handler);
}

////////////////////////////////////////////////////////////////////////////////

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
