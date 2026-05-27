#ifndef PROPAGATING_STORAGE_INL_H_
#error "Direct inclusion of this file is not allowed, include propagating_storage.h"
// For the sake of sane code completion.
#include "propagating_storage.h"
#endif
#undef PROPAGATING_STORAGE_INL_H_

#include <library/cpp/yt/compact_containers/compact_flat_map.h>

#include <library/cpp/yt/misc/global.h>

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <any>
#include <typeindex>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! Internal storage backing TPropagatingStorage.
//! Defined here so that the read paths (FindRaw, IsEmpty) can be inlined.
class TPropagatingStorageImpl
    : public TRefCounted
{
public:
    bool IsEmpty() const
    {
        return Data_.empty();
    }

    const std::any* GetRaw(const std::type_info& typeInfo) const
    {
        auto it = Data_.find(std::type_index(typeInfo));
        return it == Data_.end() ? nullptr : &it->second;
    }

    // Mutating operations and signals are not on the TCallback::Run hot path,
    // so they stay out-of-line.
    std::optional<std::any> ExchangeRaw(std::any value);
    std::optional<std::any> RemoveRaw(const std::type_info& typeInfo);
    TIntrusivePtr<TPropagatingStorageImpl> Clone() const;

    DEFINE_SIGNAL_SIMPLE(void(), OnBeforeUninstall);
    DEFINE_SIGNAL_SIMPLE(void(), OnAfterInstall);

private:
    DECLARE_NEW_FRIEND()

    TCompactFlatMap<std::type_index, std::any, 16> Data_;

    TPropagatingStorageImpl() = default;

    // NB: TRefCounted's copy ctor is deleted (the new clone needs a fresh
    // refcount, not the source's), so this can't be |= default|. Members are
    // copied explicitly to match the pre-refactor behavior where
    // TPropagatingStorageImplBase's implicit copy ctor carried over both the
    // data map and the signal subscriber lists.
    TPropagatingStorageImpl(const TPropagatingStorageImpl& other)
        : OnBeforeUninstall_(other.OnBeforeUninstall_)
        , OnAfterInstall_(other.OnAfterInstall_)
        , Data_(other.Data_)
    { }
};

YT_DEFINE_GLOBAL(TFlsSlot<TPropagatingStorage>, PropagatingStorageSlot);

class TPropagatingStorageManager
{
public:
    void InstallSwitchHandler(TPropagatingStorageGlobalSwitchHandler handler);

    //! Installs |newStorage| as the current fiber's propagating storage and
    //! returns the previously installed one.
    /*!
     *  |fls| is an in/out fiber-local-storage cache: if null on entry, this
     *  method calls GetCurrentFls() and writes the result back, so that
     *  subsequent calls within the same fiber (e.g. the restore in the guard's
     *  dtor) can skip the Y_NO_INLINE TLS lookup.
     */
    TPropagatingStorage SwitchPropagatingStorage(TPropagatingStorage newStorage, TFls*& cachedFls);

private:
    static constexpr int MaxSwitchHandlerCount_ = 16;
    std::atomic<int> SwitchHandlerCount_ = 0;
    std::array<TPropagatingStorageGlobalSwitchHandler, MaxSwitchHandlerCount_> SwitchHandlers_{};
    NThreading::TForkAwareSpinLock SwitchHandlerLock_;

    void RunSwitchHandlers(
        const TPropagatingStorage& oldStorage,
        const TPropagatingStorage& newStorage,
        int count);
};

// NB: constinit ensures static (compile-time) initialization, avoiding both
// the per-call function-local-static guard check and any init-order fiasco.
inline constinit TPropagatingStorageManager PropagatingStorageManager;

} // namespace NDetail

inline TPropagatingStorage& CurrentPropagatingStorage()
{
    return *NDetail::PropagatingStorageSlot();
}

inline const TPropagatingStorage& GetCurrentPropagatingStorage()
{
    if (auto& slot = NDetail::PropagatingStorageSlot(); slot.IsInitialized()) {
        return *slot;
    } else {
        static const TPropagatingStorage Empty;
        return Empty;
    }
}

////////////////////////////////////////////////////////////////////////////////

// [[gnu::used]] forces the compiler to emit an out-of-line copy of the
// otherwise-inlined body so that the GDB fiber printer
// (devtools/gdb/yt_fibers_printer.py) can resolve the symbol at runtime.
[[gnu::used]] inline bool TPropagatingStorage::IsNull() const
{
    return !Impl_;
}

inline bool TPropagatingStorage::IsEmpty() const
{
    return !Impl_ || Impl_->IsEmpty();
}

inline const std::any* TPropagatingStorage::FindRaw(const std::type_info& typeInfo) const
{
    return Impl_ ? Impl_->GetRaw(typeInfo) : nullptr;
}

template <class T>
bool TPropagatingStorage::Has() const
{
    return FindRaw(typeid(T)) != nullptr;
}

template <class T>
const T& TPropagatingStorage::GetOrCrash() const
{
    const auto* result = Find<T>();
    YT_VERIFY(result);
    return *result;
}

template <class T>
const T* TPropagatingStorage::Find() const
{
    const auto* result = FindRaw(typeid(T));
    return result ? std::any_cast<T>(result) : nullptr;
}

template <class T>
std::optional<T> TPropagatingStorage::Exchange(T value)
{
    auto result = ExchangeRaw(std::make_any<T>(std::move(value)));
    return result ? std::make_optional<T>(std::any_cast<T>(std::move(*result))) : std::nullopt;
}

template <class T>
std::optional<T> TPropagatingStorage::Remove()
{
    auto result = RemoveRaw(typeid(T));
    return result ? std::make_optional<T>(std::any_cast<T>(std::move(*result))) : std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TPropagatingStorageGuard::TPropagatingStorageGuard(TPropagatingStorage storage)
    : OldStorage_(NDetail::PropagatingStorageManager.SwitchPropagatingStorage(
        std::move(storage),
        CachedFls_))
{ }

Y_FORCE_INLINE TPropagatingStorageGuard::~TPropagatingStorageGuard()
{
    NDetail::PropagatingStorageManager.SwitchPropagatingStorage(
        std::move(OldStorage_),
        CachedFls_);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TPropagatingValueGuard<T>::TPropagatingValueGuard(T value)
{
    auto& storage = CurrentPropagatingStorage();
    OldValue_ = storage.Exchange<T>(std::move(value));
}

template <class T>
TPropagatingValueGuard<T>::~TPropagatingValueGuard()
{
    auto& storage = CurrentPropagatingStorage();
    if (OldValue_) {
        storage.Exchange<T>(std::move(*OldValue_));
    } else {
        storage.Remove<T>();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
