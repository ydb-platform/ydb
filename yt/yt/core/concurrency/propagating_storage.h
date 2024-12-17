#pragma once

#include "public.h"
#include "fls.h"

#include <yt/yt/core/actions/signal.h>

#include <any>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Fiber-local key-value storage that is able to propagate between fibers.
//!
//! When a new callback is created via BIND, then a copy is made into its bind state. So,
//! this storage is propagated between the fibers creating each other.
//!
//! It is a key value storage where keys are type names. Thus, you may consider it as a
//! set of singletons.
//!
//! TPropagatingStorage is copy-on-write, so copying it between fibers is cheap if no values
//! are modified.
class TPropagatingStorage
{
public:
    //! Creates a null storage.
    TPropagatingStorage();

    //! Creates an empty, non-null storage.
    static TPropagatingStorage Create();

    ~TPropagatingStorage();

    TPropagatingStorage(const TPropagatingStorage& other);
    TPropagatingStorage(TPropagatingStorage&& other);

    TPropagatingStorage& operator=(const TPropagatingStorage& other);
    TPropagatingStorage& operator=(TPropagatingStorage&& other);

    //! Returns true if the storage is null.
    //!
    //! If the propagating storage is null, it means that there is no underlying storage to keep
    //! the data in it.
    //!
    //! In all other ways, it is indistinguishable from empty storage. If you read from it, you
    //! will get nulls. If you try to modify it, the underlying storage will be created.
    //!
    //! You probably don't want to use this function, as it's mostly used in fiber scheduler to
    //! verify that propagating storage doesn't leak to unwanted places. Use IsEmpty() instead.
    bool IsNull() const;

    bool IsEmpty() const;

    template <class T>
    bool Has() const;

    template <class T>
    const T& GetOrCrash() const;

    template <class T>
    const T* Find() const;

    template <class T>
    std::optional<T> Exchange(T value);

    template <class T>
    std::optional<T> Remove();

    void RecordLocation(TSourceLocation loc);
    void PrintModificationLocationsToStderr();

    DECLARE_SIGNAL(void(), OnAfterInstall);
    DECLARE_SIGNAL(void(), OnBeforeUninstall);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

    explicit TPropagatingStorage(TIntrusivePtr<TImpl> impl);

    const std::any* FindRaw(const std::type_info& typeInfo) const;
    std::optional<std::any> ExchangeRaw(std::any value);
    std::optional<std::any> RemoveRaw(const std::type_info& typeInfo);

    void EnsureUnique();
};

////////////////////////////////////////////////////////////////////////////////

TPropagatingStorage& GetCurrentPropagatingStorage();
const TPropagatingStorage* TryGetPropagatingStorage(const NConcurrency::TFls& fls);

////////////////////////////////////////////////////////////////////////////////

// NB: Use function pointer to minimize the overhead.
using TPropagatingStorageGlobalSwitchHandler = void(*)(
    const TPropagatingStorage& oldStorage,
    const TPropagatingStorage& newStorage);

void InstallGlobalPropagatingStorageSwitchHandler(
    TPropagatingStorageGlobalSwitchHandler handler);

////////////////////////////////////////////////////////////////////////////////

class TPropagatingStorageGuard
{
public:
    explicit TPropagatingStorageGuard(
        TPropagatingStorage storage, TSourceLocation loc = YT_CURRENT_SOURCE_LOCATION);
    ~TPropagatingStorageGuard();

    TPropagatingStorageGuard(const TPropagatingStorageGuard& other) = delete;
    TPropagatingStorageGuard(TPropagatingStorageGuard&& other) = delete;
    TPropagatingStorageGuard& operator=(const TPropagatingStorageGuard& other) = delete;
    TPropagatingStorageGuard& operator=(TPropagatingStorageGuard&& other) = delete;

    const TPropagatingStorage& GetOldStorage() const;

private:
    TPropagatingStorage OldStorage_;
    TSourceLocation OldLocation_;
};

////////////////////////////////////////////////////////////////////////////////

class TNullPropagatingStorageGuard
    : public TPropagatingStorageGuard
{
public:
    TNullPropagatingStorageGuard(TSourceLocation loc = YT_CURRENT_SOURCE_LOCATION);
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TPropagatingValueGuard
{
public:
    explicit TPropagatingValueGuard(T value);
    ~TPropagatingValueGuard();

    TPropagatingValueGuard(const TPropagatingValueGuard& other) = delete;
    TPropagatingValueGuard(TPropagatingValueGuard&& other) = delete;
    TPropagatingValueGuard& operator=(const TPropagatingValueGuard& other) = delete;
    TPropagatingValueGuard& operator=(TPropagatingValueGuard&& other) = delete;

private:
    std::optional<T> OldValue_;
};

////////////////////////////////////////////////////////////////////////////////

TSourceLocation SwitchPropagatingStorageLocation(TSourceLocation loc);

void PrintLocationToStderr();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define PROPAGATING_STORAGE_INL_H_
#include "propagating_storage-inl.h"
#undef PROPAGATING_STORAGE_INL_H_
