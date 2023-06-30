#pragma once

#include "public.h"

#include <util/generic/hash_set.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TInternRegistryTraits
{
    template <class U>
    static TInternedObjectDataPtr<T> ConstructData(
        TInternRegistryPtr<T> registry,
        U&& data);
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TInternRegistry
    : public TRefCounted
{
public:
    template <class U>
    TInternedObject<T> Intern(U&& data);

    int GetSize() const;
    void Clear();

    static TInternedObjectDataPtr<T> GetDefault();

private:
    friend class TInternedObjectData<T>;

    void OnInternedDataDestroyed(TInternedObjectData<T>* data);

    struct THash
    {
        size_t operator() (const TInternedObjectData<T>* internedData) const;
        size_t operator() (const T& data) const;
    };

    struct TEqual
    {
        bool operator() (const TInternedObjectData<T>* lhs, const TInternedObjectData<T>* rhs) const;
        bool operator() (const TInternedObjectData<T>* lhs, const T& rhs) const;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    using TProfilerSet = THashSet<TInternedObjectData<T>*, THash, TEqual>;
    TProfilerSet Registry_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TInternedObjectData
    : public TRefCounted
{
public:
    ~TInternedObjectData();

    const T& GetData() const;
    size_t GetHash() const;

private:
    friend class TInternRegistry<T>;
    DECLARE_NEW_FRIEND()

    const T Data_;
    const size_t Hash_;
    const TInternRegistryPtr<T> Registry_;
    typename TInternRegistry<T>::TProfilerSet::iterator Iterator_;

    TInternedObjectData(const T& data, TInternRegistryPtr<T> registry);
    TInternedObjectData(T&& data, TInternRegistryPtr<T> registry);
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TInternedObject
{
public:
    TInternedObject();

    TInternedObject(const TInternedObject<T>& other) = default;
    TInternedObject(TInternedObject<T>&& other) = default;

    TInternedObject<T>& operator=(const TInternedObject<T>& other) = default;
    TInternedObject<T>& operator=(TInternedObject<T>&& other) = default;

    explicit operator bool() const;

    const T& operator*() const;
    const T* operator->() const;

    TInternedObjectDataPtr<T> ToDataPtr() const;
    static TInternedObject<T> FromDataPtr(TInternedObjectDataPtr<T> data);

    static bool RefEqual(const TInternedObject<T>& lhs, const TInternedObject<T>& rhs);

private:
    friend class TInternRegistry<T>;

    TInternedObjectDataPtr<T> Data_;

    explicit TInternedObject(TInternedObjectDataPtr<T> data);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define INTERN_REGISTRY_INL_H_
#include "intern_registry-inl.h"
#undef INTERN_REGISTRY_INL_H_
