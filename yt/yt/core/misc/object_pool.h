#pragma once

#include "common.h"

#include <yt/yt/core/profiling/public.h>

#include <util/generic/singleton.h>

#include <util/thread/lfstack.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Provides various traits for pooled objects of type |T|.
/*!
 * |Clean| method is called before an object is put into the pool.
 *
 * |IsReusable| method is called before an object is returned from the pool.
 *
 * |GetMaxPoolSize| method is called to determine the maximum number of
 * objects allowed to be pooled.
 */
template <class TObject, class = void>
struct TPooledObjectTraits
{ };

//! Basic version of traits. Others may consider inheriting from it.
template <class TObject>
struct TPooledObjectTraitsBase
{
    static TObject* Allocate()
    {
        return new TObject();
    }

    static void Clean(TObject*)
    { }

    static bool IsReusable(const TObject*)
    {
        return true;
    }

    static int GetMaxPoolSize()
    {
        return 256;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! A pool for reusable objects.
/*
 * Instances can be held through shared or unique handles; both return spare
 * instances back to the pool. Prefer #TObjectUniquePtr unless the instance
 * needs shared ownership.
 *
 * Both the pool and the references are thread-safe.
 *
 */

template <class TObject, class TTraits = TPooledObjectTraits<TObject>>
class TObjectPool
{
public:
    using TSharedObjectPtr = std::shared_ptr<TObject>;

    //! Returns the instance to the pool, or destroys it if it must not be pooled.
    struct TDeleter
    {
        TDeleter() = default;

        explicit TDeleter(bool pooled) noexcept;

        //! Instances created outside of the pool are destroyed, not pooled; this makes
        //! #TObjectUniquePtr accept a plain unique pointer.
        TDeleter(std::default_delete<TObject>) noexcept; // NOLINT(google-explicit-constructor)

        void operator()(TObject* obj) const;

    private:
        bool Pooled_ = false;
    };

    //! Unlike #TSharedObjectPtr needs no control block.
    using TObjectUniquePtr = std::unique_ptr<TObject, TDeleter>;

    ~TObjectPool();

    //! Either creates a fresh instance or returns a pooled one.
    TSharedObjectPtr AllocateShared();

    //! Same as #AllocateShared but the instance is held by a unique pointer.
    TObjectUniquePtr AllocateUnique();

    //! Same as #AllocateUnique but the instance never enters the pool.
    static TObjectUniquePtr AllocateUniqueUnpooled();

    int GetSize() const;

    void Release(int count);

private:
    TLockFreeStack<TObject*> PooledObjects_;
    std::atomic<int> PoolSize_ = 0;

    TObject* DoAllocate();

    //! Calls #TPooledObjectTraits::Clean and returns the instance back into the pool.
    void Reclaim(TObject* obj);

    static bool IsReusable(const TObject* obj);

    void FreeInstance(TObject* obj);

    Y_DECLARE_SINGLETON_FRIEND()
};

template <class TObject, class TTraits = TPooledObjectTraits<TObject>>
TObjectPool<TObject, TTraits>& ObjectPool();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define OBJECT_POOL_INL_H_
#include "object_pool-inl.h"
#undef OBJECT_POOL_INL_H_
