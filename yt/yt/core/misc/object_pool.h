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

    static int GetMaxPoolSize()
    {
        return 256;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! A pool for reusable objects.
/*
 * Instances are tracked via shared pointers with a special deleter
 * that returns spare instances back to the pool.
 *
 * Both the pool and the references are thread-safe.
 *
 */

template <class TObject, class TTraits = TPooledObjectTraits<TObject>>
class TObjectPool
{
public:
    using TObjectPtr = std::shared_ptr<TObject>;

    ~TObjectPool();

    //! Either creates a fresh instance or returns a pooled one.
    TObjectPtr Allocate();

    int GetSize() const;

    void Release(int count);

private:
    TLockFreeStack<TObject*> PooledObjects_;
    std::atomic<int> PoolSize_ = {0};

    //! Calls #TPooledObjectTraits::Clean and returns the instance back into the pool.
    void Reclaim(TObject* obj);

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
