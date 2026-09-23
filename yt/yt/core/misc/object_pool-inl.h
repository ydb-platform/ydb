#ifndef OBJECT_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include object_pool.h"
// For the sake of sane code completion.
#include "object_pool.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class TTraits>
TObjectPool<T, TTraits>::~TObjectPool()
{
    T* obj;
    while (PooledObjects_.Dequeue(&obj)) {
        FreeInstance(obj);
    }
}

template <class T, class TTraits>
auto TObjectPool<T, TTraits>::AllocateShared() -> TSharedObjectPtr
{
    // Do not use TDeleter here: it adds an extra branch to the destruction path.
    return TSharedObjectPtr(DoAllocate(), [] (T* obj) {
        ObjectPool<T, TTraits>().Reclaim(obj);
    });
}

template <class T, class TTraits>
TObjectPool<T, TTraits>::TDeleter::TDeleter(bool pooled) noexcept
    : Pooled_(pooled)
{ }

template <class T, class TTraits>
TObjectPool<T, TTraits>::TDeleter::TDeleter(std::default_delete<T>) noexcept
{ }

template <class T, class TTraits>
void TObjectPool<T, TTraits>::TDeleter::operator()(T* obj) const
{
    if (Pooled_) {
        ObjectPool<T, TTraits>().Reclaim(obj);
    } else {
        delete obj;
    }
}

template <class T, class TTraits>
auto TObjectPool<T, TTraits>::AllocateUnique() -> TObjectUniquePtr
{
    return TObjectUniquePtr(DoAllocate(), TDeleter(true));
}

template <class T, class TTraits>
auto TObjectPool<T, TTraits>::AllocateUniqueUnpooled() -> TObjectUniquePtr
{
    return std::unique_ptr<T>(TTraits::Allocate());
}

template <class T, class TTraits>
T* TObjectPool<T, TTraits>::DoAllocate()
{
    T* obj = nullptr;
    while (PooledObjects_.Dequeue(&obj)) {
        --PoolSize_;
        if (IsReusable(obj)) {
            break;
        }
        FreeInstance(obj);
        obj = nullptr;
    }

    if (!obj) {
        obj = TTraits::Allocate();
    }

    return obj;
}

template <class T, class TTraits>
void TObjectPool<T, TTraits>::Reclaim(T* obj)
{
    TTraits::Clean(obj);

    while (true) {
        auto poolSize = PoolSize_.load();
        if (poolSize >= TTraits::GetMaxPoolSize()) {
            FreeInstance(obj);
            break;
        } else if (PoolSize_.compare_exchange_strong(poolSize, poolSize + 1)) {
            PooledObjects_.Enqueue(obj);
            break;
        }
    }

    if (PoolSize_ > TTraits::GetMaxPoolSize()) {
        T* objToDestroy;
        if (PooledObjects_.Dequeue(&objToDestroy)) {
            --PoolSize_;
            FreeInstance(objToDestroy);
        }
    }
}

template <class T, class TTraits>
int TObjectPool<T, TTraits>::GetSize() const
{
    return PoolSize_;
}

template <class T, class TTraits>
void TObjectPool<T, TTraits>::Release(int count)
{
    T* obj;
    while (count > 0 && PooledObjects_.Dequeue(&obj)) {
        --PoolSize_;
        FreeInstance(obj);
        --count;
    }
}

template <class T, class TTraits>
bool TObjectPool<T, TTraits>::IsReusable(const T* obj)
{
    if constexpr (requires { TTraits::IsReusable(obj); }) {
        return TTraits::IsReusable(obj);
    } else {
        return true;
    }
}

template <class T, class TTraits>
void TObjectPool<T, TTraits>::FreeInstance(T* obj)
{
    delete obj;
}

template <class T, class TTraits>
TObjectPool<T, TTraits>& ObjectPool()
{
    return *Singleton<TObjectPool<T, TTraits>>();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TPooledObjectTraits<
    T,
    typename std::enable_if_t<
        std::is_convertible_v<T&, ::google::protobuf::MessageLite&>
    >
>
    : public TPooledObjectTraitsBase<T>
{
    static void Clean(T* message)
    {
        message->Clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
