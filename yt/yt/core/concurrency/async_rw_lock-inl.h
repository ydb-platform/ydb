#ifndef ASYNC_RW_LOCK_INL_H_
#error "Direct inclusion of this file is not allowed, include async_rw_lock.h"
// For the sake of sane code completion.
#include "async_rw_lock.h"
#endif
#undef ASYNC_RW_LOCK_INL_H_

#include <util/system/guard.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <typename TTraits>
TAsyncReaderWriterLockGuard<TTraits>::~TAsyncReaderWriterLockGuard()
{
    Release();
}

template <typename TTraits>
TFuture<TIntrusivePtr<TAsyncReaderWriterLockGuard<TTraits>>>
    TAsyncReaderWriterLockGuard<TTraits>::Acquire(TAsyncReaderWriterLock* lock)
{
    const auto& impl = lock->Impl_;
    return TTraits::Acquire(impl).Apply(BIND([impl = impl] () mutable {
        auto guard = New<TAsyncReaderWriterLockGuard>();
        guard->LockImpl_ = std::move(impl);
        return guard;
    }));
}

template <typename TTraits>
void TAsyncReaderWriterLockGuard<TTraits>::Release()
{
    if (LockImpl_) {
        TTraits::Release(LockImpl_);
        LockImpl_ = nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
