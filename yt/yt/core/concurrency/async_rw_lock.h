#pragma once

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <queue>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TAsyncReaderWriterLock
    : public TNonCopyable
{
public:
    TFuture<void> AcquireReader();
    void ReleaseReader();

    TFuture<void> AcquireWriter();
    void ReleaseWriter();

private:
    class TImpl final
    {
    public:
        TFuture<void> AcquireReader();
        void ReleaseReader();

        TFuture<void> AcquireWriter();
        void ReleaseWriter();

    private:
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

        int ActiveReaderCount_ = 0;
        bool HasActiveWriter_ = false;

        std::vector<TPromise<void>> ReaderPromiseQueue_;
        std::queue<TPromise<void>> WriterPromiseQueue_;
    };

    using TImplPtr = TIntrusivePtr<TImpl>;

    const TImplPtr Impl_ = New<TImpl>();

    template <class TTraits>
    friend class TAsyncReaderWriterLockGuard;

    friend struct TAsyncLockReaderTraits;
    friend struct TAsyncLockWriterTraits;
};

////////////////////////////////////////////////////////////////////////////////

template <class TTraits>
class TAsyncReaderWriterLockGuard
    : public TRefCounted
{
public:
    ~TAsyncReaderWriterLockGuard();

    static TFuture<TIntrusivePtr<TAsyncReaderWriterLockGuard>> Acquire(TAsyncReaderWriterLock* lock);
    void Release();

private:
    TAsyncReaderWriterLock::TImplPtr LockImpl_;
};

////////////////////////////////////////////////////////////////////////////////

struct TAsyncLockReaderTraits
{
    static TFuture<void> Acquire(const TAsyncReaderWriterLock::TImplPtr& impl);
    static void Release(const TAsyncReaderWriterLock::TImplPtr& impl);
};

struct TAsyncLockWriterTraits
{
    static TFuture<void> Acquire(const TAsyncReaderWriterLock::TImplPtr& impl);
    static void Release(const TAsyncReaderWriterLock::TImplPtr& impl);
};

using TAsyncLockReaderGuard = TAsyncReaderWriterLockGuard<TAsyncLockReaderTraits>;
using TAsyncLockWriterGuard = TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define ASYNC_RW_LOCK_INL_H_
#include "async_rw_lock-inl.h"
#undef ASYNC_RW_LOCK_INL_H_
