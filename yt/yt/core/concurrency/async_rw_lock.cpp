#include "async_rw_lock.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TAsyncLockReaderTraits::Acquire(const TAsyncReaderWriterLock::TImplPtr& impl)
{
    return impl->AcquireReader();
}

void TAsyncLockReaderTraits::Release(const TAsyncReaderWriterLock::TImplPtr& impl)
{
    impl->ReleaseReader();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TAsyncLockWriterTraits::Acquire(const TAsyncReaderWriterLock::TImplPtr& impl)
{
    return impl->AcquireWriter();
}

void TAsyncLockWriterTraits::Release(const TAsyncReaderWriterLock::TImplPtr& impl)
{
    impl->ReleaseWriter();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TAsyncReaderWriterLock::AcquireReader()
{
    return Impl_->AcquireReader();
}

void TAsyncReaderWriterLock::ReleaseReader()
{
    Impl_->ReleaseReader();
}

TFuture<void> TAsyncReaderWriterLock::AcquireWriter()
{
    return Impl_->AcquireWriter();
}

void TAsyncReaderWriterLock::ReleaseWriter()
{
    Impl_->ReleaseWriter();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TAsyncReaderWriterLock::TImpl::AcquireReader()
{
    auto guard = Guard(SpinLock_);

    if (!HasActiveWriter_ && WriterPromiseQueue_.empty()) {
        ++ActiveReaderCount_;
        return OKFuture;
    }

    auto promise = NewPromise<void>();
    ReaderPromiseQueue_.push_back(promise);
    return promise;
}

void TAsyncReaderWriterLock::TImpl::ReleaseReader()
{
    ReleaseReaders(1);
}

TFuture<void> TAsyncReaderWriterLock::TImpl::AcquireWriter()
{
    auto guard = Guard(SpinLock_);

    if (ActiveReaderCount_ == 0 && !HasActiveWriter_) {
        HasActiveWriter_ = true;
        return OKFuture;
    }

    auto promise = NewPromise<void>();
    WriterPromiseQueue_.push(promise);
    return promise;
}

void TAsyncReaderWriterLock::TImpl::ReleaseWriter()
{
    auto guard = Guard(SpinLock_);

    YT_VERIFY(HasActiveWriter_);
    HasActiveWriter_ = false;

    WakeNext(guard);
}

void TAsyncReaderWriterLock::TImpl::WakeNext(TGuard<NThreading::TSpinLock>& guard)
{
    YT_ASSERT_SPINLOCK_AFFINITY(*guard.GetMutex());

    while (!WriterPromiseQueue_.empty()) {
        auto promise = std::move(WriterPromiseQueue_.front());
        WriterPromiseQueue_.pop();
        YT_VERIFY(!HasActiveWriter_);
        HasActiveWriter_ = true;
        {
            auto unguard = Unguard(*guard.GetMutex());
            promise.Set();
            if (!promise.IsCanceled()) {
                return;
            }
        }
        YT_VERIFY(HasActiveWriter_);
        HasActiveWriter_ = false;
    }

    if (!ReaderPromiseQueue_.empty()) {
        std::vector<TPromise<void>> readerPromiseQueue = std::move(ReaderPromiseQueue_);
        ReaderPromiseQueue_.clear();

        ActiveReaderCount_ += readerPromiseQueue.size();
        guard.Release();

        int canceledReaders = 0;

        // Promise subscribers must be synchronous to avoid hanging on some reader.
        TForbidContextSwitchGuard contextSwitchGuard;
        for (auto& promise : readerPromiseQueue) {
            promise.Set();
            if (promise.IsCanceled()) {
                canceledReaders++;
            }
        }

        if (canceledReaders > 0) {
            ReleaseReaders(canceledReaders);
        }
    }
}

void TAsyncReaderWriterLock::TImpl::ReleaseReaders(int amount)
{
    auto guard = Guard(SpinLock_);

    YT_VERIFY(!HasActiveWriter_);
    YT_VERIFY(ActiveReaderCount_ >= amount);

    ActiveReaderCount_ -= amount;
    if (ActiveReaderCount_ == 0) {
        WakeNext(guard);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
