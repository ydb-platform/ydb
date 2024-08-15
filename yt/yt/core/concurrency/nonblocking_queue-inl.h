#ifndef NONBLOCKING_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include nonblocking_queue.h"
// For the sake of sane code completion.
#include "nonblocking_queue.h"
#endif
#undef NONBLOCKING_QUEUE_INL_H_

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TNonblockingQueue<T>::Enqueue(TFuture<T> asyncValue)
{
    auto guard = Guard(SpinLock_);
    if (PromiseQueue_.empty()) {
        AsyncValueQueue_.push(std::move(asyncValue));
    } else {
        auto promise = PromiseQueue_.front();
        PromiseQueue_.pop();
        guard.Release();
        promise.SetFrom(std::move(asyncValue));
    }
}

template <class T>
template <class TArg>
void TNonblockingQueue<T>::Enqueue(TArg&& value)
{
    Enqueue(MakeFuture<T>(std::forward<TArg>(value)));
}

template <class T>
TFuture<T> TNonblockingQueue<T>::Dequeue()
{
    auto guard = Guard(SpinLock_);
    if (AsyncValueQueue_.empty()) {
        auto promise = NewPromise<T>();
        PromiseQueue_.push(promise);
        return promise.ToFuture();
    } else {
        auto future = std::move(AsyncValueQueue_.front());
        AsyncValueQueue_.pop();
        return future;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TBoundedNonblockingQueue<T>::TBoundedNonblockingQueue(i64 sizeLimit)
    : SizeLimit_(sizeLimit)
{ }

template <class T>
TFuture<void> TBoundedNonblockingQueue<T>::Enqueue(TFuture<T> asyncValue)
{
    auto guard = Guard(SpinLock_);
    if (ConsumerQueue_.empty()) {
        AsyncValueQueue_.push(std::move(asyncValue));

        if (std::ssize(AsyncValueQueue_) <= SizeLimit_) {
            return VoidFuture;
        }

        auto promise = NewPromise<void>();
        ProducerQueue_.push(promise);
        return promise.ToFuture();
    } else {
        auto promise = ConsumerQueue_.front();
        ConsumerQueue_.pop();

        guard.Release();

        promise.SetFrom(std::move(asyncValue));

        return VoidFuture;
    }
}

template <class T>
template <class TArg>
TFuture<void> TBoundedNonblockingQueue<T>::Enqueue(TArg&& value)
{
    return Enqueue(MakeFuture<T>(std::forward<TArg>(value)));
}

template <class T>
TFuture<T> TBoundedNonblockingQueue<T>::Dequeue()
{
    auto guard = Guard(SpinLock_);
    if (AsyncValueQueue_.empty()) {
        auto promise = NewPromise<T>();
        ConsumerQueue_.push(promise);
        return promise.ToFuture();
    } else {
        auto future = std::move(AsyncValueQueue_.front());
        AsyncValueQueue_.pop();

        if (!ProducerQueue_.empty()) {
            auto promise = ProducerQueue_.front();
            ProducerQueue_.pop();

            guard.Release();

            promise.Set();
        }

        return future;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
