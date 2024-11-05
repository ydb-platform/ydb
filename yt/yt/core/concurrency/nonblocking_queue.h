#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/misc/ring_queue.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TNonblockingQueue
    : private TNonCopyable
{
public:
    void Enqueue(TFuture<T> asyncValue);

    // This template is required to enable perfect forwarding.
    template <class TArg>
    void Enqueue(TArg&& value);

    // Dequeued futures could be set in arbitrary order.
    TFuture<T> Dequeue();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    TRingQueue<TFuture<T>> AsyncValueQueue_;
    TRingQueue<TPromise<T>> PromiseQueue_;

};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TBoundedNonblockingQueue
{
public:
    explicit TBoundedNonblockingQueue(i64 sizeLimit);

    TFuture<void> Enqueue(TFuture<T> asyncValue);

    // This template is required to enable perfect forwarding.
    template <class TArg>
    TFuture<void> Enqueue(TArg&& value);

    // Dequeued futures could be set in arbitrary order.
    TFuture<T> Dequeue();

    void Drain(const TError& error);

private:
    const i64 SizeLimit_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    TRingQueue<TFuture<T>> AsyncValueQueue_;
    TRingQueue<TPromise<T>> ConsumerQueue_;
    TRingQueue<TPromise<void>> ProducerQueue_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define NONBLOCKING_QUEUE_INL_H_
#include "nonblocking_queue-inl.h"
#undef NONBLOCKING_QUEUE_INL_H_
