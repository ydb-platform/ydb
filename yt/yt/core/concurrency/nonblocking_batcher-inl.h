#ifndef NONBLOCKING_BATCHER_INL_H_
#error "Direct inclusion of this file is not allowed, include nonblocking_batcher.h"
// For the sake of sane code completion.
#include "nonblocking_batcher.h"
#endif
#undef NONBLOCKING_BATCHER_INL_H_

#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TBatchSizeLimiter::Add(const T& /*element*/)
{
    CurrentBatchSize_ += 1;
}

////////////////////////////////////////////////////////////////////////////////

template <class... TLimiters>
TCompositeBatchLimiter<TLimiters...>::TCompositeBatchLimiter(TLimiters... limiters)
    : Limiters_(std::tuple(limiters...))
{ }

template <class... TLimiters>
bool TCompositeBatchLimiter<TLimiters...>::IsFull() const
{
    bool isFull = false;
    auto adapter = [&]<size_t... I>(std::index_sequence<I...>) {
        ((isFull |= std::get<I>(Limiters_).IsFull()),...);
    };
    adapter(std::make_index_sequence<std::tuple_size_v<TLimitersTuple>>());
    return isFull;
}

template <class... TLimiters>
template <class T>
void TCompositeBatchLimiter<TLimiters...>::Add(const T& element)
{
    auto adapter = [&]<size_t... I>(std::index_sequence<I...>) {
        ((std::get<I>(Limiters_).Add(element)),...);
    };
    adapter(std::make_index_sequence<std::tuple_size_v<TLimitersTuple>>());
}

////////////////////////////////////////////////////////////////////////////////

template <class T, CBatchLimiter<T> TBatchLimiter>
TNonblockingBatcher<T, TBatchLimiter>::TNonblockingBatcher(TBatchLimiter batchLimiter, TDuration batchDuration, bool allowEmptyBatches)
    : BatchLimiter_(batchLimiter)
    , BatchDuration_(batchDuration)
    , AllowEmptyBatches_(allowEmptyBatches)
    , CurrentBatchLimiter_(BatchLimiter_)
{ }

template <class T, CBatchLimiter<T> TBatchLimiter>
TNonblockingBatcher<T, TBatchLimiter>::~TNonblockingBatcher()
{
    auto guard = Guard(SpinLock_);
    ResetTimer(guard);
}

template <class T, CBatchLimiter<T> TBatchLimiter>
template <class... U>
void TNonblockingBatcher<T, TBatchLimiter>::Enqueue(U&& ... u)
{
    auto guard = Guard(SpinLock_);
    CurrentBatch_.emplace_back(std::forward<U>(u)...);
    CurrentBatchLimiter_.Add(CurrentBatch_.back());
    StartTimer(guard);
    CheckFlush(guard);
}

template <class T, CBatchLimiter<T> TBatchLimiter>
TFuture<typename TNonblockingBatcher<T, TBatchLimiter>::TBatch> TNonblockingBatcher<T, TBatchLimiter>::DequeueBatch()
{
    auto guard = Guard(SpinLock_);
    auto promise = NewPromise<TBatch>();
    Promises_.push_back(promise);
    CheckReturn(guard);
    StartTimer(guard);
    return promise.ToFuture();
}

template <class T, CBatchLimiter<T> TBatchLimiter>
void TNonblockingBatcher<T, TBatchLimiter>::Drop()
{
    std::queue<TBatch> batches;
    std::deque<TPromise<TBatch>> promises;
    {
        auto guard = Guard(SpinLock_);
        Batches_.swap(batches);
        Promises_.swap(promises);
        CurrentBatch_.clear();
        CurrentBatchLimiter_ = BatchLimiter_;
        ResetTimer(guard);
    }
    for (auto&& promise : promises) {
        promise.Set(TBatch{});
    }
}

template <class T, CBatchLimiter<T> TBatchLimiter>
void TNonblockingBatcher<T, TBatchLimiter>::UpdateBatchDuration(TDuration batchDuration)
{
    auto guard = Guard(SpinLock_);
    BatchDuration_ = batchDuration;
}

template <class T, CBatchLimiter<T> TBatchLimiter>
void TNonblockingBatcher<T, TBatchLimiter>::UpdateBatchLimiter(TBatchLimiter batchLimiter)
{
    auto guard = Guard(SpinLock_);
    BatchLimiter_ = batchLimiter;
    if (CurrentBatch_.empty()) {
        CurrentBatchLimiter_ = BatchLimiter_;
    }
}

template <class T, CBatchLimiter<T> TBatchLimiter>
void TNonblockingBatcher<T, TBatchLimiter>::UpdateAllowEmptyBatches(bool allowEmptyBatches)
{
    auto guard = Guard(SpinLock_);
    AllowEmptyBatches_ = allowEmptyBatches;
    StartTimer(guard);
}

template <class T, CBatchLimiter<T> TBatchLimiter>
void TNonblockingBatcher<T, TBatchLimiter>::UpdateSettings(TDuration batchDuration, TBatchLimiter batchLimiter, bool allowEmptyBatches)
{
    auto guard = Guard(SpinLock_);
    BatchDuration_ = batchDuration;
    BatchLimiter_ = batchLimiter;
    AllowEmptyBatches_ = allowEmptyBatches;

    if (CurrentBatch_.empty()) {
        CurrentBatchLimiter_ = BatchLimiter_;
    }
    StartTimer(guard);
}


template <class T, CBatchLimiter<T> TBatchLimiter>
void TNonblockingBatcher<T, TBatchLimiter>::ResetTimer(TGuard<NThreading::TSpinLock>& /*guard*/)
{
    if (TimerState_ == ETimerState::Started) {
        ++FlushGeneration_;
        TDelayedExecutor::CancelAndClear(BatchFlushCookie_);
    }
    TimerState_ = ETimerState::Initial;
}

template <class T, CBatchLimiter<T> TBatchLimiter>
void TNonblockingBatcher<T, TBatchLimiter>::StartTimer(TGuard<NThreading::TSpinLock>& /*guard*/)
{
    if (TimerState_ == ETimerState::Initial && !Promises_.empty() && (AllowEmptyBatches_ || !CurrentBatch_.empty())) {
        TimerState_ = ETimerState::Started;
        BatchFlushCookie_ = TDelayedExecutor::Submit(
            BIND(&TNonblockingBatcher::OnBatchTimeout, MakeWeak(this), FlushGeneration_),
            BatchDuration_);
    }
}

template <class T, CBatchLimiter<T> TBatchLimiter>
bool TNonblockingBatcher<T, TBatchLimiter>::IsFlushNeeded(TGuard<NThreading::TSpinLock>& /*guard*/) const
{
    return
        CurrentBatchLimiter_.IsFull() ||
        TimerState_ == ETimerState::Finished;
}

template <class T, CBatchLimiter<T> TBatchLimiter>
void TNonblockingBatcher<T, TBatchLimiter>::CheckFlush(TGuard<NThreading::TSpinLock>& guard)
{
    if (!IsFlushNeeded(guard)) {
        return;
    }
    ResetTimer(guard);
    Batches_.push(std::move(CurrentBatch_));
    CurrentBatch_.clear();
    CurrentBatchLimiter_ = BatchLimiter_;
    CheckReturn(guard);
}

template <class T, CBatchLimiter<T> TBatchLimiter>
void TNonblockingBatcher<T, TBatchLimiter>::CheckReturn(TGuard<NThreading::TSpinLock>& guard)
{
    if (Promises_.empty() || Batches_.empty()) {
        return;
    }
    auto batch = std::move(Batches_.front());
    Batches_.pop();
    auto promise = std::move(Promises_.front());
    Promises_.pop_front();
    if (AllowEmptyBatches_ && !Promises_.empty()) {
        StartTimer(guard);
    }
    guard.Release();
    promise.Set(std::move(batch));
}

template <class T, CBatchLimiter<T> TBatchLimiter>
void TNonblockingBatcher<T, TBatchLimiter>::OnBatchTimeout(ui64 generation)
{
    auto guard = Guard(SpinLock_);
    if (generation != FlushGeneration_) {
        // Chunk had been prepared.
        return;
    }
    TimerState_ = ETimerState::Finished;
    CheckFlush(guard);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
