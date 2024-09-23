#pragma once

#include <yt/yt/core/actions/future.h>

#include <vector>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETNonblockingBatcherTimerState,
    (Initial)
    (Started)
    (Finished)
);

////////////////////////////////////////////////////////////////////////////////

template <class TBatchLimiter, class T>
concept CBatchLimiter = requires(TBatchLimiter& limiter, const T& element) {
   { limiter.IsFull() } -> std::same_as<bool>;
   { limiter.Add(element) } -> std::same_as<void>;
};

////////////////////////////////////////////////////////////////////////////////

class TBatchSizeLimiter
{
public:
    explicit TBatchSizeLimiter(int maxBatchSize);

    // Should return true only if batch is ready for flush.
    bool IsFull() const;

    // Invoked for each element added to the current batch.
    template <class T>
    void Add(const T& element);

private:
    int CurrentBatchSize_ = 0;
    int MaxBatchSize_;
};

////////////////////////////////////////////////////////////////////////////////

template <class... TLimiters>
class TCompositeBatchLimiter
{
public:
    using TLimitersTuple = std::tuple<TLimiters...>;

    explicit TCompositeBatchLimiter(TLimiters... limiters);

    bool IsFull() const;

    template <class T>
    void Add(const T& element);

private:
    TLimitersTuple Limiters_;
};

////////////////////////////////////////////////////////////////////////////////

//! Nonblocking MPMC queue that supports batching.
/*!
 * TNonblockingBatcher accepts is configured as follows:
 * - batchLimiter is a custom batch limiter. See TBatchSizeLimiter for an example.
 * - batchDuration is a time period to create the batch.
 * If producer exceeds batchDuration the consumer receives awaited batch.
 * If there is no consumer then the batch will be limited by batchLimiter.
 */
template <class T, CBatchLimiter<T> TBatchLimiter = TBatchSizeLimiter>
class TNonblockingBatcher
    : public TRefCounted
{
public:
    using TBatch = std::vector<T>;

    TNonblockingBatcher(TBatchLimiter batchLimiter, TDuration batchDuration, bool allowEmptyBatches = false);
    ~TNonblockingBatcher();

    template <class... U>
    void Enqueue(U&& ... u);

    TFuture<TBatch> DequeueBatch();
    void Drop();

    void UpdateBatchDuration(TDuration batchDuration);
    void UpdateBatchLimiter(TBatchLimiter batchLimiter);
    void UpdateAllowEmptyBatches(bool allowEmptyBatches);

    void UpdateSettings(TDuration batchDuration, TBatchLimiter batchLimiter, bool allowEmptyBatches);

    //! Flush all prepared and in-progress batches and set active promises with error.
    //! Used to clear batcher at the end of its lifetime.
    std::vector<TBatch> Drain();

private:
    using ETimerState = ETNonblockingBatcherTimerState;

    TBatchLimiter BatchLimiter_;
    TDuration BatchDuration_;
    bool AllowEmptyBatches_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TBatch CurrentBatch_;
    TBatchLimiter CurrentBatchLimiter_;

    ETimerState TimerState_ = ETimerState::Initial;
    std::deque<TBatch> Batches_;
    std::deque<TPromise<TBatch>> Promises_;
    TDelayedExecutorCookie BatchFlushCookie_;
    ui64 FlushGeneration_ = 0;

    void ResetTimer(TGuard<NThreading::TSpinLock>& guard);
    void StartTimer(TGuard<NThreading::TSpinLock>& guard);
    bool IsFlushNeeded(TGuard<NThreading::TSpinLock>& guard) const;
    void CheckFlush(TGuard<NThreading::TSpinLock>& guard);
    void CheckReturn(TGuard<NThreading::TSpinLock>& guard);
    void OnBatchTimeout(ui64 generation);
};

template <class T>
using TNonblockingBatcherPtr = TIntrusivePtr<TNonblockingBatcher<T>>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define NONBLOCKING_BATCHER_INL_H_
#include "nonblocking_batcher-inl.h"
#undef NONBLOCKING_BATCHER_INL_H_
