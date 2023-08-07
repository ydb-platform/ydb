#pragma once

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TSyncExpiringCache
    : public TRefCounted
{
public:
    TSyncExpiringCache(
        TCallback<TValue(const TKey&)> calculateValueAction,
        std::optional<TDuration> expirationTimeout,
        IInvokerPtr invoker);

    std::optional<TValue> Find(const TKey& key);

    TValue Get(const TKey& key);
    std::vector<TValue> Get(const std::vector<TKey>& keys);

    void Set(const TKey& key, TValue value);
    void Invalidate(const TKey& key);
    void Clear();

    void SetExpirationTimeout(std::optional<TDuration> expirationTimeout);

private:
    struct TEntry
    {
        TEntry(
            NProfiling::TCpuInstant lastAccessTime,
            NProfiling::TCpuInstant lastUpdateTime,
            TValue value);

        TEntry(TEntry&& entry);
        TEntry& operator=(TEntry&& other);

        std::atomic<NProfiling::TCpuInstant> LastAccessTime;
        NProfiling::TCpuInstant LastUpdateTime;
        TValue Value;
    };

    const TCallback<TValue(const TKey&)> CalculateValueAction_;
    const NConcurrency::TPeriodicExecutorPtr EvictionExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MapLock_);
    THashMap<TKey, TEntry> Map_;

    std::atomic<NProfiling::TCpuDuration> ExpirationTimeout_;


    void DeleteExpiredItems();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SYNC_EXPIRING_CACHE_INL_H_
#include "sync_expiring_cache-inl.h"
#undef SYNC_EXPIRING_CACHE_INL_H_
