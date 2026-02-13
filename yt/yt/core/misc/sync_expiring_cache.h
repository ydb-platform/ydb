#pragma once

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <library/cpp/yt/memory/range.h>

#include <library/cpp/yt/misc/concepts.h>

#include <optional>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
class TSyncExpiringCache
    : public TRefCounted
{
public:
    TSyncExpiringCache(
        std::optional<TDuration> expirationTimeout,
        IInvokerPtr invoker);

    template <class THeterogenousKey>
    std::optional<TValue> Find(const THeterogenousKey& key);

    template <class THeterogenousKey, CInvocable<TValue()> TValueCtor>
    TValue GetOrPut(
        const THeterogenousKey& key,
        const TValueCtor& valueCtor);

    template <class THeterogenousKey, CInvocable<TValue(int index)> TValueCtor>
    std::vector<TValue> GetOrPutMany(
        TRange<THeterogenousKey> keys,
        const TValueCtor& valueCtor);

    //! Returns the previous value, if any.
    template <class THeterogenousKey>
    std::optional<TValue> Put(const THeterogenousKey& key, TValue value);

    template <class THeterogenousKey>
    void Invalidate(const THeterogenousKey& key);

    void Clear();

    void SetExpirationTimeout(std::optional<TDuration> expirationTimeout);

private:
    const NConcurrency::TPeriodicExecutorPtr EvictionExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MapLock_);

    struct TEntry
    {
        NProfiling::TCpuInstant LastUpdateTime;
        TValue Value;
    };

    THashMap<TKey, TEntry> Map_;

    std::atomic<NProfiling::TCpuDuration> ExpirationTimeout_;

    void DeleteExpiredItems();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SYNC_EXPIRING_CACHE_INL_H_
#include "sync_expiring_cache-inl.h"
#undef SYNC_EXPIRING_CACHE_INL_H_
