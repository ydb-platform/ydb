#pragma once

#include "public.h"

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/threading/thread_local/thread_local.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! An expiring cache with separate storage for each thread.
//!
//! It is useful when some cache entries are used very frequently. So, with a
//! regular cache for this task, we might spend more on lock contention inside
//! the cache rather than on cache misses.
template <class TKey, class TValue, size_t ShardCount = 128>
class TThreadLocalExpiringCache
{
public:
    explicit TThreadLocalExpiringCache(TDuration expirationTimeout);

    std::optional<TValue> Get(const TKey& key);
    void Set(const TKey& key, TValue value);

private:
    struct TEntry
    {
        TValue Value;
        TCpuInstant LastUpdateTime;
    };

    class TCache
    {
    public:
        explicit TCache(TDuration expirationTimeout);

        std::optional<TValue> Get(const TKey& key);
        void Set(const TKey& key, TValue value);

    private:
        THashMap<TKey, TEntry> Entries_;
        i64 IterationsSinceCleanup_ = 0;
        NProfiling::TCpuDuration ExpirationTimeout_;

        static constexpr int CleanupAmortizationFactor = 3;

        void TryCleanup();
        void Cleanup();
    };

    const TDuration ExpirationTimeout_;
    ::NThreading::TThreadLocalValue<TCache, ::NThreading::EThreadLocalImpl::HotSwap, ShardCount> Cache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define TLS_EXPIRING_CACHE_INL_H_
#include "tls_expiring_cache-inl.h"
#undef TLS_EXPIRING_CACHE_INL_H_
