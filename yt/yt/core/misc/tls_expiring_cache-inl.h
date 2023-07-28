#ifndef TLS_EXPIRING_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include tls_expiring_cache.h"
// For the sake of sane code completion.
#include "tls_expiring_cache.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t NumShards>
TThreadLocalExpiringCache<TKey, TValue, NumShards>::TThreadLocalExpiringCache(
    TDuration expirationTimeout)
    : ExpirationTimeout_(expirationTimeout)
{ }

template <class TKey, class TValue, size_t NumShards>
std::optional<TValue> TThreadLocalExpiringCache<TKey, TValue, NumShards>::Get(const TKey& key)
{
    return Cache_.GetRef(ExpirationTimeout_).Get(key);
}

template <class TKey, class TValue, size_t NumShards>
void TThreadLocalExpiringCache<TKey, TValue, NumShards>::Set(const TKey& key, TValue value)
{
    Cache_.GetRef(ExpirationTimeout_).Set(key, std::move(value));
}

template <class TKey, class TValue, size_t NumShards>
TThreadLocalExpiringCache<TKey, TValue, NumShards>::TCache::TCache(TDuration expirationTimeout)
    : ExpirationTimeout_(DurationToCpuDuration(expirationTimeout))
{ }

template <class TKey, class TValue, size_t NumShards>
std::optional<TValue> TThreadLocalExpiringCache<TKey, TValue, NumShards>::TCache::Get(const TKey& key)
{
    TryCleanup();

    auto it = Entries_.find(key);
    if (it == Entries_.end()) {
        return std::nullopt;
    }

    auto now = GetCpuInstant();
    auto deadline = now - ExpirationTimeout_;
    if (it->second.LastUpdateTime < deadline) {
        Entries_.erase(it);
        return std::nullopt;
    }

    auto result = it->second.Value;
    return result;
}

template <class TKey, class TValue, size_t NumShards>
void TThreadLocalExpiringCache<TKey, TValue, NumShards>::TCache::Set(const TKey& key, TValue value)
{
    TryCleanup();

    auto now = GetCpuInstant();

    TEntry newEntry{
        .Value = std::move(value),
        .LastUpdateTime = now,
    };

    auto it = Entries_.find(key);
    if (it == Entries_.end()) {
        Entries_.emplace(key, std::move(newEntry));
    } else {
        it->second = std::move(newEntry);
    }
}

template <class TKey, class TValue, size_t NumShards>
void TThreadLocalExpiringCache<TKey, TValue, NumShards>::TCache::Cleanup()
{
    IterationsSinceCleanup_ = 0;

    if (Entries_.empty()) {
        return;
    }

    auto now = GetCpuInstant();
    auto deadline = now - ExpirationTimeout_;

    for (auto it = Entries_.begin(); it != Entries_.end();) {
        if (it->second.LastUpdateTime < deadline) {
            Entries_.erase(it++);
        } else {
            ++it;
        }
    }
}

template <class TKey, class TValue, size_t NumShards>
void TThreadLocalExpiringCache<TKey, TValue, NumShards>::TCache::TryCleanup()
{
    if (IterationsSinceCleanup_ > CleanupAmortizationFactor * std::ssize(Entries_)) {
        Cleanup();
    } else {
        ++IterationsSinceCleanup_;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
