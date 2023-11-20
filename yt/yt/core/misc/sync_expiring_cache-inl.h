#ifndef SYNC_EXPIRING_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include sync_expiring_cache.h"
// For the sake of sane code completion.
#include "sync_expiring_cache.h"
#endif

#include "collection_helpers.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TSyncExpiringCache<TKey, TValue>::TSyncExpiringCache(
    TValueCalculator valueCalculator,
    std::optional<TDuration> expirationTimeout,
    IInvokerPtr invoker)
    : ValueCalculator_(std::move(valueCalculator))
    , EvictionExecutor_(New<NConcurrency::TPeriodicExecutor>(
        invoker,
        BIND(&TSyncExpiringCache::DeleteExpiredItems, MakeWeak(this))))
{
    SetExpirationTimeout(expirationTimeout);
    EvictionExecutor_->Start();
}

template <class TKey, class TValue>
std::optional<TValue> TSyncExpiringCache<TKey, TValue>::Find(const TKey& key)
{
    auto now = NProfiling::GetCpuInstant();
    auto deadline = now - ExpirationTimeout_.load();

    auto guard = ReaderGuard(MapLock_);

    if (auto it = Map_.find(key); it != Map_.end()) {
        auto& entry = it->second;
        if (entry.LastUpdateTime >= deadline) {
            return entry.Value;
        }
    }

    return std::nullopt;
}

template <class TKey, class TValue>
TValue TSyncExpiringCache<TKey, TValue>::Get(const TKey& key)
{
    auto now = NProfiling::GetCpuInstant();
    auto deadline = now - ExpirationTimeout_.load();

    {
        auto guard = ReaderGuard(MapLock_);

        if (auto it = Map_.find(key); it != Map_.end()) {
            auto& entry = it->second;
            if (entry.LastUpdateTime >= deadline) {
                return entry.Value;
            }
        }
    }

    auto result = ValueCalculator_(key);

    {
        auto guard = WriterGuard(MapLock_);

        auto it = Map_.find(key);
        if (it != Map_.end()) {
            it->second = {now, std::move(result)};
        } else {
            auto emplaceResult = Map_.emplace(key, TEntry{now, std::move(result)});
            YT_VERIFY(emplaceResult.second);
            it = emplaceResult.first;
        }

        return it->second.Value;
    }
}

template <class TKey, class TValue>
std::vector<TValue> TSyncExpiringCache<TKey, TValue>::Get(const std::vector<TKey>& keys)
{
    auto now = NProfiling::GetCpuInstant();
    auto deadline = now - ExpirationTimeout_.load();

    std::vector<TValue> foundValues;
    std::vector<int> missingValueIndexes;

    {
        auto guard = ReaderGuard(MapLock_);

        for (int i = 0; i < std::ssize(keys); ++i) {
            if (auto it = Map_.find(keys[i]); it != Map_.end()) {
                auto& entry = it->second;
                if (entry.LastUpdateTime >= deadline) {
                    foundValues.push_back(entry.Value);
                    continue;
                }
            }

            missingValueIndexes.push_back(i);
        }
    }

    if (missingValueIndexes.empty()) {
        return foundValues;
    }

    std::vector<TValue> values;
    values.reserve(keys.size());

    int missingIndex = 0;
    for (int keyIndex = 0; keyIndex < std::ssize(keys); ++keyIndex) {
        if (missingIndex < std::ssize(missingValueIndexes) &&
            missingValueIndexes[missingIndex] == keyIndex)
        {
            values.push_back(ValueCalculator_(keys[keyIndex]));
            ++missingIndex;
        } else {
            values.push_back(std::move(foundValues[keyIndex - missingIndex]));
        }
    }

    {
        auto guard = WriterGuard(MapLock_);

        for (auto index : missingValueIndexes) {
            const auto& value = values[index];
            if (auto it = Map_.find(keys[index]); it != Map_.end()) {
                it->second = {now, value};
            } else {
                EmplaceOrCrash(Map_, keys[index], TEntry{now, value});
            }
        }
    }

    return values;
}

template <class TKey, class TValue>
template <class K>
std::optional<TValue> TSyncExpiringCache<TKey, TValue>::Set(K&& key, TValue value)
{
    auto now = NProfiling::GetCpuInstant();
    auto deadline = now - ExpirationTimeout_.load();

    auto guard = WriterGuard(MapLock_);

    if (auto it = Map_.find(key); it != Map_.end()) {
        auto oldValue = it->second.LastUpdateTime >= deadline
            ? std::make_optional(std::move(it->second.Value))
            : std::nullopt;

        it->second = {now, std::move(value)};
        return oldValue;
    } else {
        EmplaceOrCrash(Map_, std::forward<K>(key), TEntry{now, std::move(value)});
        return {};
    }
}

template <class TKey, class TValue>
void TSyncExpiringCache<TKey, TValue>::Invalidate(const TKey& key)
{
    auto guard = WriterGuard(MapLock_);

    Map_.erase(key);
}

template <class TKey, class TValue>
void TSyncExpiringCache<TKey, TValue>::Clear()
{
    decltype(Map_) map;
    {
        auto guard = WriterGuard(MapLock_);
        Map_.swap(map);
    }
}

template <class TKey, class TValue>
void TSyncExpiringCache<TKey, TValue>::SetExpirationTimeout(std::optional<TDuration> expirationTimeout)
{
    EvictionExecutor_->SetPeriod(expirationTimeout);
    ExpirationTimeout_.store(expirationTimeout
        ? NProfiling::DurationToCpuDuration(*expirationTimeout)
        : std::numeric_limits<NProfiling::TCpuDuration>::max() / 2); // effective (but safer) infinity
}

template <class TKey, class TValue>
void TSyncExpiringCache<TKey, TValue>::DeleteExpiredItems()
{
    auto deadline = NProfiling::GetCpuInstant() - ExpirationTimeout_.load();

    std::vector<TKey> keysToRemove;
    {
        auto guard = ReaderGuard(MapLock_);
        for (const auto& [key, entry] : Map_) {
            if (entry.LastUpdateTime < deadline) {
                keysToRemove.push_back(key);
            }
        }
    }

    if (keysToRemove.empty()) {
        return;
    }

    {
        auto guard = WriterGuard(MapLock_);
        for (const auto& key : keysToRemove) {
            if (auto it = Map_.find(key); it != Map_.end()) {
                auto& entry = it->second;
                if (entry.LastUpdateTime < deadline) {
                    Map_.erase(it);
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
