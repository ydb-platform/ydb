#ifndef EXPIRING_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include async_expiring_cache.h"
// For the sake of sane code completion.
#include "async_expiring_cache.h"
#endif

// COMPAT(cherepashka)
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TAsyncExpiringCache<TKey, TValue>::TEntry::TEntry(NProfiling::TCpuInstant accessDeadline)
    : AccessDeadline(accessDeadline)
    , UpdateDeadline(std::numeric_limits<NProfiling::TCpuInstant>::max())
    , Promise(NewPromise<TValue>())
    , Future(Promise.ToFuture().ToUncancelable())
{ }

template <class TKey, class TValue>
bool TAsyncExpiringCache<TKey, TValue>::TEntry::IsExpired(NProfiling::TCpuInstant now) const
{
    return now > AccessDeadline.load() || now > UpdateDeadline.load();
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TAsyncExpiringCache<TKey, TValue>::TShard::TShard(const TShard& other)
    : EntryMap(other.EntryMap)
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TAsyncExpiringCache<TKey, TValue>::TAsyncExpiringCache(
    TAsyncExpiringCacheConfigPtr config,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    const IInvokerPtr& invoker)
    : Logger_(std::move(logger))
    , ExpirationExecutor_(New<NConcurrency::TPeriodicExecutor>(
        invoker,
        BIND(&TAsyncExpiringCache::DeleteExpiredItems, MakeWeak(this))))
    , RefreshExecutor_(New<NConcurrency::TPeriodicExecutor>(
        invoker,
        BIND(&TAsyncExpiringCache::RefreshAllItems, MakeWeak(this))))
    , ShardCount_(config->ShardCount)
    , MapShards_(config->ShardCount)
    , Config_(config)
    , HitCounter_(profiler.Counter("/hit"))
    , MissedCounter_(profiler.Counter("/miss"))
    , SizeCounter_(profiler.Gauge("/size"))
{
    RefreshExecutor_->SetPeriod(config->RefreshTime);
    ExpirationExecutor_->SetPeriod(config->ExpirationPeriod);
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::EnsureStarted()
{
    auto config = GetConfig();
    if (config->BatchUpdate) {
        if (!Started_.load(std::memory_order::relaxed) && config->RefreshTime && *config->RefreshTime) {
            RefreshExecutor_->Start();
        }
        if (!Started_.exchange(true) && config->ExpirationPeriod && *config->ExpirationPeriod) {
            ExpirationExecutor_->Start();
        }
    }
}

template <class TKey, class TValue>
TAsyncExpiringCacheConfigPtr TAsyncExpiringCache<TKey, TValue>::GetConfig() const
{
    return Config_.Acquire();
}

template <class TKey, class TValue>
typename TAsyncExpiringCache<TKey, TValue>::TExtendedGetResult TAsyncExpiringCache<TKey, TValue>::GetExtended(
    const TKey& key)
{
    const auto& Logger = Logger_;
    auto config = GetConfig();
    auto now = NProfiling::GetCpuInstant();

    // Fast path.
    {
        auto [guard, map] = LockAndGetReadableShardForKey(key);

        if (auto it = map.find(key); it != map.end()) {
            const auto& entry = it->second;
            if (!entry->IsExpired(now)) {
                HitCounter_.Increment();
                entry->AccessDeadline.store(now + NProfiling::DurationToCpuDuration(config->ExpireAfterAccessTime));
                if (!entry->Future.IsSet()) {
                    YT_LOG_DEBUG("Waiting for cache entry (Key: %v)",
                        key);
                }
                return {entry->Future, false};
            }
        }
    }

    // Slow path.
    {
        auto [guard, map] = LockAndGetWritableShardForKey(key);

        if (auto it = map.find(key); it != map.end()) {
            auto& entry = it->second;
            if (entry->Promise.IsSet() && entry->IsExpired(now)) {
                Erase(map, it);
            } else {
                HitCounter_.Increment();
                entry->AccessDeadline.store(now + NProfiling::DurationToCpuDuration(config->ExpireAfterAccessTime));
                if (!entry->Future.IsSet()) {
                    YT_LOG_DEBUG("Waiting for cache entry (Key: %v)",
                        key);
                }
                return {entry->Future, false};
            }
        }

        MissedCounter_.Increment();
        auto accessDeadline = now + NProfiling::DurationToCpuDuration(config->ExpireAfterAccessTime);
        auto entry = New<TEntry>(accessDeadline);
        auto future = entry->Future;
        Add(map, key, entry);
        guard.Release();
        YT_LOG_DEBUG("Populating cache entry (Key: %v)",
            key);

        DoGet(key, nullptr, EUpdateReason::InitialFetch)
            .Subscribe(BIND([=, weakEntry = MakeWeak(entry), this, this_ = MakeStrong(this)] (const TErrorOr<TValue>& valueOrError) {
                SetResult(weakEntry, key, valueOrError, false);
            }));

        return {future, true};
    }
}

template <class TKey, class TValue>
TFuture<TValue> TAsyncExpiringCache<TKey, TValue>::Get(const TKey& key)
{
    return GetExtended(key).Future;
}

template <class TKey, class TValue>
TFuture<std::vector<TErrorOr<TValue>>> TAsyncExpiringCache<TKey, TValue>::GetMany(
    const std::vector<TKey>& keys)
{
    const auto& Logger = Logger_;
    auto config = GetConfig();
    auto now = NProfiling::GetCpuInstant();

    std::vector<TFuture<TValue>> results(keys.size());
    std::vector<TKey> keysToWaitFor;

    auto handleHit = [&] (size_t index, const TEntryPtr& entry) {
        const auto& key = keys[index];
        HitCounter_.Increment();
        results[index] = entry->Future;
        if (!entry->Future.IsSet()) {
            keysToWaitFor.push_back(key);
        }
        entry->AccessDeadline.store(now + NProfiling::DurationToCpuDuration(config->ExpireAfterAccessTime));
    };

    auto keysPerShard = SortKeysByShards(keys);

    std::vector<size_t> preliminaryIndexesToPopulate;

    // Fast path.
    {
        for (int shardIndex = 0; shardIndex < ShardCount_; ++shardIndex) {
            auto [guard, map] = LockAndGetReadableShard(shardIndex);
            for (const auto& keysPerShardItem : keysPerShard[shardIndex]) {
                const auto& key = *keysPerShardItem.Key;
                auto requestIndex = keysPerShardItem.RequestIndex;
                if (auto it = map.find(key); it != map.end()) {
                    const auto& entry = it->second;
                    if (!entry->IsExpired(now)) {
                        handleHit(requestIndex, entry);
                        continue;
                    }
                }
                preliminaryIndexesToPopulate.push_back(requestIndex);
            }
        }
    }

    // Slow path.
    if (!preliminaryIndexesToPopulate.empty()) {
        std::vector<size_t> finalIndexesToPopulate;
        std::vector<TWeakPtr<TEntry>> entriesToPopulate;

        for (auto index : preliminaryIndexesToPopulate) {
            const auto& key = keys[index];
            auto [guard, map] = LockAndGetWritableShardForKey(key);
            if (auto it = map.find(key); it != map.end()) {
                auto& entry = it->second;
                if (entry->Promise.IsSet() && entry->IsExpired(now)) {
                    Erase(map, it);
                } else {
                    handleHit(index, entry);
                    continue;
                }
            }

            MissedCounter_.Increment();

            auto accessDeadline = now + NProfiling::DurationToCpuDuration(config->ExpireAfterAccessTime);
            auto entry = New<TEntry>(accessDeadline);

            finalIndexesToPopulate.push_back(index);
            entriesToPopulate.push_back(entry);
            results[index] = entry->Future;

            Add(map, key, entry);
        }

        std::vector<TKey> keysToPopulate;
        keysToPopulate.reserve(finalIndexesToPopulate.size());
        for (auto index : finalIndexesToPopulate) {
            keysToPopulate.push_back(keys[index]);
        }

        YT_LOG_DEBUG_UNLESS(
            keysToWaitFor.empty(),
            "Waiting for cache entries (Keys: %v)",
            keysToWaitFor);

        if (!keysToPopulate.empty()) {
            YT_LOG_DEBUG("Populating cache entries (Keys: %v)",
                keysToPopulate);
            InvokeGetMany(entriesToPopulate, keysToPopulate, /*periodicRefreshTime*/ std::nullopt);
        }
    }

    return AllSet(results);
}

template <class TKey, class TValue>
std::optional<TErrorOr<TValue>> TAsyncExpiringCache<TKey, TValue>::Find(const TKey& key)
{
    EnsureStarted();

    auto config = GetConfig();
    auto now = NProfiling::GetCpuInstant();

    auto [guard, map] = LockAndGetReadableShardForKey(key);

    if (auto it = map.find(key); it != map.end()) {
        const auto& entry = it->second;
        if (!entry->IsExpired(now) && entry->Promise.IsSet()) {
            HitCounter_.Increment();
            entry->AccessDeadline.store(now + NProfiling::DurationToCpuDuration(config->ExpireAfterAccessTime));
            return entry->Future.Get();
        }
    }

    MissedCounter_.Increment();
    return std::nullopt;
}

template <class TKey, class TValue>
std::vector<std::optional<TErrorOr<TValue>>> TAsyncExpiringCache<TKey, TValue>::FindMany(const std::vector<TKey>& keys)
{
    EnsureStarted();

    auto config = GetConfig();
    auto now = NProfiling::GetCpuInstant();
    auto keysPerShard = SortKeysByShards(keys);

    std::vector<std::optional<TErrorOr<TValue>>> results(keys.size());

    for (int shardIndex = 0; shardIndex < ShardCount_; ++shardIndex) {
        auto [guard, map] = LockAndGetReadableShard(shardIndex);
        for (const auto& keysPerShardItem: keysPerShard[shardIndex]) {
            const auto& key = *keysPerShardItem.Key;
            auto requestIndex = keysPerShardItem.RequestIndex;
            if (auto it = map.find(key); it != map.end()) {
                const auto& entry = it->second;
                if (!entry->IsExpired(now) && entry->Promise.IsSet()) {
                    HitCounter_.Increment();
                    results[requestIndex] = entry->Future.Get();
                    entry->AccessDeadline.store(now + NProfiling::DurationToCpuDuration(config->ExpireAfterAccessTime));
                } else {
                    MissedCounter_.Increment();
                }
            } else {
                MissedCounter_.Increment();
            }
        }
    }
    return results;
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::InvalidateActive(const TKey& key)
{
    EnsureStarted();

    {
        auto [guard, map] = LockAndGetReadableShardForKey(key);
        if (auto it = map.find(key); it == map.end() || !it->second->Promise.IsSet()) {
            return;
        }
    }

    auto [guard, map] = LockAndGetWritableShardForKey(key);
    if (auto it = map.find(key); it != map.end() && it->second->Promise.IsSet()) {
        Erase(map, it);
    }
}

template <class TKey, class TValue>
template <class T>
void TAsyncExpiringCache<TKey, TValue>::InvalidateValue(const TKey& key, const T& value)
{
    EnsureStarted();

    {
        auto [guard, map] = LockAndGetReadableShardForKey(key);
        if (auto it = map.find(key); it != map.end() && it->second->Promise.IsSet()) {
            auto valueOrError = it->second->Promise.Get();
            if (!valueOrError.IsOK() || valueOrError.Value() != value) {
                return;
            }
        } else {
            return;
        }
    }

    auto [guard, map] = LockAndGetWritableShardForKey(key);
    if (auto it = map.find(key); it != map.end() && it->second->Promise.IsSet()) {
        auto valueOrError = it->second->Promise.Get();
        if (valueOrError.IsOK() && valueOrError.Value() == value) {
            Erase(map, it);
        }
    }
}

template <class TKey, class TValue>
template <class T>
void TAsyncExpiringCache<TKey, TValue>::ForceRefresh(const TKey& key, const T& value)
{
    EnsureStarted();

    auto config = GetConfig();
    auto now = NProfiling::GetCpuInstant();

    auto [guard, map] = LockAndGetWritableShardForKey(key);
    if (auto it = map.find(key); it != map.end() && it->second->Promise.IsSet()) {
        auto valueOrError = it->second->Promise.Get();
        if (valueOrError.IsOK() && valueOrError.Value() == value) {
            NConcurrency::TDelayedExecutor::CancelAndClear(it->second->ProbationCookie);

            auto accessDeadline = now + NProfiling::DurationToCpuDuration(config->ExpireAfterAccessTime);
            auto newEntry = New<TEntry>(accessDeadline);
            map[key] = newEntry;
            guard.Release();

            DoGet(key, &valueOrError, EUpdateReason::ForcedUpdate)
                .Subscribe(BIND([=, weakEntry = MakeWeak(newEntry), this, this_ = MakeStrong(this)] (const TErrorOr<TValue>& valueOrError) {
                    SetResult(weakEntry, key, valueOrError, false);
                }));
        }
    }
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Ping(const TKey& key)
{
    auto config = GetConfig();
    auto now = NProfiling::GetCpuInstant();
    auto [guard, map] = LockAndGetReadableShardForKey(key);

    if (auto it = map.find(key); it != map.end() && it->second->Promise.IsSet()) {
        const auto& entry = it->second;
        if (!entry->Promise.Get().IsOK()) {
            return;
        }

        entry->AccessDeadline.store(now + NProfiling::DurationToCpuDuration(config->ExpireAfterAccessTime));
        entry->UpdateDeadline.store(now + NProfiling::DurationToCpuDuration(config->ExpireAfterSuccessfulUpdateTime));
        if (!config->BatchUpdate) {
            ScheduleEntryUpdate(entry, key, config);
        }
    }
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Set(const TKey& key, TErrorOr<TValue> valueOrError)
{
    EnsureStarted();

    auto isValueOK = valueOrError.IsOK();
    auto config = GetConfig();
    auto now = NProfiling::GetCpuInstant();

    TPromise<TValue> promise;

    auto accessDeadline = now + NProfiling::DurationToCpuDuration(config->ExpireAfterAccessTime);
    auto expirationTime = isValueOK ? config->ExpireAfterSuccessfulUpdateTime : config->ExpireAfterFailedUpdateTime;
    auto updateDeadline = now + NProfiling::DurationToCpuDuration(expirationTime);

    auto [guard, map] = LockAndGetWritableShardForKey(key);
    if (auto it = map.find(key); it != map.end()) {
        const auto& entry = it->second;
        if (entry->Promise.IsSet()) {
            entry->Promise = MakePromise(std::move(valueOrError));
            entry->Future = entry->Promise.ToFuture();
        } else {
            promise = entry->Promise;
        }
        if (expirationTime == TDuration::Zero()) {
            Erase(map, it);
        } else {
            entry->AccessDeadline.store(accessDeadline);
            entry->UpdateDeadline.store(updateDeadline);
        }
    } else if (expirationTime != TDuration::Zero()) {
        auto entry = New<TEntry>(accessDeadline);
        entry->UpdateDeadline.store(updateDeadline);
        entry->Promise = MakePromise(std::move(valueOrError));
        entry->Future = entry->Promise.ToFuture();
        Add(map, key, entry);

        if (isValueOK && !config->BatchUpdate) {
            ScheduleEntryUpdate(entry, key, config);
        }
    }

    if (!promise) {
        return;
    }

    // This is deliberately racy: during concurrent sets some updates may be not visible.
    promise.TrySet(std::move(valueOrError));
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::ScheduleEntryUpdate(
    const TEntryPtr& entry,
    const TKey& key,
    const TAsyncExpiringCacheConfigPtr& config)
{
    YT_ASSERT_SPINLOCK_AFFINITY(MapShards_[GetShardIndex(key)].EntryMapSpinLock);

    if (config->RefreshTime && *config->RefreshTime) {
        NConcurrency::TDelayedExecutor::CancelAndClear(entry->ProbationCookie);
        entry->ProbationCookie = NConcurrency::TDelayedExecutor::Submit(
            BIND_NO_PROPAGATE(
                &TAsyncExpiringCache::InvokeGet,
                MakeWeak(this),
                MakeWeak(entry),
                key),
            *config->RefreshTime);
    }

    if (config->ExpirationPeriod && *config->ExpirationPeriod) {
        NConcurrency::TDelayedExecutor::MakeDelayed(
            *config->ExpirationPeriod,
            NRpc::TDispatcher::Get()->GetHeavyInvoker())
            .Subscribe(BIND_NO_PROPAGATE([key, weakEntry = MakeWeak(entry), this, weakThis_ = MakeWeak(this)] (const TError& /*error*/) {
                auto this_ = weakThis_.Lock();
                if (!this_) {
                    return;
                }
                if (auto entry = weakEntry.Lock()) {
                    TryEraseExpired(entry, key, EEraseReason::Expiration);
                }
            }));
    }
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Clear()
{
    for (int shardIndex = 0; shardIndex < ShardCount_; ++shardIndex) {
        auto [guard, map] = LockAndGetWritableShard(shardIndex);
        for (const auto& [key, entry] : map) {
            if (entry->Promise.IsSet()) {
                NConcurrency::TDelayedExecutor::CancelAndClear(entry->ProbationCookie);
            }
            OnRemoved(key);
        }

        auto mapSize = map.size();
        map.clear();
        EntryCount_.fetch_sub(mapSize);
    }

    SizeCounter_.Update(EntryCount_.load());
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::SetResult(
    const TWeakPtr<TEntry>& weakEntry,
    const TKey& key,
    const TErrorOr<TValue>& valueOrError,
    bool isPeriodicUpdate)
{
    EnsureStarted();

    auto config = GetConfig();
    auto entry = weakEntry.Lock();
    if (!entry) {
        return;
    }

    // Ignore cancelation errors during periodic update.
    if (isPeriodicUpdate && valueOrError.FindMatching(NYT::EErrorCode::Canceled)) {
        if (valueOrError.IsOK()) {
            if (!config->BatchUpdate) {
                ScheduleEntryUpdate(entry, key, config);
            }
        }
        return;
    }

    auto canCacheEntry = valueOrError.IsOK() || CanCacheError(valueOrError);

    auto promise = GetPromise(key, entry);
    auto entryUpdated = promise.TrySet(valueOrError);

    auto now = NProfiling::GetCpuInstant();
    auto [guard, map] = LockAndGetWritableShardForKey(key);

    if (!entryUpdated && !entry->Promise.IsSet()) {
        // Someone has replaced the original promise with a new one,
        // since we attempted to set it. We retire a let a concurrent writer to do the job.
        return;
    }

    auto it = map.find(key);
    if (it == map.end() || it->second != entry) {
        return;
    }

    if (!entryUpdated && canCacheEntry) {
        entry->Promise = MakePromise(valueOrError);
        entry->Future = entry->Promise.ToFuture();
        entryUpdated = true;
    }

    auto expirationTime = TDuration::Zero();
    if (canCacheEntry) {
        expirationTime = valueOrError.IsOK()
            ? config->ExpireAfterSuccessfulUpdateTime
            : config->ExpireAfterFailedUpdateTime;
    }

    auto updateDeadline = NProfiling::GetCpuInstant() + NProfiling::DurationToCpuDuration(expirationTime);

    if (entryUpdated) {
        entry->UpdateDeadline.store(updateDeadline);
    }

    if (entry->IsExpired(now) || (entryUpdated && expirationTime == TDuration::Zero())) {
        Erase(map, it);
        return;
    }

    if (valueOrError.IsOK() && !config->BatchUpdate) {
        ScheduleEntryUpdate(entry, key, config);
    }
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::InvokeGet(
    TWeakPtr<TEntry> weakEntry,
    const TKey& key)
{
    auto entry = weakEntry.Lock();
    if (!entry || TryEraseExpired(entry, key, EEraseReason::Refresh)) {
        return;
    }

    TFuture<TValue> future;
    {
        auto readerGuard = MakeReaderGuardForKey(key);
        future = entry->Future;
    }

    YT_VERIFY(future.IsSet());
    const auto& oldValue = future.Get();

    DoGet(key, &oldValue, EUpdateReason::PeriodicUpdate)
        .Subscribe(BIND(
            [
                weakEntry = std::move(weakEntry),
                key,
                this,
                this_ = MakeStrong(this)
            ] (const TErrorOr<TValue>& valueOrError) {
                SetResult(weakEntry, key, valueOrError, true);
            }));
}

template <class TKey, class TValue>
TFuture<TValue> TAsyncExpiringCache<TKey, TValue>::DoGet(
    const TKey& key,
    const TErrorOr<TValue>* /*oldValue*/,
    EUpdateReason reason) noexcept
{
    return DoGet(key, reason != EUpdateReason::InitialFetch);
}

template <class TKey, class TValue>
bool TAsyncExpiringCache<TKey, TValue>::TryEraseExpired(const TEntryPtr& entry, const TKey& key, EEraseReason reason)
{
    auto now = NProfiling::GetCpuInstant();

    auto isEntryExpired = [reason] (const auto& entry, auto now) {
        // If we try to erase entry because of refresh, we want to do it only if it is expired after access deadline.
        // If entry is expired after successful update, we want to give it a chance to be updated again.
        return reason == EEraseReason::Refresh
            ? now > entry->AccessDeadline.load()
            : entry->IsExpired(now);
    };

    if (isEntryExpired(entry, now)) {
        auto [guard, map] = LockAndGetWritableShardForKey(key);

        if (auto it = map.find(key); it != map.end() && entry == it->second && isEntryExpired(it->second, now)) {
            Erase(map, it);
        }
        return true;
    }
    return false;
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Add(TEntryMap& mapShard, const TKey& key, const TEntryPtr& entry)
{
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(MapShards_[GetShardIndex(key)].EntryMapSpinLock);

    EmplaceOrCrash(mapShard, key, entry);
    EntryCount_.fetch_add(1);
    OnAdded(key);
    SizeCounter_.Update(EntryCount_.load());
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Erase(TEntryMap& mapShard, TEntryMap::iterator it)
{
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(MapShards_[GetShardIndex(it->first)].EntryMapSpinLock);

    NConcurrency::TDelayedExecutor::CancelAndClear(it->second->ProbationCookie);
    OnRemoved(it->first);
    mapShard.erase(it);
    EntryCount_.fetch_sub(1);
    SizeCounter_.Update(EntryCount_.load());
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::InvokeGetMany(
    const std::vector<TWeakPtr<TEntry>>& weakEntries,
    const std::vector<TKey>& keys,
    std::optional<TDuration> periodicRefreshTime)
{
    auto isPeriodicUpdate = periodicRefreshTime.has_value();

    DoGetMany(keys, isPeriodicUpdate)
        .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TErrorOr<TValue>>>& valuesOrError) {
            for (size_t index = 0; index < keys.size(); ++index) {
                SetResult(
                    weakEntries[index],
                    keys[index],
                    valuesOrError.IsOK() ? valuesOrError.Value()[index] : TErrorOr<TValue>(TError(valuesOrError)),
                    isPeriodicUpdate);
            }
        }));
}

template <class TKey, class TValue>
TFuture<std::vector<TErrorOr<TValue>>> TAsyncExpiringCache<TKey, TValue>::DoGetMany(
    const std::vector<TKey>& keys,
    bool isPeriodicUpdate) noexcept
{
    std::vector<TFuture<TValue>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.push_back(DoGet(key, isPeriodicUpdate));
    }
    return AllSet(std::move(results));
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::OnAdded(const TKey& /*key*/) noexcept
{ }

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::OnRemoved(const TKey& /*key*/) noexcept
{ }

template <class TKey, class TValue>
bool TAsyncExpiringCache<TKey, TValue>::CanCacheError(const TError& /*error*/) noexcept
{
    return true;
}

template <class TKey, class TValue>
TPromise<TValue> TAsyncExpiringCache<TKey, TValue>::GetPromise(const TKey& key, const TEntryPtr& entry) noexcept
{
    auto guard = MakeReaderGuardForKey(key);
    return entry->Promise;
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::DeleteExpiredItems()
{
    std::vector<TKey> expiredKeys;
    auto config = GetConfig();
    auto now = NProfiling::GetCpuInstant();
    {
        for (int shardIndex = 0; shardIndex < ShardCount_; ++shardIndex) {
            auto [guard, map] = LockAndGetReadableShard(shardIndex);
            for (const auto& [key, entry] : map) {
                if (entry->Promise.IsSet()) {
                    if (entry->IsExpired(now)) {
                        expiredKeys.push_back(key);
                    }
                }
            }
        }
    }
    if (!expiredKeys.empty()) {
        auto keysPerShard = SortKeysByShards(expiredKeys);

        for (int shardIndex = 0; shardIndex < ShardCount_; ++shardIndex) {
            auto [guard, map] = LockAndGetWritableShard(shardIndex);
            for (const auto& keysPerShardItem: keysPerShard[shardIndex]) {
                const auto& key = *keysPerShardItem.Key;
                if (auto it = map.find(key); it != map.end()) {
                    const auto& entry = it->second;
                    if (entry->Promise.IsSet() && entry->IsExpired(now)) {
                        Erase(map, it);
                    }
                }
            }
        }
    }
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::RefreshAllItems()
{
    std::vector<TWeakPtr<TEntry>> entries;
    std::vector<TKey> keys;

    auto config = GetConfig();
    if (!config->RefreshTime || !*config->RefreshTime) {
        return;
    }

    auto now = NProfiling::GetCpuInstant();

    {
        for (int shardIndex = 0; shardIndex < ShardCount_; ++shardIndex) {
            auto [guard, map] = LockAndGetReadableShard(shardIndex);
            for (const auto& [key, entry] : map) {
                if (entry->Promise.IsSet()) {
                    if (now < entry->AccessDeadline.load() && entry->Future.Get().IsOK()) {
                        keys.push_back(key);
                        TEntryPtr strongE = entry;
                        entries.push_back(MakeWeak(strongE));
                    }
                }
            }
        }
    }

    if (!entries.empty()) {
        InvokeGetMany(entries, keys, config->RefreshTime);
    }
}

template <class TKey, class TValue>
void TAsyncExpiringCache<TKey, TValue>::Reconfigure(TAsyncExpiringCacheConfigPtr newConfig)
{
    auto oldConfig = GetConfig();

    if (oldConfig->BatchUpdate != newConfig->BatchUpdate) {
        // TODO(akozhikhov): Support this.
        THROW_ERROR_EXCEPTION("Cannot change \"batch_update\" option");
    }

    RefreshExecutor_->SetPeriod(newConfig->RefreshTime);
    ExpirationExecutor_->SetPeriod(newConfig->ExpirationPeriod);
    if (oldConfig->RefreshTime && !newConfig->RefreshTime) {
        Y_UNUSED(RefreshExecutor_->Stop());
    }
    if (oldConfig->ExpirationPeriod && !newConfig->ExpirationPeriod) {
        Y_UNUSED(ExpirationExecutor_->Stop());
    }
    if (newConfig->BatchUpdate) {
        if (!oldConfig->RefreshTime && newConfig->RefreshTime && *newConfig->RefreshTime) {
            RefreshExecutor_->Start();
        }
        if (!oldConfig->ExpirationPeriod && newConfig->ExpirationPeriod && *newConfig->ExpirationPeriod) {
            ExpirationExecutor_->Start();
        }
    }
    // NB: Resharding support is not trivial, so there are no plans to do so.
    if (oldConfig->ShardCount != newConfig->ShardCount) {
        THROW_ERROR_EXCEPTION("Cannot change \"shard_count\" option");
    }
    Config_.Store(std::move(newConfig));
}

template <class TKey, class TValue>
std::vector<std::vector<typename TAsyncExpiringCache<TKey, TValue>::TItem>> TAsyncExpiringCache<TKey, TValue>::SortKeysByShards(const std::vector<TKey>& keys) const
{
    auto keysPerShard = std::vector<std::vector<TItem>>(ShardCount_);
    for (int index = 0; index < std::ssize(keys); ++index) {
        const auto& key = keys[index];
        keysPerShard[GetShardIndex(key)].push_back({.Key=&key, .RequestIndex=index});
    }
    return keysPerShard;
}

template <class TKey, class TValue>
int TAsyncExpiringCache<TKey, TValue>::GetShardIndex(const TKey& key) const
{
    return THash<TKey>()(key) % ShardCount_;
}

template <class TKey, class TValue>
NThreading::TReaderGuard<NThreading::TReaderWriterSpinLock> TAsyncExpiringCache<TKey, TValue>::MakeReaderGuardForKey(const TKey& key)
{
    auto shardIndex = GetShardIndex(key);
    const auto& shard = MapShards_[shardIndex];
    return NThreading::ReaderGuard(shard.EntryMapSpinLock);
}

template <class TKey, class TValue>
std::pair<NThreading::TReaderGuard<NThreading::TReaderWriterSpinLock>, const typename TAsyncExpiringCache<TKey, TValue>::TEntryMap&>
TAsyncExpiringCache<TKey, TValue>::LockAndGetReadableShard(int shardIndex)
{
    const auto& shard = MapShards_[shardIndex];
    return {NThreading::ReaderGuard(shard.EntryMapSpinLock), shard.EntryMap};
}

template <class TKey, class TValue>
std::pair<NThreading::TReaderGuard<NThreading::TReaderWriterSpinLock>, const typename TAsyncExpiringCache<TKey, TValue>::TEntryMap&>
TAsyncExpiringCache<TKey, TValue>::LockAndGetReadableShardForKey(const TKey& key)
{
    return LockAndGetReadableShard(GetShardIndex(key));
}

template <class TKey, class TValue>
std::pair<NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>, typename TAsyncExpiringCache<TKey, TValue>::TEntryMap&>
TAsyncExpiringCache<TKey, TValue>::LockAndGetWritableShard(int shardIndex)
{
    auto& shard = MapShards_[shardIndex];
    return {NThreading::WriterGuard(shard.EntryMapSpinLock), shard.EntryMap};
}

template <class TKey, class TValue>
std::pair<NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>, typename TAsyncExpiringCache<TKey, TValue>::TEntryMap&>
TAsyncExpiringCache<TKey, TValue>::LockAndGetWritableShardForKey(const TKey& key)
{
    return LockAndGetWritableShard(GetShardIndex(key));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
