#pragma once

#include "public.h"
#include "cache_config.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

// TODO(cherepashka): remove dependency.
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note
 *  Thread affinity: user defined invoker
 */
template <class TKey, class TValue>
class TAsyncExpiringCache
    : public virtual TRefCounted
{
public:
    using KeyType = TKey;
    using ValueType = TValue;

    struct TExtendedGetResult
    {
        TFuture<TValue> Future;
        bool RequestInitialized;
    };

    explicit TAsyncExpiringCache(
        TAsyncExpiringCacheConfigPtr config,
        NLogging::TLogger logger = {},
        NProfiling::TProfiler profiler = {},
        // TODO(cherepashka): remove default value and move upper.
        const IInvokerPtr& invoker = NYT::NRpc::TDispatcher::Get()->GetHeavyInvoker());

    TFuture<TValue> Get(const TKey& key);
    TExtendedGetResult GetExtended(const TKey& key);
    TFuture<std::vector<TErrorOr<TValue>>> GetMany(const std::vector<TKey>& keys);

    std::optional<TErrorOr<TValue>> Find(const TKey& key);
    std::vector<std::optional<TErrorOr<TValue>>> FindMany(const std::vector<TKey>& keys);

    //! InvalidateActive removes key from the cache, if it's value is currently set.
    void InvalidateActive(const TKey& key);

    //! InvalidateValue removes key from the cache, if it's value is equal to provided.
    template <class T>
    void InvalidateValue(const TKey& key, const T& value);

    //! ForceRefresh marks current value as outdated, forcing value update.
    template <class T>
    void ForceRefresh(const TKey& key, const T& value);

    void Set(const TKey& key, TErrorOr<TValue> valueOrError);

    void Clear();

    void Reconfigure(TAsyncExpiringCacheConfigPtr config);

    enum EUpdateReason
    {
        InitialFetch,
        PeriodicUpdate,
        ForcedUpdate,
    };

protected:
    TAsyncExpiringCacheConfigPtr GetConfig() const;

    virtual TFuture<TValue> DoGet(
        const TKey& key,
        bool isPeriodicUpdate) noexcept = 0;

    virtual TFuture<TValue> DoGet(
        const TKey& key,
        const TErrorOr<TValue>* oldValue,
        EUpdateReason reason) noexcept;

    virtual TFuture<std::vector<TErrorOr<TValue>>> DoGetMany(
        const std::vector<TKey>& keys,
        bool isPeriodicUpdate) noexcept;

    //! Called under write lock.
    virtual void OnAdded(const TKey& key) noexcept;

    //! Called under write lock.
    virtual void OnRemoved(const TKey& key) noexcept;

    virtual bool CanCacheError(const TError& error) noexcept;

    //! Ping resets refresh timer period and behaves like successful entry update.
    void Ping(const TKey& key);

private:
    const NLogging::TLogger Logger_;
    const NConcurrency::TPeriodicExecutorPtr ExpirationExecutor_;
    const NConcurrency::TPeriodicExecutorPtr RefreshExecutor_;
    const int ShardCount_ = 1;

    std::atomic<bool> Started_ = false;

    struct TEntry
        : public TRefCounted
    {
        //! When this entry must be evicted with respect to access timeout.
        std::atomic<NProfiling::TCpuInstant> AccessDeadline;

        //! When this entry must be evicted with respect to update timeout.
        std::atomic<NProfiling::TCpuInstant> UpdateDeadline;

        //! Some latest known value (possibly not yet set).
        TPromise<TValue> Promise;

        //! Uncancelable version of #Promise.
        TFuture<TValue> Future;

        //! Corresponds to a future probation request.
        NConcurrency::TDelayedExecutorCookie ProbationCookie;

        //! Constructs a fresh entry.
        explicit TEntry(NProfiling::TCpuInstant accessDeadline);

        //! Check that entry is expired with respect to either access or update.
        bool IsExpired(NProfiling::TCpuInstant now) const;
    };

    using TEntryPtr = TIntrusivePtr<TEntry>;
    using TEntryMap = THashMap<TKey, TEntryPtr>;

    struct TShard
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, EntryMapSpinLock);
        TEntryMap EntryMap;

        TShard() = default;
        TShard(TShard&& other) = default;
        TShard(const TShard& other);
    };

    std::vector<TShard> MapShards_;
    std::atomic<int> EntryCount_ = 0;

    TAtomicIntrusivePtr<TAsyncExpiringCacheConfig> Config_;

    NProfiling::TCounter HitCounter_;
    NProfiling::TCounter MissedCounter_;
    NProfiling::TGauge SizeCounter_;

    void EnsureStarted();

    void SetResult(
        const TWeakPtr<TEntry>& weakEntry,
        const TKey& key,
        const TErrorOr<TValue>& valueOrError,
        bool isPeriodicUpdate);

    void InvokeGetMany(
        const std::vector<TWeakPtr<TEntry>>& weakEntries,
        const std::vector<TKey>& keys,
        std::optional<TDuration> periodicRefreshTime);

    void InvokeGet(
        TWeakPtr<TEntry> weakEntry,
        const TKey& key);

    enum EEraseReason
    {
        Refresh,
        Expiration,
    };

    bool TryEraseExpired(
        const TEntryPtr& Entry,
        const TKey& key,
        EEraseReason reason);

    void Add(TEntryMap& mapShard, const TKey& key, const TEntryPtr& entry);
    void Erase(TEntryMap& mapShard, TEntryMap::iterator it);

    void DeleteExpiredItems();
    void RefreshAllItems();

    // Schedules entry expiration and refresh.
    void ScheduleEntryUpdate(
        const TEntryPtr& entry,
        const TKey& key,
        const TAsyncExpiringCacheConfigPtr& config);

    TPromise<TValue> GetPromise(const TKey& key, const TEntryPtr& entry) noexcept;

    struct TItem
    {
        const TKey* Key;
        int RequestIndex;
    };
    std::vector<std::vector<TItem>> SortKeysByShards(const std::vector<TKey>& keys) const;
    int GetShardIndex(const TKey& key) const;

    NThreading::TReaderGuard<NThreading::TReaderWriterSpinLock> MakeReaderGuardForKey(const TKey& key);

    std::pair<NThreading::TReaderGuard<NThreading::TReaderWriterSpinLock>, const TEntryMap&> LockAndGetReadableShard(int shardIndex);
    std::pair<NThreading::TReaderGuard<NThreading::TReaderWriterSpinLock>, const TEntryMap&> LockAndGetReadableShardForKey(const TKey& key);

    std::pair<NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>, TEntryMap&> LockAndGetWritableShard(int shardIndex);
    std::pair<NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>, TEntryMap&> LockAndGetWritableShardForKey(const TKey& key);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define EXPIRING_CACHE_INL_H_
#include "async_expiring_cache-inl.h"
#undef EXPIRING_CACHE_INL_H_

