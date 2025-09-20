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

    void EnsureStarted();

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

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TKey, TEntryPtr> Map_;

    TAtomicIntrusivePtr<TAsyncExpiringCacheConfig> Config_;

    NProfiling::TCounter HitCounter_;
    NProfiling::TCounter MissedCounter_;
    NProfiling::TGauge SizeCounter_;

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

    void Erase(THashMap<TKey, TEntryPtr>::iterator it);

    void DeleteExpiredItems();
    void RefreshAllItems();

    // Schedules entry expiration and refresh.
    void ScheduleEntryUpdate(
        const TEntryPtr& entry,
        const TKey& key,
        const TAsyncExpiringCacheConfigPtr& config);

    TPromise<TValue> GetPromise(const TEntryPtr& entry) noexcept;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define EXPIRING_CACHE_INL_H_
#include "async_expiring_cache-inl.h"
#undef EXPIRING_CACHE_INL_H_

