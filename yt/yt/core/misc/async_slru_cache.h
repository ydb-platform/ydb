#pragma once

#include "public.h"
#include "cache_config.h"
#include "memory_usage_tracker.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
class TAsyncSlruCacheBase;

template <class TKey, class TValue, class THash = THash<TKey>>
class TAsyncCacheValueBase
    : public virtual TRefCounted
{
public:
    virtual ~TAsyncCacheValueBase();

    const TKey& GetKey() const;

    void UpdateWeight() const;

protected:
    explicit TAsyncCacheValueBase(const TKey& key);

private:
    using TCache = TAsyncSlruCacheBase<TKey, TValue, THash>;
    friend class TAsyncSlruCacheBase<TKey, TValue, THash>;

    TWeakPtr<TCache> Cache_;
    TKey Key_;
    typename TCache::TItem* Item_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

//! Manages lists for TAsyncSlruCacheBase. It contains two lists, Younger and Older,
//! and TouchBuffer.
//!
//! TouchBuffer is a temporary buffer to keep items which were touched recently. It is
//! used for optimization, to allow touching items without holding a write lock. The
//! following important invariant must hold: any item it TouchBuffer must be also present
//! in either Younger or Older. In particular, TouchBuffer must not contain items that
//! were already freed. So, it's IMPORTANT to call DrainTouchBuffer() before removing
//! anything.
//!
//! Younger and Older store the items in a linked list. The rules are as follows:
//! - When an item is inserted, it's pushed to the head Younger.
//! - When an item is touched, it's moved to the head of Older.
//! - Total weight of Older cannot exceed (capacity * (1 - youngerSizeFraction)).
//! - Total weight of all items cannot exceed capacity.
//! - The items from the tail of Older are evicted to the head of Younger.
//! - The items from the tail of Younger are removed from this ListManager.
template <class TItem, class TDerived>
class TAsyncSlruCacheListManager
{
public:
    void PushToYounger(TItem* item, i64 weight);

    void MoveToYounger(TItem* item);
    void MoveToOlder(TItem* item);

    void PopFromLists(TItem* item);

    void UpdateWeight(TItem* item, i64 weightDelta);

    void UpdateCookie(TItem* item, i64 countDelta, i64 weightDelta);

    TIntrusiveListWithAutoDelete<TItem, TDelete> TrimNoDelete();

    bool TouchItem(TItem* item);

    //! Drains touch buffer. You MUST call this function before trying to remove anything from the
    //! lists (i.e. calling PopFromLists() or TrimNoDelete()), otherwise you may catch use-after-free
    //! bugs. It's not necessary to call it when moving from Younger to Older or from Older to Younger,
    //! though.
    void DrainTouchBuffer();

    void Reconfigure(i64 capacity, double youngerSizeFraction);

    void SetTouchBufferCapacity(i64 touchBufferCapacity);

protected:
    TDerived* AsDerived()
    {
        return static_cast<TDerived*>(this);
    }

    const TDerived* AsDerived() const
    {
        return static_cast<const TDerived*>(this);
    }

    // Callbacks to be overloaded in derived classes.
    void OnYoungerUpdated(i64 deltaCount, i64 deltaWeight);
    void OnOlderUpdated(i64 deltaCount, i64 deltaWeight);
    void OnCookieUpdated(i64 deltaCount, i64 deltaWeight);

private:
    TIntrusiveListWithAutoDelete<TItem, TDelete> YoungerLruList;
    TIntrusiveListWithAutoDelete<TItem, TDelete> OlderLruList;

    std::vector<TItem*> TouchBuffer;
    std::atomic<int> TouchBufferPosition = 0;

    i64 YoungerWeightCounter = 0;
    i64 OlderWeightCounter = 0;
    i64 CookieWeightCounter = 0;

    std::atomic<i64> Capacity;
    std::atomic<double> YoungerSizeFraction;
};

////////////////////////////////////////////////////////////////////////////////

//! Base class for asynchronous caches.
//!
//! The cache is asynchronous. It means that the items are not inserted immediately. Instead,
//! one may call BeginInsert() to indicate that the item is being inserted. Then, the caller
//! may either insert the item or cancel the insertion.
//!
//! It is divided onto shards. Each shard behaves as a small cache independent of others. The
//! item is put into a shard according to hash of its key.
//!
//! The cache also optionally supports resurrection. If you try to lookup a value in the cache
//! which is already evicted but it still present in memory (because someone else holds a strong
//! pointer to this value), it returns back to the cache. This behavior may be overloaded by
//! overriding IsResurrectionSupported() function.
//!
//! This cache is quite complex and has many invariants. Read about them below and change the
//! code carefully.
template <class TKey, class TValue, class THash = THash<TKey>>
class TAsyncSlruCacheBase
    : public virtual TRefCounted
{
public:
    using TValuePtr = TIntrusivePtr<TValue>;
    using TValueFuture = TFuture<TValuePtr>;
    using TValuePromise = TPromise<TValuePtr>;

    class TInsertCookie
    {
    public:
        TInsertCookie() = default;
        explicit TInsertCookie(const TKey& key);
        TInsertCookie(TInsertCookie&& other);
        TInsertCookie(const TInsertCookie& other) = delete;
        ~TInsertCookie();

        TInsertCookie& operator = (TInsertCookie&& other);
        TInsertCookie& operator = (const TInsertCookie& other) = delete;

        const TKey& GetKey() const;
        TValueFuture GetValue() const;
        bool IsActive() const;

        void UpdateWeight(i64 newWeight);

        void Cancel(const TError& error);
        void EndInsert(TValuePtr value);

    private:
        friend class TAsyncSlruCacheBase;

        TKey Key_;
        TIntrusivePtr<TAsyncSlruCacheBase> Cache_;
        TValueFuture ValueFuture_;
        std::atomic<bool> Active_ = false;
        bool InsertedIntoSmallGhost_ = false;
        bool InsertedIntoLargeGhost_ = false;

        TInsertCookie(
            const TKey& key,
            TIntrusivePtr<TAsyncSlruCacheBase> cache,
            TValueFuture valueFuture,
            bool active);

        void Abort();
    };

    // NB: Shards store reference to the cache, so the cache cannot be simply copied or moved.
    TAsyncSlruCacheBase(const TAsyncSlruCacheBase&) = delete;
    TAsyncSlruCacheBase(TAsyncSlruCacheBase&&) = delete;
    TAsyncSlruCacheBase& operator=(const TAsyncSlruCacheBase&) = delete;
    TAsyncSlruCacheBase& operator=(TAsyncSlruCacheBase&&) = delete;

    int GetSize() const;
    i64 GetCapacity() const;

    std::vector<TValuePtr> GetAll();

    TValuePtr Find(const TKey& key);
    TValueFuture Lookup(const TKey& key);
    void Touch(const TValuePtr& value);

    TInsertCookie BeginInsert(const TKey& key, i64 cookieWeight = 0);
    void TryRemove(const TKey& key, bool forbidResurrection = false);
    void TryRemoveValue(const TValuePtr& value, bool forbidResurrection = false);

    void UpdateWeight(const TKey& key);
    void UpdateWeight(const TValuePtr& value);

    virtual void Reconfigure(const TSlruCacheDynamicConfigPtr& config);

protected:
    const TSlruCacheConfigPtr Config_;

    explicit TAsyncSlruCacheBase(
        TSlruCacheConfigPtr config,
        const NProfiling::TProfiler& profiler = {});

    // Called once when the value is inserted to the cache.
    // If item weight ever changes, UpdateWeight() should be called to apply the changes.
    virtual i64 GetWeight(const TValuePtr& value) const;

    virtual void OnAdded(const TValuePtr& value);
    virtual void OnRemoved(const TValuePtr& value);

    //! Called on weight updates and on operations with weighted cookies.
    virtual void OnWeightUpdated(i64 weightDelta);

    //! Returns true if resurrection is supported. Note that the function must always returns the same value.
    virtual bool IsResurrectionSupported() const;

protected:
    /*!
     * Every request counts to one of the following metric types:
     *
     * SyncHit* - Item is present in the cache and contains the value.
     *
     * AsyncHit* - Item is present in the cache and contains the value future.
     * Caller should wait till the concurrent request sets the value.
     *
     * Missed* - Item is missing in the cache and should be requested.
     *
     * Hit/Missed counters are updated immediately, while the update of
     * all Weight* metrics can be delayed till the EndInsert call,
     * because we do not know the weight of the object before it arrives.
     */
    struct TCounters
    {
        explicit TCounters(const NProfiling::TProfiler& profiler);

        NProfiling::TCounter SyncHitWeightCounter;
        NProfiling::TCounter AsyncHitWeightCounter;
        NProfiling::TCounter MissedWeightCounter;
        NProfiling::TCounter SyncHitCounter;
        NProfiling::TCounter AsyncHitCounter;
        NProfiling::TCounter MissedCounter;
    };

    //! For testing purposes only.
    const TCounters& GetSmallGhostCounters() const;
    const TCounters& GetLargeGhostCounters() const;

private:
    friend class TAsyncCacheValueBase<TKey, TValue, THash>;

    struct TItem
        : public TIntrusiveListItem<TItem>
    {
        TItem();
        explicit TItem(TValuePtr value);

        TValueFuture GetValueFuture() const;

        TValuePromise ValuePromise;
        TValuePtr Value;
        i64 CachedWeight = 0;
        //! Counter for accurate calculation of AsyncHitWeight.
        //! It can be updated concurrently under the ReadLock.
        std::atomic<int> AsyncHitCount = 0;
        bool Younger = false;
    };

    struct TGhostItem
        : public TIntrusiveListItem<TGhostItem>
    {
        explicit TGhostItem(TKey key)
            : Key(std::move(key))
        { }

        TKey Key;
        //! The value associated with this item. If Inserted == true and Value is null, then we refer to some
        //! old item freed from the memory. If the main cache was bigger, than the item would be present in it.
        //! So, we still need to keep such items in ghost shards.
        TWeakPtr<TValue> Value;
        i64 CachedWeight = 0;
        //! Counter for accurate calculation of AsyncHitWeight.
        //! It can be updated concurrently under the ReadLock.
        std::atomic<int> AsyncHitCount = 0;
        bool Younger = false;
        bool Inserted = false;
    };

    //! Ghost shard does not store any values really. They just simulate the cache with a different capacity and
    //! update the corresponding counters. It is used to estimate what would happen if the cache had different
    //! capacity, thus helping to tweak cache capacity. See YT-15782.
    //!
    //! Ghost shards are lighter than ordinary shards and don't contain ValueMap, only ItemMap. Each value in
    //! the ghost shard is in one of the following states:
    //!
    //! 1. Inserting. The item is present in ItemMap, but not present in the lists. The item in ItemMap has
    //!    Inserted == false. You MUST NOT remove or update the item in this state in any place except CancelInsert()
    //!    or EndInsert(). If CancelInsert() is called, then the state becomes Destroyed. If EndInsert() is called,
    //!    then the state becomes Inserted.
    //! 2. Inserted. The value is present in both ItemMap and lists. The item in ItemMap has Inserted == true.
    //!    Its state can be changed only to Destroyed. The value is stored as weak pointer here, so ghost shards do
    //!    not hold values. If the value is removed, then we just assume that there was some old value with the
    //!    given key, but don't know which one.
    //! 3. Destroyed. The value is not present in ItemMap, and the corresponding item is removed from the lists and
    //!    freed.
    //!
    //! Ghost shards do not support resurrection.
    class TGhostShard
        : private TAsyncSlruCacheListManager<TGhostItem, TGhostShard>
    {
    public:
        using TValuePtr = TIntrusivePtr<TValue>;

        void Find(const TKey& key);
        void Lookup(const TKey& key);
        void Touch(const TValuePtr& value);

        //! If BeginInsert() returns true, then it must be paired with either CancelInsert() or EndInsert()
        //! called with the same key. Do not call CancelInsert() or EndInsert() without matching BeginInsert().
        bool BeginInsert(const TKey& key, i64 cookieWeight);
        void CancelInsert(const TKey& key);
        void EndInsert(const TValuePtr& value, i64 weight);

        //! Inserts the value back to the cache immediately. Called when the value is resurected in the
        //! main cache.
        void Resurrect(const TValuePtr& value, i64 weight);

        //! If value is null, remove by key. Otherwise, remove by value. Note that value.GetKey() == key
        //! must hold in the latter case.
        void TryRemove(const TKey& key, const TValuePtr& value);

        void UpdateWeight(const TKey& key, i64 newWeight);

        void UpdateCookieWeight(const TKey& key, i64 newWeight);

        using TAsyncSlruCacheListManager<TGhostItem, TGhostShard>::SetTouchBufferCapacity;

        void Reconfigure(i64 capacity, double youngerSizeFraction);

        DEFINE_BYVAL_RW_PROPERTY(TCounters*, Counters);

    private:
        friend class TAsyncSlruCacheListManager<TGhostItem, TGhostShard>;

        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock);

        THashMap<TKey, TGhostItem*, THash> ItemMap_;

        bool DoLookup(const TKey& key, bool allowAsyncHits);
        void Trim(NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>& guard);
    };

    //! Cache shard. Each shard is a small cache that can store a subset of keys. It consists of lists (see
    //! TAsyncSlruCacheListManager), ValueMap and ItemMap (see below).
    //!
    //! The values in the shard may be in various states:
    //!
    //! 1. Inserting. The item is present in ItemMap, but isn't added into the lists. ValueMap doesn't contain
    //!    the value corresponding to this item, as there's no such value yet. Value in the item is null. You
    //!    MUST NOT remove or update the item in this state in any place except CancelInsert() or EndInsert().
    //!    If CancelInsert() is called, then the state becomes Destroyed. If EndInsert() is called, then the state
    //!    becomes Inserted.
    //! 2. Inserted. The item is present in ItemMap and in the lists, and the corresponding value is present in
    //!    ValueMap. Value in the item must be non-null. Value->Item_ and Value->Cache_ must be set. The state
    //!    can be only changed to Ready for Resurrection or Destroying.
    //! 3. Ready for Resurrection. The item is freed and removed from ItemMap and the lists. ValueMap holds the
    //!    corresponding value. value->Cache_ must be non-null, and value->Item_ must be null. The state can be
    //!    changed to Inserted if resurrection happens, or Destroying if it didn't happen when refcount of the
    //!    value reached zero.
    //! 4. Destroying. The refcount of the value reached zero. It's not present in ItemMap and the lists, but is
    //!    still present in ValueMap. It's not allowed to return such value into the cache, and its state can be
    //!    only changed to Destroyed. To distinguish between Ready for Resurrection and Destroying, one may use
    //!    DangerousGetPtr() on the pointer from ValueMap. If it returned null, then the state is Destroying.
    //! 5. Destroyed. The value and its corresponding item are freed and are not present anywhere.
    class TShard
        : public TAsyncSlruCacheListManager<TItem, TShard>
    {
    public:
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock);

        //! Holds pointers to values for any given key. They are stored to allow resurrection. When the value
        //! is freed, it will be removed from ValueMap. When the value is in Destroying state, the value will still
        //! reside in ValueMap, you need to be careful with it.
        THashMap<TKey, TValue*, THash> ValueMap;

        //! Holds pointers to items in the lists for any given key.
        THashMap<TKey, TItem*, THash> ItemMap;

        TAsyncSlruCacheBase* Parent;

        TGhostShard SmallGhost;
        TGhostShard LargeGhost;

        //! Trims the lists and releases the guard. Returns the list of evicted items.
        std::vector<TValuePtr> Trim(NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>& guard);

    protected:
        void OnYoungerUpdated(i64 deltaCount, i64 deltaWeight);
        void OnOlderUpdated(i64 deltaCount, i64 deltaWeight);
        void OnCookieUpdated(i64 deltaCount, i64 deltaWeight);

        friend class TAsyncSlruCacheListManager<TItem, TShard>;
    };

    friend class TShard;

    std::unique_ptr<TShard[]> Shards_;

    std::atomic<int> Size_ = 0;
    std::atomic<i64> Capacity_;

    TCounters Counters_;
    TCounters SmallGhostCounters_;
    TCounters LargeGhostCounters_;

    std::atomic<i64> YoungerWeightCounter_ = 0;
    std::atomic<i64> OlderWeightCounter_ = 0;
    std::atomic<i64> YoungerSizeCounter_ = 0;
    std::atomic<i64> OlderSizeCounter_ = 0;

    std::atomic<i64> CookieSizeCounter_ = 0;
    std::atomic<i64> CookieWeightCounter_ = 0;

    std::atomic<bool> GhostCachesEnabled_;

    TShard* GetShardByKey(const TKey& key) const;

    TValueFuture DoLookup(TShard* shard, const TKey& key);

    void DoTryRemove(const TKey& key, const TValuePtr& value, bool forbidResurrection);

    //! Calls OnAdded on OnRemoved for the values evicted with Trim(). If the trim was caused by insertion, then
    //! insertedValue must be the value, insertion of which caused trim. Otherwise, insertedValue must be nullptr.
    //! If the trim was causes by weight update or weighted cookie, then weightDelta represents weight changes.
    void NotifyOnTrim(const std::vector<TValuePtr>& evictedValues, const TValuePtr& insertedValue, i64 weightDelta = 0);

    void UpdateCookieWeight(const TInsertCookie& insertCookie, i64 newWeight);
    void EndInsert(const TInsertCookie& insertCookie, TValuePtr value);
    void CancelInsert(const TInsertCookie& insertCookie, const TError& error);
    void Unregister(const TKey& key);
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = THash<TKey>>
class TMemoryTrackingAsyncSlruCacheBase
    : public TAsyncSlruCacheBase<TKey, TValue, THash>
{
public:
    explicit TMemoryTrackingAsyncSlruCacheBase(
        TSlruCacheConfigPtr config,
        IMemoryUsageTrackerPtr memoryTracker,
        const NProfiling::TProfiler& profiler = {});
    ~TMemoryTrackingAsyncSlruCacheBase();

    void Reconfigure(const TSlruCacheDynamicConfigPtr& config) override;

protected:
    using TValuePtr = typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr;

    void OnAdded(const TValuePtr& value) override;
    void OnRemoved(const TValuePtr& value) override;
    void OnWeightUpdated(i64 weightChanged) override;

private:
    const IMemoryUsageTrackerPtr MemoryTracker_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ASYNC_SLRU_CACHE_INL_H_
#include "async_slru_cache-inl.h"
#undef ASYNC_SLRU_CACHE_INL_H_
