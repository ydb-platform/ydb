#pragma once

#include "public.h"
#include "cache_config.h"

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/memory_usage_tracker.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
class TSyncSlruCacheBase;

template <class TKey, class TValue, class THash = THash<TKey>>
class TSyncCacheValueBase
    : public virtual TRefCounted
{
public:
    const TKey& GetKey() const;

protected:
    explicit TSyncCacheValueBase(const TKey& key);

private:
    const TKey Key_;

};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = THash<TKey>>
class TSyncSlruCacheBase
    : public virtual TRefCounted
{
public:
    using TValuePtr = TIntrusivePtr<TValue>;

    int GetSize() const;
    std::vector<TValuePtr> GetAll() const;
    i64 GetCapacity() const;

    TValuePtr Find(const TKey& key);

    bool TryInsert(const TValuePtr& value, TValuePtr* existingValue = nullptr);
    bool TryRemove(const TKey& key);
    bool TryRemove(const TValuePtr& value);
    void UpdateWeight(const TKey& key);
    void UpdateWeight(const TValuePtr& value);
    void Clear();

    virtual void Reconfigure(const TSlruCacheDynamicConfigPtr& config);

protected:
    const TSlruCacheConfigPtr Config_;

    std::atomic<i64> Capacity_;
    std::atomic<double> YoungerSizeFraction_;

    explicit TSyncSlruCacheBase(
        TSlruCacheConfigPtr config,
        const NProfiling::TProfiler& profiler = {});

    virtual i64 GetWeight(const TValuePtr& value) const;
    virtual void OnAdded(const TValuePtr& value);
    virtual void OnRemoved(const TValuePtr& value);
    virtual void OnWeightUpdated(i64 weightDelta);

private:
    struct TItem
        : public TIntrusiveListItem<TItem>
    {
        explicit TItem(TValuePtr value);

        TValuePtr Value;
        i64 CachedWeight;
        bool Younger;
    };

    struct TShard
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock);

        TIntrusiveListWithAutoDelete<TItem, TDelete> YoungerLruList;
        TIntrusiveListWithAutoDelete<TItem, TDelete> OlderLruList;

        i64 YoungerWeightCounter = 0;
        i64 OlderWeightCounter = 0;

        THashMap<TKey, TItem*, THash> ItemMap;

        std::vector<TItem*> TouchBuffer;
        std::atomic<int> TouchBufferPosition = {0};
    };

    std::unique_ptr<TShard[]> Shards_;

    std::atomic<int> Size_ = 0;

    NProfiling::TCounter HitWeightCounter_;
    NProfiling::TCounter MissedWeightCounter_;
    NProfiling::TCounter DroppedWeightCounter_;
    std::atomic<i64> YoungerWeightCounter_ = 0;
    std::atomic<i64> OlderWeightCounter_ = 0;

    TShard* GetShardByKey(const TKey& key) const;

    bool Touch(TShard* shard, TItem* item);
    void DrainTouchBuffer(TShard* shard);

    void Trim(TShard* shard, NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>& guard);

    void PushToYounger(TShard* shard, TItem* item);
    void MoveToYounger(TShard* shard, TItem* item);
    void MoveToOlder(TShard* shard, TItem* item);
    void Pop(TShard* shard, TItem* item);
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = THash<TKey>>
class TMemoryTrackingSyncSlruCacheBase
    : public TSyncSlruCacheBase<TKey, TValue, THash>
{
public:
    TMemoryTrackingSyncSlruCacheBase(
        TSlruCacheConfigPtr config,
        IMemoryUsageTrackerPtr memoryTracker,
        const NProfiling::TProfiler& profiler = {});
    ~TMemoryTrackingSyncSlruCacheBase();

    void Reconfigure(const TSlruCacheDynamicConfigPtr& config) override;

protected:
    using TValuePtr = typename TSyncSlruCacheBase<TKey, TValue, THash>::TValuePtr;

    void OnAdded(const TValuePtr& value) override;
    void OnRemoved(const TValuePtr& value) override;
    void OnWeightUpdated(i64 weightChanged) override;

private:
    const IMemoryUsageTrackerPtr MemoryTracker_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = THash<TKey>>
class TSimpleLruCache
{
public:
    explicit TSimpleLruCache(i64 maxWeight);

    int GetSize() const;

    const TValue& Get(const TKey& key);
    TValue* Find(const TKey& key);
    TValue* FindNoTouch(const TKey& key);
    TValue* Insert(const TKey& key, TValue value, i64 weight = 1);

    void SetMaxWeight(i64 maxWeight);

    void Clear();

private:
    struct TItem
    {
        TItem(TValue value, i64 weight);

        TValue Value;
        const i64 Weight;
        typename std::list<typename THashMap<TKey, TItem, THash>::iterator>::iterator LruListIterator;
    };

    i64 MaxWeight_ = 0;
    i64 CurrentWeight_ = 0;

    using TItemMap = THashMap<TKey, TItem, THash>;
    TItemMap ItemMap_;
    mutable std::list<typename TItemMap::iterator> LruList_;

    void Pop();
    void UpdateLruList(typename TItemMap::iterator);
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash = THash<TKey>>
class TMultiLruCache
{
public:
    explicit TMultiLruCache(i64 maxWeight);

    int GetSize() const;

    const TValue& Get(const TKey& key);
    TValue* Find(const TKey& key);

    TValue* Insert(const TKey& key, TValue value, i64 weight = 1);
    std::optional<TValue> TryExtract(const TKey& key);
    TValue Pop();

    void Clear();

private:
    struct TItem;

    using TItemMap = THashMap<TKey, std::deque<TItem>, THash>;
    using TLruList = typename std::list<typename TItemMap::iterator>;

    struct TItem
    {
        TItem(TValue value, i64 weight);

        TValue Value;
        const i64 Weight;

        typename TLruList::iterator LruListIterator;
    };

    void UpdateLruList(typename std::deque<TItem>::iterator listIt);

private:
    i64 MaxWeight_ = 0;
    i64 CurrentWeight_ = 0;

    TItemMap ItemMap_;
    TLruList LruList_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SYNC_CACHE_INL_H_
#include "sync_cache-inl.h"
#undef SYNC_CACHE_INL_H_
