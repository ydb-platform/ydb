#ifndef ASYNC_SLRU_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include async_slru_cache.h"
// For the sake of sane code completion.
#include "async_slru_cache.h"
#endif

#include <util/system/yield.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TItem::TItem()
    : ValuePromise(NewPromise<TValuePtr>())
{ }

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TItem::TItem(TValuePtr value)
    : ValuePromise(MakePromise(value))
    , Value(std::move(value))
{ }

template <class TKey, class TValue, class THash>
auto TAsyncSlruCacheBase<TKey, TValue, THash>::TItem::GetValueFuture() const -> TValueFuture
{
    return ValuePromise
        .ToFuture()
        .ToUncancelable();
}

////////////////////////////////////////////////////////////////////////////////

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::PushToYounger(TItem* item, i64 weight)
{
    YT_ASSERT(item->Empty());
    YoungerLruList.PushFront(item);
    item->CachedWeight = weight;
    YoungerWeightCounter += weight;
    AsDerived()->OnYoungerUpdated(1, weight);
    item->Younger = true;
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::MoveToYounger(TItem* item)
{
    YT_ASSERT(!item->Empty());
    item->Unlink();
    YoungerLruList.PushFront(item);
    if (!item->Younger) {
        i64 weight = item->CachedWeight;
        OlderWeightCounter -= weight;
        AsDerived()->OnOlderUpdated(-1, -weight);
        YoungerWeightCounter += weight;
        AsDerived()->OnYoungerUpdated(1, weight);
        item->Younger = true;
    }
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::MoveToOlder(TItem* item)
{
    YT_ASSERT(!item->Empty());
    item->Unlink();
    OlderLruList.PushFront(item);
    if (item->Younger) {
        i64 weight = item->CachedWeight;
        YoungerWeightCounter -= weight;
        AsDerived()->OnYoungerUpdated(-1, -weight);
        OlderWeightCounter += weight;
        AsDerived()->OnOlderUpdated(1, weight);
        item->Younger = false;
    }
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::PopFromLists(TItem* item)
{
    if (item->Empty()) {
        return;
    }

    YT_VERIFY(TouchBufferPosition.load() == 0);

    i64 weight = item->CachedWeight;
    if (item->Younger) {
        YoungerWeightCounter -= weight;
        AsDerived()->OnYoungerUpdated(-1, -weight);
    } else {
        OlderWeightCounter -= weight;
        AsDerived()->OnOlderUpdated(-1, -weight);
    }
    item->Unlink();
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::UpdateWeight(TItem* item, i64 weightDelta)
{
    YT_VERIFY(!item->Empty());
    if (item->Younger) {
        YoungerWeightCounter += weightDelta;
        AsDerived()->OnYoungerUpdated(0, weightDelta);
    } else {
        OlderWeightCounter += weightDelta;
        AsDerived()->OnOlderUpdated(0, weightDelta);
    }
    item->CachedWeight += weightDelta;
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::UpdateCookie(TItem* item, i64 countDelta, i64 weightDelta)
{
    YT_VERIFY(item->Empty());
    CookieWeightCounter += weightDelta;
    item->CachedWeight += weightDelta;
    AsDerived()->OnCookieUpdated(countDelta, weightDelta);
}

template <class TItem, class TDerived>
TIntrusiveListWithAutoDelete<TItem, TDelete> TAsyncSlruCacheListManager<TItem, TDerived>::TrimNoDelete()
{
    // Move from older to younger.
    auto capacity = Capacity.load();
    auto youngerSizeFraction = YoungerSizeFraction.load();
    while (!OlderLruList.Empty() && OlderWeightCounter > capacity * (1 - youngerSizeFraction)) {
        auto* item = &*(--OlderLruList.End());
        MoveToYounger(item);
    }

    // Evict from younger.
    TIntrusiveListWithAutoDelete<TItem, TDelete> evictedItems;
    while (!YoungerLruList.Empty() && static_cast<i64>(YoungerWeightCounter + OlderWeightCounter + CookieWeightCounter) > capacity) {
        auto* item = &*(--YoungerLruList.End());
        PopFromLists(item);
        evictedItems.PushBack(item);
    }

    return evictedItems;
}

template <class TItem, class TDerived>
bool TAsyncSlruCacheListManager<TItem, TDerived>::TouchItem(TItem* item)
{
    if (item->Empty()) {
        return false;
    }

    int capacity = std::ssize(TouchBuffer);
    int index = TouchBufferPosition++;
    if (index >= capacity) {
        // Drop touch request due to buffer overflow.
        // NB: We still return false since the other thread is already responsible for
        // draining the buffer.
        return false;
    }

    TouchBuffer[index] = item;
    return index == capacity - 1;
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::DrainTouchBuffer()
{
    int count = std::min<int>(TouchBufferPosition.load(), std::ssize(TouchBuffer));
    for (int index = 0; index < count; ++index) {
        MoveToOlder(TouchBuffer[index]);
    }
    TouchBufferPosition = 0;
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::Reconfigure(i64 capacity, double youngerSizeFraction)
{
    Capacity = capacity;
    YoungerSizeFraction = youngerSizeFraction;
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::SetTouchBufferCapacity(i64 touchBufferCapacity)
{
    TouchBuffer.resize(touchBufferCapacity);
}

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::OnYoungerUpdated(i64 /*deltaCount*/, i64 /*deltaWeight*/)
{ }

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::OnOlderUpdated(i64 /*deltaCount*/, i64 /*deltaWeight*/)
{ }

template <class TItem, class TDerived>
void TAsyncSlruCacheListManager<TItem, TDerived>::OnCookieUpdated(i64 /*deltaCount*/, i64 /*deltaWeight*/)
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
const TKey& TAsyncCacheValueBase<TKey, TValue, THash>::GetKey() const
{
    return Key_;
}

template <class TKey, class TValue, class THash>
void TAsyncCacheValueBase<TKey, TValue, THash>::UpdateWeight() const
{
    if (auto cache = Cache_.Lock()) {
        cache->UpdateWeight(GetKey());
    }
}

template <class TKey, class TValue, class THash>
TAsyncCacheValueBase<TKey, TValue, THash>::TAsyncCacheValueBase(const TKey& key)
    : Key_(key)
{ }

template <class TKey, class TValue, class THash>
NYT::TAsyncCacheValueBase<TKey, TValue, THash>::~TAsyncCacheValueBase()
{
    if (auto cache = Cache_.Lock()) {
        cache->Unregister(Key_);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TCounters::TCounters(
    const NProfiling::TProfiler& profiler)
{
    MissedWeightCounter = profiler.Counter("/missed_weight");
    MissedCounter = profiler.Counter("/missed_count");

    auto profilerWithSyncTag = profiler.WithTag("hit_type", "sync");
    SyncHitWeightCounter = profilerWithSyncTag.Counter("/hit_weight");
    SyncHitCounter = profilerWithSyncTag.Counter("/hit_count");

    auto profilerWithAsyncTag = profiler.WithTag("hit_type", "async");
    AsyncHitWeightCounter = profilerWithAsyncTag.Counter("/hit_weight");
    AsyncHitCounter = profilerWithAsyncTag.Counter("/hit_count");
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TAsyncSlruCacheBase(
    TSlruCacheConfigPtr config,
    const NProfiling::TProfiler& profiler)
    : Config_(std::move(config))
    , Capacity_(Config_->Capacity)
    , Counters_(profiler)
    , SmallGhostCounters_(profiler.WithPrefix("/small_ghost_cache"))
    , LargeGhostCounters_(profiler.WithPrefix("/large_ghost_cache"))
{
    static_assert(
        std::derived_from<TValue, TAsyncCacheValueBase<TKey, TValue, THash>>,
        "TValue must be derived from TAsyncCacheValueBase");

    auto youngerSegmentProfiler = profiler.WithTag("segment", "younger");
    auto olderSegmentProfiler = profiler.WithTag("segment", "older");

    youngerSegmentProfiler.AddFuncGauge("/weight", MakeStrong(this), [this] {
        return YoungerWeightCounter_.load();
    });
    olderSegmentProfiler.AddFuncGauge("/weight", MakeStrong(this), [this] {
        return OlderWeightCounter_.load();
    });
    youngerSegmentProfiler.AddFuncGauge("/size", MakeStrong(this), [this] {
        return YoungerSizeCounter_.load();
    });
    olderSegmentProfiler.AddFuncGauge("/size", MakeStrong(this), [this] {
        return OlderSizeCounter_.load();
    });

    auto cookieSegmentProfiler = profiler.WithTag("segment", "cookie");
    cookieSegmentProfiler.AddFuncGauge("/size", MakeStrong(this), [this] {
        return CookieSizeCounter_.load();
    });
    cookieSegmentProfiler.AddFuncGauge("/weight", MakeStrong(this), [this] {
        return CookieWeightCounter_.load();
    });

    GhostCachesEnabled_.store(Config_->EnableGhostCaches);

    YT_VERIFY(IsPowerOf2(Config_->ShardCount));
    Shards_.reset(new TShard[Config_->ShardCount]);

    i64 shardCapacity = std::max<i64>(1, Config_->Capacity / Config_->ShardCount);
    i64 touchBufferCapacity = Config_->TouchBufferCapacity / Config_->ShardCount;
    for (int index = 0; index < Config_->ShardCount; ++index) {
        auto& shard = Shards_[index];

        shard.SetTouchBufferCapacity(touchBufferCapacity);
        shard.Reconfigure(shardCapacity, Config_->YoungerSizeFraction);

        if (GhostCachesEnabled_.load()) {
            shard.SmallGhost.SetCounters(&SmallGhostCounters_);
            shard.LargeGhost.SetCounters(&LargeGhostCounters_);

            shard.SmallGhost.SetTouchBufferCapacity(touchBufferCapacity);
            shard.LargeGhost.SetTouchBufferCapacity(touchBufferCapacity);

            shard.SmallGhost.Reconfigure(
                static_cast<i64>(shardCapacity * Config_->SmallGhostCacheRatio),
                Config_->YoungerSizeFraction);
            shard.LargeGhost.Reconfigure(
                static_cast<i64>(shardCapacity * Config_->LargeGhostCacheRatio),
                Config_->YoungerSizeFraction);
        }

        shard.Parent = this;
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(const TSlruCacheDynamicConfigPtr& config)
{
    i64 capacity = config->Capacity.value_or(Config_->Capacity);
    i64 shardCapacity = std::max<i64>(1, capacity / Config_->ShardCount);
    double youngerSizeFraction = config->YoungerSizeFraction.value_or(Config_->YoungerSizeFraction);

    Capacity_.store(capacity);

    if (!config->EnableGhostCaches) {
        GhostCachesEnabled_.store(false);
    }

    for (int shardIndex = 0; shardIndex < Config_->ShardCount; ++shardIndex) {
        auto& shard = Shards_[shardIndex];

        if (GhostCachesEnabled_.load()) {
            shard.SmallGhost.Reconfigure(
                static_cast<i64>(shardCapacity * Config_->SmallGhostCacheRatio),
                youngerSizeFraction);
            shard.LargeGhost.Reconfigure(
                static_cast<i64>(shardCapacity * Config_->LargeGhostCacheRatio),
                youngerSizeFraction);
        }

        auto writerGuard = WriterGuard(shard.SpinLock);
        shard.Reconfigure(shardCapacity, youngerSizeFraction);
        shard.DrainTouchBuffer();
        NotifyOnTrim(shard.Trim(writerGuard), nullptr);
    }
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr
TAsyncSlruCacheBase<TKey, TValue, THash>::Find(const TKey& key)
{
    auto* shard = GetShardByKey(key);

    if (GhostCachesEnabled_.load()) {
        shard->SmallGhost.Find(key);
        shard->LargeGhost.Find(key);
    }

    auto readerGuard = ReaderGuard(shard->SpinLock);

    auto itemIt = shard->ItemMap.find(key);
    if (itemIt == shard->ItemMap.end()) {
        Counters_.MissedCounter.Increment();
        return nullptr;
    }

    auto* item = itemIt->second;
    auto value = item->Value;
    if (!value) {
        Counters_.MissedCounter.Increment();
        return nullptr;
    }

    bool needToDrain = shard->TouchItem(item);

    Counters_.SyncHitWeightCounter.Increment(item->CachedWeight);
    Counters_.SyncHitCounter.Increment();

    readerGuard.Release();

    if (needToDrain) {
        auto writerGuard = WriterGuard(shard->SpinLock);
        shard->DrainTouchBuffer();
    }

    return value;
}

template <class TKey, class TValue, class THash>
int TAsyncSlruCacheBase<TKey, TValue, THash>::GetSize() const
{
    return Size_.load();
}

template <class TKey, class TValue, class THash>
i64 TAsyncSlruCacheBase<TKey, TValue, THash>::GetCapacity() const
{
    return Capacity_.load();
}

template <class TKey, class TValue, class THash>
std::vector<typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr>
TAsyncSlruCacheBase<TKey, TValue, THash>::GetAll()
{
    std::vector<TValuePtr> result;
    result.reserve(GetSize());

    for (int shardIndex = 0; shardIndex < Config_->ShardCount; ++shardIndex) {
        const auto& shard = Shards_[shardIndex];

        auto readerGuard = ReaderGuard(shard.SpinLock);
        for (const auto& [key, rawValue] : shard.ValueMap) {
            if (auto value = DangerousGetPtr<TValue>(rawValue)) {
                result.push_back(value);
            }
        }
    }
    return result;
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValueFuture
TAsyncSlruCacheBase<TKey, TValue, THash>::Lookup(const TKey& key)
{
    auto* shard = GetShardByKey(key);

    if (GhostCachesEnabled_.load()) {
        shard->SmallGhost.Lookup(key);
        shard->LargeGhost.Lookup(key);
    }

    auto valueFuture = DoLookup(shard, key);
    if (!valueFuture) {
        Counters_.MissedCounter.Increment();
    }
    return valueFuture;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Touch(const TValuePtr& value)
{
    auto* shard = GetShardByKey(value->GetKey());

    if (GhostCachesEnabled_.load()) {
        shard->SmallGhost.Touch(value);
        shard->LargeGhost.Touch(value);
    }

    auto readerGuard = ReaderGuard(shard->SpinLock);

    if (value->Cache_.Lock() != this || !value->Item_) {
        return;
    }

    auto needToDrain = shard->TouchItem(value->Item_);

    readerGuard.Release();

    if (needToDrain) {
        auto writerGuard = WriterGuard(shard->SpinLock);
        shard->DrainTouchBuffer();
    }
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValueFuture
TAsyncSlruCacheBase<TKey, TValue, THash>::DoLookup(TShard* shard, const TKey& key)
{
    auto readerGuard = ReaderGuard(shard->SpinLock);

    auto& itemMap = shard->ItemMap;
    const auto& valueMap = shard->ValueMap;

    if (auto itemIt = itemMap.find(key); itemIt != itemMap.end()) {
        auto* item = itemIt->second;
        bool needToDrain = shard->TouchItem(item);
        auto valueFuture = item->GetValueFuture();

        if (item->Value) {
            Counters_.SyncHitWeightCounter.Increment(item->CachedWeight);
            Counters_.SyncHitCounter.Increment();
        } else {
            Counters_.AsyncHitCounter.Increment();
            item->AsyncHitCount.fetch_add(1);
        }

        readerGuard.Release();

        if (needToDrain) {
            auto writerGuard = WriterGuard(shard->SpinLock);
            shard->DrainTouchBuffer();
        }

        return valueFuture;
    }

    auto valueIt = valueMap.find(key);
    if (valueIt == valueMap.end()) {
        return {};
    }

    auto value = DangerousGetPtr(valueIt->second);
    if (!value) {
        return {};
    }

    readerGuard.Release();

    auto writerGuard = WriterGuard(shard->SpinLock);

    if (auto itemIt = itemMap.find(key); itemIt != itemMap.end()) {
        auto* item = itemIt->second;

        shard->TouchItem(item);
        auto valueFuture = item->GetValueFuture();

        if (item->Value) {
            Counters_.SyncHitWeightCounter.Increment(item->CachedWeight);
            Counters_.SyncHitCounter.Increment();
        } else {
            Counters_.AsyncHitCounter.Increment();
            item->AsyncHitCount.fetch_add(1);
        }

        shard->DrainTouchBuffer();

        return valueFuture;
    }

    shard->DrainTouchBuffer();

    {
        auto* item = new TItem(value);
        value->Item_ = item;

        auto valueFuture = item->GetValueFuture();

        YT_VERIFY(itemMap.emplace(key, item).second);
        ++Size_;

        i64 weight = GetWeight(item->Value);
        shard->PushToYounger(item, weight);
        Counters_.SyncHitWeightCounter.Increment(weight);
        Counters_.SyncHitCounter.Increment();

        // NB: Releases the lock.
        NotifyOnTrim(shard->Trim(writerGuard), value);

        if (GhostCachesEnabled_.load()) {
            shard->SmallGhost.Resurrect(value, weight);
            shard->LargeGhost.Resurrect(value, weight);
        }

        return valueFuture;
    }
}

template <class TKey, class TValue, class THash>
auto TAsyncSlruCacheBase<TKey, TValue, THash>::BeginInsert(const TKey& key, i64 cookieWeight) -> TInsertCookie
{
    auto* shard = GetShardByKey(key);

    if (auto valueFuture = DoLookup(shard, key)) {
        if (GhostCachesEnabled_.load()) {
            if (valueFuture.IsSet() && valueFuture.Get().IsOK()) {
                bool smallInserted = shard->SmallGhost.BeginInsert(key, cookieWeight);
                bool largeInserted = shard->LargeGhost.BeginInsert(key, cookieWeight);
                if (smallInserted || largeInserted) {
                    const auto& value = valueFuture.Get().Value();
                    i64 weight = GetWeight(value);
                    if (smallInserted) {
                        shard->SmallGhost.EndInsert(value, weight);
                    }
                    if (largeInserted) {
                        shard->LargeGhost.EndInsert(value, weight);
                    }
                }
            } else {
                shard->SmallGhost.Lookup(key);
                shard->LargeGhost.Lookup(key);
            }
        }

        return TInsertCookie(
            key,
            nullptr,
            std::move(valueFuture),
            false);
    }

    while (true) {
        auto guard = WriterGuard(shard->SpinLock);

        shard->DrainTouchBuffer();

        auto& itemMap = shard->ItemMap;
        auto& valueMap = shard->ValueMap;

        auto itemIt = itemMap.find(key);
        if (itemIt != itemMap.end()) {
            auto* item = itemIt->second;
            shard->TouchItem(item);
            auto valueFuture = item->GetValueFuture();

            if (item->Value) {
                Counters_.SyncHitWeightCounter.Increment(item->CachedWeight);
                Counters_.SyncHitCounter.Increment();
            } else {
                Counters_.AsyncHitCounter.Increment();
                item->AsyncHitCount.fetch_add(1);
            }

            auto value = item->Value;
            i64 weight = item->CachedWeight;

            guard.Release();

            if (GhostCachesEnabled_.load()) {
                if (value) {
                    if (shard->SmallGhost.BeginInsert(key, cookieWeight)) {
                        shard->SmallGhost.EndInsert(value, weight);
                    }
                    if (shard->LargeGhost.BeginInsert(key, cookieWeight)) {
                        shard->LargeGhost.EndInsert(value, weight);
                    }
                } else {
                    shard->SmallGhost.Lookup(key);
                    shard->LargeGhost.Lookup(key);
                }
            }

            return TInsertCookie(
                key,
                nullptr,
                std::move(valueFuture),
                false);
        }

        auto valueIt = valueMap.find(key);
        if (valueIt == valueMap.end()) {
            auto* item = new TItem();
            auto valueFuture = item->GetValueFuture();

            YT_VERIFY(itemMap.emplace(key, item).second);
            ++Size_;

            Counters_.MissedCounter.Increment();

            shard->UpdateCookie(item, /*countDelta*/ 1, cookieWeight);
            if (cookieWeight > 0) {
                // NB: Releases the lock.
                NotifyOnTrim(shard->Trim(guard), nullptr, cookieWeight);
            }

            guard.Release();

            auto insertCookie = TInsertCookie(
                key,
                this,
                std::move(valueFuture),
                true);

            if (GhostCachesEnabled_.load()) {
                insertCookie.InsertedIntoSmallGhost_ = shard->SmallGhost.BeginInsert(key, cookieWeight);
                insertCookie.InsertedIntoLargeGhost_ = shard->LargeGhost.BeginInsert(key, cookieWeight);
            }

            return insertCookie;
        }

        if (auto value = DangerousGetPtr(valueIt->second)) {
            auto* item = new TItem(value);
            value->Item_ = item;

            YT_VERIFY(itemMap.emplace(key, item).second);
            ++Size_;

            i64 weight = GetWeight(item->Value);
            shard->PushToYounger(item, weight);
            Counters_.SyncHitWeightCounter.Increment(weight);
            Counters_.SyncHitCounter.Increment();

            // NB: Releases the lock.
            NotifyOnTrim(shard->Trim(guard), value);

            guard.Release();

            if (GhostCachesEnabled_.load()) {
                shard->SmallGhost.Resurrect(value, weight);
                shard->LargeGhost.Resurrect(value, weight);
            }

            return TInsertCookie(
                key,
                nullptr,
                MakeFuture(value),
                false);
        }

        // Back off.
        // Hopefully the object we had just extracted will be destroyed soon
        // and thus vanish from ValueMap.
        guard.Release();
        ThreadYield();
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::UpdateCookieWeight(const TInsertCookie& insertCookie, i64 newWeight)
{
    YT_VERIFY(newWeight >= 0);
    auto key = insertCookie.GetKey();

    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    auto itemIt = shard->ItemMap.find(key);
    if (itemIt == shard->ItemMap.end()) {
        // Insert cookie canceled.
        YT_VERIFY(!insertCookie.IsActive());
        return;
    }
    auto* item = itemIt->second;

    i64 weightDelta = newWeight - item->CachedWeight;
    shard->UpdateCookie(item, /*countDelta*/ 0, weightDelta);
    if (weightDelta > 0) {
        shard->DrainTouchBuffer();

        // NB: Releases the lock.
        NotifyOnTrim(shard->Trim(guard), nullptr, weightDelta);
    } else {
        guard.Release();
        OnWeightUpdated(weightDelta);
    }

    if (insertCookie.InsertedIntoSmallGhost_) {
        shard->SmallGhost.UpdateCookieWeight(key, newWeight);
    }
    if (insertCookie.InsertedIntoLargeGhost_) {
        shard->LargeGhost.UpdateCookieWeight(key, newWeight);
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::EndInsert(const TInsertCookie& insertCookie, TValuePtr value)
{
    YT_VERIFY(value);
    auto key = value->GetKey();

    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    shard->DrainTouchBuffer();

    value->Cache_ = MakeWeak(this);

    auto* item = GetOrCrash(shard->ItemMap, key);
    item->Value = value;
    value->Item_ = item;
    auto promise = item->ValuePromise;

    YT_VERIFY(shard->ValueMap.emplace(key, value.Get()).second);

    auto cookieWeight = item->CachedWeight;
    shard->UpdateCookie(item, /*countDelta*/ -1, -cookieWeight);

    i64 weight = GetWeight(item->Value);
    shard->PushToYounger(item, weight);
    // MissedCounter and AsyncHitCounter have already been incremented in BeginInsert.
    Counters_.MissedWeightCounter.Increment(weight);
    Counters_.AsyncHitWeightCounter.Increment(weight * item->AsyncHitCount.load());

    // NB: Releases the lock.
    NotifyOnTrim(shard->Trim(guard), value, -cookieWeight);

    // We do not want to break the ghost cache invariants, according to which either EndInsert
    // or CancelInsert must be called for each item in Inserting state. So we end the insertion
    // even after ghost cache is disabled.
    if (insertCookie.InsertedIntoSmallGhost_) {
        shard->SmallGhost.EndInsert(value, weight);
    }
    if (insertCookie.InsertedIntoLargeGhost_) {
        shard->LargeGhost.EndInsert(value, weight);
    }

    promise.Set(value);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::CancelInsert(const TInsertCookie& insertCookie, const TError& error)
{
    const auto& key = insertCookie.Key_;
    auto* shard = GetShardByKey(key);

    // We do not want to break the ghost cache invariants, according to which either EndInsert
    // or CancelInsert must be called for each item in Inserting state. So we end the insertion
    // even after ghost cache is disabled.
    if (insertCookie.InsertedIntoSmallGhost_) {
        shard->SmallGhost.CancelInsert(key);
    }
    if (insertCookie.InsertedIntoLargeGhost_) {
        shard->LargeGhost.CancelInsert(key);
    }

    auto guard = WriterGuard(shard->SpinLock);

    shard->DrainTouchBuffer();

    auto& itemMap = shard->ItemMap;
    auto itemIt = itemMap.find(key);
    YT_VERIFY(itemIt != itemMap.end());

    auto* item = itemIt->second;
    auto promise = item->ValuePromise;

    itemMap.erase(itemIt);
    --Size_;

    YT_VERIFY(!item->Value);

    auto cookieWeight = item->CachedWeight;
    shard->UpdateCookie(item, /*countDelta*/ -1, -cookieWeight);

    delete item;

    guard.Release();

    if (cookieWeight > 0) {
        OnWeightUpdated(-cookieWeight);
    }

    promise.Set(error);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::Unregister(const TKey& key)
{
    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    shard->DrainTouchBuffer();

    YT_VERIFY(shard->ItemMap.find(key) == shard->ItemMap.end());
    YT_VERIFY(shard->ValueMap.erase(key) == 1);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TryRemove(const TKey& key, bool forbidResurrection)
{
    DoTryRemove(key, nullptr, forbidResurrection);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TryRemoveValue(const TValuePtr& value, bool forbidResurrection)
{
    DoTryRemove(value->GetKey(), value, forbidResurrection);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::DoTryRemove(
    const TKey& key,
    const TValuePtr& value,
    bool forbidResurrection)
{
    auto* shard = GetShardByKey(key);

    if (GhostCachesEnabled_.load()) {
        shard->SmallGhost.TryRemove(key, value);
        shard->LargeGhost.TryRemove(key, value);
    }

    auto guard = WriterGuard(shard->SpinLock);

    shard->DrainTouchBuffer();

    auto& itemMap = shard->ItemMap;
    auto& valueMap = shard->ValueMap;

    auto valueIt = valueMap.find(key);
    if (valueIt == valueMap.end()) {
        return;
    }

    if (value && valueIt->second != value) {
        return;
    }

    if (forbidResurrection || !IsResurrectionSupported()) {
        valueIt->second->Cache_.Reset();
        valueMap.erase(valueIt);
    }

    auto itemIt = itemMap.find(key);
    if (itemIt == itemMap.end()) {
        return;
    }

    auto* item = itemIt->second;
    auto actualValue = item->Value;
    if (!actualValue) {
        return;
    }

    itemMap.erase(itemIt);
    --Size_;

    shard->PopFromLists(item);

    YT_VERIFY(actualValue->Item_ == item);
    actualValue->Item_ = nullptr;

    delete item;

    guard.Release();

    OnRemoved(actualValue);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::UpdateWeight(const TKey& key)
{
    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    shard->DrainTouchBuffer();

    auto itemIt = shard->ItemMap.find(key);
    if (itemIt == shard->ItemMap.end()) {
        return;
    }

    auto item = itemIt->second;
    if (!item->Value) {
        return;
    }

    i64 newWeight = GetWeight(item->Value);
    i64 weightDelta = newWeight - item->CachedWeight;

    shard->UpdateWeight(item, weightDelta);

    // If item weight increases, it means that some parts of the item were missing in cache,
    // so add delta to missed weight.
    if (weightDelta > 0) {
        Counters_.MissedWeightCounter.Increment(weightDelta);
    }

    NotifyOnTrim(shard->Trim(guard), nullptr, weightDelta);

    if (GhostCachesEnabled_.load()) {
        shard->SmallGhost.UpdateWeight(key, newWeight);
        shard->LargeGhost.UpdateWeight(key, newWeight);
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::UpdateWeight(const TValuePtr& value)
{
    UpdateWeight(value->GetKey());
}

template <class TKey, class TValue, class THash>
auto TAsyncSlruCacheBase<TKey, TValue, THash>::GetShardByKey(const TKey& key) const -> TShard*
{
    return &Shards_[THash()(key) & (Config_->ShardCount - 1)];
}

template <class TKey, class TValue, class THash>
i64 TAsyncSlruCacheBase<TKey, TValue, THash>::GetWeight(const TValuePtr& /*value*/) const
{
    return 1;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::OnAdded(const TValuePtr& /*value*/)
{ }

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::OnRemoved(const TValuePtr& /*value*/)
{ }

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::OnWeightUpdated(i64 /*weightDelta*/)
{ }

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::IsResurrectionSupported() const
{
    return true;
}

template <class TKey, class TValue, class THash>
auto TAsyncSlruCacheBase<TKey, TValue, THash>::GetSmallGhostCounters() const -> const TCounters&
{
    return SmallGhostCounters_;
}

template <class TKey, class TValue, class THash>
auto TAsyncSlruCacheBase<TKey, TValue, THash>::GetLargeGhostCounters() const -> const TCounters&
{
    return LargeGhostCounters_;
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::DoLookup(const TKey& key, bool allowAsyncHits)
{
    auto readerGuard = ReaderGuard(SpinLock);

    auto itemIt = ItemMap_.find(key);
    if (itemIt == ItemMap_.end()) {
        return false;
    }

    auto* item = itemIt->second;
    if (!allowAsyncHits && !item->Inserted) {
        return false;
    }

    bool needToDrain = this->TouchItem(item);

    if (item->Inserted) {
        Counters_->SyncHitWeightCounter.Increment(item->CachedWeight);
        Counters_->SyncHitCounter.Increment();
    } else {
        Counters_->AsyncHitCounter.Increment();
        item->AsyncHitCount.fetch_add(1);
    }

    readerGuard.Release();

    if (needToDrain) {
        auto writerGuard = WriterGuard(SpinLock);
        this->DrainTouchBuffer();
    }

    return true;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::Find(const TKey& key)
{
    if (!DoLookup(key, false)) {
        Counters_->MissedCounter.Increment();
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::Lookup(const TKey& key)
{
    if (!DoLookup(key, true)) {
        Counters_->MissedCounter.Increment();
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::Touch(const TValuePtr& value)
{
    auto readerGuard = ReaderGuard(SpinLock);

    if (!value) {
        return;
    }

    auto itemIt = ItemMap_.find(value->GetKey());
    if (itemIt == ItemMap_.end() || itemIt->second->Value.Lock() != value) {
        return;
    }
    auto* item = itemIt->second;

    auto needToDrain = this->TouchItem(item);

    readerGuard.Release();

    if (needToDrain) {
        auto writerGuard = WriterGuard(SpinLock);
        this->DrainTouchBuffer();
    }
}

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::BeginInsert(const TKey& key, i64 cookieWeight)
{
    if (DoLookup(key, true)) {
        return false;
    }

    auto guard = WriterGuard(SpinLock);

    this->DrainTouchBuffer();

    auto itemIt = ItemMap_.find(key);
    if (itemIt != ItemMap_.end()) {
        auto* item = itemIt->second;
        this->TouchItem(item);

        if (item->Inserted) {
            Counters_->SyncHitWeightCounter.Increment(item->CachedWeight);
            Counters_->SyncHitCounter.Increment();
        } else {
            Counters_->AsyncHitCounter.Increment();
            item->AsyncHitCount.fetch_add(1);
        }

        return false;
    }

    auto* item = new TGhostItem(key);
    Counters_->MissedCounter.Increment();
    YT_VERIFY(ItemMap_.emplace(key, item).second);

    this->UpdateCookie(item, /*countDelta*/ 1, cookieWeight);
    if (cookieWeight > 0) {
        // NB: Releases the lock.
        Trim(guard);
    }

    return true;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::CancelInsert(const TKey& key)
{
    auto guard = WriterGuard(SpinLock);

    this->DrainTouchBuffer();

    auto itemIt = ItemMap_.find(key);
    YT_VERIFY(itemIt != ItemMap_.end());

    auto* item = itemIt->second;
    YT_VERIFY(!item->Inserted);

    ItemMap_.erase(itemIt);

    this->UpdateCookie(item, /*countDelta*/ -1, -item->CachedWeight);

    delete item;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::EndInsert(const TValuePtr& value, i64 weight)
{
    YT_VERIFY(value);
    auto key = value->GetKey();

    auto guard = WriterGuard(SpinLock);

    this->DrainTouchBuffer();

    auto* item = GetOrCrash(ItemMap_, key);

    YT_VERIFY(!item->Inserted);
    this->UpdateCookie(item, /*countDelta*/ -1, -item->CachedWeight);

    item->Value = value;
    item->Inserted = true;

    this->PushToYounger(item, weight);
    // MissedCounter and AsyncHitCounter have already been incremented in BeginInsert.
    Counters_->MissedWeightCounter.Increment(weight);
    Counters_->AsyncHitWeightCounter.Increment(weight * item->AsyncHitCount.load());

    // NB: Releases the lock.
    Trim(guard);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::Resurrect(const TValuePtr& value, i64 weight)
{
    YT_VERIFY(value);
    auto key = value->GetKey();

    auto guard = WriterGuard(SpinLock);

    this->DrainTouchBuffer();

    auto itemIt = ItemMap_.find(key);
    if (itemIt != ItemMap_.end()) {
        return;
    }

    auto* item = new TGhostItem(key);
    item->Value = value;
    item->Inserted = true;

    YT_VERIFY(ItemMap_.emplace(key, item).second);

    this->PushToYounger(item, weight);

    Counters_->SyncHitWeightCounter.Increment(weight);
    Counters_->SyncHitCounter.Increment();

    // NB: Releases the lock.
    Trim(guard);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::TryRemove(const TKey& key, const TValuePtr& value)
{
    auto guard = WriterGuard(SpinLock);

    this->DrainTouchBuffer();

    auto itemIt = ItemMap_.find(key);
    if (itemIt == ItemMap_.end()) {
        return;
    }

    auto* item = itemIt->second;
    if (!item->Inserted) {
        return;
    }
    auto actualValue = item->Value.Lock();
    // If value is null, it means that we don't care about the removed value and remove just by key.
    // If actualValue is null, then it refers to the value removed from the main cache, and always
    // doesn't match our provided value. Otherwise, just compare the values. Note that the condition
    // can be simplified just to (value && value != actualValue), but is retained as-is to make the
    // intention more clear.
    if (value && (!actualValue || value != actualValue)) {
        return;
    }
    actualValue.Reset();

    ItemMap_.erase(itemIt);

    this->PopFromLists(item);

    delete item;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::UpdateWeight(const TKey& key, i64 newWeight)
{
    auto guard = WriterGuard(SpinLock);

    this->DrainTouchBuffer();

    auto itemIt = ItemMap_.find(key);
    if (itemIt == ItemMap_.end()) {
        return;
    }

    auto item = itemIt->second;
    if (!item->Inserted) {
        return;
    }

    i64 weightDelta = newWeight - item->CachedWeight;

    TAsyncSlruCacheListManager<TGhostItem, TGhostShard>::UpdateWeight(item, weightDelta);

    // If item weight increases, it means that some parts of the item were missing in cache,
    // so add delta to missed weight.
    if (weightDelta > 0) {
        Counters_->MissedWeightCounter.Increment(weightDelta);
    }

    // NB: Releases the lock.
    Trim(guard);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::UpdateCookieWeight(const TKey& key, i64 newWeight)
{
    auto guard = WriterGuard(SpinLock);

    auto itemIt = ItemMap_.find(key);
    if (itemIt == ItemMap_.end()) {
        // Insert cookie canceled.
        return;
    }

    auto* item = itemIt->second;
    YT_VERIFY(!item->Inserted);

    i64 weightDelta = newWeight - item->CachedWeight;
    this->UpdateCookie(item, /*countDelta*/ 0, weightDelta);
    if (weightDelta > 0) {
        this->DrainTouchBuffer();

        // NB: Releases the lock.
        Trim(guard);
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::Reconfigure(i64 capacity, double youngerSizeFraction)
{
    auto writerGuard = WriterGuard(SpinLock);
    TAsyncSlruCacheListManager<TGhostItem, TGhostShard>::Reconfigure(capacity, youngerSizeFraction);
    this->DrainTouchBuffer();
    Trim(writerGuard);
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TGhostShard::Trim(NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>& guard)
{
    auto evictedItems = this->TrimNoDelete();
    for (const auto& item : evictedItems) {
        YT_VERIFY(ItemMap_.erase(item.Key) == 1);
    }

    // NB. Evicted items must die outside of critical section.
    guard.Release();
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
std::vector<typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValuePtr>
TAsyncSlruCacheBase<TKey, TValue, THash>::TShard::Trim(NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>& guard)
{
    auto evictedItems = this->TrimNoDelete();

    Parent->Size_ -= static_cast<int>(evictedItems.Size());

    std::vector<TValuePtr> evictedValues;
    for (const auto& item : evictedItems) {
        auto value = item.Value;

        YT_VERIFY(ItemMap.erase(value->GetKey()) == 1);

        if (!Parent->IsResurrectionSupported()) {
            YT_VERIFY(ValueMap.erase(value->GetKey()) == 1);
            value->Cache_.Reset();
        }

        YT_VERIFY(value->Item_ == &item);
        value->Item_ = nullptr;

        evictedValues.push_back(std::move(value));
    }

    // NB. Evicted items must die outside of critical section.
    guard.Release();

    return evictedValues;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TShard::OnYoungerUpdated(i64 deltaCount, i64 deltaWeight)
{
    Parent->YoungerSizeCounter_ += deltaCount;
    Parent->YoungerWeightCounter_ += deltaWeight;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TShard::OnOlderUpdated(i64 deltaCount, i64 deltaWeight)
{
    Parent->OlderSizeCounter_ += deltaCount;
    Parent->OlderWeightCounter_ += deltaWeight;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TShard::OnCookieUpdated(i64 deltaCount, i64 deltaWeight)
{
    Parent->CookieSizeCounter_ += deltaCount;
    Parent->CookieWeightCounter_ += deltaWeight;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::NotifyOnTrim(
    const std::vector<TValuePtr>& evictedValues,
    const TValuePtr& insertedValue,
    i64 weightDelta)
{
    if (weightDelta != 0) {
        OnWeightUpdated(weightDelta);
    }
    if (insertedValue) {
        OnAdded(insertedValue);
    }
    for (const auto& value : evictedValues) {
        OnRemoved(value);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie(const TKey& key)
    : Key_(key)
{ }

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie(TInsertCookie&& other)
    : Key_(std::move(other.Key_))
    , Cache_(std::move(other.Cache_))
    , ValueFuture_(std::move(other.ValueFuture_))
    , Active_(other.Active_.exchange(false))
    , InsertedIntoSmallGhost_(std::exchange(other.InsertedIntoSmallGhost_, false))
    , InsertedIntoLargeGhost_(std::exchange(other.InsertedIntoLargeGhost_, false))
{ }

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::~TInsertCookie()
{
    Abort();
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie& TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::operator =(TInsertCookie&& other)
{
    if (this != &other) {
        Abort();
        Key_ = std::move(other.Key_);
        Cache_ = std::move(other.Cache_);
        ValueFuture_ = std::move(other.ValueFuture_);
        Active_ = other.Active_.exchange(false);
        InsertedIntoSmallGhost_ = std::exchange(other.InsertedIntoSmallGhost_, false);
        InsertedIntoLargeGhost_ = std::exchange(other.InsertedIntoLargeGhost_, false);
    }
    return *this;
}

template <class TKey, class TValue, class THash>
const TKey& TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::GetKey() const
{
    return Key_;
}

template <class TKey, class TValue, class THash>
typename TAsyncSlruCacheBase<TKey, TValue, THash>::TValueFuture
TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::GetValue() const
{
    YT_ASSERT(ValueFuture_);
    return ValueFuture_;
}

template <class TKey, class TValue, class THash>
bool TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::IsActive() const
{
    return Active_;
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::UpdateWeight(i64 newWeight)
{
    if (Active_) {
        Cache_->UpdateCookieWeight(*this, newWeight);
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::Cancel(const TError& error)
{
    if (Active_.exchange(false)) {
        Cache_->CancelInsert(*this, error);
    }
}

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::EndInsert(TValuePtr value)
{
    if (Active_.exchange(false)) {
        Cache_->EndInsert(*this, value);
    }
}

template <class TKey, class TValue, class THash>
TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::TInsertCookie(
    const TKey& key,
    TIntrusivePtr<TAsyncSlruCacheBase> cache,
    TValueFuture valueFuture,
    bool active)
    : Key_(key)
    , Cache_(std::move(cache))
    , ValueFuture_(std::move(valueFuture))
    , Active_(active)
{ }

template <class TKey, class TValue, class THash>
void TAsyncSlruCacheBase<TKey, TValue, THash>::TInsertCookie::Abort()
{
    if (Active_.load()) {
        Cancel(TError(NYT::EErrorCode::Canceled, "Cache item insertion aborted"));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::TMemoryTrackingAsyncSlruCacheBase(
    TSlruCacheConfigPtr config,
    IMemoryUsageTrackerPtr memoryTracker,
    const NProfiling::TProfiler& profiler)
    : TAsyncSlruCacheBase<TKey, TValue, THash>(
        std::move(config),
        profiler)
    , MemoryTracker_(std::move(memoryTracker))
{
    MemoryTracker_->SetLimit(this->GetCapacity());
}

template <class TKey, class TValue, class THash>
TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::~TMemoryTrackingAsyncSlruCacheBase()
{
    MemoryTracker_->SetLimit(0);
}

template <class TKey, class TValue, class THash>
void TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::OnWeightUpdated(i64 weightDelta)
{
    if (weightDelta > 0) {
        MemoryTracker_->Acquire(weightDelta);
    } else {
        MemoryTracker_->Release(-weightDelta);
    }
}

template <class TKey, class TValue, class THash>
void TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::OnAdded(const TValuePtr& value)
{
    MemoryTracker_->Acquire(this->GetWeight(value));
}

template <class TKey, class TValue, class THash>
void TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::OnRemoved(const TValuePtr& value)
{
    MemoryTracker_->Release(this->GetWeight(value));
}

template <class TKey, class TValue, class THash>
void TMemoryTrackingAsyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(const TSlruCacheDynamicConfigPtr& config)
{
    if (auto newCapacity = config->Capacity) {
        MemoryTracker_->SetLimit(*newCapacity);
    }
    TAsyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
