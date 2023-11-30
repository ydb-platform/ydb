#ifndef SYNC_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include sync_cache.h"
// For the sake of sane code completion.
#include "sync_cache.h"
#endif

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
const TKey& TSyncCacheValueBase<TKey, TValue, THash>::GetKey() const
{
    return Key_;
}

template <class TKey, class TValue, class THash>
TSyncCacheValueBase<TKey, TValue, THash>::TSyncCacheValueBase(const TKey& key)
    : Key_(key)
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TSyncSlruCacheBase<TKey, TValue, THash>::TItem::TItem(TValuePtr value)
    : Value(std::move(value))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TSyncSlruCacheBase<TKey, TValue, THash>::TSyncSlruCacheBase(
    TSlruCacheConfigPtr config,
    const NProfiling::TProfiler& profiler)
    : Config_(std::move(config))
    , Capacity_(Config_->Capacity)
    , YoungerSizeFraction_(Config_->YoungerSizeFraction)
    , HitWeightCounter_(profiler.Counter("/hit"))
    , MissedWeightCounter_(profiler.Counter("/missed"))
    , DroppedWeightCounter_(profiler.Counter("/dropped"))
{
    profiler.AddFuncGauge("/younger", MakeStrong(this), [this] {
        return YoungerWeightCounter_.load();
    });
    profiler.AddFuncGauge("/older", MakeStrong(this), [this] {
        return OlderWeightCounter_.load();
    });

    Shards_.reset(new TShard[Config_->ShardCount]);

    int touchBufferCapacity = Config_->TouchBufferCapacity / Config_->ShardCount;
    for (int index = 0; index < Config_->ShardCount; ++index) {
        Shards_[index].TouchBuffer.resize(touchBufferCapacity);
    }
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::Clear()
{
    for (int i = 0; i < Config_->ShardCount; ++i) {
        auto& shard = Shards_[i];
        auto guard = WriterGuard(shard.SpinLock);

        shard.TouchBufferPosition = 0;

        shard.ItemMap.clear();

        TIntrusiveListWithAutoDelete<TItem, TDelete> youngerLruList;
        shard.YoungerLruList.Swap(youngerLruList);

        TIntrusiveListWithAutoDelete<TItem, TDelete> olderLruList;
        shard.OlderLruList.Swap(olderLruList);

        int totalItemCount = 0;
        i64 totalYoungerWeight = 0;
        i64 totalOlderWeight = 0;
        for (const auto& item : youngerLruList) {
            totalYoungerWeight += GetWeight(item.Value);
            ++totalItemCount;
        }
        for (const auto& item : olderLruList) {
            totalOlderWeight += GetWeight(item.Value);
            ++totalItemCount;
        }

        shard.YoungerWeightCounter -= totalYoungerWeight;
        shard.OlderWeightCounter -= totalOlderWeight;
        YoungerWeightCounter_.fetch_sub(totalYoungerWeight, std::memory_order::relaxed);
        OlderWeightCounter_.fetch_sub(totalOlderWeight, std::memory_order::relaxed);
        Size_ -= totalItemCount;

        // NB: Lists must die outside the critical section.
        guard.Release();
    }
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(const TSlruCacheDynamicConfigPtr& config)
{
    Capacity_.store(config->Capacity.value_or(Config_->Capacity));
    YoungerSizeFraction_.store(config->YoungerSizeFraction.value_or(Config_->YoungerSizeFraction));

    for (int index = 0; index < Config_->ShardCount; ++index) {
        auto* shard = &Shards_[index];
        auto guard = WriterGuard(shard->SpinLock);
        DrainTouchBuffer(shard);
        Trim(shard, guard);
    }
}

template <class TKey, class TValue, class THash>
typename TSyncSlruCacheBase<TKey, TValue, THash>::TValuePtr
TSyncSlruCacheBase<TKey, TValue, THash>::Find(const TKey& key)
{
    auto* shard = GetShardByKey(key);

    auto readerGuard = ReaderGuard(shard->SpinLock);

    auto itemIt = shard->ItemMap.find(key);
    if (itemIt == shard->ItemMap.end()) {
        return nullptr;
    }

    auto* item = itemIt->second;
    bool needToDrain = Touch(shard, item);
    auto value = item->Value;

    auto weight = GetWeight(item->Value);
    HitWeightCounter_.Increment(weight);

    readerGuard.Release();

    if (needToDrain) {
        auto writerGuard = WriterGuard(shard->SpinLock);
        DrainTouchBuffer(shard);
    }

    return value;
}

template <class TKey, class TValue, class THash>
std::vector<typename TSyncSlruCacheBase<TKey, TValue, THash>::TValuePtr>
TSyncSlruCacheBase<TKey, TValue, THash>::GetAll() const
{
    std::vector<TValuePtr> result;
    result.reserve(GetSize());
    for (int index = 0; index < Config_->ShardCount; ++index) {
        const auto& shard = Shards_[index];
        auto guard = ReaderGuard(shard.SpinLock);
        for (const auto& [key, item] : shard.ItemMap) {
            result.push_back(item->Value);
        }
    }
    return result;
}

template <class TKey, class TValue, class THash>
i64 TSyncSlruCacheBase<TKey, TValue, THash>::GetCapacity() const
{
    return Capacity_.load();
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::UpdateWeight(const TValuePtr& value)
{
    UpdateWeight(value->GetKey());
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::UpdateWeight(const TKey& key)
{
    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    DrainTouchBuffer(shard);

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

    YT_VERIFY(!item->Empty());

    if (item->Younger) {
        shard->YoungerWeightCounter += weightDelta;
        YoungerWeightCounter_.fetch_add(weightDelta, std::memory_order::relaxed);
    } else {
        shard->OlderWeightCounter += weightDelta;
        OlderWeightCounter_.fetch_add(weightDelta, std::memory_order::relaxed);
    }

    item->CachedWeight += weightDelta;

    // If item weight increases, it means that some parts of the item were missing in cache,
    // so add delta to missed weight.
    if (weightDelta > 0) {
        MissedWeightCounter_.Increment(weightDelta);
    }

    OnWeightUpdated(weightDelta);

    Trim(shard, guard);
}

template <class TKey, class TValue, class THash>
bool TSyncSlruCacheBase<TKey, TValue, THash>::TryInsert(const TValuePtr& value, TValuePtr* existingValue)
{
    const auto& key = value->GetKey();
    auto weight = GetWeight(value);
    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    DrainTouchBuffer(shard);

    auto itemIt = shard->ItemMap.find(key);
    if (itemIt != shard->ItemMap.end()) {
        DroppedWeightCounter_.Increment(weight);
        if (existingValue) {
            *existingValue = itemIt->second->Value;
        }
        return false;
    }

    auto* item = new TItem(value);
    YT_VERIFY(shard->ItemMap.emplace(key, item).second);
    ++Size_;

    MissedWeightCounter_.Increment(weight);

    PushToYounger(shard, item);

    // NB: Releases the lock.
    Trim(shard, guard);

    OnAdded(value);

    return true;
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::Trim(TShard* shard, NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>& guard)
{
    auto capacity = Capacity_.load();
    auto youngerSizeFraction = YoungerSizeFraction_.load();

    // Move from older to younger.
    while (!shard->OlderLruList.Empty() &&
        Config_->ShardCount * shard->OlderWeightCounter > capacity * (1 - youngerSizeFraction))
    {
        auto* item = &*(--shard->OlderLruList.End());
        MoveToYounger(shard, item);
    }

    // Evict from younger.
    std::vector<TValuePtr> evictedValues;
    while (!shard->YoungerLruList.Empty() &&
        static_cast<i64>(Config_->ShardCount * (shard->YoungerWeightCounter + shard->OlderWeightCounter)) > capacity)
    {
        auto* item = &*(--shard->YoungerLruList.End());
        auto value = item->Value;

        Pop(shard, item);

        YT_VERIFY(shard->ItemMap.erase(value->GetKey()) == 1);
        --Size_;

        evictedValues.emplace_back(std::move(item->Value));

        delete item;
    }

    guard.Release();

    for (const auto& value : evictedValues) {
        OnRemoved(value);
    }
}

template <class TKey, class TValue, class THash>
bool TSyncSlruCacheBase<TKey, TValue, THash>::TryRemove(const TKey& key)
{
    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    DrainTouchBuffer(shard);

    auto it = shard->ItemMap.find(key);
    if (it == shard->ItemMap.end()) {
        return false;
    }

    auto* item = it->second;
    auto value = item->Value;

    shard->ItemMap.erase(it);
    --Size_;

    Pop(shard, item);

    delete item;

    guard.Release();

    OnRemoved(value);

    return true;
}

template <class TKey, class TValue, class THash>
bool TSyncSlruCacheBase<TKey, TValue, THash>::TryRemove(const TValuePtr& value)
{
    const auto& key = value->GetKey();
    auto* shard = GetShardByKey(key);

    auto guard = WriterGuard(shard->SpinLock);

    DrainTouchBuffer(shard);

    auto itemIt = shard->ItemMap.find(key);
    if (itemIt == shard->ItemMap.end()) {
        return false;
    }

    auto* item = itemIt->second;
    if (item->Value != value) {
        return false;
    }

    shard->ItemMap.erase(itemIt);
    --Size_;

    Pop(shard, item);

    delete item;

    guard.Release();

    OnRemoved(value);

    return true;
}

template <class TKey, class TValue, class THash>
auto TSyncSlruCacheBase<TKey, TValue, THash>::GetShardByKey(const TKey& key) const -> TShard*
{
    return &Shards_[THash()(key) % Config_->ShardCount];
}

template <class TKey, class TValue, class THash>
bool TSyncSlruCacheBase<TKey, TValue, THash>::Touch(TShard* shard, TItem* item)
{
    int capacity = shard->TouchBuffer.size();
    int index = shard->TouchBufferPosition++;
    if (index >= capacity) {
        // Drop touch request due to buffer overflow.
        // NB: We still return false since the other thread is already responsible for
        // draining the buffer.
        return false;
    }

    shard->TouchBuffer[index] = item;
    return index == capacity - 1;
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::DrainTouchBuffer(TShard* shard)
{
    int count = std::min(
        shard->TouchBufferPosition.load(),
        static_cast<int>(shard->TouchBuffer.size()));
    for (int index = 0; index < count; ++index) {
        MoveToOlder(shard, shard->TouchBuffer[index]);
    }
    shard->TouchBufferPosition = 0;
}

template <class TKey, class TValue, class THash>
i64 TSyncSlruCacheBase<TKey, TValue, THash>::GetWeight(const TValuePtr& /*value*/) const
{
    return 1;
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::OnAdded(const TValuePtr& /*value*/)
{ }

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::OnRemoved(const TValuePtr& /*value*/)
{ }

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::OnWeightUpdated(i64/*weightDelta*/)
{ }

template <class TKey, class TValue, class THash>
int TSyncSlruCacheBase<TKey, TValue, THash>::GetSize() const
{
    return Size_.load(std::memory_order::relaxed);
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::PushToYounger(TShard* shard, TItem* item)
{
    YT_ASSERT(item->Empty());
    shard->YoungerLruList.PushFront(item);
    auto weight = GetWeight(item->Value);
    item->CachedWeight = weight;
    shard->YoungerWeightCounter += weight;
    YoungerWeightCounter_.fetch_add(weight, std::memory_order::relaxed);
    item->Younger = true;
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::MoveToYounger(TShard* shard, TItem* item)
{
    YT_ASSERT(!item->Empty());
    item->Unlink();
    shard->YoungerLruList.PushFront(item);
    if (!item->Younger) {
        auto weight = GetWeight(item->Value);
        shard->YoungerWeightCounter += weight;
        shard->OlderWeightCounter -= weight;
        OlderWeightCounter_.fetch_sub(weight, std::memory_order::relaxed);
        YoungerWeightCounter_.fetch_add(weight, std::memory_order::relaxed);
        item->Younger = true;
    }
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::MoveToOlder(TShard* shard, TItem* item)
{
    YT_ASSERT(!item->Empty());
    item->Unlink();
    shard->OlderLruList.PushFront(item);
    if (item->Younger) {
        auto weight = GetWeight(item->Value);
        shard->YoungerWeightCounter -= weight;
        shard->OlderWeightCounter += weight;
        YoungerWeightCounter_.fetch_sub(weight, std::memory_order::relaxed);
        OlderWeightCounter_.fetch_add(weight, std::memory_order::relaxed);
        item->Younger = false;
    }
}

template <class TKey, class TValue, class THash>
void TSyncSlruCacheBase<TKey, TValue, THash>::Pop(TShard* shard, TItem* item)
{
    if (item->Empty()) {
        return;
    }
    auto weight = GetWeight(item->Value);
    if (item->Younger) {
        shard->YoungerWeightCounter -= weight;
        YoungerWeightCounter_.fetch_sub(weight, std::memory_order::relaxed);
    } else {
        shard->OlderWeightCounter -= weight;
        OlderWeightCounter_.fetch_sub(weight, std::memory_order::relaxed);
    }
    item->Unlink();
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TMemoryTrackingSyncSlruCacheBase<TKey, TValue, THash>::TMemoryTrackingSyncSlruCacheBase(
    TSlruCacheConfigPtr config,
    IMemoryUsageTrackerPtr memoryTracker,
    const NProfiling::TProfiler& profiler)
    : TSyncSlruCacheBase<TKey, TValue, THash>(
        std::move(config),
        profiler)
    , MemoryTracker_(std::move(memoryTracker))
{
    MemoryTracker_->SetLimit(this->Capacity_.load());
}

template <class TKey, class TValue, class THash>
TMemoryTrackingSyncSlruCacheBase<TKey, TValue, THash>::~TMemoryTrackingSyncSlruCacheBase()
{
    MemoryTracker_->SetLimit(0);
}

template <class TKey, class TValue, class THash>
void TMemoryTrackingSyncSlruCacheBase<TKey, TValue, THash>::OnWeightUpdated(i64 weightDelta)
{
    if (weightDelta > 0) {
        MemoryTracker_->Acquire(weightDelta);
    } else {
        MemoryTracker_->Release(-weightDelta);
    }
}

template <class TKey, class TValue, class THash>
void TMemoryTrackingSyncSlruCacheBase<TKey, TValue, THash>::OnAdded(const TValuePtr& value)
{
    MemoryTracker_->Acquire(this->GetWeight(value));
}

template <class TKey, class TValue, class THash>
void TMemoryTrackingSyncSlruCacheBase<TKey, TValue, THash>::OnRemoved(const TValuePtr& value)
{
    MemoryTracker_->Release(this->GetWeight(value));
}

template <class TKey, class TValue, class THash>
void TMemoryTrackingSyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(const TSlruCacheDynamicConfigPtr& config)
{
    TSyncSlruCacheBase<TKey, TValue, THash>::Reconfigure(config);
    MemoryTracker_->SetLimit(this->Capacity_.load());
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TSimpleLruCache<TKey, TValue, THash>::TSimpleLruCache(size_t maxWeight)
    : MaxWeight_(maxWeight)
{ }

template <class TKey, class TValue, class THash>
size_t TSimpleLruCache<TKey, TValue, THash>::GetSize() const
{
    return ItemMap_.size();
}

template <class TKey, class TValue, class THash>
const TValue& TSimpleLruCache<TKey, TValue, THash>::Get(const TKey& key)
{
    auto it = ItemMap_.find(key);
    YT_VERIFY(it != ItemMap_.end());
    UpdateLruList(it);
    return it->second.Value;
}

template <class TKey, class TValue, class THash>
TValue* TSimpleLruCache<TKey, TValue, THash>::Find(const TKey& key)
{
    auto it = ItemMap_.find(key);
    if (it == ItemMap_.end()) {
        return nullptr;
    }
    UpdateLruList(it);
    return &(it->second.Value);
}

template <class TKey, class TValue, class THash>
TValue* TSimpleLruCache<TKey, TValue, THash>::FindNoTouch(const TKey& key)
{
    auto it = ItemMap_.find(key);
    if (it == ItemMap_.end()) {
        return nullptr;
    }
    return &(it->second.Value);
}

template <class TKey, class TValue, class THash>
TValue* TSimpleLruCache<TKey, TValue, THash>::Insert(const TKey& key, TValue value, size_t weight)
{
    {
        auto mapIt = ItemMap_.find(key);
        if (mapIt != ItemMap_.end()) {
            LruList_.erase(mapIt->second.LruListIterator);
            CurrentWeight_ -= mapIt->second.Weight;
            ItemMap_.erase(mapIt);
        }
    }

    auto item = TItem(std::move(value), weight);
    while (GetSize() > 0 && CurrentWeight_ + weight > MaxWeight_) {
        Pop();
    }
    auto insertResult = ItemMap_.emplace(key, std::move(item));
    YT_VERIFY(insertResult.second);
    auto mapIt = insertResult.first;
    LruList_.push_front(mapIt);
    mapIt->second.LruListIterator = LruList_.begin();
    CurrentWeight_ += weight;

    return &mapIt->second.Value;
}

template <class TKey, class TValue, class THash>
void TSimpleLruCache<TKey, TValue, THash>::SetMaxWeight(size_t maxWeight)
{
    MaxWeight_ = maxWeight;
}

template <class TKey, class TValue, class THash>
void TSimpleLruCache<TKey, TValue, THash>::Clear()
{
    ItemMap_.clear();
    LruList_.clear();
    CurrentWeight_ = 0;
}

template <class TKey, class TValue, class THash>
void TSimpleLruCache<TKey, TValue, THash>::Pop()
{
    auto mapIt = LruList_.back();
    CurrentWeight_ -= mapIt->second.Weight;
    ItemMap_.erase(mapIt);
    LruList_.pop_back();
}

template <class TKey, class TValue, class THash>
void TSimpleLruCache<TKey, TValue, THash>::UpdateLruList(typename TItemMap::iterator mapIt)
{
    LruList_.erase(mapIt->second.LruListIterator);
    LruList_.push_front(mapIt);
    mapIt->second.LruListIterator = LruList_.begin();
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash>
TMultiLruCache<TKey, TValue, THash>::TMultiLruCache(size_t maxWeight)
    : MaxWeight_(maxWeight)
{ }

template <class TKey, class TValue, class THash>
size_t TMultiLruCache<TKey, TValue, THash>::GetSize() const
{
    return LruList_.size();
}

template <class TKey, class TValue, class THash>
const TValue& TMultiLruCache<TKey, TValue, THash>::Get(const TKey& key)
{
    auto* value = Find(key);
    YT_VERIFY(value != nullptr);
    return *value;
}

template <class TKey, class TValue, class THash>
TValue* TMultiLruCache<TKey, TValue, THash>::Find(const TKey& key)
{
    auto it = ItemMap_.find(key);
    if (it == ItemMap_.end()) {
        return nullptr;
    }

    auto item = std::move(it->second.back());
    it->second.pop_back();
    it->second.push_front(std::move(item));

    UpdateLruList(it->second.begin());
    return &(it->second.front().Value);

}

template <class TKey, class TValue, class THash>
TValue* TMultiLruCache<TKey, TValue, THash>::Insert(const TKey& key, TValue value, size_t weight)
{
    YT_VERIFY(weight <= MaxWeight_);

    while (GetSize() > 0 && CurrentWeight_ + weight > MaxWeight_) {
        Pop();
    }

    CurrentWeight_ += weight;

    auto mapIt = ItemMap_.emplace(key, std::deque<TItem>()).first;
    mapIt->second.emplace_front(std::move(value), weight);
    mapIt->second.front().LruListIterator = LruList_.insert(LruList_.begin(), mapIt);
    return &(mapIt->second.front().Value);
}

template <class TKey, class TValue, class THash>
std::optional<TValue> TMultiLruCache<TKey, TValue, THash>::Extract(const TKey& key)
{
    auto mapIt = ItemMap_.find(key);
    if (mapIt == ItemMap_.end()) {
        return std::nullopt;
    }

    auto item = std::move(mapIt->second.back());
    CurrentWeight_ -= item.Weight;

    mapIt->second.pop_back();
    if (mapIt->second.empty()) {
        ItemMap_.erase(mapIt);
    }

    auto lruListIt = item.LruListIterator;
    LruList_.erase(lruListIt);

    return std::move(item.Value);
}

template <class TKey, class TValue, class THash>
TValue TMultiLruCache<TKey, TValue, THash>::Pop()
{
    auto listIt = std::prev(LruList_.end());
    auto mapIt = *listIt;

    YT_VERIFY(!mapIt->second.empty());
    YT_VERIFY(mapIt->second.back().LruListIterator == listIt);

    auto item = std::move(mapIt->second.back());
    CurrentWeight_ -= item.Weight;

    mapIt->second.pop_back();
    if (mapIt->second.empty()) {
        ItemMap_.erase(mapIt);
    }

    LruList_.erase(listIt);

    return std::move(item.Value);
}

template <class TKey, class TValue, class THash>
void TMultiLruCache<TKey, TValue, THash>::Clear()
{
    ItemMap_.clear();
    LruList_.clear();
    CurrentWeight_ = 0;
}

template <class TKey, class TValue, class THash>
TMultiLruCache<TKey, TValue, THash>::TItem::TItem(TValue value, size_t weight)
    : Value(std::move(value))
    , Weight(weight)
{ }

template <class TKey, class TValue, class THash>
void TMultiLruCache<TKey, TValue, THash>::UpdateLruList(typename std::deque<TItem>::iterator dequeIt)
{
    auto mapIt = *dequeIt->LruListIterator;
    LruList_.erase(dequeIt->LruListIterator);
    LruList_.push_front(std::move(mapIt));
    dequeIt->LruListIterator = LruList_.begin();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
