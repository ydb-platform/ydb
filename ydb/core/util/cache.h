#pragma once
#include "defs.h"
#include <util/generic/intrlist.h>
#include <util/generic/map.h>
#include <util/generic/hash_set.h>
#include <functional>

namespace NKikimr {
namespace NCache {

struct TCacheStatistics {
    ui64 FindCount;
    ui64 FindHitCount;
    ui64 FindWithoutPromoteCount;
    ui64 FindWithoutPromoteHitCount;
    ui64 EvictionCount;
    ui64 KeyEvictionCount;
    ui64 InsertCount;
    ui64 InsertExistingCount;
    ui64 PopCount;
    ui64 PopEmptyCount;
    ui64 EraseCount;
    ui64 EraseMissingCount;
    ui64 PopWhileOverflowCount;
    ui64 ClearCount;

    TCacheStatistics()
    {
        Zero(*this);
    }
};

class TDefaultStatisticsPolicy {
public:
    TDefaultStatisticsPolicy() {}
    ~TDefaultStatisticsPolicy() {}

    const TCacheStatistics& GetStatistics() const {
        return Statistics;
    }

    void IncFindCount() { ++Statistics.FindCount; }
    void IncFindHitCount() { ++Statistics.FindHitCount; }
    void IncFindWithoutPromoteCount() { ++Statistics.FindWithoutPromoteCount; }
    void IncFindWithoutPromoteHitCount() { ++Statistics.FindWithoutPromoteHitCount; }
    void IncInsertCount() { ++Statistics.InsertCount; }
    void IncInsertExistingCount() { ++Statistics.InsertExistingCount; }
    void IncEraseCount() { ++Statistics.EraseCount; }
    void IncEraseMissingCount() { ++Statistics.EraseMissingCount; }
    void IncPopCount() { ++Statistics.PopCount; }
    void IncPopEmptyCount() { ++Statistics.PopEmptyCount; }
    void IncPopWhileOverflowCount() { ++Statistics.PopWhileOverflowCount; }
    void IncClearCount() { ++Statistics.ClearCount; }
    void IncEvictionCount() { ++Statistics.EvictionCount; }
    void IncKeyEvictionCount() { ++Statistics.KeyEvictionCount; }
    void ClearStatistics() {
        Zero(Statistics);
    }

private:
    TCacheStatistics Statistics;
};

class TNullStatisticsPolicy {
public:
    TNullStatisticsPolicy() {}
    ~TNullStatisticsPolicy() {}

    const TCacheStatistics& GetStatistics() const {
        return Statistics;
    }

    void IncFindCount() {}
    void IncFindHitCount() {}
    void IncFindWithoutPromoteCount() {}
    void IncFindWithoutPromoteHitCount() {}
    void IncInsertCount() {}
    void IncInsertExistingCount() {}
    void IncEraseCount() {}
    void IncEraseMissingCount() {}
    void IncPopCount() {}
    void IncPopEmptyCount() {}
    void IncPopWhileOverflowCount() {}
    void IncClearCount() {}
    void IncEvictionCount() {}
    void IncKeyEvictionCount() {}
    void ClearStatistics() {}

private:
    const static TCacheStatistics Statistics;
};

template <typename TKey, typename TValue>
class ICache {
public:
    typedef ICache<TKey, TValue> TSelf;

    typedef std::function<void(const TKey& key, TValue& value, ui64 size)> TEvictionCallback;
    typedef std::function<void(const TKey& key)> TKeyEvictionCallback;
    typedef std::function<ui64(const TKey& key, const TValue& value)> TMeasureCallback;
    typedef std::function<bool(const TSelf& cache)> TOverflowCallback;

    virtual ~ICache() {}
    virtual ui64 GetUsedSize() const = 0;
    virtual ui64 GetCount() const = 0;
    // returns false if key was not found
    virtual bool Find(const TKey& key, TValue*& value) = 0;
    // returns false if key was not found
    virtual bool FindWithoutPromote(const TKey& key, TValue*& value) const = 0;
    // returns false if key was already exist. currentValue will be always filled
    virtual bool Insert(const TKey& key, const TValue& value, TValue*& currentValue) = 0;
    // return false if key was not found
    virtual bool Erase(const TKey& key) = 0;
    virtual bool IsEmpty() const = 0;
    virtual void Clear() = 0;
    // erases some unused item, return false if cache was empty
    virtual bool Pop() = 0;
    virtual void PopWhileOverflow() = 0;
    virtual void OnInsert(const TKey& key, const TValue& value, ui64 size) = 0;
    virtual void OnEvict(const TKey& key, TValue& value, ui64 size) = 0;
    virtual void OnKeyEvict(const TKey& key) = 0;
    virtual const TCacheStatistics& GetStatistics() const = 0;
    virtual void ClearStatistics() = 0;
    virtual bool IsOverflow() const = 0;
    virtual void SetEvictionCallback(const TEvictionCallback& callback) = 0;
    virtual void SetKeyEvictionCallback(const TKeyEvictionCallback& callback) = 0;
    virtual void SetOverflowCallback(const TOverflowCallback& callback) = 0;
    virtual ui64 Measure(const TKey& key, const TValue& value) const = 0;
    // returns new size of key-value pair
    virtual ui64 UpdateUsedSize(const TKey& key, const TValue& newValue, ui64 oldSize) = 0;

    static ui64 DefaultMeasureCallback(const TKey& key, const TValue& value) {
        Y_UNUSED(key);
        Y_UNUSED(value);
        return 1;
    }

    static void DefaultEvictionCallback(const TKey& key, TValue& value, ui64 size) {
        Y_UNUSED(key);
        Y_UNUSED(value);
        Y_UNUSED(size);
    }

    static void DefaultKeyEvictionCallback(const TKey& key) {
        Y_UNUSED(key);
    }

    static bool DefaultOverflowCallback(const TSelf& cache) {
        Y_UNUSED(cache);
        return false;
    }
};

template <typename TKey, typename TValue, typename TDerived, typename TStatisticsPolicy>
class TCacheBase : public ICache<TKey, TValue>, private TNonCopyable {
public:
    typedef TCacheBase<TKey, TValue, TDerived, TStatisticsPolicy> TSelf;
    typedef ICache<TKey, TValue> TInterface;
    typedef typename TInterface::TEvictionCallback TEvictionCallback;
    typedef typename TInterface::TKeyEvictionCallback TKeyEvictionCallback;
    typedef typename TInterface::TMeasureCallback TMeasureCallback;
    typedef typename TInterface::TOverflowCallback TOverflowCallback;

    TCacheBase()
        : TCacheBase(&TSelf::DefaultMeasureCallback)
    {
    }

    explicit TCacheBase(const TMeasureCallback& measureCallback)
        : MeasureCallback(measureCallback)
        , EvictionCallback(&TSelf::DefaultEvictionCallback)
        , KeyEvictionCallback(&TSelf::DefaultKeyEvictionCallback)
        , OverflowCallback(&TSelf::DefaultOverflowCallback)
        , Count(0)
        , UsedSize(0)
    {
    }

    ui64 GetUsedSize() const override final {
        return UsedSize;
    }

    ui64 GetCount() const override final {
        return Count;
    }

    bool Find(const TKey& key, TValue*& value) override final {
        value = nullptr;
        Statistics.IncFindCount();
        bool result = static_cast<TDerived*>(this)->DoFind(key, value);
        if (result) {
            Statistics.IncFindHitCount();
        }

        return result;
    }

    bool FindWithoutPromote(const TKey& key, TValue*& value) const override final {
        value = nullptr;
        Statistics.IncFindWithoutPromoteCount();
        bool result = static_cast<const TDerived*>(this)->DoFindWithoutPromote(key, value);
        if (result) {
            Statistics.IncFindWithoutPromoteHitCount();
        }

        return result;
    }

    bool Insert(const TKey& key, const TValue& value, TValue*& currentValue) override final {
        currentValue = nullptr;
        Statistics.IncInsertCount();
        bool result = static_cast<TDerived*>(this)->DoInsert(key, value, currentValue);
        if (!result) {
            Statistics.IncInsertExistingCount();
        }

        return result;
    }

    bool Erase(const TKey& key) override final {
        Statistics.IncEraseCount();
        bool result = static_cast<TDerived*>(this)->DoErase(key);
        if (!result) {
            Statistics.IncEraseMissingCount();
        }

        return result;
    }

    bool Pop() override final {
        Statistics.IncPopCount();
        bool result = static_cast<TDerived*>(this)->DoPop();
        if (!result) {
            Statistics.IncPopEmptyCount();
        }

        return result;
    }

    void PopWhileOverflow() override final {
        Statistics.IncPopWhileOverflowCount();
        while (IsOverflow()) {
            bool result = static_cast<TDerived*>(this)->DoPop();
            if (!result)
                break;
        }
    }

    bool IsEmpty() const override final {
        return GetCount() == 0;
    }

    void Clear() override final {
        Statistics.IncClearCount();
        for (;;) {
            bool result = static_cast<TDerived*>(this)->DoPop();
            if (!result)
                break;
        }
    }

    void OnInsert(const TKey& key, const TValue& value, ui64 size) override {
        Y_UNUSED(key);
        Y_UNUSED(value);
        UsedSize += size;
        ++Count;
    }

    void OnEvict(const TKey& key, TValue& value, ui64 size) override {
        EvictionCallback(key, value, size);
        Statistics.IncEvictionCount();
        UsedSize -= size;
        --Count;
    }

    void OnKeyEvict(const TKey& key) override {
        KeyEvictionCallback(key);
        Statistics.IncKeyEvictionCount();
    }

    const TCacheStatistics& GetStatistics() const override final {
        return Statistics.GetStatistics();
    }

    void ClearStatistics() override final {
        Statistics.ClearStatistics();
    }

    void SetEvictionCallback(const TEvictionCallback& callback) override final {
        EvictionCallback = callback;
    }

    void SetKeyEvictionCallback(const TKeyEvictionCallback& callback) override final {
        KeyEvictionCallback = callback;
    }

    void SetOverflowCallback(const TOverflowCallback& callback) override final {
        OverflowCallback = callback;
    }

    bool IsOverflow() const override final {
        return OverflowCallback(*this);
    }

    ui64 Measure(const TKey& key, const TValue& value) const override final {
        return MeasureCallback(key, value);
    }

    ui64 UpdateUsedSize(const TKey& key, const TValue& newValue, ui64 oldSize) override final {
        ui64 newSize = MeasureCallback(key, newValue);
        Y_ABORT_UNLESS(UsedSize >= oldSize);
        UsedSize -= oldSize;
        UsedSize += newSize;
        return newSize;
    }

private:
    const TMeasureCallback MeasureCallback;
    TEvictionCallback EvictionCallback;
    TKeyEvictionCallback KeyEvictionCallback;
    TOverflowCallback OverflowCallback;
    mutable TStatisticsPolicy Statistics;
    ui64 Count;
    ui64 UsedSize;
};

template <typename TKey, typename TValue, typename TContainer, typename TStatisticsPolicy = TDefaultStatisticsPolicy>
class TUnboundedCacheBase : public TCacheBase<TKey, TValue, TUnboundedCacheBase<TKey, TValue, TContainer, TStatisticsPolicy>, TStatisticsPolicy> {
public:
    typedef TCacheBase<TKey, TValue, TUnboundedCacheBase<TKey, TValue, TContainer, TStatisticsPolicy>, TStatisticsPolicy> TBase;
    typedef TUnboundedCacheBase<TKey, TValue, TContainer, TStatisticsPolicy> TSelf;
    typedef typename TBase::TMeasureCallback TMeasureCallback;

    TUnboundedCacheBase()
        : TUnboundedCacheBase(&TBase::DefaultMeasureCallback)
    {
    }

    explicit TUnboundedCacheBase(const TMeasureCallback& measureCallback)
        : TBase(measureCallback)
    {
    }

    bool DoFind(const TKey& key, TValue*& value) {
        auto it = Items.find(key);
        if (it == Items.end())
            return false;

        value = &it->second;
        return true;
    }

    bool DoFindWithoutPromote(const TKey& key, TValue*& value) const {
        auto it = Items.find(key);
        if (it == Items.end())
            return false;

        value = const_cast<TValue*>(&it->second);
        return true;
    }

    bool DoPop() {
        if (Items.empty())
            return false;

        auto it = Items.begin();
        ui64 size = TSelf::Measure(it->first, it->second);
        this->OnEvict(it->first, const_cast<TValue&>(it->second), size);
        Items.erase(it);
        return true;
    }

    bool DoInsert(const TKey& key, const TValue& value, TValue*& currentValue) {
        auto it = Items.find(key);
        if (it != Items.end()) {
            currentValue = &it->second;
            return false;
        }

        ui64 size = TSelf::Measure(key, value);
        it = Items.insert(std::make_pair(key, value)).first;
        currentValue = &it->second;
        this->OnInsert(key, value, size);
        return true;
    }

    bool DoErase(const TKey& key) {
        auto it = Items.find(key);
        if (it == Items.end())
            return false;

        ui64 size = TSelf::Measure(key, it->second);
        this->OnEvict(it->first, const_cast<TValue&>(it->second), size);
        Items.erase(it);

        return true;
    }

private:
    TContainer Items;
};

template <typename TKey, typename TValue, typename TStatisticsPolicy = TDefaultStatisticsPolicy>
class TUnboundedCacheOnMap : public TUnboundedCacheBase<TKey, TValue, TMap<TKey, TValue>, TStatisticsPolicy> {
public:
    using TBase = TUnboundedCacheBase<TKey, TValue, TMap<TKey, TValue>, TStatisticsPolicy>;
    using TMeasureCallback = typename TBase::TMeasureCallback;
    TUnboundedCacheOnMap()
    {}

    explicit TUnboundedCacheOnMap(const TMeasureCallback& measureCallback)
        : TBase(measureCallback)
    {}
};

template <typename TKey, typename TValue, typename TKeyHash = ::THash<TKey>, typename TStatisticsPolicy = TDefaultStatisticsPolicy>
class TUnboundedCacheOnHash : public TUnboundedCacheBase<TKey, TValue, THashMap<TKey, TValue, TKeyHash>, TStatisticsPolicy> {
public:
    using TBase = TUnboundedCacheBase<TKey, TValue, THashMap<TKey, TValue, TKeyHash>, TStatisticsPolicy>;
    using TMeasureCallback = typename TBase::TMeasureCallback;
    TUnboundedCacheOnHash()
    {}

    explicit TUnboundedCacheOnHash(const TMeasureCallback& measureCallback)
        : TBase(measureCallback)
    {}
};

template <typename TKey, typename TValue>
class TSizeBasedOverflowCallback : private TNonCopyable {
public:
    TSizeBasedOverflowCallback(ui64 maxSize)
        : MaxSize(maxSize)
    {
    }

    void SetMaxSize(ui64 maxSize) {
        MaxSize = maxSize;
    }

    bool operator() (const ICache<TKey, TValue>& cache) const {
        return cache.GetUsedSize() >= MaxSize;
    }

private:
    ui64 MaxSize;
};

template <typename TKey, typename TValue, typename TKeyHash = ::THash<TKey>, typename TStatisticsPolicy = TDefaultStatisticsPolicy>
class TLruCache : public TCacheBase<TKey, TValue, TLruCache<TKey, TValue, TKeyHash, TStatisticsPolicy>, TStatisticsPolicy> {
public:
    typedef TCacheBase<TKey, TValue, TLruCache<TKey, TValue, TKeyHash, TStatisticsPolicy>, TStatisticsPolicy> TBase;
    typedef TLruCache<TKey, TValue, TKeyHash, TStatisticsPolicy> TSelf;
    typedef typename TBase::TMeasureCallback TMeasureCallback;

    TLruCache()
        : TLruCache(&TBase::DefaultMeasureCallback)
    {
    }

    explicit TLruCache(const TMeasureCallback& measureCallback)
        : TBase(measureCallback)
    {
    }

    struct TItem : public TIntrusiveListItem<TItem> {
        typedef TIntrusiveListItem<TItem> TBase;
        explicit TItem(const TKey& key, const TValue& value = TValue())
            : TBase()
            , Key(key)
            , Value(value)
        {
        }

        TItem(const TItem& other)
            : TBase()
            , Key(other.Key)
            , Value(other.Value)
        {
        }

        bool operator == (const TItem& other) const {
            return Key == other.Key;
        }

        TKey Key;
        TValue Value;

        struct THash {
            size_t operator() (const TItem& item) const {
                return TKeyHash()(item.Key);
            }
        };
    };

    typedef TIntrusiveList<TItem> TListType;
    typedef THashSet<TItem, typename TItem::THash> TIndex;

    bool DoFind(const TKey& key, TValue*& value) {
        TItem tmpItem(key);
        auto indexIt = Index.find(tmpItem);
        if (indexIt == Index.end())
            return false;

        value = &const_cast<TValue&>(indexIt->Value);
        Promote(indexIt);
        return true;
    }

    bool DoFindWithoutPromote(const TKey& key, TValue*& value) const {
        TItem tmpItem(key);
        auto indexIt = Index.find(tmpItem);
        if (indexIt == Index.end())
            return false;

        value = &const_cast<TValue&>(indexIt->Value);
        return true;
    }

    bool DoPop() {
        if (Index.empty())
            return false;

        auto item = List.PopBack();
        ui64 size = TSelf::Measure(item->Key, item->Value);
        this->OnEvict(item->Key, item->Value, size);
        auto itemIt = Index.find(*item);
        Y_DEBUG_ABORT_UNLESS(itemIt != Index.end());
        Index.erase(itemIt);
        return true;
    }

    bool DoInsert(const TKey& key, const TValue& value, TValue*& currentValue) {
        TItem tmpItem(key, value);
        auto indexIt = Index.find(tmpItem);
        if (indexIt != Index.end()) {
            currentValue = const_cast<TValue*>(&indexIt->Value);
            Promote(indexIt);
            return false;
        }

        TSelf::PopWhileOverflow();
        ui64 size = TSelf::Measure(key, value);
        indexIt = Index.insert(tmpItem).first;
        List.PushFront(const_cast<TItem*>(&*indexIt));
        currentValue = const_cast<TValue*>(&indexIt->Value);
        this->OnInsert(key, value, size);
        return true;
    }

    bool DoErase(const TKey& key) {
        TItem tmpItem(key);
        auto indexIt = Index.find(tmpItem);
        if (indexIt == Index.end())
            return false;

        auto item = const_cast<TItem*>(&*indexIt);
        ui64 size = TSelf::Measure(item->Key, item->Value);
        this->OnEvict(item->Key, item->Value, size);
        item->Unlink();
        Index.erase(indexIt);
        return true;
    }

private:
    void Promote(const typename TIndex::iterator& indexIt) {
        auto item = const_cast<TItem*>(&*indexIt);
        item->Unlink();
        List.PushFront(item);
    }

private:
    TListType List;
    TIndex Index;
};

struct T2QCacheConfig : public TThrRefBase {
    double InSizeRatio;
    double OutKeyRatio;

    T2QCacheConfig()
        : InSizeRatio(0.25)
        , OutKeyRatio(0.5)
    {
    }
};

template <typename TKey, typename TValue, typename TKeyHash = ::THash<TKey>, typename TStatisticsPolicy = TDefaultStatisticsPolicy>
class T2QCache : public TCacheBase<TKey, TValue, T2QCache<TKey, TValue, TKeyHash, TStatisticsPolicy>, TStatisticsPolicy> {
public:
    typedef TCacheBase<TKey, TValue, T2QCache<TKey, TValue, TKeyHash, TStatisticsPolicy>, TStatisticsPolicy> TBase;
    typedef T2QCache<TKey, TValue, TKeyHash, TStatisticsPolicy> TSelf;
    typedef typename TBase::TMeasureCallback TMeasureCallback;
    typedef typename TLruCache<TKey, TValue, TKeyHash, TStatisticsPolicy>::TItem TItem;
    typedef typename TLruCache<TKey, TValue, TKeyHash, TStatisticsPolicy>::TListType TListType;
    typedef typename TLruCache<TKey, TValue, TKeyHash, TStatisticsPolicy>::TIndex TIndex;

    struct TItemKey : public TIntrusiveListItem <TItemKey> {
        typedef TIntrusiveListItem<TItemKey> TBase;
        explicit TItemKey(const TKey& key)
            : TBase()
            , Key(key)
        {
        }

        TItemKey(const TItemKey& other)
            : TBase()
            , Key(other.Key)
        {
        }

        bool operator == (const TItemKey& other) const {
            return Key == other.Key;
        }

        TKey Key;

        struct THash {
            size_t operator() (const TItemKey& item) const {
                return TKeyHash()(item.Key);
            }
        };
    };

    typedef TIntrusiveList<TItemKey> TKeyList;
    typedef THashSet<TItemKey, typename TItemKey::THash> TKeyIndex;

    explicit T2QCache(const TIntrusivePtr<T2QCacheConfig>& config)
        : T2QCache(config, &TBase::DefaultMeasureCallback)
    {
    }

    T2QCache(const TIntrusivePtr<T2QCacheConfig>& config, const TMeasureCallback& measureCallback)
        : TBase(measureCallback)
        , Config(config)
        , InSize(0)
    {
    }

    bool DoFind(const TKey& key, TValue*& value) {
        TItem tmpItem(key);
        auto mainIndexIt = MainIndex.find(tmpItem);
        if (mainIndexIt != MainIndex.end()) {
            value = &const_cast<TValue&>(mainIndexIt->Value);
            PromoteMain(mainIndexIt);
            return true;
        }

        auto inIndexIt = InIndex.find(tmpItem);
        if (inIndexIt != InIndex.end()) {
            value = &const_cast<TValue&>(inIndexIt->Value);
            return true;
        }

        return false;
    }

    bool DoFindWithoutPromote(const TKey& key, TValue*& value) const {
        TItem tmpItem(key);
        auto mainIndexIt = MainIndex.find(tmpItem);
        if (mainIndexIt != MainIndex.end()) {
            value = &const_cast<TValue&>(mainIndexIt->Value);
            return true;
        }

        auto inIndexIt = InIndex.find(tmpItem);
        if (inIndexIt != InIndex.end()) {
            value = &const_cast<TValue&>(inIndexIt->Value);
            return true;
        }

        return false;
    }

    bool DoPop() {
        if (!InIndex.empty() && (InSize > Config->InSizeRatio * TSelf::GetUsedSize())) {
            auto inItem = InList.PopBack();

            TItemKey tmpItemKey(inItem->Key);
            auto outInsertedIt = OutIndex.insert(tmpItemKey).first;
            OutList.PushFront(const_cast<TItemKey*>(&*outInsertedIt));

            ui64 size = TSelf::Measure(inItem->Key, inItem->Value);
            this->OnEvict(inItem->Key, inItem->Value, size);
            auto inItemIt = InIndex.find(*inItem);
            Y_DEBUG_ABORT_UNLESS(inItemIt != InIndex.end());
            InIndex.erase(inItemIt);
            InSize -= size;

            if (OutIndex.size() > Config->OutKeyRatio * TSelf::GetCount()) {
                auto outItem = OutList.PopBack();
                auto outItemIt = OutIndex.find(*outItem);
                Y_DEBUG_ABORT_UNLESS(outItemIt != OutIndex.end());
                this->OnKeyEvict(outItemIt->Key);
                OutIndex.erase(outItemIt);
            }

            return true;
        }

        if (!MainIndex.empty()) {
            auto item = MainList.PopBack();
            ui64 size = TSelf::Measure(item->Key, item->Value);
            this->OnEvict(item->Key, item->Value, size);
            auto mainIndexIt = MainIndex.find(*item);
            Y_DEBUG_ABORT_UNLESS(mainIndexIt != MainIndex.end());
            MainIndex.erase(mainIndexIt);
            return true;
        }

        return false;
    }

    bool DoInsert(const TKey& key, const TValue& value, TValue*& currentValue) {
        TItem tmpItem(key, value);
        auto mainIndexIt = MainIndex.find(tmpItem);
        if (mainIndexIt != MainIndex.end()) {
            currentValue = const_cast<TValue*>(&mainIndexIt->Value);
            PromoteMain(mainIndexIt);
            return false;
        }

        auto inIndexIt = InIndex.find(tmpItem);
        if (inIndexIt != InIndex.end()) {
            currentValue = const_cast<TValue*>(&inIndexIt->Value);
            return false;
        }

        TItemKey tmpItemKey(key);
        auto outIndexIt = OutIndex.find(tmpItemKey);
        bool isOut = (outIndexIt != OutIndex.end());

        TSelf::PopWhileOverflow();

        ui64 size = TSelf::Measure(key, value);
        if (isOut) {
            auto insertedIt = MainIndex.insert(tmpItem).first;
            MainList.PushFront(const_cast<TItem*>(&*insertedIt));
            currentValue = const_cast<TValue*>(&insertedIt->Value);
        } else {
            auto insertedIt = InIndex.insert(tmpItem).first;
            InList.PushFront(const_cast<TItem*>(&*insertedIt));
            InSize += size;
            currentValue = const_cast<TValue*>(&insertedIt->Value);
        }

        this->OnInsert(key, value, size);
        return true;
    }

    bool DoErase(const TKey& key) {
        TItem tmpItem(key);
        auto mainIndexIt = MainIndex.find(tmpItem);
        if (mainIndexIt != MainIndex.end()) {
            auto item = const_cast<TItem*>(&*mainIndexIt);
            EraseFromOut(item->Key);
            ui64 size = TSelf::Measure(item->Key, item->Value);
            this->OnEvict(item->Key, item->Value, size);
            item->Unlink();
            MainIndex.erase(mainIndexIt);
            return true;
        }

        auto inIndexIt = InIndex.find(tmpItem);
        if (inIndexIt != InIndex.end()) {
            auto item = const_cast<TItem*>(&*inIndexIt);
            EraseFromOut(item->Key);
            ui64 size = TSelf::Measure(item->Key, item->Value);
            this->OnEvict(item->Key, item->Value, size);
            item->Unlink();
            InIndex.erase(inIndexIt);
            InSize -= size;
            return true;
        }

        return false;
    }

private:
    void PromoteMain(const typename TIndex::iterator& mainIndexIt) {
        auto item = const_cast<TItem*>(&*mainIndexIt);
        item->Unlink();
        MainList.PushFront(item);
    }

    void EraseFromOut(const TKey& key) {
        TItemKey tmpItemKey(key);
        auto outIndexIt = OutIndex.find(tmpItemKey);
        if (outIndexIt != OutIndex.end()) {
            this->OnKeyEvict(key);
            auto item = const_cast<TItemKey*>(&*outIndexIt);
            item->Unlink();
            OutIndex.erase(outIndexIt);
        }
    }

private:
    const TIntrusivePtr<T2QCacheConfig> Config;
    TIndex MainIndex;
    TListType MainList;
    TIndex InIndex;
    TListType InList;
    ui64 InSize;
    TKeyIndex OutIndex;
    TKeyList OutList;
};

}
}
