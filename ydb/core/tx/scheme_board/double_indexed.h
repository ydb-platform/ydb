#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NKikimr {

namespace NPrivate {

// used in TDoubleIndexedMap
struct TDefaultBaseItem {};

// used in TDoubleIndexedCache
struct TTimestampedItem {
    TInstant LastAccess;
};

template <
    typename TPrimaryKey,
    typename TSecondaryKey,
    typename TValue,
    typename TMergeFunc,
    template <typename...> typename TPIndex = THashMap,
    template <typename...> typename TSIndex = THashMap,
    template <typename...> typename TValueContainer = TList,
    typename TBaseItem = TDefaultBaseItem
>
class TDoubleIndexedMapImpl {
    struct TItem;

public:
    using TStorage = TValueContainer<TItem>;
    using TPrimaryIndex = TPIndex<TPrimaryKey, typename TStorage::iterator>;
    using TSecondaryIndex = TSIndex<TSecondaryKey, typename TStorage::iterator>;

private:
    struct TItem: public TBaseItem {
        TValue Value;
        typename TPrimaryIndex::iterator PrimaryIndexIterator;
        typename TSecondaryIndex::iterator SecondaryIndexIterator;

        explicit TItem(
            const TValue& value,
            typename TPrimaryIndex::iterator primaryIndexIterator,
            typename TSecondaryIndex::iterator secondaryIndexIterator)
            : Value(value)
            , PrimaryIndexIterator(primaryIndexIterator)
            , SecondaryIndexIterator(secondaryIndexIterator)
        {
        }

        explicit TItem(
            TValue&& value,
            typename TPrimaryIndex::iterator primaryIndexIterator,
            typename TSecondaryIndex::iterator secondaryIndexIterator) noexcept
            : Value(std::move(value))
            , PrimaryIndexIterator(primaryIndexIterator)
            , SecondaryIndexIterator(secondaryIndexIterator)
        {
        }
    };

protected:
    template <typename TV>
    void EmplaceInStorage(TV&& val) {
        Storage.emplace_back(TItem(std::forward<TV>(val), PrimaryIndex.end(), SecondaryIndex.end()));
    }

    template <typename TK, typename std::enable_if_t<std::is_same_v<std::decay_t<TK>, TPrimaryKey>, int> = 0>
    auto EmplaceInIndex(TK&& key) {
        return PrimaryIndex.emplace(std::forward<TK>(key), Storage.end());
    }

    template <typename TK, typename std::enable_if_t<std::is_same_v<std::decay_t<TK>, TSecondaryKey>, int> = 0>
    auto EmplaceInIndex(TK&& key) {
        return SecondaryIndex.emplace(std::forward<TK>(key), Storage.end());
    }

    static void CrossSetIterators(typename TStorage::iterator st, typename TPrimaryIndex::iterator ind) {
        st->PrimaryIndexIterator = ind;
        ind->second = st;
    }

    static void CrossSetIterators(typename TStorage::iterator st, typename TSecondaryIndex::iterator ind) {
        st->SecondaryIndexIterator = ind;
        ind->second = st;
    }

    static typename TPrimaryIndex::iterator& IndexIterator(typename TPrimaryIndex::iterator it) {
        return it->second->PrimaryIndexIterator;
    }

    static typename TSecondaryIndex::iterator& IndexIterator(typename TSecondaryIndex::iterator it) {
        return it->second->SecondaryIndexIterator;
    }

    static typename TSecondaryIndex::iterator AnotherIndexIterator(typename TPrimaryIndex::iterator it) {
        return it->second->SecondaryIndexIterator;
    }

    static typename TPrimaryIndex::iterator AnotherIndexIterator(typename TSecondaryIndex::iterator it) {
        return it->second->PrimaryIndexIterator;
    }

    TSecondaryIndex& AnotherIndex(typename TPrimaryIndex::iterator) {
        return SecondaryIndex;
    }

    TPrimaryIndex& AnotherIndex(typename TSecondaryIndex::iterator) {
        return PrimaryIndex;
    }

    template <typename TPi, typename TSi>
    void DeleteIndex(const typename TPi::key_type& key, TPi& pIndex, TSi& sIndex) {
        auto pIt = pIndex.find(key);
        if (pIt == pIndex.end()) {
            return;
        }

        // storage entry must be kept by other index, we clean only one
        Y_ABORT_UNLESS(AnotherIndexIterator(pIt) != sIndex.end());

        IndexIterator(pIt) = pIndex.end();
        pIndex.erase(pIt);
    }

    template <typename TPi, typename TSi>
    void Erase(const typename TPi::key_type& key, TPi& pIndex, TSi& sIndex) {
        auto pIt = pIndex.find(key);
        if (pIt == pIndex.end()) {
            return;
        }

        auto sIt = AnotherIndexIterator(pIt);
        if (sIt != sIndex.end()) {
            sIndex.erase(sIt);
        }

        Storage.erase(pIt->second);
        pIndex.erase(pIt);
    }

    template <typename TI>
    static TValue* FindPtr(const typename TI::key_type& key, TI& index) {
        auto it = index.find(key);
        if (it == index.end()) {
            return nullptr;
        }

        return &it->second->Value;
    }

public:
    explicit TDoubleIndexedMapImpl(const TMergeFunc& mf = TMergeFunc())
        : Merge(mf)
    {
    }

    template <typename TK, typename TV>
    TValue& Upsert(TK&& key, TV&& val) {
        auto it = EmplaceInIndex(std::forward<TK>(key));

        if (it.second) {
            EmplaceInStorage(std::forward<TV>(val));
            CrossSetIterators(std::prev(Storage.end()), it.first);
        } else {
            Merge(it.first->second->Value, std::forward<TV>(val));
        }

        return it.first->second->Value;
    }

    template <typename TPk, typename TSk, typename TV>
    TValue& Upsert(TPk&& pKey, TSk&& sKey, TV&& val) {
        auto pIt = EmplaceInIndex(std::forward<TPk>(pKey));
        auto sIt = EmplaceInIndex(std::forward<TSk>(sKey));

        if (pIt.second && sIt.second) {
            // no pKey, no sKey, insert new element

            EmplaceInStorage(std::forward<TV>(val));
            auto it = std::prev(Storage.end());
            CrossSetIterators(it, pIt.first);
            CrossSetIterators(it, sIt.first);
        } else if (!pIt.second && sIt.second) {
            // has pKey, no sKey, merge val into exited pKey

            Y_ABORT_UNLESS(AnotherIndexIterator(pIt.first) == AnotherIndex(pIt.first).end());
            Merge(pIt.first->second->Value, std::forward<TV>(val));
            CrossSetIterators(pIt.first->second, sIt.first);
        } else if (pIt.second && !sIt.second) {
            // no pKey, has sKey, merge val into exited sKey

            Y_ABORT_UNLESS(AnotherIndexIterator(sIt.first) == AnotherIndex(sIt.first).end());
            Merge(sIt.first->second->Value, std::forward<TV>(val));
            CrossSetIterators(sIt.first->second, pIt.first);
        } else if (pIt.first->second != sIt.first->second) {
            // has pKey, has sKey, pKey is not equal to sKey, merge val into merged pKey and sKey

            Merge(pIt.first->second->Value, std::forward<TV>(Merge(sIt.first->second->Value, std::forward<TV>(val))));
            Storage.erase(sIt.first->second);
            CrossSetIterators(pIt.first->second, sIt.first);
        } else {
            // has pKey, has sKey, pKey is equal to sKey, merge val into existed item;

            Merge(pIt.first->second->Value, std::forward<TV>(val));
        }

        return pIt.first->second->Value;
    }

    void DeleteIndex(const TPrimaryKey& key) {
        DeleteIndex(key, PrimaryIndex, SecondaryIndex);
    }

    void DeleteIndex(const TSecondaryKey& key) {
        DeleteIndex(key, SecondaryIndex, PrimaryIndex);
    }

    void Erase(const TPrimaryKey& key) {
        Erase(key, PrimaryIndex, SecondaryIndex);
    }

    void Erase(const TSecondaryKey& key) {
        Erase(key, SecondaryIndex, PrimaryIndex);
    }

    TValue* FindPtr(const TPrimaryKey& key) {
        return FindPtr(key, PrimaryIndex);
    }

    const TValue* FindPtr(const TPrimaryKey& key) const {
        return FindPtr(key, PrimaryIndex);
    }

    TValue* FindPtr(const TSecondaryKey& key) {
        return FindPtr(key, SecondaryIndex);
    }

    const TValue* FindPtr(const TSecondaryKey& key) const {
        return FindPtr(key, SecondaryIndex);
    }

    auto Size() const {
        return StorageSize();
    }

    auto StorageSize() const {
        return Storage.size();
    }

    const TPrimaryIndex& GetPrimaryIndex() const {
        return PrimaryIndex;
    }

    const TSecondaryIndex& GetSecondaryIndex() const {
        return SecondaryIndex;
    }

protected:
    TStorage Storage;
    TPrimaryIndex PrimaryIndex;
    TSecondaryIndex SecondaryIndex;

private:
    TMergeFunc Merge;

}; // TDoubleIndexedMapImpl

} // NPrivate

template <
    typename TPrimaryKey,
    typename TSecondaryKey,
    typename TValue,
    typename TMergeFunc,
    template <typename...> typename TPIndex = THashMap,
    template <typename...> typename TSIndex = THashMap,
    template <typename...> typename TValueContainer = TList
>
class TDoubleIndexedMap: public NPrivate::TDoubleIndexedMapImpl<
    TPrimaryKey,
    TSecondaryKey,
    TValue,
    TMergeFunc,
    TPIndex,
    TSIndex,
    TValueContainer> {

public:
    explicit TDoubleIndexedMap(const TMergeFunc& mf = TMergeFunc())
        : NPrivate::TDoubleIndexedMapImpl<
              TPrimaryKey,
              TSecondaryKey,
              TValue,
              TMergeFunc,
              TPIndex,
              TSIndex,
              TValueContainer>(mf)
    {
    }
};

template <
    typename TPrimaryKey,
    typename TSecondaryKey,
    typename TValue,
    typename TMergeFunc,
    typename TEvictFunc,
    template <typename...> typename TPIndex = THashMap,
    template <typename...> typename TSIndex = THashMap,
    template <typename...> typename TValueContainer = TList
>
class TDoubleIndexedCache: public NPrivate::TDoubleIndexedMapImpl<
    TPrimaryKey,
    TSecondaryKey,
    TValue,
    TMergeFunc,
    TPIndex,
    TSIndex,
    TValueContainer,
    NPrivate::TTimestampedItem> {

public:
    typedef TInstant (*TInstantGetter)();

private:
    template <typename TI>
    void Promote(const typename TI::key_type& key, TI& index) {
        auto it = index.find(key);
        if (it == index.end()) {
            return;
        }

        this->Storage.splice(this->Storage.end(), this->Storage, it->second);
        auto st = std::prev(this->Storage.end());

        if (st->PrimaryIndexIterator != this->PrimaryIndex.end()) {
            this->CrossSetIterators(st, st->PrimaryIndexIterator);
        }

        if (st->SecondaryIndexIterator != this->SecondaryIndex.end()) {
            this->CrossSetIterators(st, st->SecondaryIndexIterator);
        }

        st->LastAccess = Now();
    }

public:
    explicit TDoubleIndexedCache(
            const TDuration keep,
            TInstantGetter now,
            const TMergeFunc& mf = TMergeFunc(),
            const TEvictFunc& ef = TEvictFunc())
        : NPrivate::TDoubleIndexedMapImpl<
              TPrimaryKey,
              TSecondaryKey,
              TValue,
              TMergeFunc,
              TPIndex,
              TSIndex,
              TValueContainer,
              NPrivate::TTimestampedItem>(mf)
        , Keep(keep)
        , Now(now)
        , OnEvict(ef)
    {
    }

    void Promote(const TPrimaryKey& key) {
        Promote(key, this->PrimaryIndex);
    }

    void Promote(const TSecondaryKey& key) {
        Promote(key, this->SecondaryIndex);
    }

    bool Shrink() {
        if (this->Storage.empty()) {
            return false;
        }

        auto& item = this->Storage.front();
        if ((Now() - item.LastAccess) < Keep) {
            return false;
        }

        if (item.PrimaryIndexIterator != this->PrimaryIndex.end()) {
            this->PrimaryIndex.erase(item.PrimaryIndexIterator);
        }
        if (item.SecondaryIndexIterator != this->SecondaryIndex.end()) {
            this->SecondaryIndex.erase(item.SecondaryIndexIterator);
        }

        OnEvict(item.Value);
        this->Storage.pop_front();

        return true;
    }

private:
    TDuration Keep;
    TInstantGetter Now;
    TEvictFunc OnEvict;

}; // TDoubleIndexedCache

} // NKikimr
