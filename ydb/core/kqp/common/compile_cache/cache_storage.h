#pragma once

#include <library/cpp/cache/cache.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr::NKqp {

// The owner holds the lock and destroys dropped values after unlocking.
// Weight is immutable while the value is stored.
template <typename TKey, typename TValue, typename TWeight = TUniformSizeProvider<TValue>>
class TCacheStorage {
public:
    struct TEvicted {
        TKey Key;
        TValue Value;
    };
    using TDropped = TVector<TEvicted>;

    TCacheStorage(size_t maxSize, TDuration ttl)
        : Ttl(ttl)
        , List(maxSize)
    {}

    TValue* Find(const TKey& key, bool promote, TInstant now, TDropped& dropped) {
        return Find(key, [promote](const TValue&) { return promote; }, now, dropped);
    }

    template <typename TShouldPromote>
    TValue* Find(const TKey& key, TShouldPromote shouldPromote, TInstant now, TDropped& dropped) {
        auto it = Index.find(key);
        if (it == Index.end()) {
            return nullptr;
        }
        TItem& item = Mutable(*it);
        if (Expired(item, now)) {
            List.Erase(&item);
            dropped.push_back(Drop(it));
            return nullptr;
        }
        if (shouldPromote(item.Value.Value)) {
            item.Value.TouchedAt = TouchTime(now);
            List.Promote(&item);
        }
        return &item.Value.Value;
    }

    // Only removals due to capacity count as evictions.
    size_t Insert(const TKey& key, TValue value, TInstant now, TDropped& dropped) {
        Erase(key, dropped);
        const size_t before = dropped.size();
        auto [it, inserted] = Index.emplace(key, TEntry{std::move(value), TouchTime(now)});
        Y_ABORT_UNLESS(inserted);
        for (auto* item = List.Insert(&Mutable(*it)); item; item = List.RemoveIfOverflown()) {
            dropped.push_back(Drop(Index.find(item->Key)));
        }
        return dropped.size() - before;
    }

    bool Erase(const TKey& key, TDropped& dropped) {
        auto it = Index.find(key);
        if (it == Index.end()) {
            return false;
        }
        List.Erase(&Mutable(*it));
        dropped.push_back(Drop(it));
        return true;
    }

    size_t EraseExpired(TInstant now, TDropped& dropped) {
        if (!HasTtl()) {
            return 0;
        }
        const size_t before = dropped.size();
        while (List.GetSize() && Expired(*List.GetOldest(), now)) {
            TItem* item = List.GetOldest();
            List.Erase(item);
            dropped.push_back(Drop(Index.find(item->Key)));
        }
        return dropped.size() - before;
    }

    void Clear(TVector<TValue>& dropped) {
        dropped.reserve(dropped.size() + Index.size());
        for (const TItem& item : Index) {
            dropped.push_back(std::move(Mutable(item).Value.Value));
        }
        Index.clear();   // item destructors unlink from the list; counters need the reset below
        List = TList(List.GetMaxSize());
    }

    template <typename F>
    void ForEach(F&& f) const {
        for (const TItem& item : Index) {
            f(item.Key, item.Value.Value, item.Value.TouchedAt);
        }
    }

    bool HasTtl() const { return Ttl != TDuration::Zero(); }
    size_t Size() const { return Index.size(); }
    size_t TotalWeight() const { return List.GetTotalSize(); }

private:
    struct TEntry {
        TValue Value;
        TInstant TouchedAt;
    };

    struct TEntryWeight {
        size_t operator()(const TEntry& entry) const { return TWeight{}(entry.Value); }
    };

    using TList = TLRUList<TKey, TEntry, TEntryWeight>;
    using TItem = typename TList::TItem;

    using TIndex = THashSet<TItem, typename TItem::THash, typename TItem::TEqualTo>;

    // Only the key participates in the hash.
    static TItem& Mutable(const TItem& item) {
        return const_cast<TItem&>(item);
    }

    // Callers sample the clock before locking. Keep expiry ordered along the LRU.
    TInstant TouchTime(TInstant now) {
        LastTouch = Max(LastTouch, now);
        return LastTouch;
    }

    bool Expired(const TItem& item, TInstant now) const {
        return HasTtl() && item.Value.TouchedAt + Ttl <= now;
    }

    TEvicted Drop(typename TIndex::iterator it) {
        Y_ABORT_UNLESS(it != Index.end());
        TItem& item = Mutable(*it);
        TEvicted evicted{item.Key, std::move(item.Value.Value)};
        Index.erase(it);
        return evicted;
    }

    const TDuration Ttl;

    TIndex Index;
    TList List;
    TInstant LastTouch = TInstant::Zero();
};

} // namespace NKikimr::NKqp
