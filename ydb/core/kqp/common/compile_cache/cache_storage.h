#pragma once

#include <library/cpp/cache/cache.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr::NKqp {

// Values of a cache budget. 0 means no limit. Which of them a cache enforces is decided at compile
// time by its policy type (see TCacheStorage); the values come at construction.
struct TCacheLimits {
    size_t MaxEntries = 0;
    ui64 MaxBytes = 0;
    ui64 MaxEntryBytes = 0;     // a value bigger than this is not admitted at all
};

// LRU + TTL over TLRUList with a budget chosen at compile time by TPolicy:
//   static ui64 Size(const TValue&)        accounted size of a value, in bytes
//   static constexpr bool LimitEntries     enforce TCacheLimits::MaxEntries
//   static constexpr bool LimitBytes       enforce TCacheLimits::MaxBytes
//   static constexpr bool LimitEntryBytes  enforce TCacheLimits::MaxEntryBytes (admission)
// The checks a policy turns off are not compiled in. Total bytes are always tracked.
//
// Not thread-safe; the owner locks and maintains its own indexes from the dropped entries. Every
// operation that removes entries appends them to a caller-provided `dropped` buffer: the owner
// passes one buffer through all layers and destroys the values after it releases its lock.
template <typename TKey, typename TValue, typename TPolicy>
class TCacheStorage {
public:
    struct TEvicted {
        TKey Key;
        TValue Value;
    };
    using TDropped = TVector<TEvicted>;

    // ttl == Zero disables expiry. The list itself is unbounded; the budget is enforced here.
    TCacheStorage(const TCacheLimits& limits, TDuration ttl)
        : Limits(limits)
        , Ttl(ttl)
        , List(Max<size_t>())
    {}

    // Borrows the stored value until this key is removed/replaced or the storage is cleared. A
    // mutation through the pointer that changes TPolicy::Size must be followed by Reaccount().
    // An expired entry is a miss.
    TValue* Find(const TKey& key, bool promote, TInstant now) {
        auto it = Index.find(key);
        if (it == Index.end()) {
            return nullptr;
        }
        TItem& item = Mutable(*it);
        if (HasTtl() && item.Value.ExpiredAt <= now) {
            return nullptr;
        }
        if (promote) {
            Touch(item, now);
        }
        return &item.Value.Value;
    }

    // Replaces under the same key and appends the replaced entry (if any) and everything evicted by
    // the budget to `dropped`. Returns whether the value was admitted: a value the policy rejects
    // is not stored, and the old entry under the key is dropped anyway since it is superseded.
    bool Insert(const TKey& key, TValue value, TInstant now, TDropped& dropped) {
        Erase(key, dropped);
        const ui64 bytes = TPolicy::Size(value);
        if (!Admits(bytes)) {
            return false;
        }
        auto inserted = Index.emplace(key, TEntry{std::move(value), now + Ttl, bytes});
        Y_ABORT_UNLESS(inserted.second);
        TItem* item = &Mutable(*inserted.first);
        Y_ABORT_UNLESS(!List.Insert(item));   // the list has no budget of its own
        TotalBytes += bytes;
        EvictOverBudget(item, dropped);
        return true;
    }

    // Re-reads the size of an entry mutated in place and counts it as a use, like a promoting
    // Find. Evicts other entries while over budget. Returns false if the policy no longer admits
    // the entry and it was dropped.
    bool Reaccount(const TKey& key, TInstant now, TDropped& dropped) {
        auto it = Index.find(key);
        if (it == Index.end()) {
            return false;
        }
        TItem& item = Mutable(*it);
        const ui64 bytes = TPolicy::Size(item.Value.Value);
        TotalBytes = TotalBytes - item.Value.Bytes + bytes;
        item.Value.Bytes = bytes;
        if (!Admits(bytes)) {
            List.Erase(&item);
            dropped.push_back(Drop(it));
            return false;
        }
        Touch(item, now);
        EvictOverBudget(&item, dropped);
        return true;
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

    // Drops the entry if it exists and is past its TTL; the hit path uses it to retire an
    // expired entry on access instead of serving it or leaving it to the next sweep.
    bool EraseIfExpired(const TKey& key, TInstant now, TDropped& dropped) {
        if (!HasTtl()) {
            return false;
        }
        auto it = Index.find(key);
        if (it == Index.end() || it->Value.ExpiredAt > now) {
            return false;
        }
        List.Erase(&Mutable(*it));
        dropped.push_back(Drop(it));
        return true;
    }

    // Sweeps expired entries oldest first; returns how many were appended.
    size_t EraseExpired(TInstant now, TDropped& dropped) {
        if (!HasTtl()) {
            return 0;
        }
        const size_t before = dropped.size();
        while (List.GetSize() && List.GetOldest()->Value.ExpiredAt <= now) {
            TItem* item = List.GetOldest();
            List.Erase(item);
            dropped.push_back(Drop(Index.find(item->Key)));
        }
        return dropped.size() - before;
    }

    // Empties the storage. The values are moved into `graveyard` so that their destructors run
    // wherever the caller chooses (outside the owner's lock).
    void Clear(TVector<TValue>& graveyard) {
        graveyard.reserve(graveyard.size() + Index.size());
        for (const TItem& item : Index) {
            graveyard.push_back(std::move(Mutable(item).Value.Value));
        }
        Index.clear();   // item destructors unlink from the list; counters need the reset below
        List = TList(Max<size_t>());
        TotalBytes = 0;
    }

    // Visits every entry as f(key, value) in no particular order; expired entries included.
    template <typename F>
    void ForEach(F&& f) const {
        for (const TItem& item : Index) {
            f(item.Key, item.Value.Value);
        }
    }

    bool HasTtl() const { return Ttl != TDuration::Zero(); }
    size_t Size() const { return Index.size(); }
    ui64 GetTotalBytes() const { return TotalBytes; }

private:
    struct TEntry {
        TValue Value;
        TInstant ExpiredAt;
        ui64 Bytes = 0;     // as accounted in TotalBytes; the value may have changed since
    };

    // The list only orders entries; its own size budget is disabled (unbounded, one per entry).
    struct TOnePerEntry {
        size_t operator()(const TEntry&) const { return 1; }
    };

    using TList = TLRUList<TKey, TEntry, TOnePerEntry>;
    using TItem = typename TList::TItem;

    // Lookups go by TKey directly; no temporary TItem with a copied key and an empty value.
    struct TItemHash {
        size_t operator()(const TItem& item) const { return THash<TKey>()(item.Key); }
        size_t operator()(const TKey& key) const { return THash<TKey>()(key); }
    };
    struct TItemEqual {
        bool operator()(const TItem& a, const TItem& b) const { return a.Key == b.Key; }
        bool operator()(const TItem& item, const TKey& key) const { return item.Key == key; }
    };
    using TIndex = THashSet<TItem, TItemHash, TItemEqual>;

    // THashSet hands out its elements as const because the key feeds the hash. The list links and
    // the entry live in the same item and change in place; the key never does, so the set stays
    // valid. TCache in library/cpp/cache does the same.
    static TItem& Mutable(const TItem& item) {
        return const_cast<TItem&>(item);
    }

    // A use: moves the item to the fresh end of the list and restarts its TTL. The two always go
    // together, so the list stays ordered by expiry and EraseExpired can stop at the first live
    // entry. TLRUList knows nothing about TTL; this is the only place that ties them.
    void Touch(TItem& item, TInstant now) {
        item.Value.ExpiredAt = now + Ttl;
        List.Promote(&item);
    }

    bool Admits(ui64 bytes) const {
        if constexpr (TPolicy::LimitEntryBytes) {
            return !Limits.MaxEntryBytes || bytes <= Limits.MaxEntryBytes;
        } else {
            Y_UNUSED(bytes);
            return true;
        }
    }

    bool OverBudget() const {
        bool over = false;
        if constexpr (TPolicy::LimitEntries) {
            over = over || (Limits.MaxEntries && Index.size() > Limits.MaxEntries);
        }
        if constexpr (TPolicy::LimitBytes) {
            over = over || (Limits.MaxBytes && TotalBytes > Limits.MaxBytes);
        }
        return over;
    }

    // Evicts the least recently used entries while over budget; `keep` (the newest) always stays.
    void EvictOverBudget(const TItem* keep, TDropped& dropped) {
        while (OverBudget()) {
            TItem* oldest = List.GetOldest();
            if (!oldest || oldest == keep) {
                break;
            }
            List.Erase(oldest);
            dropped.push_back(Drop(Index.find(oldest->Key)));
        }
    }

    // The item must already be unlinked from the list.
    TEvicted Drop(typename TIndex::iterator it) {
        Y_ABORT_UNLESS(it != Index.end());
        TItem& item = Mutable(*it);
        TotalBytes -= item.Value.Bytes;
        TEvicted evicted{item.Key, std::move(item.Value.Value)};
        Index.erase(it);
        return evicted;
    }

    const TCacheLimits Limits;
    const TDuration Ttl;

    TIndex Index;
    TList List;
    ui64 TotalBytes = 0;
};

} // namespace NKikimr::NKqp
