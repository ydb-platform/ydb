#pragma once

#include "cache_storage.h"

#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NKikimr::NKqp {

// Thread-safe owner over TCacheStorage. The hooks are bound at compile time (CRTP), so the base
// itself makes no virtual calls; TDerived::Now() may (TimeProvider in production) and is called
// before the lock. TDerived must define, and befriend this base so they can stay private:
//   void OnDropped(const TEvicted&)   called under Lock for every entry that leaves the storage
//                                     one at a time: eviction, replacement, erase, expiry;
//   void OnCleared()                  called under Lock after Clear() emptied the storage;
//   static TInstant Now()             the clock, TAppData::TimeProvider in production code.
// There are no defaults on purpose: a misspelled hook or a forgotten clock fails to compile
// instead of silently doing nothing.
//
// Nothing that can be done outside the critical section is done inside it: the clock is read
// before the lock is taken, and values that leave the storage are destroyed after it is released.
// The caller declares a TGraveyard before taking the lock and the *Locked methods append dropped
// entries to it; the storage writes into the same buffer. Freeing a compiled plan under the lock
// would stall every session on the node.
template <typename TDerived, typename TKey, typename TValue, typename TPolicy>
class TKqpCache {
public:
    using TStorage = TCacheStorage<TKey, TValue, TPolicy>;
    using TEvicted = typename TStorage::TEvicted;
    using TGraveyard = typename TStorage::TDropped;

    struct TInsertResult {
        bool Admitted = false;   // false: the policy rejected the value, nothing was stored
        size_t Dropped = 0;      // entries that left the storage: replaced or evicted
    };

    TKqpCache(const TCacheLimits& limits, TDuration ttl)
        : Storage(limits, ttl)
    {}

    TValue Find(const TKey& key, bool promote) {
        const TInstant now = NowFor(promote);
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        const TValue* found = FindEntryLocked(key, promote, now, graveyard);
        return found ? *found : TValue();
    }

    // Replaces under the same key.
    TInsertResult Insert(const TKey& key, TValue value) {
        const TInstant now = TDerived::Now();
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        return InsertLocked(key, std::move(value), now, graveyard);
    }

    bool Erase(const TKey& key) {
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        return EraseLocked(key, graveyard);
    }

    size_t EraseExpired() {
        const TInstant now = TDerived::Now();
        TGraveyard graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        return EraseExpiredLocked(now, graveyard);
    }

    void Clear() {
        TVector<TValue> graveyard;
        TGuard<TAdaptiveLock> guard(Lock);
        Storage.Clear(graveyard);
        Self().OnCleared();
    }

    size_t Size() const {
        TGuard<TAdaptiveLock> guard(Lock);
        return Storage.Size();
    }

    // In TPolicy::Size units.
    ui64 TotalBytes() const {
        TGuard<TAdaptiveLock> guard(Lock);
        return Storage.GetTotalBytes();
    }

protected:
    // The clock is read only when a lookup needs it: promotion refreshes the TTL and an enabled
    // TTL must be checked. Call it before taking the lock.
    TInstant NowFor(bool promote) const {
        return (promote || Storage.HasTtl()) ? TDerived::Now() : TInstant::Zero();
    }

    // Caller holds Lock. The pointer must not escape the critical section or outlive removal or
    // replacement of the entry; a mutation that changes TPolicy::Size must be followed by
    // ReaccountLocked(). An expired entry is dropped on the spot (through OnDropped, into
    // `graveyard`) and reported as a miss.
    template <typename TShouldPromote>
    TValue* FindEntryLocked(const TKey& key, TShouldPromote promote, TInstant now, TGraveyard& graveyard) {
        if (TValue* value = Storage.Find(key, promote, now)) {
            return value;
        }
        const size_t before = graveyard.size();
        Storage.EraseIfExpired(key, now, graveyard);
        LazyExpired += Notify(before, graveyard);
        return nullptr;
    }

    TInsertResult InsertLocked(const TKey& key, TValue value, TInstant now, TGraveyard& graveyard) {
        const size_t before = graveyard.size();
        const bool admitted = Storage.Insert(key, std::move(value), now, graveyard);
        return {admitted, Notify(before, graveyard)};
    }

    // Caller holds Lock and has just mutated the entry in place.
    // Returns false if the policy no longer admits it and it was dropped: any pointer to it is
    // dangling then.
    bool ReaccountLocked(const TKey& key, TGraveyard& graveyard) {
        const size_t before = graveyard.size();
        const bool kept = Storage.Reaccount(key, graveyard);
        Notify(before, graveyard);
        return kept;
    }

    bool EraseLocked(const TKey& key, TGraveyard& graveyard) {
        const size_t before = graveyard.size();
        Storage.Erase(key, graveyard);
        return Notify(before, graveyard) != 0;
    }

    // Reports the entries retired on lookup since the previous sweep as well, so the owner's
    // eviction accounting sees every expiry exactly once.
    size_t EraseExpiredLocked(TInstant now, TGraveyard& graveyard) {
        const size_t before = graveyard.size();
        Storage.EraseExpired(now, graveyard);
        return Notify(before, graveyard) + std::exchange(LazyExpired, size_t(0));
    }

    size_t SizeLocked() const {
        return Storage.Size();
    }

    ui64 TotalBytesLocked() const {
        return Storage.GetTotalBytes();
    }

    // Caller holds Lock; f(key, value) must not touch the cache.
    template <typename F>
    void ForEachLocked(F&& f) const {
        Storage.ForEach(std::forward<F>(f));
    }

    mutable TAdaptiveLock Lock;

private:
    // Reports every entry the storage appended to `graveyard` since `from`.
    size_t Notify(size_t from, const TGraveyard& graveyard) {
        for (size_t i = from; i < graveyard.size(); ++i) {
            Self().OnDropped(graveyard[i]);
        }
        return graveyard.size() - from;
    }

    TDerived& Self() {
        return static_cast<TDerived&>(*this);
    }

    TStorage Storage;
    size_t LazyExpired = 0;   // expired entries retired by lookups, not yet reported by a sweep
};

} // namespace NKikimr::NKqp
