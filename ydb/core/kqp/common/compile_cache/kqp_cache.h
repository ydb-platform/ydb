#pragma once

#include "cache_storage.h"

#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NKikimr::NKqp {

// OnDropped and OnCleared run under Lock. Declare TDropped before the guard so
// destruction of evicted query plans or peephole fragments happens after unlocking.
template <typename TDerived, typename TKey, typename TValue, typename TWeight = TUniformSizeProvider<TValue>>
class TKqpCache {
public:
    using TStorage = TCacheStorage<TKey, TValue, TWeight>;
    using TEvicted = typename TStorage::TEvicted;
    using TDropped = typename TStorage::TDropped;

    TKqpCache(size_t maxSize, TDuration ttl)
        : Storage(maxSize, ttl)
    {}

    TValue Find(const TKey& key, bool promote) {
        const TInstant now = NowFor(promote);
        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);
        const TValue* found = FindEntryLocked(key, promote, now, dropped);
        return found ? *found : TValue();
    }

    size_t Insert(const TKey& key, TValue value) {
        const TInstant now = TDerived::Now();
        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);
        return InsertLocked(key, std::move(value), now, dropped);
    }

    bool Erase(const TKey& key) {
        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);
        return EraseLocked(key, dropped);
    }

    size_t EraseExpired() {
        const TInstant now = TDerived::Now();
        TDropped dropped;
        TGuard<TAdaptiveLock> guard(Lock);
        return EraseExpiredLocked(now, dropped);
    }

    void Clear() {
        TVector<TValue> dropped;
        TGuard<TAdaptiveLock> guard(Lock);
        Storage.Clear(dropped);
        Self().OnCleared();
    }

    size_t Size() const {
        TGuard<TAdaptiveLock> guard(Lock);
        return Storage.Size();
    }

    size_t TotalWeight() const {
        TGuard<TAdaptiveLock> guard(Lock);
        return Storage.TotalWeight();
    }

protected:
    TInstant NowFor(bool promote) const {
        return (promote || Storage.HasTtl()) ? TDerived::Now() : TInstant::Zero();
    }

    template <typename TShouldPromote>
    TValue* FindEntryLocked(const TKey& key, TShouldPromote promote, TInstant now, TDropped& dropped) {
        const size_t before = dropped.size();
        auto* found = Storage.Find(key, promote, now, dropped);
        LazyExpired += Notify(before, dropped);
        return found;
    }

    size_t InsertLocked(const TKey& key, TValue value, TInstant now, TDropped& dropped) {
        const size_t before = dropped.size();
        const size_t evicted = Storage.Insert(key, std::move(value), now, dropped);
        Notify(before, dropped);
        return evicted;
    }

    bool EraseLocked(const TKey& key, TDropped& dropped) {
        const size_t before = dropped.size();
        Storage.Erase(key, dropped);
        return Notify(before, dropped) != 0;
    }

    // Reports the entries retired on lookup since the previous sweep as well, so the owner's
    // eviction accounting sees every expiry exactly once.
    size_t EraseExpiredLocked(TInstant now, TDropped& dropped) {
        const size_t before = dropped.size();
        Storage.EraseExpired(now, dropped);
        return Notify(before, dropped) + std::exchange(LazyExpired, size_t(0));
    }

    size_t SizeLocked() const {
        return Storage.Size();
    }

    template <typename F>
    void ForEachLocked(F&& f) const {
        Storage.ForEach(std::forward<F>(f));
    }

    mutable TAdaptiveLock Lock;

private:
    size_t Notify(size_t from, const TDropped& dropped) {
        for (size_t i = from; i < dropped.size(); ++i) {
            Self().OnDropped(dropped[i]);
        }
        return dropped.size() - from;
    }

    TDerived& Self() {
        return static_cast<TDerived&>(*this);
    }

    TStorage Storage;
    size_t LazyExpired = 0;   // expired entries retired by lookups, not yet reported by a sweep
};

} // namespace NKikimr::NKqp
