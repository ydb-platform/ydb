#ifndef MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include map.h"
// For the sake of sane code completion.
#include "map.h"
#endif
#undef MAP_INL_H_

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash, class TEqual, class TLock>
TSyncMap<TKey, TValue, THash, TEqual, TLock>::TSyncMap()
    : Snapshot_(new TSnapshot{})
{ }

template <class TKey, class TValue, class THash, class TEqual, class TLock>
TSyncMap<TKey, TValue, THash, TEqual, TLock>::~TSyncMap()
{
    delete Snapshot_.load();
}

template <class TKey, class TValue, class THash, class TEqual, class TLock>
template <class TFindKey>
TValue* TSyncMap<TKey, TValue, THash, TEqual, TLock>::Find(const TFindKey& key)
{
    {
        auto snapshot = AcquireSnapshot();

        if (auto it = snapshot->Map->find(key); it != snapshot->Map->end()) {
            return &(it->second->Value);
        }

        if (!snapshot->Dirty) {
            return nullptr;
        }
    }

    {
        auto guard = Guard(Lock_);

        OnMiss();

        auto* snapshot = Snapshot_.load();

        // Do another lookup, in case dirty was promoted.
        if (auto it = snapshot->Map->find(key); it != snapshot->Map->end()) {
            return &(it->second->Value);
        }

        if (!snapshot->Dirty) {
            return nullptr;
        }

        if (auto it = DirtyMap_->find(key); it != DirtyMap_->end()) {
            return &(it->second->Value);
        }

        return nullptr;
    }
}

template <class TKey, class TValue, class THash, class TEqual, class TLock>
template <class TCtor, class TFindKey>
std::pair<TValue*, bool> TSyncMap<TKey, TValue, THash, TEqual, TLock>::FindOrInsert(
    const TFindKey& key,
    TCtor&& ctor)
{
    return FindOr(
        key,
        [] (TCtor&& ctor) {
            return New<TEntry>(ctor());
        },
        std::forward<TCtor>(ctor));
}

template <class TKey, class TValue, class THash, class TEqual, class TLock>
template <class TFindKey, class... TArgs>
std::pair<TValue*, bool> TSyncMap<TKey, TValue, THash, TEqual, TLock>::FindOrEmplace(
    const TFindKey& key,
    TArgs&&... args)
{
    return FindOr(
        key,
        [] (TArgs&&... args) {
            return New<TEntry>(std::forward<TArgs>(args)...);
        },
        std::forward<TArgs>(args)...);
}

template <class TKey, class TValue, class THash, class TEqual, class TLock>
auto TSyncMap<TKey, TValue, THash, TEqual, TLock>::AcquireSnapshot() -> THazardPtr<TSnapshot>
{
    return THazardPtr<TSnapshot>::Acquire([&] {
        return Snapshot_.load();
    });
}

template <class TKey, class TValue, class THash, class TEqual, class TLock>
void TSyncMap<TKey, TValue, THash, TEqual, TLock>::UpdateSnapshot(TIntrusivePtr<TMap> map, bool dirty)
{
    if (!dirty) {
        Misses_ = 0;
    }

    auto* newSnapshot = new TSnapshot{std::move(map), dirty};
    auto* oldSnapshot = Snapshot_.exchange(newSnapshot);
    RetireHazardPointer(oldSnapshot, [] (auto* ptr) {
        delete ptr;
    });
}

template <class TKey, class TValue, class THash, class TEqual, class TLock>
void TSyncMap<TKey, TValue, THash, TEqual, TLock>::OnMiss()
{
    if (!DirtyMap_) {
        return;
    }

    Misses_++;
    if (Misses_ < DirtyMap_->size()) {
        return;
    }

    UpdateSnapshot(std::move(DirtyMap_), false);
}

template <class TKey, class TValue, class THash, class TEqual, class TLock>
template <class TFindKey, class TInserter, class... TArgs>
std::pair<TValue*, bool> TSyncMap<TKey, TValue, THash, TEqual, TLock>::FindOr(
    const TFindKey& key,
    TInserter&& inserter,
    TArgs&&... args)
{
    {
        auto snapshot = AcquireSnapshot();

        if (auto it = snapshot->Map->find(key); it != snapshot->Map->end()) {
            return {&(it->second->Value), false};
        }
    }

    {
        auto guard = Guard(Lock_);

        auto* snapshot = Snapshot_.load();
        if (auto it = snapshot->Map->find(key); it != snapshot->Map->end()) {
            OnMiss();
            return {&(it->second->Value), false};
        }

        if (snapshot->Dirty) {
            if (auto it = DirtyMap_->find(key); it != DirtyMap_->end()) {
                OnMiss();
                return {&(it->second->Value), false};
            }
        }

        if (!snapshot->Dirty) {
            DirtyMap_ = New<TMap>(*snapshot->Map);
            UpdateSnapshot(snapshot->Map, true);
        }

        auto [newIt, inserted] = DirtyMap_->emplace(key, inserter(std::forward<TArgs>(args)...));
        YT_VERIFY(inserted);
        return {&(newIt->second->Value), true};
    }
}

template <class TKey, class TValue, class THash, class TEqual, class TLock>
void TSyncMap<TKey, TValue, THash, TEqual, TLock>::Flush()
{
    {
        auto snapshot = AcquireSnapshot();

        if (!snapshot->Dirty) {
            return;
        }
    }

    {
        auto guard = Guard(Lock_);

        auto* snapshot = Snapshot_.load();

        // Do another lookup, in case dirty was promoted.
        if (!snapshot->Dirty) {
            return;
        }

        UpdateSnapshot(std::move(DirtyMap_), false);
    }
}

template <class TKey, class TValue, class THash, class TEqual, class TLock>
template <class TFn>
void TSyncMap<TKey, TValue, THash, TEqual, TLock>::IterateReadOnly(TFn&& fn)
{
    auto snapshot = AcquireSnapshot();

    for (const auto& [key, entry] : *snapshot->Map) {
        fn(key, entry->Value);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
