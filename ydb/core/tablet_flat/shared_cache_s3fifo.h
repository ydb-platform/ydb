#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>
#include <util/generic/intrlist.h>

namespace NKikimr::NCache {

template <typename TKey, typename TKeyHash, typename TKeyEqual>
class TGhostQueue {
    struct TGhost {
        TKey Key;
        ui64 Size; // zero size is tombstone

        TGhost(const TKey& key, ui64 size)
            : Key(key)
            , Size(size)
        {}
    };

    struct TGhostHash {
        inline size_t operator()(const TGhost* ghost) const {
            return TKeyHash()(ghost->Key);
        }
    };

    struct TGhostEqual {
        inline bool operator()(const TGhost* left, const TGhost* right) const {
            return TKeyEqual()(left->Key, right->Key);
        }
    };

public:
    TGhostQueue(ui64 limit)
        : Limit(limit)
    {}

    void Add(const TKey& key, ui64 size) {
        if (Y_UNLIKELY(size == 0)) {
            Y_DEBUG_ABORT_S("Empty " << key.ToString() << " page");
            return;
        }

        TGhost* ghost = &GhostsQueue.emplace_back(key, size);
        if (Y_UNLIKELY(!GhostsSet.emplace(ghost).second)) {
            GhostsQueue.pop_back();
            Y_DEBUG_ABORT_S("Duplicated " << key.ToString() << " page");
        }

        Size += ghost->Size;

        EvictWhileFull();
    }

    bool Erase(const TKey& key, ui64 size) {
        TGhost key_(key, size);
        if (auto it = GhostsSet.find(&key_); it != GhostsSet.end()) {
            TGhost* ghost = const_cast<TGhost*>(*it);
            Y_DEBUG_ABORT_UNLESS(ghost->Size == size);
            Y_ABORT_UNLESS(Size >= ghost->Size);
            Size -= ghost->Size;
            ghost->Size = 0;// mark as deleted
            GhostsSet.erase(it);
            return true;
        }
        return false;
    }
    
    void UpdateLimit(ui64 limit) {
        Limit = limit;
        EvictWhileFull();
    }

    TString Dump() {
        TStringBuilder result;
        size_t size = 0;
        for (auto it : GhostsQueue) {
            const TGhost* ghost = &it;
            if (ghost->Size) { // isn't deleted
                Y_DEBUG_ABORT_UNLESS(GhostsSet.contains(ghost));
                if (size != 0) result << ", ";
                result << "{" << ghost->Key.ToString() << " " << ghost->Size << "b}";
                size++;
            }
        }
        Y_DEBUG_ABORT_UNLESS(GhostsSet.size() == size);
        return result;
    }

private:
    void EvictWhileFull() {
        while (!GhostsQueue.empty() && Size > Limit) {
            TGhost* ghost = &GhostsQueue.front();
            if (ghost->Size) { // isn't deleted
                Y_ABORT_UNLESS(Size >= ghost->Size);
                Size -= ghost->Size;
                Y_DEBUG_ABORT_UNLESS(GhostsSet.erase(ghost));
            }
            GhostsQueue.pop_front();
        }
    }

    ui64 Limit;
    ui64 Size = 0;
    THashSet<TGhost*, TGhostHash, TGhostEqual> GhostsSet;
    TDeque<TGhost> GhostsQueue;
};

template <typename TItem
        , typename TKey
        , typename TKeyHash
        , typename TKeyEqual
        , typename TSize
        , typename TLocation
        , typename TFrequency
    >
class TS3FIFOCache : public ICacheCache<TItem> {
    enum class ELocation {
        None,
        SmallQueue,
        MainQueue
    };

    struct TLimit {
        ui64 SmallQueueLimit;
        ui64 MainQueueLimit;

        TLimit(ui64 limit)
            : SmallQueueLimit(limit / 10)
            , MainQueueLimit(limit - SmallQueueLimit)
        {}
    };

    struct TQueue {
        ELocation Location;
        TIntrusiveList<TItem> Queue;
        ui64 Size = 0;
    };

public:
    TS3FIFOCache(ui64 limit)
        : Limit(limit)
        , SmallQueue(ELocation::SmallQueue)
        , MainQueue(ELocation::MainQueue)
        , GhostQueue(limit)
    {}

    TItem* EvictNext() override {
        if (SmallQueue.Queue.Empty() && MainQueue.Queue.Empty()) {
            return nullptr;
        }

        // TODO: account passive pages inside the cache
        TLimit savedLimit = std::exchange(Limit, TLimit(SmallQueue.Size + MainQueue.Size - 1));

        TItem* evictedItem = EvictOneIfFull();
        Y_DEBUG_ABORT_UNLESS(evictedItem);
        
        Limit = savedLimit;

        return evictedItem;
    }

    TIntrusiveList<TItem> Touch(TItem* item) override {
        const ELocation location = GetLocation(item);
        switch (location) {
            case ELocation::SmallQueue:
            case ELocation::MainQueue: {
                TouchFast(item);
                return {};
            }
            case ELocation::None:
                return Insert(item);
            default:
                Y_ABORT("Unknown item location");
        }
    }

    void TouchFast(TItem* item) {
        Y_DEBUG_ABORT_UNLESS(GetLocation(item) != ELocation::None);

        ui32 frequency = GetFrequency(item);
        if (frequency < 3) {
            SetFrequency(item, frequency + 1);
        }
    }

    TIntrusiveList<TItem> Insert(TItem* item) {
        Y_DEBUG_ABORT_UNLESS(GetLocation(item) == ELocation::None);

        Push(EraseGhost(item) ? MainQueue : SmallQueue, item);
        SetFrequency(item, 0);

        TIntrusiveList<TItem> evictedList;
        while (TItem* evictedItem = EvictOneIfFull()) {
            evictedList.PushBack(evictedItem);
        }

        return evictedList;
    }

    void Erase(TItem* item) override {
        const ELocation location = GetLocation(item);
        switch (location) {
            case ELocation::None:
                break;
            case ELocation::SmallQueue:
                Erase(SmallQueue, item);
                break;
            case ELocation::MainQueue:
                Erase(MainQueue, item);
                break;
            default:
                Y_ABORT("Unknown item location");
        }

        SetFrequency(item, 0);
        EraseGhost(item);
    }

    void UpdateLimit(ui64 limit) override {
        Limit = limit;
        GhostQueue.UpdateLimit(limit);
    }

private:
    TItem* EvictOneIfFull() {
        while (true) {
            if (!SmallQueue.Queue.Empty() && SmallQueue.Size > Limit.SmallQueueLimit) {
                TItem* item = Pop(SmallQueue);
                if (GetFrequency(item) > 1) { // TODO: why 1?
                    Push(MainQueue, item);
                } else {
                    AddGhost(item);
                    return item;
                }
            } else if (!MainQueue.Queue.Empty() && MainQueue.Size > Limit.MainQueueLimit) {
                TItem* item = Pop(MainQueue);
                if (ui32 frequency = GetFrequency(item); frequency > 0) {
                    SetFrequency(item, frequency - 1);
                    Push(MainQueue, item);
                } else {
                    return item;
                }
            } else {
                break;
            }
        }
        
        return nullptr;
    }

    TItem* Pop(TQueue& queue) {
        Y_DEBUG_ABORT_UNLESS(!queue.Queue.Empty());
        Y_ABORT_UNLESS(GetLocation(queue.Queue.Front()) == queue.Location);
        Y_DEBUG_ABORT_UNLESS(queue.Size >= GetSize(queue.Queue.Front()));

        TItem* item = queue.Queue.PopFront();
        queue.Size -= GetSize(item);
        SetLocation(item, ELocation::None);

        return item;
    }

    void Push(TQueue& queue, TItem* item) {
        Y_ABORT_UNLESS(GetLocation(item) == ELocation::None);

        queue.Queue.PushBack(item);
        queue.Size += GetSize(item);
        SetLocation(item, queue.Location);
    }

    void Erase(TQueue& queue, TItem* item) {
        Y_ABORT_UNLESS(GetLocation(item) == queue.Location);
        Y_DEBUG_ABORT_UNLESS(queue.Size >= GetSize(item));

        item->Unlink();
        queue.Size -= GetSize(item);
        SetLocation(item, ELocation::None);
    }

    void AddGhost(const TItem* item) {
        GhostQueue.Add(GetKey(item), GetSize(item));
    }

    bool EraseGhost(const TItem* item) {
        return GhostQueue.Erase(GetKey(item), GetSize(item));
    }

    TKey GetKey(const TItem* item) const {
        return TKey::Get(item);
    }

    ui64 GetSize(const TItem* item) const {
        return TSize::Get(item);
    }

    ELocation GetLocation(const TItem* item) const {
        return static_cast<ELocation>(TLocation::Get(item));
    }

    void SetLocation(TItem* item, ELocation location) const {
        TLocation::Set(item, static_cast<ui32>(location));
    }

    ui32 GetFrequency(const TItem* item) const {
        return TFrequency::Get(item);
    }

    void SetFrequency(TItem* item, ui32 frequency) const {
        TFrequency::Set(item, frequency);
    }

private:
    TLimit Limit;

    TQueue SmallQueue;
    TQueue MainQueue;
    TGhostQueue<TKey, TKeyHash, TKeyEqual> GhostQueue;

};

}
