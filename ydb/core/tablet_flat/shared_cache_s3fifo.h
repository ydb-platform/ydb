#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>
#include <util/generic/intrlist.h>

namespace NKikimr::NCache {

// TODO: remove template args and make some page base class

template <typename TKey
        , typename TKeyHash
        , typename TKeyEqual>
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

template <typename TPage
        , typename TKey
        , typename TKeyHash
        , typename TKeyEqual
        , typename TSize
        , typename TLocation
        , typename TFrequency
    >
class TS3FIFOCache : public ICacheCache<TPage> {
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
        TIntrusiveList<TPage> Queue;
        ui64 Size = 0;
    };

public:
    TS3FIFOCache(ui64 limit)
        : Limit(limit)
        , SmallQueue(ELocation::SmallQueue)
        , MainQueue(ELocation::MainQueue)
        , GhostQueue(limit)
    {}

    TPage* EvictNext() override {
        if (SmallQueue.Queue.Empty() && MainQueue.Queue.Empty()) {
            return nullptr;
        }

        // TODO: account passive pages inside the cache
        TLimit savedLimit = std::exchange(Limit, TLimit(SmallQueue.Size + MainQueue.Size - 1));

        TPage* evictedPage = EvictOneIfFull();
        Y_DEBUG_ABORT_UNLESS(evictedPage);
        
        Limit = savedLimit;

        return evictedPage;
    }

    TIntrusiveList<TPage> Touch(TPage* page) override {
        const ELocation location = GetLocation(page);
        switch (location) {
            case ELocation::SmallQueue:
            case ELocation::MainQueue: {
                TouchFast(page);
                return {};
            }
            case ELocation::None:
                return Insert(page);
            default:
                Y_ABORT("Unknown page location");
        }
    }

    void Erase(TPage* page) override {
        const ELocation location = GetLocation(page);
        switch (location) {
            case ELocation::None:
                break;
            case ELocation::SmallQueue:
                Erase(SmallQueue, page);
                break;
            case ELocation::MainQueue:
                Erase(MainQueue, page);
                break;
            default:
                Y_ABORT("Unknown page location");
        }

        SetFrequency(page, 0);
        EraseGhost(page);
    }

    void UpdateLimit(ui64 limit) override {
        Limit = limit;
        GhostQueue.UpdateLimit(limit);
    }

private:
    TPage* EvictOneIfFull() {
        while (true) {
            if (!SmallQueue.Queue.Empty() && SmallQueue.Size > Limit.SmallQueueLimit) {
                TPage* page = Pop(SmallQueue);
                if (GetFrequency(page) > 1) { // TODO: why 1?
                    Push(MainQueue, page);
                } else {
                    AddGhost(page);
                    return page;
                }
            } else if (!MainQueue.Queue.Empty() && MainQueue.Size > Limit.MainQueueLimit) {
                TPage* page = Pop(MainQueue);
                if (ui32 frequency = GetFrequency(page); frequency > 0) {
                    SetFrequency(page, frequency - 1);
                    Push(MainQueue, page);
                } else {
                    return page;
                }
            } else {
                break;
            }
        }
        
        return nullptr;
    }

    void TouchFast(TPage* page) {
        Y_DEBUG_ABORT_UNLESS(GetLocation(page) != ELocation::None);

        ui32 frequency = GetFrequency(page);
        if (frequency < 3) {
            SetFrequency(page, frequency + 1);
        }
    }

    TIntrusiveList<TPage> Insert(TPage* page) {
        Y_DEBUG_ABORT_UNLESS(GetLocation(page) == ELocation::None);

        Push(EraseGhost(page) ? MainQueue : SmallQueue, page);
        SetFrequency(page, 0);

        TIntrusiveList<TPage> evictedList;
        while (TPage* evictedPage = EvictOneIfFull()) {
            evictedList.PushBack(evictedPage);
        }

        return evictedList;
    }

    TPage* Pop(TQueue& queue) {
        Y_DEBUG_ABORT_UNLESS(!queue.Queue.Empty());
        Y_ABORT_UNLESS(GetLocation(queue.Queue.Front()) == queue.Location);
        Y_DEBUG_ABORT_UNLESS(queue.Size >= GetSize(queue.Queue.Front()));

        TPage* page = queue.Queue.PopFront();
        queue.Size -= GetSize(page);
        SetLocation(page, ELocation::None);

        return page;
    }

    void Push(TQueue& queue, TPage* page) {
        Y_ABORT_UNLESS(GetLocation(page) == ELocation::None);

        queue.Queue.PushBack(page);
        queue.Size += GetSize(page);
        SetLocation(page, queue.Location);
    }

    void Erase(TQueue& queue, TPage* page) {
        Y_ABORT_UNLESS(GetLocation(page) == queue.Location);
        Y_DEBUG_ABORT_UNLESS(queue.Size >= GetSize(page));

        page->Unlink();
        queue.Size -= GetSize(page);
        SetLocation(page, ELocation::None);
    }

    void AddGhost(const TPage* page) {
        GhostQueue.Add(GetKey(page), GetSize(page));
    }

    bool EraseGhost(const TPage* page) {
        return GhostQueue.Erase(GetKey(page), GetSize(page));
    }

    TKey GetKey(const TPage* page) const {
        return TKey::Get(page);
    }

    ui64 GetSize(const TPage* page) const {
        return TSize::Get(page);
    }

    ELocation GetLocation(const TPage* page) const {
        return static_cast<ELocation>(TLocation::Get(page));
    }

    void SetLocation(TPage* page, ELocation location) const {
        TLocation::Set(page, static_cast<ui32>(location));
    }

    ui32 GetFrequency(const TPage* page) const {
        return TFrequency::Get(page);
    }

    void SetFrequency(TPage* page, ui32 frequency) const {
        TFrequency::Set(page, frequency);
    }

private:
    TLimit Limit;

    TQueue SmallQueue;
    TQueue MainQueue;
    TGhostQueue<TKey, TKeyHash, TKeyEqual> GhostQueue;

};

}
