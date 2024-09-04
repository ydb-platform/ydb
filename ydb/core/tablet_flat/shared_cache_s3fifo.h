#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>
#include <util/generic/intrlist.h>

namespace NKikimr::NCache {

template <typename TItem
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
        , SmallQueue{ELocation::SmallQueue}
        , MainQueue{ELocation::MainQueue}
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
            case ELocation::MainQueue:
                SetFrequency(item, Min(3, GetFrequency(item)));
                return; // fast track, no evictions
            case ELocation::None:
                if 
            default:
                Y_ABORT("Unknown item location");
        }

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
                // TODO: delete ghost?
                Y_ABORT("Unknown item location");
        }
    }

    void UpdateLimit(ui64 limit) override {
        Limit = limit;
    }

private:
    TItem* EvictOneIfFull() {
        while (true) {
            if (!SmallQueue.Queue.Empty() && SmallQueue.Size > Limit.SmallQueueLimit) {
                TItem* item = Pop(SmallQueue);
                if (GetFrequency(item) > 1) { // TODO: why 1?
                    Push(MainQueue, item);
                } else {
                    // TODO: add to ghosts
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
        Y_ABORT_UNLESS(GetLocation(queue.Queue.Back()) == queue.Location);
        Y_DEBUG_ABORT_UNLESS(queue.Size >= GetSize(queue.Queue.Back()));

        TItem* item = queue.Queue.PopBack();
        queue.Size -= GetSize(item);
        SetLocation(item, ELocation::None);

        return item;
    }

    void Push(TQueue& queue, TItem* item) {
        Y_ABORT_UNLESS(GetLocation(item) == ELocation::None);

        queue.Queue.PushFront(item);
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

    ui64 GetSize(const TItem* item) const {
        return SizeOp.Get(item);
    }

    ELocation GetLocation(const TItem* item) const {
        return static_cast<ELocation>(LocationOp.Get(item));

    }

    void SetLocation(TItem* item, ELocation location) const {
        LocationOp.Set(item, static_cast<ui32>(location));
    }

    ui32 GetFrequency(const TItem* item) const {
        return FrequencyOp.Get(item);
    }

    ui32 SetFrequency(TItem* item, ui32 frequency) const {
        FrequencyOp.Set(item, frequency);
    }

private:
    TLimit Limit;

    TQueue SmallQueue;
    TQueue MainQueue;

    TSize SizeOp;
    TLocation LocationOp;
    TFrequency FrequencyOp;
};

}
