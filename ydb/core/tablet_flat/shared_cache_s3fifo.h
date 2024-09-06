#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>
#include <util/generic/intrlist.h>

namespace NKikimr::NCache {

namespace {

/*

The ghost FIFO queue G can be implemented as part of
the indexing structure. For example, we can store object
fingerprint and insertion time of ghost entries in a bucket
based hash table [33, 37, 93, 158]. The fingerprint is a 4-
byte hash of the object ID. The insertion time is a virtual
timestamp, counting the number of objects inserted into G
thus far. Let 𝑆G denote the size of the ghost queue. If the
current time is 𝑁 (i.e., there were 𝑁 insertions into G), then
all the entries whose timestamp is lower than 𝑁 − 𝑆G are no
longer in G. A ghost entry is removed from the hash table
when the object is requested or during hash collision — when
the slot is needed to store another entry.

*/

class TSimpleHashTable {
    struct TItem {
        size_t Hash = 0;
        ui64 Timestamp = 0;
    };

public:
    TSimpleHashTable(size_t limit)
        : Now(1)
        , TTL(limit)
        , HashTable(limit * 2)
    {
    }

    // Note: doesn't support shrinking
    void UpdateLimit(size_t limit) {
        TTL = limit;

        if (HashTable.size() < limit * 2) {
            Resize(Max(limit * 2, HashTable.size() * 2));
        }
    }

    void Add(size_t hash) {
        const auto index = GetIndex(hash);
        HashTable[index] = {hash, Now};
        Now++;
    }

    bool Erase(size_t hash) {
        const auto index = GetIndex(hash);
        const auto& item = HashTable[index];

        if (item.Hash == hash && IsRecent(HashTable[index].Timestamp)) {
            HashTable[index] = {};
            return true;
        }

        return false;
    }

private:
    void Resize(size_t size) {
        TVector<TItem> oldHashTable(size);
        oldHashTable.swap(HashTable);

        for (const auto item : oldHashTable) {
            if (IsRecent(item.Timestamp)) {
                HashTable[GetIndex(item.Hash)] = item;
            }
        }
    }

    bool IsRecent(ui64 timestamp) const {
        return timestamp && Now - timestamp <= TTL;
    }

    size_t GetIndex(size_t hash) const {
        return hash % HashTable.size();
    }

    ui64 Now;
    ui64 TTL;
    TVector<TItem> HashTable;
};

}

template <typename TItem
        , typename TSize
        , typename THash
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

        Push(EraseGhost() ? MainQueue : SmallQueue, item);
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

    void AddGhost(const TItem* item) {
        GhostQueue.Add(GetHash(item));
    }

    bool EraseGhost(const TItem* item) {
        return GhostQueue.Erase(GetHash(item));
    } 

    ui64 GetSize(const TItem* item) const {
        return TSize::Get(item);
    }

    size_t GetHash(const TItem* item) const {
        return THash::Get(item);
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

    ui32 SetFrequency(TItem* item, ui32 frequency) const {
        TFrequency::Set(item, frequency);
    }

private:
    TLimit Limit;

    TQueue SmallQueue;
    TQueue MainQueue;
    TSimpleHashTable GhostQueue;

};

}
