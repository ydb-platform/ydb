#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NCache {

// TODO: remove template args and make some page base class

template <typename TPageKey
        , typename TPageKeyHash
        , typename TPageKeyEqual>
class TTS3FIFOGhostPageQueue {
    struct TGhostPage {
        TPageKey Key;
        ui64 Size; // zero size is tombstone

        TGhostPage(const TPageKey& key, ui64 size)
            : Key(key)
            , Size(size)
        {}
    };

    struct TGhostPageHash {
        inline size_t operator()(const TGhostPage* ghost) const {
            return TPageKeyHash()(ghost->Key);
        }
    };

    struct TGhostPageEqual {
        inline bool operator()(const TGhostPage* left, const TGhostPage* right) const {
            return TPageKeyEqual()(left->Key, right->Key);
        }
    };

public:
    TTS3FIFOGhostPageQueue(ui64 limit)
        : Limit(limit)
    {}

    void Add(const TPageKey& key, ui64 size) {
        if (Y_UNLIKELY(size == 0)) {
            Y_DEBUG_ABORT_S("Empty " << key.ToString() << " page");
            return;
        }

        TGhostPage* ghost = &GhostsQueue.emplace_back(key, size);
        if (Y_UNLIKELY(!GhostsSet.emplace(ghost).second)) {
            GhostsQueue.pop_back();
            Y_DEBUG_ABORT_S("Duplicated " << key.ToString() << " page");
        }

        Size += ghost->Size;

        EvictWhileFull();
    }

    bool Erase(const TPageKey& key, ui64 size) {
        TGhostPage key_(key, size);
        if (auto it = GhostsSet.find(&key_); it != GhostsSet.end()) {
            TGhostPage* ghost = const_cast<TGhostPage*>(*it);
            Y_DEBUG_ABORT_UNLESS(ghost->Size == size);
            Y_ABORT_UNLESS(Size >= ghost->Size);
            Size -= ghost->Size;
            ghost->Size = 0; // mark as deleted
            GhostsSet.erase(it);
            return true;
        }
        return false;
    }
    
    void UpdateLimit(ui64 limit) {
        Limit = limit;
        EvictWhileFull();
    }

    TString Dump() const {
        TStringBuilder result;
        size_t count = 0;
        ui64 size = 0;
        for (auto it = GhostsQueue.begin(); it != GhostsQueue.end(); it++) {
            const TGhostPage* ghost = &*it;
            if (ghost->Size) { // isn't deleted
                Y_DEBUG_ABORT_UNLESS(GhostsSet.contains(ghost));
                if (count != 0) result << ", ";
                result << "{" << ghost->Key.ToString() << " " << ghost->Size << "b}";
                count++;
                size += ghost->Size;
            }
        }
        Y_DEBUG_ABORT_UNLESS(GhostsSet.size() == count);
        Y_DEBUG_ABORT_UNLESS(Size == size);
        return result;
    }

private:
    void EvictWhileFull() {
        while (!GhostsQueue.empty() && Size > Limit) {
            TGhostPage* ghost = &GhostsQueue.front();
            if (ghost->Size) { // isn't deleted
                Y_ABORT_UNLESS(Size >= ghost->Size);
                Size -= ghost->Size;
                Y_ABORT_UNLESS(GhostsSet.erase(ghost));
            }
            GhostsQueue.pop_front();
        }
    }

    ui64 Limit;
    ui64 Size = 0;
    THashSet<TGhostPage*, TGhostPageHash, TGhostPageEqual> GhostsSet;
    TDeque<TGhostPage> GhostsQueue;
};

template <typename TPage
        , typename TPageKey
        , typename TPageKeyHash
        , typename TPageKeyEqual
        , typename TPageSize
        , typename TPageLocation
        , typename TPageFrequency
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
        TQueue(ELocation location)
            : Location(location)
        {}

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
                EraseGhost(page);
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
    }

    void UpdateLimit(ui64 limit) override {
        Limit = limit;
        GhostQueue.UpdateLimit(limit);
    }

    TString Dump() const {
        TStringBuilder result;

        auto dump = [&](const TQueue& queue) {
            size_t count = 0;
            ui64 size = 0;
            for (auto it = queue.Queue.begin(); it != queue.Queue.end(); it++) {
                const TPage* page = &*it;
                if (count != 0) result << ", ";
                result << "{" << GetKey(page).ToString() << " " << GetFrequency(page) << "f " << GetSize(page) << "b}";
                count++;
                size += GetSize(page);
            }
            Y_DEBUG_ABORT_UNLESS(queue.Size == size);
        };

        result << "SmallQueue: ";
        dump(SmallQueue);
        result << Endl << "MainQueue: ";
        dump(MainQueue);
        result << Endl << "GhostQueue: ";
        result << GhostQueue.Dump();

        return result;
    }

private:
    TPage* EvictOneIfFull() {
        while (true) {
            if (!SmallQueue.Queue.Empty() && SmallQueue.Size > Limit.SmallQueueLimit) {
                TPage* page = Pop(SmallQueue);
                if (ui32 frequency = GetFrequency(page); frequency > 1) { // load inserts, first read touches, second read touches
                    Push(MainQueue, page);
                } else {
                    if (frequency) SetFrequency(page, 0);
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
        Y_ABORT_UNLESS(queue.Size >= GetSize(queue.Queue.Front()));

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
        Y_ABORT_UNLESS(queue.Size >= GetSize(page));

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

    TPageKey GetKey(const TPage* page) const {
        return TPageKey::Get(page);
    }

    ui64 GetSize(const TPage* page) const {
        return TPageSize::Get(page);
    }

    ELocation GetLocation(const TPage* page) const {
        return static_cast<ELocation>(TPageLocation::Get(page));
    }

    void SetLocation(TPage* page, ELocation location) const {
        TPageLocation::Set(page, static_cast<ui32>(location));
    }

    ui32 GetFrequency(const TPage* page) const {
        return TPageFrequency::Get(page);
    }

    void SetFrequency(TPage* page, ui32 frequency) const {
        TPageFrequency::Set(page, frequency);
    }

private:
    TLimit Limit;
    TQueue SmallQueue;
    TQueue MainQueue;
    TTS3FIFOGhostPageQueue<TPageKey, TPageKeyHash, TPageKeyEqual> GhostQueue;

};

}
