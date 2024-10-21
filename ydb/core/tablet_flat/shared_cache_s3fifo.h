#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NCache {

// TODO: remove template args and make some page base class

enum class ES3FIFOPageLocation {
    None,
    SmallQueue,
    MainQueue
};

template <typename TPageTraits>
class TS3FIFOGhostPageQueue {
    using TPageKey = typename TPageTraits::TPageKey;

    struct TGhostPage {
        TPageKey Key;
        ui64 Size; // zero size is tombstone

        TGhostPage(const TPageKey& key, ui64 size)
            : Key(key)
            , Size(size)
        {}
    };

    struct TGhostPageHash {
        using is_transparent = void;

        inline size_t operator()(const TGhostPage* ghost) const {
            return TPageTraits::GetHash(ghost->Key);
        }

        inline size_t operator()(const TPageKey& key) const {
            return TPageTraits::GetHash(key);
        }
    };

    struct TGhostPageEqual {
        using is_transparent = void;

        inline bool operator()(const TGhostPage* left, const TGhostPage* right) const {
            return TPageTraits::Equals(left->Key, right->Key);
        }

        inline bool operator()(const TGhostPage* left, const TPageKey& right) const {
            return TPageTraits::Equals(left->Key, right);
        }
    };

public:
    TS3FIFOGhostPageQueue(ui64 limit)
        : Limit(limit)
    {}

    void Add(const TPageKey& key, ui64 size) {
        if (Y_UNLIKELY(size == 0)) {
            Y_DEBUG_ABORT_S("Empty " << TPageTraits::ToString(key) << " page");
            return;
        }

        TGhostPage* ghost = &GhostsQueue.emplace_back(key, size);
        if (Y_UNLIKELY(!GhostsSet.emplace(ghost).second)) {
            GhostsQueue.pop_back();
            Y_DEBUG_ABORT_S("Duplicated " << TPageTraits::ToString(key) << " page");
            return;
        }

        Size += ghost->Size;

        EvictWhileFull();
    }

    bool Erase(const TPageKey& key, ui64 size) {
        if (auto it = GhostsSet.find(key); it != GhostsSet.end()) {
            TGhostPage* ghost = *it;
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
                result << "{" << TPageTraits::ToString(ghost->Key) << " " << ghost->Size << "b}";
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
                bool erased = GhostsSet.erase(ghost);
                Y_ABORT_UNLESS(erased);
            }
            GhostsQueue.pop_front();
        }
    }

    ui64 Limit;
    ui64 Size = 0;
    // TODO: store ghost withing PageMap
    THashSet<TGhostPage*, TGhostPageHash, TGhostPageEqual> GhostsSet;
    TDeque<TGhostPage> GhostsQueue;
};

template <typename TPage, typename TPageTraits>
class TS3FIFOCache : public ICacheCache<TPage> {
    using TPageKey = typename TPageTraits::TPageKey;

    struct TLimit {
        ui64 SmallQueueLimit;
        ui64 MainQueueLimit;

        TLimit(ui64 limit)
            : SmallQueueLimit(limit / 10)
            , MainQueueLimit(limit - SmallQueueLimit)
        {}
    };

    struct TQueue {
        TQueue(ES3FIFOPageLocation location)
            : Location(location)
        {}

        ES3FIFOPageLocation Location;
        TIntrusiveList<TPage> Queue;
        ui64 Size = 0;
    };

public:
    TS3FIFOCache(ui64 limit)
        : Limit(limit)
        , SmallQueue(ES3FIFOPageLocation::SmallQueue)
        , MainQueue(ES3FIFOPageLocation::MainQueue)
        , GhostQueue(limit)
    {}

    TIntrusiveList<TPage> EvictNext() override {
        if (SmallQueue.Queue.Empty() && MainQueue.Queue.Empty()) {
            return {};
        }

        // TODO: account passive pages inside the cache
        TLimit savedLimit = std::exchange(Limit, TLimit(SmallQueue.Size + MainQueue.Size - 1));

        TIntrusiveList<TPage> evictedList;
        if (TPage* evictedPage = EvictOneIfFull()) {
            evictedList.PushBack(evictedPage);
        } else {
            Y_DEBUG_ABORT("Unexpected empty eviction");
        }
        
        Limit = savedLimit;

        return evictedList;
    }

    TIntrusiveList<TPage> Touch(TPage* page) override {
        const ES3FIFOPageLocation location = TPageTraits::GetLocation(page);
        switch (location) {
            case ES3FIFOPageLocation::SmallQueue:
            case ES3FIFOPageLocation::MainQueue: {
                TouchFast(page);
                return {};
            }
            case ES3FIFOPageLocation::None:
                return Insert(page);
            default:
                Y_ABORT("Unknown page location");
        }
    }

    void Erase(TPage* page) override {
        const ES3FIFOPageLocation location = TPageTraits::GetLocation(page);
        switch (location) {
            case ES3FIFOPageLocation::None:
                EraseGhost(page);
                break;
            case ES3FIFOPageLocation::SmallQueue:
                Erase(SmallQueue, page);
                break;
            case ES3FIFOPageLocation::MainQueue:
                Erase(MainQueue, page);
                break;
            default:
                Y_ABORT("Unknown page location");
        }

        TPageTraits::SetFrequency(page, 0);
    }

    void UpdateLimit(ui64 limit) override {
        Limit = limit;
        GhostQueue.UpdateLimit(limit);
    }

    ui64 GetSize() const override {
        return SmallQueue.Size + MainQueue.Size;
    }

    TString Dump() const override {
        TStringBuilder result;

        auto dump = [&](const TQueue& queue) {
            size_t count = 0;
            ui64 size = 0;
            for (auto it = queue.Queue.begin(); it != queue.Queue.end(); it++) {
                const TPage* page = &*it;
                if (count != 0) result << ", ";
                result << "{" << TPageTraits::GetKeyToString(page) << " " << TPageTraits::GetFrequency(page) << "f " << TPageTraits::GetSize(page) << "b}";
                count++;
                size += TPageTraits::GetSize(page);
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
                if (ui32 frequency = TPageTraits::GetFrequency(page); frequency > 1) { // load inserts, first read touches, second read touches
                    Push(MainQueue, page);
                } else {
                    if (frequency) TPageTraits::SetFrequency(page, 0);
                    AddGhost(page);
                    return page;
                }
            } else if (!MainQueue.Queue.Empty() && MainQueue.Size > Limit.MainQueueLimit) {
                TPage* page = Pop(MainQueue);
                if (ui32 frequency = TPageTraits::GetFrequency(page); frequency > 0) {
                    TPageTraits::SetFrequency(page, frequency - 1);
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
        Y_DEBUG_ABORT_UNLESS(TPageTraits::GetLocation(page) != ES3FIFOPageLocation::None);

        ui32 frequency = TPageTraits::GetFrequency(page);
        if (frequency < 3) {
            TPageTraits::SetFrequency(page, frequency + 1);
        }
    }

    TIntrusiveList<TPage> Insert(TPage* page) {
        Y_DEBUG_ABORT_UNLESS(TPageTraits::GetLocation(page) == ES3FIFOPageLocation::None);

        Push(EraseGhost(page) ? MainQueue : SmallQueue, page);
        TPageTraits::SetFrequency(page, 0);

        TIntrusiveList<TPage> evictedList;
        while (TPage* evictedPage = EvictOneIfFull()) {
            evictedList.PushBack(evictedPage);
        }

        return evictedList;
    }

    TPage* Pop(TQueue& queue) {
        Y_DEBUG_ABORT_UNLESS(!queue.Queue.Empty());
        Y_ABORT_UNLESS(TPageTraits::GetLocation(queue.Queue.Front()) == queue.Location);
        Y_ABORT_UNLESS(queue.Size >= TPageTraits::GetSize(queue.Queue.Front()));

        TPage* page = queue.Queue.PopFront();
        queue.Size -= TPageTraits::GetSize(page);
        TPageTraits::SetLocation(page, ES3FIFOPageLocation::None);

        return page;
    }

    void Push(TQueue& queue, TPage* page) {
        Y_ABORT_UNLESS(TPageTraits::GetLocation(page) == ES3FIFOPageLocation::None);

        queue.Queue.PushBack(page);
        queue.Size += TPageTraits::GetSize(page);
        TPageTraits::SetLocation(page, queue.Location);
    }

    void Erase(TQueue& queue, TPage* page) {
        Y_ABORT_UNLESS(TPageTraits::GetLocation(page) == queue.Location);
        Y_ABORT_UNLESS(queue.Size >= TPageTraits::GetSize(page));

        page->Unlink();
        queue.Size -= TPageTraits::GetSize(page);
        TPageTraits::SetLocation(page, ES3FIFOPageLocation::None);
    }

    void AddGhost(const TPage* page) {
        GhostQueue.Add(TPageTraits::GetKey(page), TPageTraits::GetSize(page));
    }

    bool EraseGhost(const TPage* page) {
        return GhostQueue.Erase(TPageTraits::GetKey(page), TPageTraits::GetSize(page));
    }

private:
    TLimit Limit;
    TQueue SmallQueue;
    TQueue MainQueue;
    TS3FIFOGhostPageQueue<TPageTraits> GhostQueue;

};

}
