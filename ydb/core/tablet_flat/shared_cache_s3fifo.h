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

public:
    bool Add(const TPageKey& key) {
        size_t hash = TPageTraits::GetHash(key);

        if (GhostsSet.insert(hash).second) {
            GhostsQueue.push_back(hash);
            return true;
        }
        
        return false;
    }

    void Limit(size_t limit) {
        while (GhostsQueue.size() > limit) {
            bool erased = GhostsSet.erase(GhostsQueue.front());
            Y_DEBUG_ABORT_UNLESS(erased);
            GhostsQueue.pop_front();
        }
    }

    bool Contains(const TPageKey& key) {
        size_t hash = TPageTraits::GetHash(key);
        return GhostsSet.contains(hash);
    }

    TString Dump() const {
        TStringBuilder result;
        size_t count = 0;
        for (size_t hash : GhostsQueue) {
            Y_DEBUG_ABORT_UNLESS(GhostsSet.contains(hash));
            if (count != 0) result << ", ";
            result << hash;
            count++;
        }
        Y_DEBUG_ABORT_UNLESS(GhostsSet.size() == count);
        return result;
    }

private:
    // Note: only hashes are stored, all the collisions just ignored
    THashSet<size_t> GhostsSet;
    TDeque<size_t> GhostsQueue;
};

template <typename TPage, typename TPageTraits>
class TS3FIFOCache : public ICacheCache<TPage> {
    using TPageKey = typename TPageTraits::TPageKey;

    static const ui32 MaxMainQueueReinserts = 20;

    struct TLimit {
        ui64 TotalLimit;
        ui64 SmallQueueLimit;
        ui64 MainQueueLimit;

        TLimit(ui64 limit)
            : TotalLimit(limit)
            , SmallQueueLimit(limit / 10)
            , MainQueueLimit(limit - SmallQueueLimit)
        {}
    };

    struct TQueue {
        TQueue(ES3FIFOPageLocation location)
            : Location(location)
        {}

        ES3FIFOPageLocation Location;
        TIntrusiveList<TPage> Queue;
        ui64 Count = 0;
        ui64 Size = 0;
    };

public:
    TS3FIFOCache(ui64 limit)
        : Limit(limit)
        , SmallQueue(ES3FIFOPageLocation::SmallQueue)
        , MainQueue(ES3FIFOPageLocation::MainQueue)
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
            Y_DEBUG_ABORT_UNLESS(queue.Count == count);
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
        ui32 mainQueueReinserts = 0;

        while (GetSize() > Limit.TotalLimit) {
            if (SmallQueue.Size > Limit.SmallQueueLimit) {
                TPage* page = Pop(SmallQueue);
                if (ui32 frequency = TPageTraits::GetFrequency(page); frequency > 1) { // load inserts, first read touches, second read touches
                    TPageTraits::SetFrequency(page, 0);
                    Push(MainQueue, page);
                } else {
                    if (frequency) { // the page is used only once
                        TPageTraits::SetFrequency(page, 0);
                    }
                    AddGhost(page);
                    return page;
                }
            } else {
                TPage* page = Pop(MainQueue);
                if (ui32 frequency = TPageTraits::GetFrequency(page); frequency > 0 && mainQueueReinserts < MaxMainQueueReinserts) {
                    mainQueueReinserts++;
                    TPageTraits::SetFrequency(page, frequency - 1);
                    Push(MainQueue, page);
                } else {
                    if (frequency) { // reinserts limit exceeded
                        TPageTraits::SetFrequency(page, 0);
                    }
                    return page;
                }
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

        Push(IsGhost(page) ? MainQueue : SmallQueue, page);
        TPageTraits::SetFrequency(page, 0);

        TIntrusiveList<TPage> evictedList;
        while (TPage* evictedPage = EvictOneIfFull()) {
            evictedList.PushBack(evictedPage);
        }

        return evictedList;
    }

    TPage* Pop(TQueue& queue) {
        Y_ABORT_UNLESS(!queue.Queue.Empty());
        Y_ABORT_UNLESS(TPageTraits::GetLocation(queue.Queue.Front()) == queue.Location);
        Y_ABORT_UNLESS(queue.Count > 0);
        Y_ABORT_UNLESS(queue.Size >= TPageTraits::GetSize(queue.Queue.Front()));

        TPage* page = queue.Queue.PopFront();
        queue.Count--;
        queue.Size -= TPageTraits::GetSize(page);
        TPageTraits::SetLocation(page, ES3FIFOPageLocation::None);

        return page;
    }

    void Push(TQueue& queue, TPage* page) {
        Y_ABORT_UNLESS(TPageTraits::GetLocation(page) == ES3FIFOPageLocation::None);

        queue.Queue.PushBack(page);
        queue.Count++;
        queue.Size += TPageTraits::GetSize(page);
        TPageTraits::SetLocation(page, queue.Location);
    }

    void Erase(TQueue& queue, TPage* page) {
        Y_ABORT_UNLESS(TPageTraits::GetLocation(page) == queue.Location);
        Y_ABORT_UNLESS(queue.Count > 0);
        Y_ABORT_UNLESS(queue.Size >= TPageTraits::GetSize(page));

        page->Unlink();
        queue.Count--;
        queue.Size -= TPageTraits::GetSize(page);
        TPageTraits::SetLocation(page, ES3FIFOPageLocation::None);
    }

    void AddGhost(const TPage* page) {
        if (GhostQueue.Add(TPageTraits::GetKey(page))) {
            GhostQueue.Limit(SmallQueue.Count + MainQueue.Count);
        }
    }

    bool IsGhost(const TPage* page) {
        return GhostQueue.Contains(TPageTraits::GetKey(page));
    }

private:
    TLimit Limit;
    TQueue SmallQueue;
    TQueue MainQueue;
    TS3FIFOGhostPageQueue<TPageTraits> GhostQueue;

};

}
