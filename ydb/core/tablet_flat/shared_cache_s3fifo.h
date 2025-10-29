#pragma once
#include "defs.h"
#include "util_fmt_abort.h"
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NSharedCache {

// TODO: remove template args and make some page base class

enum class ES3FIFOPageLocation : ui32 {
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
    // Note: only hashes are stored, all collisions are simply ignored
    THashSet<size_t> GhostsSet;
    TDeque<size_t> GhostsQueue;
};

template <typename TPage, typename TPageTraits>
class TS3FIFOCache {
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

    // returns evicted element
    TPage* EvictNext() Y_WARN_UNUSED_RESULT {
        if (SmallQueue.Queue.Empty() && MainQueue.Queue.Empty()) {
            return nullptr;
        }

        // TODO: account passive pages inside the cache
        TLimit savedLimit = std::exchange(Limit, TLimit(SmallQueue.Size + MainQueue.Size - 1));

        TPage* evictedPage = EvictOneIfFull();
        Y_ASSERT(evictedPage);
        
        Limit = savedLimit;

        return evictedPage;
    }

    // returns evicted elements as list
    TIntrusiveList<TPage> Insert(TPage* page) Y_WARN_UNUSED_RESULT {
        auto& queue = IsGhost(page)
            ? MainQueue 
            : SmallQueue;

        return Insert(queue, page);
    }

    TIntrusiveList<TPage> InsertUntouched(TPage* page) Y_WARN_UNUSED_RESULT {
        return Insert(SmallQueue, page);
    }

    void Erase(TPage* page) {
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
                Y_TABLET_ERROR("Unknown page location");
        }

        TPageTraits::SetFrequency(page, 0);
    }

    // WARN: does not evict items
    void UpdateLimit(ui64 limit) {
        Limit = limit;
    }

    TIntrusiveList<TPage> EnsureLimits() Y_WARN_UNUSED_RESULT {
        TIntrusiveList<TPage> evictedList;
        while (TPage* evictedPage = EvictOneIfFull()) {
            evictedList.PushBack(evictedPage);
        }

        return evictedList;
    }

    ui64 GetLimit() const {
        return Limit.TotalLimit;
    }

    ui64 GetSize() const {
        return SmallQueue.Size + MainQueue.Size;
    }

    TString Dump() const {
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
        result << " MainQueue: ";
        dump(MainQueue);
        result << " GhostQueue: ";
        result << GhostQueue.Dump();

        return result;
    }

private:
    TIntrusiveList<TPage> Insert(TQueue& queue, TPage* page) Y_WARN_UNUSED_RESULT {
        Push(queue, page);
        TPageTraits::SetFrequency(page, 0);

        return EnsureLimits();
    }

    TPage* EvictOneIfFull() {
        ui32 mainQueueReinserts = 0;

        while (GetSize() > Limit.TotalLimit) {
            if (SmallQueue.Size > Limit.SmallQueueLimit) {
                TPage* page = Pop(SmallQueue);
                if (ui32 frequency = TPageTraits::GetFrequency(page); frequency > 0) {
                    TPageTraits::SetFrequency(page, 0);
                    Push(MainQueue, page);
                } else { // frequency = 0
                    AddGhost(page);
                    return page;
                }
            } else {
                TPage* page = Pop(MainQueue);
                if (ui32 frequency = TPageTraits::GetFrequency(page); frequency > 0 && mainQueueReinserts < MaxMainQueueReinserts) {
                    TPageTraits::SetFrequency(page, frequency - 1);
                    Push(MainQueue, page);
                    mainQueueReinserts++;
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

    TPage* Pop(TQueue& queue) {
        Y_ENSURE(!queue.Queue.Empty());
        Y_ENSURE(TPageTraits::GetLocation(queue.Queue.Front()) == queue.Location);
        Y_ENSURE(queue.Count > 0);
        Y_ENSURE(queue.Size >= TPageTraits::GetSize(queue.Queue.Front()));

        TPage* page = queue.Queue.PopFront();
        queue.Count--;
        queue.Size -= TPageTraits::GetSize(page);
        TPageTraits::SetLocation(page, ES3FIFOPageLocation::None);

        return page;
    }

    void Push(TQueue& queue, TPage* page) {
        Y_ENSURE(TPageTraits::GetLocation(page) == ES3FIFOPageLocation::None);

        queue.Queue.PushBack(page);
        queue.Count++;
        queue.Size += TPageTraits::GetSize(page);
        TPageTraits::SetLocation(page, queue.Location);
    }

    void Erase(TQueue& queue, TPage* page) {
        Y_ENSURE(TPageTraits::GetLocation(page) == queue.Location);
        Y_ENSURE(queue.Count > 0);
        Y_ENSURE(queue.Size >= TPageTraits::GetSize(page));

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
