#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NCache {

// TODO: remove template args and make some page base class

template <typename TPage, typename TPageTraits>
class TClockProCache : public ICacheCache<TPage> {
    using TPageKey = typename TPageTraits::TPageKey;

    enum class EPageEntryType {
        Test,
        Cold,
        Hot
    };

    struct TPageEntry : public TIntrusiveListItem<TPageEntry> {
        EPageEntryType Type;
        TPageKey Key; // TODO: don't store key twice?
        TPage* Page;
        ui64 Size;
        bool Referenced;
    };

    struct TPageKeyHash {
        inline size_t operator()(const TPageKey& key) const {
            return TPageTraits::GetHash(key);
        }
    };

    struct TPageKeyEqual {
        inline bool operator()(const TPageKey& left, const TPageKey& right) const {
            return TPageTraits::Equals(left, right);
        }
    };

public:
    TClockProCache(ui64 limit)
        : MaxSize(limit)
        , ColdTarget(limit)
    {}

    TPage* EvictNext() override {
        // if (SmallQueue.Queue.Empty() && MainQueue.Queue.Empty()) {
        //     return nullptr;
        // }

        // // TODO: account passive pages inside the cache
        // TLimit savedLimit = std::exchange(Limit, TLimit(SmallQueue.Size + MainQueue.Size - 1));

        // TPage* evictedPage = EvictOneIfFull();
        // Y_DEBUG_ABORT_UNLESS(evictedPage);
        
        // Limit = savedLimit;

        // return evictedPage;

        return {};
    }

    TIntrusiveList<TPage> Touch(TPage* page) override {
        if (auto it = Entries.find(TPageTraits::GetKey(page)); it != Entries.end()) {
            TPageEntry& entry = *it->second;
            if (entry.Page) {
                entry.Referenced = true;
                return {};
            } else {
                return Fill(entry, page);
            }
        } else {
            return Insert(page);
        }
    }

    void Erase(TPage* page) override {
        Y_UNUSED(page);
        // const EClockProPageLocation location = TPageTraits::GetLocation(page);
        // switch (location) {
        //     case EClockProPageLocation::None:
        //         EraseGhost(page);
        //         break;
        //     case EClockProPageLocation::SmallQueue:
        //         Erase(SmallQueue, page);
        //         break;
        //     case EClockProPageLocation::MainQueue:
        //         Erase(MainQueue, page);
        //         break;
        //     default:
        //         Y_ABORT("Unknown page location");
        // }

        // TPageTraits::SetFrequency(page, 0);
    }

    void UpdateLimit(ui64 limit) override {
        MaxSize = limit;
    }

    TString Dump() const {
        TStringBuilder result;

        // auto dump = [&](const TQueue& queue) {
        //     size_t count = 0;
        //     ui64 size = 0;
        //     for (auto it = queue.Queue.begin(); it != queue.Queue.end(); it++) {
        //         const TPage* page = &*it;
        //         if (count != 0) result << ", ";
        //         result << "{" << TPageTraits::GetKeyToString(page) << " " << TPageTraits::GetFrequency(page) << "f " << TPageTraits::GetSize(page) << "b}";
        //         count++;
        //         size += TPageTraits::GetSize(page);
        //     }
        //     Y_DEBUG_ABORT_UNLESS(queue.Size == size);
        // };

        // result << "SmallQueue: ";
        // dump(SmallQueue);
        // result << Endl << "MainQueue: ";
        // dump(MainQueue);
        // result << Endl << "GhostQueue: ";
        // result << GhostQueue.Dump();

        return result;
    }

private:
    TIntrusiveList<TPage> Fill(TPageEntry& entry, TPage* page) {
        entry.Page = page;

        return EvictWhileFull();
    }

    TIntrusiveList<TPage> Insert(TPage* page) {
        Y_UNUSED(page);

        return EvictWhileFull();
    }

    TIntrusiveList<TPage> EvictWhileFull() {
        TIntrusiveList<TPage> evictedList;
        
        // while (TPage* evictedPage = EvictOneIfFull()) {
        //     evictedList.PushBack(evictedPage);
        // }

        return evictedList;
    }

private:
    ui64 MaxSize;
    ui64 ColdTarget;
    ui64 ReservedSize = 0;

    // TODO: unify this with TPageMap
    THashMap<TPageKey, THolder<TPageEntry>, TPageKeyHash, TPageKeyEqual> Entries;

    TPageEntry* HandHot = nullptr;
    TPageEntry* HandCold = nullptr;
    TPageEntry* HandTest = nullptr;
    ui64 SizeHot = 0, SizeCold = 0, SizeTest = 0;
    size_t CountHot = 0, CountCold = 0, CountTest = 0;
};

}
