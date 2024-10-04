#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NCache {

// TODO: remove template args and make some page base class

enum class EClockProPageLocation {
    None,
    Cold,
    Hot
};

template <typename TPage, typename TPageTraits>
class TClockProCache : public ICacheCache<TPage> {
    using TPageKey = typename TPageTraits::TPageKey;

    struct TPageEntry : public TIntrusiveListItem<TPageEntry> {
        TPageKey Key; // TODO: don't store key twice?
        TPage* Page;
        ui64 Size;

        TPageEntry(const TPageKey& key, TPage* page, ui64 size)
            : Key(key)
            , Page(page)
            , Size(size)
        {}
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
            TPageEntry* entry = it->second.Get();
            if (entry->Page) {
                TouchFast(entry);
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
    // sets referenced flag for a 'Cold resident' or a 'Hot' page
    void TouchFast(TPageEntry* entry) {
        Y_DEBUG_ABORT_UNLESS(entry->Page);
        Y_ABORT_IF(TPageTraits::GetLocation(entry->Page) == EClockProPageLocation::None);
        TPageTraits::SetReferenced(entry->Page, true);
    }

    // transforms a 'Cold non-resident' page to a 'Hot' page
    TIntrusiveList<TPage> Fill(TPageEntry* entry, TPage* page) {
        Y_DEBUG_ABORT_UNLESS(!entry->Page);
        Y_ABORT_UNLESS(TPageTraits::GetLocation(page) == EClockProPageLocation::None);
        Y_ABORT_IF(TPageTraits::GetReferenced(page));
        Y_ABORT_UNLESS(entry->Size == TPageTraits::GetSize(page));

        Y_ABORT_UNLESS(SizeTest >= entry->Size);
        SizeTest -= entry->Size;

        UnlinkEntry(entry);
        entry->Page = page;
        LinkEntry(entry);

        TPageTraits::SetLocation(page, EClockProPageLocation::Hot);
        SizeHot += entry->Size;

        ColdTarget = Min(ColdTarget + entry->Size, GetTargetSize());

        return EvictWhileFull();
    }

    // adds a 'Cold resident' page
    TIntrusiveList<TPage> Insert(TPage* page) {
        Y_ABORT_UNLESS(TPageTraits::GetLocation(page) == EClockProPageLocation::None);

        auto entry_ = MakeHolder<TPageEntry>(TPageTraits::GetKey(page), page, TPageTraits::GetSize(page));
        auto inserted = Entries.emplace(entry_->Key, std::move(entry_));
        Y_ABORT_UNLESS(inserted.second);
        TPageEntry* entry = inserted.first->second.Get();

        LinkEntry(entry);

        TPageTraits::SetLocation(entry->Page, EClockProPageLocation::Cold);
        SizeCold += entry->Size;

        return EvictWhileFull();
    }

    TIntrusiveList<TPage> EvictWhileFull() {
        TIntrusiveList<TPage> evictedList;
        
        // while (TPage* evictedPage = EvictOneIfFull()) {
        //     evictedList.PushBack(evictedPage);
        // }

        return evictedList;
    }

    void LinkEntry(TPageEntry* entry) {
        if (HandHot == nullptr) { // first element
            HandHot = HandCold = HandTest = entry;
        } else {
            entry->LinkBefore(HandHot);
        }

        if (HandHot == HandCold) {
            HandCold = HandCold->Prev();
        }
    }

    void UnlinkEntry(TPageEntry* entry) {
        if (entry == HandHot) {
            HandHot = HandHot->Prev();
        }
        if (entry == HandCold) {
            HandCold = HandCold->Prev();
        }
        if (entry == HandTest) {
            HandTest = HandTest->Prev();
        }

        if (entry->Empty()) { // the last entry in the cache
            HandHot = HandCold = HandTest = nullptr;
        } else {
            entry->Unlink();
        }
    }

    ui64 GetTargetSize() {
        if (MaxSize > ReservedSize) {
            return MaxSize - ReservedSize;
        }
        return 1; // prevents an infinite loop while evicting
    }

private:
    ui64 MaxSize;
    ui64 ColdTarget;
    ui64 ReservedSize = 0;

    // TODO: unify this with TPageMap
    THashMap<TPageKey, THolder<TPageEntry>, TPageKeyHash, TPageKeyEqual> Entries;

    TIntrusiveListItem<TPageEntry>* HandHot = nullptr;
    TIntrusiveListItem<TPageEntry>* HandCold = nullptr;
    TIntrusiveListItem<TPageEntry>* HandTest = nullptr;
    ui64 SizeHot = 0, SizeCold = 0, SizeTest = 0;
};

}
