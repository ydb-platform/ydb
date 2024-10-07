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
    Hot,
    Cold
};

template <typename TPage, typename TPageTraits>
class TClockProCache : public ICacheCache<TPage> {
    using TPageKey = typename TPageTraits::TPageKey;

    struct TPageEntry : public TIntrusiveListItem<TPageEntry> {
        TPageKey Key;
        TPage* Page;
        ui64 Size;

        TPageEntry(const TPageKey& key, TPage* page, ui64 size)
            : Key(key)
            , Page(page)
            , Size(size)
        {}
    };

    struct TPageKeyHash {
        using is_transparent = void;
        
        inline size_t operator()(const THolder<TPageEntry>& entry) const {
            return TPageTraits::GetHash(entry->Key);
        }

        inline size_t operator()(const TPageKey& key) const {
            return TPageTraits::GetHash(key);
        }
    };

    struct TPageKeyEqual {
        using is_transparent = void;
        
        inline bool operator()(const THolder<TPageEntry>& left, const THolder<TPageEntry>& right) const {
            return TPageTraits::Equals(left->Key, right->Key);
        }

        inline bool operator()(const THolder<TPageEntry>& left, const TPageKey& right) const {
            return TPageTraits::Equals(left->Key, right);
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
            TPageEntry* entry = it->Get();
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

        size_t count = 0;
        ui64 sizeHot = 0, sizeCold = 0, sizeTest = 0; 

        auto it = HandHot;
        while (it != nullptr) {
            if (count != 0) result << ", ";
            TPageEntry* entry = it->Node();
            if (entry == HandHot) result << "Hot>";
            if (entry == HandCold) result << "Cold>";
            if (entry == HandTest) result << "Test>";
            
            result << "{" << TPageTraits::ToString(entry->Key) << " ";

            count++;
            if (entry->Page) {
                auto location = TPageTraits::GetLocation(entry->Page);
                switch (location) {
                    case EClockProPageLocation::Hot:
                        result << "H ";
                        sizeHot += entry->Size;
                        break;
                    case EClockProPageLocation::Cold:
                        result << "C ";
                        sizeCold += entry->Size;
                        break;
                    default:
                        Y_ABORT("Unknown location");
                }
            } else {
                result << "T ";
                sizeTest += entry->Size;
            }

            if (entry->Page) {
                result << TPageTraits::GetReferenced(entry->Page) << "r ";
            }
            result << entry->Size << "b}";

            it = it->Next();
            if (it == HandHot) break;
        }
        
        Y_DEBUG_ABORT_UNLESS(sizeHot == SizeHot);
        Y_DEBUG_ABORT_UNLESS(sizeCold == SizeCold);
        Y_DEBUG_ABORT_UNLESS(sizeTest == SizeTest);
        if (count == 0) {
            Y_DEBUG_ABORT_UNLESS(!HandHot);
            Y_DEBUG_ABORT_UNLESS(!HandCold);
            Y_DEBUG_ABORT_UNLESS(!HandTest);
        }

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
        auto inserted = Entries.emplace(std::move(entry_));
        Y_ABORT_UNLESS(inserted.second);
        TPageEntry* entry = inserted.first->Get();

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
    THashSet<THolder<TPageEntry>, TPageKeyHash, TPageKeyEqual> Entries;

    TIntrusiveListItem<TPageEntry>* HandHot = nullptr;
    TIntrusiveListItem<TPageEntry>* HandCold = nullptr;
    TIntrusiveListItem<TPageEntry>* HandTest = nullptr;
    ui64 SizeHot = 0, SizeCold = 0, SizeTest = 0;
};

}
