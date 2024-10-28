#pragma once
#include "defs.h"
#include <ydb/core/util/cache_cache_iface.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NCache {

// TODO: remove template args and make some page base class

// TODO: metrics

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
        
        inline size_t operator()(const TPageEntry& entry) const {
            return TPageTraits::GetHash(entry.Key);
        }

        inline size_t operator()(const TPageKey& key) const {
            return TPageTraits::GetHash(key);
        }
    };

    struct TPageKeyEqual {
        using is_transparent = void;
        
        inline bool operator()(const TPageEntry& left, const TPageEntry& right) const {
            return TPageTraits::Equals(left.Key, right.Key);
        }

        inline bool operator()(const TPageEntry& left, const TPageKey& right) const {
            return TPageTraits::Equals(left.Key, right);
        }
    };

public:
    TClockProCache(ui64 limit)
        : Limit(limit)
        , ColdTarget(limit)
    {}

    TIntrusiveList<TPage> EvictNext() override {
        if (GetSize() == 0) {
            return {};
        }

        ui64 savedLimit = std::exchange(Limit, SizeHot + SizeCold - 1);
        ui64 savedColdTarget = std::exchange(ColdTarget, Min(ColdTarget, Limit));

        TIntrusiveList<TPage> evictedList = EvictWhileFull();

        Limit = savedLimit;
        ColdTarget = savedColdTarget;

        return evictedList;
    }

    TIntrusiveList<TPage> Touch(TPage* page) override {
        if (TPageTraits::GetLocation(page) != EClockProPageLocation::None) {
            // touch a 'Cold resident' or a 'Hot' page:
            TPageTraits::SetReferenced(page, true);
            return {};
        } else if (auto it = Entries.find(TPageTraits::GetKey(page)); it != Entries.end()) {
            // transforms a 'Cold non-resident' ('Test') page to a 'Hot' page:
            TPageEntry* entry = AsEntry(it);
            Y_ABORT_UNLESS(!entry->Page);
            return Fill(entry, page);
        } else {
            // adds a 'Cold resident' page
            return Add(page);
        }
    }

    void Erase(TPage* page) override {
        if (auto it = Entries.find(TPageTraits::GetKey(page)); it != Entries.end()) {
            TPageEntry* entry = AsEntry(it);

            EraseEntry(entry);

            Entries.erase(it);
        } else {
            Y_ABORT_UNLESS(TPageTraits::GetLocation(page) == EClockProPageLocation::None);
            Y_ABORT_UNLESS(!TPageTraits::GetReferenced(page));
        }
    }

    void UpdateLimit(ui64 limit) override {
        if (ColdTarget == Limit) {
            Limit = limit;
            ColdTarget = limit;
        } else {
            Limit = limit;
            ColdTarget = Min(ColdTarget, Limit);
        }
    }

    ui64 GetSize() const override {
        return SizeHot + SizeCold;
    }

    TString Dump() const override {
        TStringBuilder result;

        size_t count = 0;
        ui64 sizeHot = 0, sizeCold = 0, sizeTest = 0; 

        auto ptr = HandHot;
        while (ptr != nullptr) {
            TPageEntry* entry = ptr->Node();
            auto it = Entries.find(entry->Key);
            Y_DEBUG_ABORT_UNLESS(it != Entries.end());
            Y_DEBUG_ABORT_UNLESS(AsEntry(it) == entry);

            if (count != 0) result << ", ";
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

            Advance(ptr);
            if (ptr == HandHot) break;
        }
        
        Y_DEBUG_ABORT_UNLESS(sizeHot == SizeHot);
        Y_DEBUG_ABORT_UNLESS(sizeCold == SizeCold);
        Y_DEBUG_ABORT_UNLESS(sizeTest == SizeTest);
        Y_DEBUG_ABORT_UNLESS(count == Entries.size());
        if (count == 0) {
            Y_DEBUG_ABORT_UNLESS(!HandHot);
            Y_DEBUG_ABORT_UNLESS(!HandCold);
            Y_DEBUG_ABORT_UNLESS(!HandTest);
        }

        if (count) result << "; ";
        result << "ColdTarget: " << ColdTarget;

        return result;
    }

private:
    TIntrusiveList<TPage> Add(TPage* page) {
        Y_DEBUG_ABORT_UNLESS(TPageTraits::GetLocation(page) == EClockProPageLocation::None);

        auto inserted = Entries.emplace(TPageTraits::GetKey(page), page, TPageTraits::GetSize(page));
        Y_ABORT_UNLESS(inserted.second);
        TPageEntry* entry = AsEntry(inserted.first);

        LinkEntry(entry);

        TPageTraits::SetLocation(entry->Page, EClockProPageLocation::Cold);
        SizeCold += entry->Size;

        return EvictWhileFull();
    }

    TIntrusiveList<TPage> Fill(TPageEntry* entry, TPage* page) {
        Y_DEBUG_ABORT_UNLESS(!entry->Page);
        Y_DEBUG_ABORT_UNLESS(TPageTraits::GetLocation(page) == EClockProPageLocation::None);
        Y_ABORT_UNLESS(!TPageTraits::GetReferenced(page));
        Y_ABORT_UNLESS(TPageTraits::GetSize(page) == entry->Size);

        Y_ABORT_UNLESS(SizeTest >= entry->Size);
        SizeTest -= entry->Size;

        UnlinkEntry(entry);

        entry->Page = page;
        TPageTraits::SetLocation(page, EClockProPageLocation::Hot);
        SizeHot += entry->Size;
        
        LinkEntry(entry);

        ColdTarget = Min(ColdTarget + entry->Size, Limit);

        return EvictWhileFull();
    }

    TIntrusiveList<TPage> EvictWhileFull() {
        TIntrusiveList<TPage> evictedList;
        
        while (SizeHot + SizeCold > Limit) {
            RunHandCold(evictedList);
        }

        return evictedList;
    }

    void RunHandCold(TIntrusiveList<TPage>& evictedList) {
        Y_ABORT_UNLESS(HandCold);
        TPageEntry* entry = HandCold->Node();

        if (IsCold(entry)) {
            if (TPageTraits::GetReferenced(entry->Page)) {
                TPageTraits::SetReferenced(entry->Page, false);
                
                Y_ABORT_UNLESS(SizeCold >= entry->Size);
                SizeCold -= entry->Size;

                TPageTraits::SetLocation(entry->Page, EClockProPageLocation::Hot);
                SizeHot += entry->Size;
            } else {
                Y_ABORT_UNLESS(SizeCold >= entry->Size);
                SizeCold -= entry->Size;

                TPageTraits::SetLocation(entry->Page, EClockProPageLocation::None);
                evictedList.PushBack(entry->Page);
                entry->Page = nullptr;

                SizeTest += entry->Size;

                // TODO: should we advance HandCold before that call?
                while (SizeTest > Limit) {
                    RunHandTest(evictedList);
                }
            }
        }

        Advance(HandCold);

        while (SizeHot > Limit - ColdTarget) {
            RunHandHot(evictedList);
        }
    }

    void RunHandHot(TIntrusiveList<TPage>& evictedList) {
        Y_ABORT_UNLESS(HandHot);

        if (HandHot == HandTest) {
            RunHandTest(evictedList);
            if (!HandHot) {
                return;
            }
        }

        TPageEntry* entry = HandHot->Node();

        if (IsHot(entry)) {
            if (TPageTraits::GetReferenced(entry->Page)) {
                TPageTraits::SetReferenced(entry->Page, false);
            } else {
                Y_ABORT_UNLESS(SizeHot >= entry->Size);
                SizeHot -= entry->Size;

                TPageTraits::SetLocation(entry->Page, EClockProPageLocation::Cold);

                SizeCold += entry->Size;
            }
        }

        Advance(HandHot);
    }

    void RunHandTest(TIntrusiveList<TPage>& evictedList) {
        Y_ABORT_UNLESS(HandTest);

        if (HandTest == HandCold) {
            RunHandCold(evictedList);
            if (!HandTest) {
                return;
            }
        }

        TPageEntry* entry = HandTest->Node();

        if (IsTest(entry)) {
            Y_ABORT_UNLESS(SizeTest >= entry->Size);
            SizeTest -= entry->Size;

            ColdTarget -= Min(ColdTarget, entry->Size);

            UnlinkEntry(entry);

            auto it = Entries.find(entry->Key);
            Y_ABORT_UNLESS(it != Entries.end());
            Y_ABORT_UNLESS(AsEntry(it) == entry);
            Entries.erase(it);
        }

        Advance(HandTest);
    }

    void LinkEntry(TPageEntry* entry) {
        if (HandHot == nullptr) { // first element
            HandHot = HandCold = HandTest = entry;
        } else {
            entry->LinkBefore(HandHot);
        }

        if (HandHot == HandCold) {
            HandCold = HandCold->Prev()->Node();
        }
    }

    void UnlinkEntry(TPageEntry* entry) {
        if (entry->Empty()) { // the last entry in the cache
            HandHot = HandCold = HandTest = nullptr;
        } else {
            if (entry == HandHot) {
                HandHot = HandHot->Prev()->Node();
            }
            if (entry == HandCold) {
                HandCold = HandCold->Prev()->Node();
            }
            if (entry == HandTest) {
                HandTest = HandTest->Prev()->Node();
            }
            entry->Unlink();
        }
    }

    void EraseEntry(TPageEntry* entry) {
        if (entry->Page) {
            switch (TPageTraits::GetLocation(entry->Page)) {
                case EClockProPageLocation::Hot:
                    Y_ABORT_UNLESS(SizeHot >= entry->Size);
                    SizeHot -= entry->Size;
                    break;
                case EClockProPageLocation::Cold:
                    Y_ABORT_UNLESS(SizeCold >= entry->Size);
                    SizeCold -= entry->Size;
                    break;
                default:
                    Y_ABORT("Unexpected page location");
            }

            TPageTraits::SetReferenced(entry->Page, false);
            TPageTraits::SetLocation(entry->Page, EClockProPageLocation::None);
        } else {
            Y_ABORT_UNLESS(SizeTest >= entry->Size);
            SizeTest -= entry->Size;
        }

        UnlinkEntry(entry);
    }

    bool IsHot(TPageEntry* entry) const {
        return entry->Page && TPageTraits::GetLocation(entry->Page) == EClockProPageLocation::Hot;
    }

    bool IsCold(TPageEntry* entry) const {
        return entry->Page && TPageTraits::GetLocation(entry->Page) == EClockProPageLocation::Cold;
    }

    bool IsTest(TPageEntry* entry) const {
        return entry->Page == nullptr;
    }

    void Advance(TPageEntry*& ptr) const {
        if (ptr) {
            ptr = ptr->Next()->Node();
        }
    }

    TPageEntry* AsEntry(typename THashSet<TPageEntry, TPageKeyHash, TPageKeyEqual>::iterator it) const {
        return const_cast<TPageEntry*>(&*it);
    }

private:
    ui64 Limit;
    ui64 ColdTarget;

    // TODO: unify this with TPageMap
    THashSet<TPageEntry, TPageKeyHash, TPageKeyEqual> Entries;

    TPageEntry* HandHot = nullptr;
    TPageEntry* HandCold = nullptr;
    TPageEntry* HandTest = nullptr;
    ui64 SizeHot = 0;
    ui64 SizeCold = 0;
    ui64 SizeTest = 0;
};

}
