#pragma once

#include <ydb/core/util/cache_cache_iface.h>

namespace NKikimr::NSharedCache::NTest {

    static constexpr ui32 MaxCacheTier = 3;

    struct TPage : public TIntrusiveListItem<TPage> {
        ui32 Id;
        size_t Size;

        TPage(ui32 id, size_t size) 
            : Id(id), Size(size)
        {}

        ui32 CacheId : 4 = 0;
        ui32 CacheTier : 2 = MaxCacheTier;
    };

    struct TPageTraits {
        struct TPageKey {
            ui32 Id;

            TPageKey(ui32 id)
                : Id(id)
            {}
        };
        
        static ui64 GetSize(const TPage* page) {
            return page->Size;
        }

        static ui32 GetCacheId(const TPage* page) {
            return page->CacheId;
        }

        static void SetCacheId(TPage* page, ui32 id) {
            Y_ENSURE(id < (1 << 4));
            page->CacheId = id;
        }

        static ui32 GetTier(TPage* page) {
            return page->CacheTier;
        }

        static ui32 SetTier(TPage* page, ui32 tier) {
            Y_ENSURE(tier < (1 << 2));
            return page->CacheTier = tier;
        }
    };

    class TSimpleCache : public ICacheCache<TPage> {
    public:
        TIntrusiveList<TPage> EvictNext() override {
            TIntrusiveList<TPage> result;
            
            if (!List.empty()) {
                TPage* page = List.front();
                List.pop_front();
                Map.erase(page->Id);
                result.PushBack(page);
            };

            return result;
        }

        TIntrusiveList<TPage> Touch(TPage* page) override {
            if (Map.contains(page->Id)) {
                List.erase(Map[page->Id]);
            }
            List.push_back(page);
            Map[page->Id] = prev(List.end());

            TIntrusiveList<TPage> evictedList;

            while (GetSize() > Limit) {
                TPage* page = List.front();
                List.pop_front();
                Map.erase(page->Id);
                evictedList.PushBack(page);
            }

            return evictedList;
        }

        void Erase(TPage* page) override {
            if (Map.contains(page->Id)) {
                List.erase(Map[page->Id]);
                Map.erase(page->Id);
            }
        }

        void UpdateLimit(ui64 limit) override {
            Limit = limit;
        }

        ui64 GetLimit() const override {
            return Limit;
        }

        ui64 GetSize() const override {
            ui64 size = 0;
            for (auto page : List) {
                size += page->Size;
            }
            return size;
        }

        TString Dump() const override {
            TStringBuilder result;
            size_t count = 0;
            for (auto it = List.begin(); it != List.end(); it++) {
                TPage* page = *it;
                if (count != 0) result << ", ";
                result << "{" << page->Id << " " << page->Size << "b}";
                count++;
                Y_ENSURE(*Map.FindPtr(page->Id) == it);
            }
            Y_ENSURE(Map.size() == count);
            return result;
        }
    
    private:
        ui64 Limit = 0;
        TList<TPage*> List;
        THashMap<ui32, TList<TPage*>::iterator> Map;
    };

} // namespace NKikimr::NSharedCache::NTest
