#pragma once
#include "flat_sausage_gut.h"
#include "shared_handle.h"
#include "shared_cache_events.h"
#include <ydb/core/util/page_map.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

using namespace NSharedCache;

class TPrivatePageCache {
public:
    // TODO: provide a way to get TSharedData from used TSharedPageRef directly
    struct TPinnedPage {
        TSharedPageRef SharedBody;
        TSharedData PinnedBody;

        explicit TPinnedPage(TSharedPageRef sharedBody)
            : SharedBody(std::move(sharedBody))
            , PinnedBody(TPinnedPageRef(SharedBody).GetData())
        {}
    };

    struct TPageCollection;

    struct TStats {
        ui64 PageCollections = 0;
        ui64 SharedBodyBytes = 0;
        ui64 StickyBytes = 0;
        ui64 TryKeepInMemoryBytes = 0;
    };

    struct TPage : TNonCopyable {
        const TPageId Id;
        const size_t Size;
        const TSharedPageRef SharedBody;
        const TPageCollection* const PageCollection;

        TPage(TPageId id, size_t size, TSharedPageRef sharedBody, TPageCollection* pageCollection);

        bool IsSticky() const noexcept {
            // Note: because this method doesn't use TPage flags
            // it may be called from multiple threads later
            // also it doesn't affect offloading, only touched memory counting
            return PageCollection->IsStickyPage(Id);
        }
    };

    struct TPageCollection : public TThrRefBase {
        TPage* FindPage(TPageId pageId) const noexcept {
            return PageMap[pageId].Get();
        }

        EPage GetPageType(TPageId pageId) const noexcept {
            return EPage(PageCollection->Page(pageId).Type);
        }

        ui64 GetPageSize(TPageId pageId) const noexcept {
            return PageCollection->Page(pageId).Size;
        }

        const TPageMap<THolder<TPage>>& GetPageMap() const noexcept {
            return PageMap;
        }

        bool IsStickyPage(TPageId pageId) const noexcept {
            return StickyPages.contains(pageId);
        }

        ECacheMode GetCacheMode() const noexcept {
            return CacheMode;
        }

        // Mutable methods can be only called on a construction stage or from a Private Cache
        // Otherwise stat counters would be out of sync

        bool AddPage(TPageId pageId, TSharedPageRef sharedBody) {
            return PageMap.emplace(pageId, MakeHolder<TPage>(
                pageId,
                GetPageSize(pageId),
                std::move(sharedBody),
                this));
        }

        bool AddStickyPage(TPageId pageId, TSharedPageRef sharedBody) {
            Y_ENSURE(sharedBody.IsUsed());
            AddPage(pageId, sharedBody);
            return StickyPages.emplace(pageId, std::move(sharedBody)).second;
        }

        bool DropPage(TPageId pageId) {
            return PageMap.erase(pageId);
        }

        void Clear() {
            PageMap.clear();
            StickyPages.clear();
        }

        void SetCacheMode(ECacheMode cacheMode) {
            CacheMode = cacheMode;
        }

        const TLogoBlobID Id;
        const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;

        explicit TPageCollection(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection);
        TPageCollection(const TPageCollection &pageCollection);

    private:
        // all pages in PageMap have valid unused shared body
        TPageMap<THolder<TPage>> PageMap;

        // storing sticky pages used refs guarantees that they won't be offload from Shared Cache
        THashMap<TPageId, TSharedPageRef> StickyPages;
        ECacheMode CacheMode = ECacheMode::Regular;
    };

public:
    TPageCollection* FindPageCollection(const TLogoBlobID &id) const;
    TPageCollection* GetPageCollection(const TLogoBlobID &id) const;
    THashMap<TLogoBlobID, THashSet<TPageId>> AddPageCollection(TIntrusivePtr<TPageCollection> pageCollection);
    void DropPageCollection(TPageCollection *pageCollection);

    const TStats& GetStats() const { return Stats; }

    TSharedPageRef TryGetPage(TPageId pageId, TPageCollection *pageCollection);

    void DropPage(TPageId pageId, TPageCollection *pageCollection);
    void AddPage(TPageId pageId, TSharedPageRef sharedBody, TPageCollection *pageCollection);
    void AddStickyPage(TPageId pageId, TSharedPageRef sharedBody, TPageCollection *pageCollection);
    bool UpdateCacheMode(ECacheMode newCacheMode, TPageCollection *pageCollection);

    THashMap<TLogoBlobID, TIntrusivePtr<TPageCollection>> DetachPrivatePageCache();

private:
    THashMap<TLogoBlobID, TIntrusivePtr<TPageCollection>> PageCollections;

    TStats Stats;
};

}}
