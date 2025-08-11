#pragma once
#include "flat_sausage_gut.h"
#include "shared_handle.h"
#include "shared_cache_events.h"
#include <ydb/core/util/cache_cache.h>
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

    struct TInfo;

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
        const TInfo* const Info;

        TPage(TPageId id, size_t size, TSharedPageRef sharedBody, TInfo* info);

        bool IsSticky() const noexcept {
            // Note: because this method doesn't use TPage flags
            // it may be called from multiple threads later
            // also it doesn't affect offloading, only touched memory counting
            return Info->IsStickyPage(Id);
        }
    };

    struct TInfo : public TThrRefBase {
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

        // Mutable methods can be only called on a construction stage or from Private Cache
        // Otherwise Stats counters would be out of sync

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

        ECacheMode GetCacheMode() const noexcept {
            return CacheMode;
        }

        const TLogoBlobID Id;
        const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;

        explicit TInfo(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection);
        TInfo(const TInfo &info);

    private:
        // all pages in PageMap have valid unused shared body
        TPageMap<THolder<TPage>> PageMap;

        // storing sticky pages used refs guarantees that they won't be offload from Shared Cache
        THashMap<TPageId, TSharedPageRef> StickyPages;
        ECacheMode CacheMode = ECacheMode::Regular;
    };

public:
    TInfo* FindPageCollection(const TLogoBlobID &id) const;
    TInfo* GetPageCollection(const TLogoBlobID &id) const;
    THashMap<TLogoBlobID, THashSet<TPageId>> AddPageCollection(TIntrusivePtr<TInfo> info);
    void DropPageCollection(TInfo *info);

    const TStats& GetStats() const { return Stats; }

    TSharedPageRef TryGetPage(TPageId pageId, TInfo *info);

    void DropPage(TPageId pageId, TInfo *info);
    void AddPage(TPageId pageId, TSharedPageRef sharedBody, TInfo *info);
    void AddStickyPage(TPageId pageId, TSharedPageRef sharedBody, TInfo *info);
    bool UpdateCacheMode(ECacheMode newCacheMode, TInfo *info);

    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> DetachPrivatePageCache();

private:
    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> PageCollections;

    TStats Stats;
};

}}
