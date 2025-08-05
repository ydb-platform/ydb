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
    using TPinned = THashMap<TLogoBlobID, THashMap<TPageId, TSharedPageRef>>;

    struct TInfo;

    struct TStats {
        ui64 TotalCollections = 0; // total number of registered collections
        ui64 TotalSharedBody = 0; // total number of bytes currently referenced from shared cache
        ui64 TotalPinnedBody = 0; // total number of bytes currently pinned in memory
        ui64 TotalExclusive = 0; // total number of bytes exclusive to this cache (not from shared cache)
        size_t CurrentCacheHits = 0; // = Touches.Size()
        ui64 CurrentCacheHitSize = 0; // = Touches.Where(t => !t.Sticky).Sum(t => t.Size)
        size_t CurrentCacheMisses = 0; // = ToLoad.Size()
    };

    struct TPage : public TIntrusiveListItem<TPage> {
        enum ELoadState {
            LoadStateNo,
            LoadStateLoaded,
        };

        ui32 LoadState : 2;
        
        const TPageId Id;
        const size_t Size;

        TInfo* const Info;
        TSharedPageRef SharedBody;
        TSharedData PinnedBody;

        TPage(size_t size, TPageId pageId, TInfo* info);

        TPage(const TPage&) = delete;
        TPage(TPage&&) = delete;

        bool IsUnnecessary() const noexcept {
            return (
                LoadState == LoadStateNo &&
                !SharedBody);
        }

        bool IsSticky() const noexcept {
            // Note: because this method doesn't use TPage flags
            // it may be called from multiple threads later
            // also it doesn't affect offloading, only touched memory counting
            return Info->IsSticky(Id);
        }

        void Fill(TSharedPageRef sharedBody) {
            SharedBody = std::move(sharedBody);
            LoadState = LoadStateLoaded;
            PinnedBody = TPinnedPageRef(SharedBody).GetData();
        }

        void ProvideSharedBody(TSharedPageRef sharedBody) {
            SharedBody = std::move(sharedBody);
            SharedBody.UnUse();
            LoadState = LoadStateNo;
            PinnedBody = { };
        }
    };

    struct TInfo : public TThrRefBase {
        ui32 Total() const noexcept {
            return PageMap.size();
        }

        TPage* GetPage(TPageId pageId) const noexcept {
            return PageMap[pageId].Get();
        }

        EPage GetPageType(TPageId pageId) const noexcept {
            return EPage(PageCollection->Page(pageId).Type);
        }

        ui64 GetPageSize(TPageId pageId) const noexcept {
            return PageCollection->Page(pageId).Size;
        }

        TPage* EnsurePage(TPageId pageId) {
            auto* page = GetPage(pageId);
            if (!page) {
                PageMap.emplace(pageId, THolder<TPage>(page = new TPage(PageCollection->Page(pageId).Size, pageId, this)));
            }
            return page;
        }

        // Note: this method is only called during a page collection creation
        void Fill(TPageId pageId, TSharedPageRef sharedBody, bool sticky) {
            if (sticky) {
                AddSticky(pageId, sharedBody);
            }
            EnsurePage(pageId)->ProvideSharedBody(std::move(sharedBody));
        }

        void AddSticky(TPageId pageId, TSharedPageRef page) {
            Y_ENSURE(page.IsUsed());
            if (StickyPages.emplace(pageId, page).second) {
                StickyPagesSize += TPinnedPageRef(page)->size();
            }
        }

        bool IsSticky(TPageId pageId) const noexcept {
            return StickyPages.contains(pageId);
        }

        ui64 GetStickySize() const noexcept {
            return StickyPagesSize;
        }

        const TLogoBlobID Id;
        const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TPageMap<THolder<TPage>> PageMap;

        explicit TInfo(TIntrusiveConstPtr<NPageCollection::IPageCollection> pack);
        TInfo(const TInfo &info);

    private:
        // storing sticky pages used refs guarantees that they won't be offload from Shared Cache
        THashMap<TPageId, TSharedPageRef> StickyPages;
        ui64 StickyPagesSize = 0;
    };

public:
    TIntrusivePtr<TInfo> GetPageCollection(TLogoBlobID id) const;
    void RegisterPageCollection(TIntrusivePtr<TInfo> info);
    void ForgetPageCollection(TIntrusivePtr<TInfo> info);

    TInfo* Info(TLogoBlobID id);

    const TStats& GetStats() const { return Stats; }

    const TSharedData* Lookup(TPageId pageId, TInfo *collection);

    void CountTouches(TPinned &pinned, ui32 &touchedUnpinnedPages, ui64 &touchedUnpinnedMemory, ui64 &touchedPinnedMemory);
    void PinTouches(TPinned &pinned, ui32 &touchedPages, ui32 &pinnedPages, ui64 &pinnedMemory);

    void CountToLoad(TPinned &pinned, ui32 &toLoadPages, ui64 &toLoadMemory);
    THashMap<TPrivatePageCache::TInfo*, TVector<TPageId>> GetToLoad() const;

    void ResetTouchesAndToLoad(bool verifyEmpty);

    void DropSharedBody(TInfo *collectionInfo, TPageId pageId);

    void ProvideBlock(TPageId pageId, TSharedPageRef sharedBody, TInfo *info);
    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> DetachPrivatePageCache();

    void TouchSharedCache(const TPinned &pinned);
    THashMap<TLogoBlobID, THashSet<TPageId>> GetSharedCacheTouches();

private:
    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> PageCollections;
    THashMap<TLogoBlobID, THashSet<TPageId>> SharedCacheTouches;

    TStats Stats;

    TIntrusiveList<TPage> Touches;
    TIntrusiveList<TPage> ToLoad;

    void TryLoad(TPage *page);
    void TryUnload(TPage *page);
    void TryEraseIfUnnecessary(TPage *page);
};

}}
