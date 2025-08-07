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
    using TPinned = THashMap<TLogoBlobID, THashMap<TPageId, TPinnedPageRef>>;

    struct TInfo;

    struct TStats {
        ui64 TotalCollections = 0;
        ui64 TotalSharedBody = 0;

        size_t NewlyPinnedCount = 0;
        ui64 NewlyPinnedSize = 0;

        size_t ToLoadCount = 0;
        ui64 ToLoadSize = 0;
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

        ui64 GetStickySize() const noexcept {
            return StickyPagesSize;
        }

        bool AddPage(TPageId pageId, TSharedPageRef sharedBody) {
            return PageMap.emplace(pageId, MakeHolder<TPage>(
                pageId,
                GetPageSize(pageId),
                std::move(sharedBody),
                this));
        }

        void AddStickyPage(TPageId pageId, TSharedPageRef sharedBody) {
            Y_ENSURE(sharedBody.IsUsed());
            if (StickyPages.emplace(pageId, sharedBody).second) {
                StickyPagesSize += GetPageSize(pageId);
            }
            AddPage(pageId, std::move(sharedBody));
        }

        bool DropPage(TPageId pageId) {
            return PageMap.erase(pageId);
        }

        void Clear() {
            PageMap.clear();
            StickyPages.clear();
            StickyPagesSize = 0;
        }

        bool UpdateCacheMode(ECacheMode newCacheMode) {
            if (CacheMode == newCacheMode) {
                return false;
            }
            CacheMode = newCacheMode;
            return true;
        }

        ECacheMode GetCacheMode() const noexcept {
            return CacheMode;
        }

        ui64 GetTryKeepInMemorySize() const noexcept {
            return CacheMode == ECacheMode::TryKeepInMemory ? PageCollection->BackingSize() : 0;
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
        ui64 StickyPagesSize = 0;
        ECacheMode CacheMode = ECacheMode::Regular;
    };

public:
    TIntrusivePtr<TInfo> GetPageCollection(const TLogoBlobID &id) const;
    void RegisterPageCollection(TIntrusivePtr<TInfo> info);
    void ForgetPageCollection(TIntrusivePtr<TInfo> info);

    TInfo* Info(TLogoBlobID id);

    const TStats& GetStats() const { return Stats; }

    const TSharedData* Lookup(TPageId pageId, TInfo *info);

    void CountTouches(ui32 &touchedUnpinnedPages, ui64 &touchedUnpinnedMemory, ui64 &touchedPinnedMemory);
    void PinTouches(ui32 &touchedPages, ui32 &pinnedPages, ui64 &pinnedMemory);
    void CountToLoad(ui32 &toLoadPages, ui64 &toLoadMemory);

    // TODO: move this methods somewhere else (probably to TPageCollectionTxEnv)
    // and keep page states and counters there
    void BeginTransaction(TPinned* pinned);
    void EndTransaction();

    void DropPage(TPageId pageId, TInfo *info);
    void AddPage(TPageId pageId, TSharedPageRef sharedBody, TInfo *info);

    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> DetachPrivatePageCache();

    THashMap<TLogoBlobID, TVector<TPageId>> GetToLoad();
    void TranslatePinnedToSharedCacheTouches();
    THashMap<TLogoBlobID, THashSet<TPageId>> GetSharedCacheTouches();

private:
    void ToLoadPage(TPageId pageId, TInfo *info);

private:
    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> PageCollections;
    THashMap<TLogoBlobID, THashSet<TPageId>> SharedCacheTouches;

    TStats Stats;

    TPinned* Pinned;
    THashMap<TLogoBlobID, THashSet<TPageId>> ToLoad;
};

}}
