#pragma once
#include "flat_page_iface.h"
#include "flat_sausage_gut.h"
#include "shared_handle.h"
#include "shared_cache_events.h"
#include <ydb/core/util/cache_cache.h>
#include <ydb/core/util/page_map.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

using namespace NSharedCache;

struct TPrivatePageCachePinPad : public TAtomicRefCount<TPrivatePageCachePinPad> {
    // no internal state
};

struct TPrivatePageCacheWaitPad : public TExplicitSimpleCounter {
    // no internal state
};

class TPrivatePageCache {
    using TPageId = NTable::NPage::TPageId;
    using TPinned = THashMap<TLogoBlobID, THashMap<TPageId, TIntrusivePtr<TPrivatePageCachePinPad>>>;
    using EPage = NTable::NPage::EPage;

public:
    struct TInfo;

    struct TStats {
        ui64 TotalCollections = 0; // total number of registered collections
        ui64 TotalSharedBody = 0; // total number of bytes currently referenced from shared cache
        ui64 TotalPinnedBody = 0; // total number of bytes currently pinned in memory
        ui64 TotalExclusive = 0; // total number of bytes exclusive to this cache (not from shared cache)
        ui64 PinnedSetSize = 0; // number of bytes pinned by transactions (even those not currently loaded)
        ui64 PinnedLoadSize = 0; // number of bytes pinned by transactions (which are currently being loaded)
        size_t CurrentCacheHits = 0; // = Touches.Size()
        ui64 CurrentCacheHitSize = 0; // = Touches.Where(t => !t.Sticky).Sum(t => t.Size)
        size_t CurrentCacheMisses = 0; // = ToLoad.Size()
    };

    struct TPage : public TIntrusiveListItem<TPage> {
        using TWaitQueue = TOneOneQueueInplace<TPrivatePageCacheWaitPad *, 64>;
        using TWaitQueuePtr = TAutoPtr<TWaitQueue, TWaitQueue::TCleanDestructor>;

        enum ELoadState {
            LoadStateNo,
            LoadStateLoaded,
            LoadStateRequested,
            LoadStateRequestedAsync,
        };

        ui32 LoadState : 2;
        
        const TPageId Id;
        const size_t Size;

        TInfo* const Info;
        TIntrusivePtr<TPrivatePageCachePinPad> PinPad;
        TWaitQueuePtr WaitQueue;
        TSharedPageRef SharedBody;
        TSharedData PinnedBody;

        TPage(size_t size, TPageId pageId, TInfo* info);

        TPage(const TPage&) = delete;
        TPage(TPage&&) = delete;

        bool IsUnnecessary() const noexcept {
            return (
                LoadState == LoadStateNo &&
                !PinPad &&
                !WaitQueue &&
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

        const TSharedData* GetPinnedBody() const noexcept {
            return LoadState == LoadStateLoaded ? &PinnedBody : nullptr;
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
            Y_ABORT_UNLESS(page.IsUsed());
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
        ui64 Users;

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
    TPage::TWaitQueuePtr ForgetPageCollection(TIntrusivePtr<TInfo> info);

    void LockPageCollection(TLogoBlobID id);
    // Return true for page collections removed after unlock.
    bool UnlockPageCollection(TLogoBlobID id);

    TInfo* Info(TLogoBlobID id);

    const TStats& GetStats() const { return Stats; }

    std::pair<ui32, ui64> Request(TVector<ui32> &pages, TPrivatePageCacheWaitPad *waitPad, TInfo *info); // blocks to load, bytes to load

    const TSharedData* Lookup(TPageId pageId, TInfo *collection);

    void CountTouches(TPinned &pinned, ui32 &newPages, ui64 &newMemory, ui64 &pinnedMemory);
    void PinTouches(TPinned &pinned, ui32 &touchedPages, ui32 &pinnedPages, ui64 &pinnedMemory);
    void PinToLoad(TPinned &pinned, ui32 &pinnedPages, ui64 &pinnedMemory);
    void UnpinPages(TPinned &pinned, size_t &unpinnedPages);
    THashMap<TPrivatePageCache::TInfo*, TVector<TPageId>> GetToLoad() const;
    void ResetTouchesAndToLoad(bool verifyEmpty);

    void DropSharedBody(TInfo *collectionInfo, TPageId pageId);

    TPage::TWaitQueuePtr ProvideBlock(NSharedCache::TEvResult::TLoaded&& loaded, TInfo *collectionInfo);
    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> DetachPrivatePageCache();
    THashMap<TLogoBlobID, THashSet<TPageId>> GetPrepareSharedTouched();

private:
    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> PageCollections;
    THashMap<TLogoBlobID, THashSet<TPageId>> ToTouchShared;

    TStats Stats;

    TIntrusiveList<TPage> Touches;
    TIntrusiveList<TPage> ToLoad;

    TIntrusivePtr<TPrivatePageCachePinPad> Pin(TPage *page);
    void Unpin(TPage *page, TPrivatePageCachePinPad *pad);

    void TryLoad(TPage *page);
    void TryUnload(TPage *page);
    void TryEraseIfUnnecessary(TPage *page);
};

}}
