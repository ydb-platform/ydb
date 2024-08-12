#pragma once
#include "flat_page_iface.h"
#include "flat_sausage_gut.h"
#include "shared_handle.h"
#include "shared_cache_events.h"
#include <ydb/core/util/cache_cache.h>
#include <ydb/core/util/page_map.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

struct TPrivatePageCachePinPad : public TAtomicRefCount<TPrivatePageCachePinPad> {
    // no internal state
};

struct TPrivatePageCacheWaitPad : public TExplicitSimpleCounter {
    // no internal state
};

class TPrivatePageCache {
    using TPinned = THashMap<TLogoBlobID, THashMap<ui32, TIntrusivePtr<TPrivatePageCachePinPad>>>;
    using EPage = NTable::NPage::EPage;
    using TPageId = NTable::NPage::TPageId;

public:
    struct TInfo;

    struct TStats {
        ui64 TotalCollections = 0; // total number of registered collections
        ui64 TotalSharedBody = 0; // total number of bytes currently referenced from shared cache
        ui64 TotalPinnedBody = 0; // total number of bytes currently pinned in memory
        ui64 TotalExclusive = 0; // total number of bytes exclusive to this cache (not from shared cache)
        ui64 TotalSharedPending = 0; // total number of bytes waiting for transfer to shared cache
        ui64 TotalSticky = 0; // total number of bytes marked as sticky (never unloaded from memory)
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
        ui32 Sticky : 1;
        ui32 SharedPending : 1;
        
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
                !Sticky &&
                !SharedPending &&
                !PinPad &&
                !WaitQueue &&
                !SharedBody);
        }

        void Fill(TSharedData data, bool sticky = false) {
            Y_DEBUG_ABORT_UNLESS(!SharedBody && !SharedPending, "Populating cache with shared data already present");
            Sticky = sticky;
            LoadState = LoadStateLoaded;
            PinnedBody = std::move(data);
        }

        void Fill(TSharedPageRef shared, bool sticky = false) {
            Sticky = sticky;
            SharedPending = false;
            SharedBody = std::move(shared);
            LoadState = LoadStateLoaded;
            PinnedBody = TPinnedPageRef(SharedBody).GetData();
        }

        const TSharedData* GetPinnedBody() const noexcept {
            return LoadState == LoadStateLoaded ? &PinnedBody : nullptr;
        }
    };

    struct TInfo : public TThrRefBase {
        ui32 Total() const noexcept {
            return PageMap.size();
        }

        const TSharedData* Lookup(TPageId pageId) const noexcept {
            auto* page = GetPage(pageId);
            return page ? page->GetPinnedBody() : nullptr;
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

        TPage* EnsurePage(TPageId pageId) noexcept {
            auto* page = GetPage(pageId);
            if (!page) {
                PageMap.emplace(pageId, THolder<TPage>(page = new TPage(PageCollection->Page(pageId).Size, pageId, this)));
            }
            return page;
        }

        void Fill(const NPageCollection::TLoadedPage &paged, bool sticky = false) noexcept {
            EnsurePage(paged.PageId)->Fill(paged.Data, sticky);
        }

        void Fill(NSharedCache::TEvResult::TLoaded&& loaded, bool sticky = false) noexcept {
            EnsurePage(loaded.PageId)->Fill(std::move(loaded.Page), sticky);
        }

        const TLogoBlobID Id;
        const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TPageMap<THolder<TPage>> PageMap;
        ui64 Users;

        explicit TInfo(TIntrusiveConstPtr<NPageCollection::IPageCollection> pack);
        TInfo(const TInfo &info);
    };

public:
    void RegisterPageCollection(TIntrusivePtr<TInfo> info);
    TPage::TWaitQueuePtr ForgetPageCollection(TLogoBlobID id);

    void LockPageCollection(TLogoBlobID id);
    // Return true for page collections removed after unlock.
    bool UnlockPageCollection(TLogoBlobID id);

    TInfo* Info(TLogoBlobID id);

    void MarkSticky(TPageId pageId, TInfo *collectionInfo);

    const TStats& GetStats() const { return Stats; }

    std::pair<ui32, ui64> Request(TVector<ui32> &pages, TPrivatePageCacheWaitPad *waitPad, TInfo *info); // blocks to load, bytes to load

    const TSharedData* Lookup(ui32 page, TInfo *collection);
    TSharedPageRef LookupShared(ui32 page, TInfo *collection);

    void CountTouches(TPinned &pinned, ui32 &newPages, ui64 &newMemory, ui64 &pinnedMemory);
    void PinTouches(TPinned &pinned, ui32 &touchedPages, ui32 &pinnedPages, ui64 &pinnedMemory);
    void PinToLoad(TPinned &pinned, ui32 &pinnedPages, ui64 &pinnedMemory);
    void RepinPages(TPinned &newPinned, TPinned &oldPinned, size_t &pinnedPages);
    void UnpinPages(TPinned &pinned, size_t &unpinnedPages);
    THashMap<TPrivatePageCache::TInfo*, TVector<ui32>> GetToLoad() const;
    void ResetTouchesAndToLoad(bool verifyEmpty);

    void UpdateSharedBody(TInfo *collectionInfo, TPageId pageId, TSharedPageRef shared);
    void DropSharedBody(TInfo *collectionInfo, TPageId pageId);

    TPage::TWaitQueuePtr ProvideBlock(NSharedCache::TEvResult::TLoaded&& loaded, TInfo *collectionInfo);
    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> DetachPrivatePageCache();
    THashMap<TLogoBlobID, THashMap<TPageId, TSharedData>> GetPrepareSharedTouched();

private:
    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> PageCollections;
    THashMap<TLogoBlobID, THashMap<TPageId, TSharedData>> ToTouchShared;

    TStats Stats;

    TIntrusiveList<TPage> Touches;
    TIntrusiveList<TPage> ToLoad;

    TIntrusivePtr<TPrivatePageCachePinPad> Pin(TPage *page);
    void Unpin(TPage *page, TPrivatePageCachePinPad *pad);

    void TryLoad(TPage *page);
    void TryUnload(TPage *page);
    void TryEraseIfUnnecessary(TPage *page);
    void TryShareBody(TPage *page);
};

}}
