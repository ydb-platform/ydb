#pragma once
#include "defs.h"
#include "flat_sausage_gut.h"
#include "flat_sausage_fetch.h"
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

        ui64 LoadState : 2;
        ui64 CacheGeneration : 3;
        ui64 Sticky : 1;
        ui64 SharedPending : 1;
        ui64 Padding : 1;
        const ui64 Size : 24;
        const ui64 Id : 32;

        TInfo* const Info;
        TIntrusivePtr<TPrivatePageCachePinPad> PinPad;
        TWaitQueuePtr WaitQueue;
        TSharedPageRef SharedBody;
        TSharedData PinnedBody;

        TPage(ui32 size, ui32 pageId, TInfo* info);

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
            Y_VERIFY_DEBUG(!SharedBody && !SharedPending, "Populating cache with shared data already present");
            Sticky = sticky;
            LoadState = LoadStateLoaded;
            PinnedBody = std::move(data);
        }

        void Fill(TSharedPageRef shared, bool sticky = false) {
            Sticky = sticky;
            SharedPending = false;
            LoadState = LoadStateLoaded;
            SharedBody = std::move(shared);
            PinnedBody = TPinnedPageRef(SharedBody).GetData();
        }

        const TSharedData* GetBody() const noexcept {
            return LoadState == LoadStateLoaded ? &PinnedBody : nullptr;
        }

        TSharedPageRef GetShared() const noexcept {
            if (LoadState == LoadStateLoaded) {
                if (SharedBody) {
                    return SharedBody;
                } else {
                    return TSharedPageRef::MakePrivate(PinnedBody);
                }
            }

            return { };
        }

        struct TWeight {
            static ui64 Get(TPage *x) {
                return x->Size;
            }
        };
    };

    struct TInfo : public TThrRefBase {
        ui32 Total() const noexcept {
            return PageMap.size();
        }

        const TSharedData* Lookup(ui32 pageId) const noexcept {
            auto* page = GetPage(pageId);
            return page ? page->GetBody() : nullptr;
        }

        TPage* GetPage(ui32 pageId) const noexcept {
            return PageMap[pageId].Get();
        }

        TPage* EnsurePage(ui32 pageId) noexcept {
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

        void UpdateSharedBody(ui32 pageId, TSharedPageRef shared, TStats *stats) noexcept {
            auto* page = GetPage(pageId);
            if (!page) {
                return;
            }
            Y_VERIFY_DEBUG(page->SharedPending, "Shared cache accepted a page that is not pending, possible bug");
            if (Y_UNLIKELY(!page->SharedPending)) {
                return;
            }
            // Shared cache accepted our page and provided its shared reference
            stats->TotalSharedPending -= page->Size;
            page->SharedPending = false;
            if (Y_LIKELY(!page->SharedBody)) {
                stats->TotalSharedBody += page->Size;
                if (Y_LIKELY(page->PinnedBody)) {
                    stats->TotalExclusive -= page->Size;
                }
            }
            page->SharedBody = std::move(shared);
            if (page->LoadState == TPage::LoadStateLoaded) {
                if (Y_UNLIKELY(!page->PinnedBody))
                    stats->TotalPinnedBody += page->Size;
                page->PinnedBody = TPinnedPageRef(page->SharedBody).GetData();
            } else {
                if (Y_LIKELY(page->PinnedBody))
                    stats->TotalPinnedBody -= page->Size;
                page->PinnedBody = { };
                page->SharedBody.UnUse();
            }
        }

        void DropSharedBody(ui32 pageId, TStats *stats) noexcept {
            if (auto* page = GetPage(pageId)) {
                Y_VERIFY_DEBUG(!page->SharedPending, "Shared cache dropped page sharing request, possible bug");
                if (Y_UNLIKELY(page->SharedPending)) {
                    // Shared cache rejected our page so we should drop it too
                    stats->TotalSharedPending -= page->Size;
                    page->SharedPending = false;
                    if (page->LoadState != TPage::LoadStateLoaded) {
                        if (Y_LIKELY(page->PinnedBody)) {
                            stats->TotalPinnedBody -= page->Size;
                            stats->TotalExclusive -= page->Size;
                            page->PinnedBody = { };
                        }
                    }
                }
                if (!page->SharedBody.IsUsed()) {
                    if (Y_LIKELY(page->SharedBody)) {
                        stats->TotalSharedBody -= page->Size;
                        if (Y_UNLIKELY(page->PinnedBody)) {
                            stats->TotalExclusive += page->Size;
                        }
                        page->SharedBody = { };
                    }
                    if (page->IsUnnecessary()) {
                        if (Y_UNLIKELY(page->PinnedBody)) {
                            stats->TotalPinnedBody -= page->Size;
                            stats->TotalExclusive -= page->Size;
                            page->PinnedBody = { };
                        }
                        PageMap.erase(pageId);
                    }
                }
            }
        }

        const TLogoBlobID Id;
        const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TPageMap<THolder<TPage>> PageMap;
        ui64 Users;

        explicit TInfo(TIntrusiveConstPtr<NPageCollection::IPageCollection> pack);
        TInfo(const TInfo &info);
    };

public:
    TPrivatePageCache(const TCacheCacheConfig &cacheConfig);

    void RegisterPageCollection(TIntrusivePtr<TInfo> info);
    TPage::TWaitQueuePtr ForgetPageCollection(TLogoBlobID id);

    void LockPageCollection(TLogoBlobID id);
    // Return true for page collections removed after unlock.
    bool UnlockPageCollection(TLogoBlobID id);

    TInfo* Info(TLogoBlobID id);

    void Touch(ui32 page, TInfo *collectionInfo);
    TIntrusivePtr<TPrivatePageCachePinPad> Pin(ui32 page, TInfo *collectionInfo);
    void Unpin(ui32 page, TPrivatePageCachePinPad *pad, TInfo *collectionInfo);
    void MarkSticky(ui32 pageId, TInfo *collectionInfo);

    const TStats& GetStats() const { return Stats; }

    void UpdateSharedBody(TInfo *collectionInfo, ui32 pageId, TSharedPageRef shared) noexcept {
        collectionInfo->UpdateSharedBody(pageId, std::move(shared), &Stats);
    }

    void DropSharedBody(TInfo *collectionInfo, ui32 pageId) noexcept {
        collectionInfo->DropSharedBody(pageId, &Stats);
    }

    std::pair<ui32, ui64> Load(TVector<ui32> &pages, TPrivatePageCacheWaitPad *waitPad, TInfo *info); // blocks to load, bytes to load

    const TSharedData* Lookup(ui32 page, TInfo *collection);
    TSharedPageRef LookupShared(ui32 page, TInfo *collection);
    TPage::TWaitQueuePtr ProvideBlock(NSharedCache::TEvResult::TLoaded&& loaded, TInfo *collectionInfo);

    void UpdateCacheSize(ui64 cacheSize);
    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> DetachPrivatePageCache();
    THashMap<TLogoBlobID, THashMap<ui32, TSharedData>> GetPrepareSharedTouched();
private:
    THashMap<TLogoBlobID, TIntrusivePtr<TInfo>> PageCollections;
    THashMap<TLogoBlobID, THashMap<ui32, TSharedData>> ToTouchShared;

    TStats Stats;
    TCacheCache<TPage, TPage::TWeight> Cache;

    bool Restore(TPage *page);
    void Touch(TPage *page);
    void Evict(TPage *pages);
};

}}
