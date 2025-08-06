#include "flat_sausagecache.h"
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

TPrivatePageCache::TPage::TPage(size_t size, TPageId pageId, TInfo* info)
    : LoadState(LoadStateNo)
    , Id(pageId)
    , Size(size)
    , Info(info)
{}

TPrivatePageCache::TInfo::TInfo(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection)
    : Id(pageCollection->Label())
    , PageCollection(std::move(pageCollection))
{
    PageMap.resize(PageCollection->Total());
}

TPrivatePageCache::TInfo::TInfo(const TInfo &info)
    : Id(info.Id)
    , PageCollection(info.PageCollection)
    , StickyPages(info.StickyPages)
    , StickyPagesSize(info.StickyPagesSize)
{
    PageMap.resize(info.PageMap.size());
    for (const auto& [pageId, page] : info.PageMap) {
        Y_ASSERT(page);
        EnsurePage(pageId)->ProvideSharedBody(page->SharedBody);
    }
}

TIntrusivePtr<TPrivatePageCache::TInfo> TPrivatePageCache::GetPageCollection(const TLogoBlobID &id) const {
    auto it = PageCollections.find(id);
    Y_ENSURE(it != PageCollections.end(), "trying to get unknown page collection");
    return it->second;
}

void TPrivatePageCache::RegisterPageCollection(TIntrusivePtr<TInfo> info) {
    auto inserted = PageCollections.insert(decltype(PageCollections)::value_type(info->Id, info));
    Y_ENSURE(inserted.second, "double registration of page collection is forbidden");
    ++Stats.TotalCollections;

    for (const auto& [pageId, page] : info->PageMap) {
        Y_ASSERT(page);
        Y_ENSURE(page->SharedBody, "new filled page can't be without a shared body");

        Stats.TotalSharedBody += page->Size;
        if (page->PinnedBody)
            Stats.TotalPinnedBody += page->Size;

        TryUnload(page.Get());
        // notify shared cache that we have a page handle
        SharedCacheTouches[page->Info->Id].insert(page->Id);
    }
}

void TPrivatePageCache::ForgetPageCollection(TIntrusivePtr<TInfo> info) {
    for (const auto& [pageId, page] : info->PageMap) {
        Y_ASSERT(page);

        if (page->SharedBody)
            Stats.TotalSharedBody -= page->Size;
        if (page->PinnedBody)
            Stats.TotalPinnedBody -= page->Size;
        if (page->PinnedBody && !page->SharedBody)
            Stats.TotalExclusive -= page->Size;
    }

    info->Clear();

    PageCollections.erase(info->Id);
    SharedCacheTouches.erase(info->Id);
    --Stats.TotalCollections;
}

TPrivatePageCache::TInfo* TPrivatePageCache::Info(TLogoBlobID id) {
    auto *x = PageCollections.FindPtr(id);
    if (x)
        return x->Get();
    else
        return nullptr;
}

void TPrivatePageCache::TryLoad(TPage *page) {
    if (page->LoadState == TPage::LoadStateLoaded) {
        return;
    }

    if (page->LoadState == TPage::LoadStateNo && page->SharedBody) {
        if (page->SharedBody.Use()) {
            if (Y_LIKELY(!page->PinnedBody))
                Stats.TotalPinnedBody += page->Size;
            page->PinnedBody = TPinnedPageRef(page->SharedBody).GetData();
            page->LoadState = TPage::LoadStateLoaded;
            return;
        }

        page->SharedBody.Drop();
        Stats.TotalSharedBody -= page->Size;
        if (Y_UNLIKELY(page->PinnedBody)) {
            Stats.TotalExclusive += page->Size;
        }
    }
}

void TPrivatePageCache::TPrivatePageCache::TryUnload(TPage *page) {
    if (page->LoadState == TPage::LoadStateLoaded) {
        page->LoadState = TPage::LoadStateNo;
        if (Y_LIKELY(page->PinnedBody)) {
            Stats.TotalPinnedBody -= page->Size;
            if (!page->SharedBody) {
                Stats.TotalExclusive -= page->Size;
            }
            page->PinnedBody = { };
        }
        page->SharedBody.UnUse();
    }
}

// page may be made free after this call
void TPrivatePageCache::TPrivatePageCache::TryEraseIfUnnecessary(TPage *page) {
    if (page->IsUnnecessary()) {
        if (Y_UNLIKELY(page->PinnedBody)) {
            Stats.TotalPinnedBody -= page->Size;
            Stats.TotalExclusive -= page->Size;
            page->PinnedBody = { };
        }
        const TPageId pageId = page->Id;
        auto* info = page->Info;
        Y_DEBUG_ABORT_UNLESS(info->PageMap[pageId].Get() == page);
        bool erased = info->PageMap.erase(pageId);
        Y_ENSURE(erased);
    }
}

const TSharedData* TPrivatePageCache::Lookup(TPageId pageId, TInfo *info) {
    TPage *page = info->EnsurePage(pageId);
    
    TryLoad(page);

    if (page->LoadState == TPage::LoadStateLoaded) {
        if (page->Empty()) {
            Touches.PushBack(page);
            Stats.CurrentCacheHits++;
            if (!page->IsSticky()) {
                Stats.CurrentCacheHitSize += page->Size;
            }
        }
        return &page->PinnedBody;
    }

    if (page->Empty()) {
        Y_DEBUG_ABORT_UNLESS(info->GetPageType(page->Id) != EPage::FlatIndex, "flat index pages should have been sticked and preloaded");
        ToLoad.PushBack(page);
        Stats.CurrentCacheMisses++;
    }
    return nullptr;
}

void TPrivatePageCache::CountTouches(TPinned &pinned, ui32 &touchedUnpinnedPages, ui64 &touchedUnpinnedMemory, ui64 &touchedPinnedMemory) {
    if (pinned.empty()) {
        touchedUnpinnedPages += Stats.CurrentCacheHits;
        touchedUnpinnedMemory += Stats.CurrentCacheHitSize;
        return;
    }

    for (auto &page : Touches) {
        bool isPinned = pinned[page.Info->Id].contains(page.Id);

        if (!isPinned) {
            touchedUnpinnedPages++;
        }

        // Note: it seems useless to count sticky pages in tx usage
        // also we want to read index from Env
        if (!page.IsSticky()) {
            if (isPinned) {
                touchedPinnedMemory += page.Size;
            } else {
                touchedUnpinnedMemory += page.Size;
            }
        }
    }
}

void TPrivatePageCache::PinTouches(TPinned &pinned, ui32 &touchedPages, ui32 &pinnedPages, ui64 &pinnedMemory) {
    for (auto &page : Touches) {
        if (Y_UNLIKELY(!page.SharedBody)) {
            continue;
        }

        auto &pinnedCollection = pinned[page.Info->Id];
        
        // would insert only if first seen
        if (auto inserted = pinnedCollection.insert(std::make_pair(page.Id, page.SharedBody)); inserted.second) {
            Y_ENSURE(inserted.first->second.IsUsed());

            pinnedPages++;
            // Note: it seems useless to count sticky pages in tx usage
            // also we want to read index from Env
            if (!page.IsSticky()) {
                pinnedMemory += page.Size;
            }
        }
        touchedPages++;
    }
}

void TPrivatePageCache::CountToLoad(TPinned &pinned, ui32 &toLoadPages, ui64 &toLoadMemory) {
    for (auto &page : ToLoad) {
        auto &pinnedCollection = pinned[page.Info->Id];

        Y_ASSERT(!pinnedCollection.contains(page.Id));

        toLoadPages++;
        // Note: it seems useless to count sticky pages in tx usage
        // also we want to read index from Env
        if (!page.IsSticky()) {
            toLoadMemory += page.Size;
        }
    }
}

THashMap<TPrivatePageCache::TInfo*, TVector<TPageId>> TPrivatePageCache::GetToLoad() const {
    THashMap<TPrivatePageCache::TInfo*, TVector<TPageId>> result;
    for (auto &page : ToLoad) {
        result[page.Info].push_back(page.Id);
    }
    return result;
}

void TPrivatePageCache::ResetTouchesAndToLoad(bool verifyEmpty) {
    if (verifyEmpty) {
        Y_ENSURE(!Touches);
        Y_ENSURE(!Stats.CurrentCacheHits);
        Y_ENSURE(!Stats.CurrentCacheHitSize);
        Y_ENSURE(!ToLoad);
        Y_ENSURE(!Stats.CurrentCacheMisses);
    }

    while (Touches) {
        TPage *page = Touches.PopBack();
        TryUnload(page);
    }
    Stats.CurrentCacheHits = 0;
    Stats.CurrentCacheHitSize = 0;
    Stats.CurrentCacheMisses = 0;

    while (ToLoad) {
        TPage *page = ToLoad.PopBack();
        TryEraseIfUnnecessary(page);
    }
}

void TPrivatePageCache::DropSharedBody(TPageId pageId, TInfo *info) {
    TPage *page = info->GetPage(pageId);
    if (!page)
        return;

    if (!page->SharedBody.IsUsed()) {
        if (Y_LIKELY(page->SharedBody)) {
            Stats.TotalSharedBody -= page->Size;
            if (Y_UNLIKELY(page->PinnedBody)) {
                Stats.TotalExclusive += page->Size;
            }
            page->SharedBody = { };
        }
        TryEraseIfUnnecessary(page);
    }
}

void TPrivatePageCache::ProvideBlock(TPageId pageId, TSharedPageRef sharedBody, TInfo *info)
{
    Y_ENSURE(sharedBody.IsUsed());
    TPage *page = info->EnsurePage(pageId);

    if (Y_UNLIKELY(page->SharedBody))
        Stats.TotalSharedBody -= page->Size;
    if (Y_UNLIKELY(page->PinnedBody))
        Stats.TotalPinnedBody -= page->Size;
    if (Y_UNLIKELY(page->PinnedBody && !page->SharedBody))
        Stats.TotalExclusive -= page->Size;

    page->ProvideSharedBody(std::move(sharedBody));
    Stats.TotalSharedBody += page->Size;
}

THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TInfo>> TPrivatePageCache::DetachPrivatePageCache() {
    THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TInfo>> ret;

    for (const auto &xpair : PageCollections) {
        TIntrusivePtr<TInfo> info(new TInfo(*xpair.second));
        ret.insert(std::make_pair(xpair.first, info));
    }

    return ret;
}

void TPrivatePageCache::TouchSharedCache(const TPinned &pinned) {
    // report all touched pages:
    
    // 1. these that were touched on this transaction retry
    for (auto &page : Touches) {
        SharedCacheTouches[page.Info->Id].insert(page.Id);
    }

    // 2. and these that were touched on previous transaction retries
    for (const auto& [pageCollectionId, pages] : pinned) {
        if (auto *info = Info(pageCollectionId)) {
            auto& touches = SharedCacheTouches[pageCollectionId];
            for (auto& page : pages) {
                touches.insert(page.first);
            }
        }
    }
}


THashMap<TLogoBlobID, THashSet<TPageId>> TPrivatePageCache::GetSharedCacheTouches() {
    return std::move(SharedCacheTouches);
}

}}
