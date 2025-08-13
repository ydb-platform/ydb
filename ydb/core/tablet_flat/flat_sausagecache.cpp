#include "flat_sausagecache.h"
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

TSharedPageRef UnUse(TSharedPageRef sharedBody) {
    sharedBody.UnUse();
    return sharedBody;
}

TPrivatePageCache::TPage::TPage(TPageId id, size_t size, TSharedPageRef sharedBody, TPageCollection* pageCollection)
    : Id(id)
    , Size(size)
    , SharedBody(UnUse(std::move(sharedBody)))
    , PageCollection(pageCollection)
{
    Y_ENSURE(SharedBody);
}

TPrivatePageCache::TPageCollection::TPageCollection(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection)
    : Id(pageCollection->Label())
    , PageCollection(std::move(pageCollection))
{
    PageMap.resize(PageCollection->Total());
}

TPrivatePageCache::TPageCollection::TPageCollection(const TPageCollection &pageCollection)
    : Id(pageCollection.Id)
    , PageCollection(pageCollection.PageCollection)
    , StickyPages(pageCollection.StickyPages)
    , CacheMode(pageCollection.CacheMode)
{
    PageMap.resize(pageCollection.PageMap.size());
    for (const auto& [pageId, page] : pageCollection.PageMap) {
        Y_ASSERT(page);
        AddPage(pageId, page->SharedBody);
    }
}

TPrivatePageCache::TPageCollection* TPrivatePageCache::FindPageCollection(const TLogoBlobID &id) const {
    auto *pageCollection = PageCollections.FindPtr(id);
    return pageCollection ? pageCollection->Get() : nullptr;
}

TPrivatePageCache::TPageCollection* TPrivatePageCache::GetPageCollection(const TLogoBlobID &id) const {
    auto pageCollection = FindPageCollection(id);
    Y_ENSURE(pageCollection, "trying to get unknown page collection");
    return pageCollection;
}

THashMap<TLogoBlobID, THashSet<TPageId>> TPrivatePageCache::AddPageCollection(TIntrusivePtr<TPageCollection> pageCollection) {
    auto inserted = PageCollections.insert(decltype(PageCollections)::value_type(pageCollection->Id, pageCollection));
    Y_ENSURE(inserted.second, "double registration of page collection is forbidden");
    ++Stats.PageCollections;

    THashMap<TLogoBlobID, THashSet<TPageId>> sharedCacheTouches;
    for (const auto& [pageId, page] : pageCollection->GetPageMap()) {
        Y_ASSERT(page);
        
        Stats.SharedBodyBytes += page->Size;
        if (pageCollection->IsStickyPage(pageId)) {
            Stats.StickyBytes += page->Size;
        }

        // notify shared cache that we have a page handle
        sharedCacheTouches[page->PageCollection->Id].insert(page->Id);
    }

    if (pageCollection->GetCacheMode() == ECacheMode::TryKeepInMemory) {
        Stats.TryKeepInMemoryBytes += pageCollection->PageCollection->BackingSize();
    }

    return sharedCacheTouches;
}

void TPrivatePageCache::DropPageCollection(TPageCollection *pageCollection) {
    for (const auto& [pageId, page] : pageCollection->GetPageMap()) {
        Y_ASSERT(page);

        Stats.SharedBodyBytes -= page->Size;
        if (pageCollection->IsStickyPage(pageId)) {
            Stats.StickyBytes -= page->Size;
        }
    }

    if (pageCollection->GetCacheMode() == ECacheMode::TryKeepInMemory) {
        Stats.TryKeepInMemoryBytes -= pageCollection->PageCollection->BackingSize();
    }

    pageCollection->Clear();

    PageCollections.erase(pageCollection->Id);
    --Stats.PageCollections;
}

TSharedPageRef TPrivatePageCache::TryGetPage(TPageId pageId, TPageCollection *pageCollection) {
    auto page = pageCollection->FindPage(pageId);
    if (!page) {
        return {};
    }

    auto sharedBody = page->SharedBody;
    if (!sharedBody.Use()) {
        DropPage(pageId, pageCollection);
        return {};
    }

    return std::move(sharedBody);
}

void TPrivatePageCache::DropPage(TPageId pageId, TPageCollection *pageCollection) {
    if (pageCollection->DropPage(pageId)) {
        Y_ENSURE(!pageCollection->IsStickyPage(pageId));
        Stats.SharedBodyBytes -= pageCollection->GetPageSize(pageId);
    }
}

void TPrivatePageCache::AddPage(TPageId pageId, TSharedPageRef sharedBody, TPageCollection *pageCollection)
{
    if (pageCollection->AddPage(pageId, std::move(sharedBody))) {
        Stats.SharedBodyBytes += pageCollection->GetPageSize(pageId);
    }
}

void TPrivatePageCache::AddStickyPage(TPageId pageId, TSharedPageRef sharedBody, TPageCollection *pageCollection)
{
    AddPage(pageId, sharedBody, pageCollection);
    if (pageCollection->AddStickyPage(pageId, std::move(sharedBody))) {
        Stats.StickyBytes += pageCollection->GetPageSize(pageId);
    }
}

bool TPrivatePageCache::UpdateCacheMode(ECacheMode newCacheMode, TPageCollection *pageCollection)
{
    auto oldCacheMode = pageCollection->GetCacheMode();
    if (oldCacheMode == newCacheMode) {
        return false;
    }

    auto tryKeepInMemoryBytesDelta = pageCollection->PageCollection->BackingSize();
    if (oldCacheMode == ECacheMode::TryKeepInMemory) {
        Stats.TryKeepInMemoryBytes -= tryKeepInMemoryBytesDelta;
    }

    pageCollection->SetCacheMode(newCacheMode);

    if (newCacheMode == ECacheMode::TryKeepInMemory) {
        Stats.TryKeepInMemoryBytes += tryKeepInMemoryBytesDelta;
    }

    return true;
};

THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TPageCollection>> TPrivatePageCache::DetachPrivatePageCache() {
    THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TPageCollection>> result;

    for (const auto &[pageCollectionId, pageCollection] : PageCollections) {
        result.insert(std::make_pair(pageCollectionId, MakeIntrusive<TPageCollection>(*pageCollection)));
    }

    return result;
}

}}
