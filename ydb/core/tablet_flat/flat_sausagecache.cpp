#include "flat_sausagecache.h"
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

TSharedPageRef UnUse(TSharedPageRef sharedBody) {
    sharedBody.UnUse();
    return sharedBody;
}

TPrivatePageCache::TPage::TPage(TPageOffset offset, size_t size, TSharedPageRef sharedBody, TPageCollection* pageCollection)
    : Offset(offset)
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
    PageMap.reserve(PageCollection->Total());
}

TPrivatePageCache::TPageCollection::TPageCollection(const TPageCollection &pageCollection)
    : Id(pageCollection.Id)
    , PageCollection(pageCollection.PageCollection)
    , StickyPages(pageCollection.StickyPages)
    , CacheMode(pageCollection.CacheMode)
{
    PageMap.reserve(pageCollection.PageMap.size());
    for (const auto& page : pageCollection.PageMap) {
        Y_ASSERT(page);
        AddPage({page->Offset, page->Size}, page->SharedBody);
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

THashMap<TLogoBlobID, THashSet<TPageOffset>> TPrivatePageCache::AddPageCollection(TIntrusivePtr<TPageCollection> pageCollection) {
    auto inserted = PageCollections.insert(decltype(PageCollections)::value_type(pageCollection->Id, pageCollection));
    Y_ENSURE(inserted.second, "double registration of page collection is forbidden");
    ++Stats.PageCollections;

    THashMap<TLogoBlobID, THashSet<TPageOffset>> sharedCacheTouches;
    for (const auto& page : pageCollection->GetPageMap()) {
        Y_ASSERT(page);

        Stats.SharedBodyBytes += page->Size;
        if (pageCollection->IsStickyPage(page->Offset)) {
            Stats.StickyBytes += page->Size;
        }

        // notify shared cache that we have a page handle
        sharedCacheTouches[page->PageCollection->Id].insert(page->Offset);
    }

    if (pageCollection->GetCacheMode() == ECacheMode::TryKeepInMemory) {
        Stats.TryKeepInMemoryBytes += pageCollection->PageCollection->BackingSize();
    }

    return sharedCacheTouches;
}

void TPrivatePageCache::DropPageCollection(TPageCollection *pageCollection) {
    for (const auto& page : pageCollection->GetPageMap()) {
        Y_ASSERT(page);

        Stats.SharedBodyBytes -= page->Size;
        if (pageCollection->IsStickyPage(page->Offset)) {
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

TSharedPageRef TPrivatePageCache::TryGetPage(TPageOffset offset, TPageCollection *pageCollection) {
    auto page = pageCollection->FindPage(offset);
    if (!page) {
        return {};
    }

    auto sharedBody = page->SharedBody;
    if (!sharedBody.Use()) {
        DropPage(offset, pageCollection);
        return {};
    }

    return std::move(sharedBody);
}

void TPrivatePageCache::DropPage(TPageOffset offset, TPageCollection* pageCollection) {
    ui64 size;
    if (pageCollection->DropPage(offset, &size)) {
        Y_ENSURE(!pageCollection->IsStickyPage(offset),
            "Cannot drop sticky page " << offset << " from collection " << pageCollection->Id);
        Stats.SharedBodyBytes -= size;
    }
}

void TPrivatePageCache::AddPage(TPageLocation location, TSharedPageRef sharedBody, TPageCollection *pageCollection)
{
    if (pageCollection->AddPage(location, std::move(sharedBody))) {
        Stats.SharedBodyBytes += location.Size;
    }
}

void TPrivatePageCache::AddStickyPage(TPageLocation location, TSharedPageRef sharedBody, TPageCollection *pageCollection)
{
    AddPage(location, sharedBody, pageCollection);
    if (pageCollection->AddStickyPage(location, std::move(sharedBody))) {
        Stats.StickyBytes += location.Size;
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
