#include "flat_sausagecache.h"
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

TSharedPageRef UnUse(TSharedPageRef sharedBody) {
    sharedBody.UnUse();
    return sharedBody;
}

TPrivatePageCache::TPage::TPage(TPageId id, size_t size, TSharedPageRef sharedBody, TInfo* info)
    : Id(id)
    , Size(size)
    , SharedBody(UnUse(std::move(sharedBody)))
    , Info(info)
{
    Y_ENSURE(SharedBody);
}

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
    , CacheMode(info.CacheMode)
{
    PageMap.resize(info.PageMap.size());
    for (const auto& [pageId, page] : info.PageMap) {
        Y_ASSERT(page);
        AddPage(pageId, page->SharedBody);
    }
}

TPrivatePageCache::TInfo* TPrivatePageCache::FindPageCollection(const TLogoBlobID &id) const {
    auto *pageCollection = PageCollections.FindPtr(id);
    return pageCollection ? pageCollection->Get() : nullptr;
}

TPrivatePageCache::TInfo* TPrivatePageCache::GetPageCollection(const TLogoBlobID &id) const {
    auto pageCollection = FindPageCollection(id);
    Y_ENSURE(pageCollection, "trying to get unknown page collection");
    return pageCollection;
}

THashMap<TLogoBlobID, THashSet<TPageId>> TPrivatePageCache::AddPageCollection(TIntrusivePtr<TInfo> info) {
    auto inserted = PageCollections.insert(decltype(PageCollections)::value_type(info->Id, info));
    Y_ENSURE(inserted.second, "double registration of page collection is forbidden");
    ++Stats.PageCollections;

    THashMap<TLogoBlobID, THashSet<TPageId>> sharedCacheTouches;
    for (const auto& [pageId, page] : info->GetPageMap()) {
        Y_ASSERT(page);
        
        Stats.SharedBodyBytes += page->Size;
        if (info->IsStickyPage(pageId)) {
            Stats.StickyBytes += page->Size;
        }

        // notify shared cache that we have a page handle
        sharedCacheTouches[page->Info->Id].insert(page->Id);
    }

    return sharedCacheTouches;
}

void TPrivatePageCache::DropPageCollection(TInfo *info) {
    for (const auto& [pageId, page] : info->GetPageMap()) {
        Y_ASSERT(page);

        Stats.SharedBodyBytes -= page->Size;
        if (info->IsStickyPage(pageId)) {
            Stats.StickyBytes -= page->Size;
        }
    }

    info->Clear();

    PageCollections.erase(info->Id);
    --Stats.PageCollections;
}

TSharedPageRef TPrivatePageCache::TryGetPage(TPageId pageId, TInfo *info) {
    auto page = info->FindPage(pageId);
    if (!page) {
        return {};
    }

    auto sharedBody = page->SharedBody;
    if (!sharedBody.Use()) {
        DropPage(pageId, info);
        return {};
    }

    return std::move(sharedBody);
}

void TPrivatePageCache::DropPage(TPageId pageId, TInfo *info) {
    if (info->DropPage(pageId)) {
        Y_ENSURE(!info->IsStickyPage(pageId));
        Stats.SharedBodyBytes -= info->GetPageSize(pageId);
    }
}

void TPrivatePageCache::AddPage(TPageId pageId, TSharedPageRef sharedBody, TInfo *info)
{
    if (info->AddPage(pageId, std::move(sharedBody))) {
        Stats.SharedBodyBytes += info->GetPageSize(pageId);
    }
}

void TPrivatePageCache::AddStickyPage(TPageId pageId, TSharedPageRef sharedBody, TInfo *info)
{
    AddPage(pageId, sharedBody, info);
    if (info->AddStickyPage(pageId, std::move(sharedBody))) {
        Stats.StickyBytes += info->GetPageSize(pageId);
    }
}

bool TPrivatePageCache::UpdateCacheMode(ECacheMode newCacheMode, TInfo *info)
{
    auto oldCacheMode = info->GetCacheMode();
    if (oldCacheMode == newCacheMode) {
        return false;
    }

    auto tryKeepInMemoryBytesDelta = info->PageCollection->BackingSize();
    if (oldCacheMode == ECacheMode::TryKeepInMemory) {
        Stats.TryKeepInMemoryBytes -= tryKeepInMemoryBytesDelta;
    }

    info->SetCacheMode(newCacheMode);

    if (newCacheMode == ECacheMode::TryKeepInMemory) {
        Stats.TryKeepInMemoryBytes += tryKeepInMemoryBytesDelta;
    }

    return true;
};

THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TInfo>> TPrivatePageCache::DetachPrivatePageCache() {
    THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TInfo>> ret;

    for (const auto &xpair : PageCollections) {
        TIntrusivePtr<TInfo> info(new TInfo(*xpair.second));
        ret.insert(std::make_pair(xpair.first, info));
    }

    return ret;
}

}}
