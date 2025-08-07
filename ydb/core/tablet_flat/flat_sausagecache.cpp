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
    , StickyPagesSize(info.StickyPagesSize)
    , CacheMode(info.CacheMode)
{
    PageMap.resize(info.PageMap.size());
    for (const auto& [pageId, page] : info.PageMap) {
        Y_ASSERT(page);
        AddPage(pageId, page->SharedBody);
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

    for (const auto& [pageId, page] : info->GetPageMap()) {
        Y_ASSERT(page);
        
        Stats.TotalSharedBody += page->Size;

        // notify shared cache that we have a page handle
        SharedCacheTouches[page->Info->Id].insert(page->Id);
    }
}

void TPrivatePageCache::ForgetPageCollection(TIntrusivePtr<TInfo> info) {
    for (const auto& [pageId, page] : info->GetPageMap()) {
        Y_ASSERT(page);

        Stats.TotalSharedBody -= page->Size;
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

void TPrivatePageCache::ToLoadPage(TPageId pageId, TInfo *info) {
    if (ToLoad[info->Id].insert(pageId).second) {
        Stats.ToLoadCount++;
        Y_ASSERT(!info->IsStickyPage(pageId));
        Stats.ToLoadSize += info->GetPageSize(pageId);
    }
}

const TSharedData* TPrivatePageCache::Lookup(TPageId pageId, TInfo *info) {
    Y_ENSURE(Pinned, "can be called only in a transaction context");
    auto& pinnedCollection = (*Pinned)[info->Id];
    auto* pinnedPage = pinnedCollection.FindPtr(pageId);
    if (pinnedPage) {
        // pinned pages do not need to be counted again
        return &pinnedPage->GetData();
    }

    auto page = info->FindPage(pageId);
    if (!page) {
        ToLoadPage(pageId, info);
        return nullptr;
    }

    auto sharedBody = page->SharedBody;
    if (!sharedBody.Use()) {
        DropPage(pageId, info);
        ToLoadPage(pageId, info);
        return nullptr;
    }

    auto emplaced = pinnedCollection.emplace(pageId, TPinnedPageRef(std::move(sharedBody)));
    Y_ENSURE(emplaced.second);

    // a transaction has pinned a new page, count it as a cache hit:
    Stats.NewlyPinnedCount++;
    if (!page->IsSticky()) {
        Stats.NewlyPinnedSize += page->Size;
    }
    
    return &emplaced.first->second.GetData();
}

void TPrivatePageCache::BeginTransaction(TPinned* pinned) {
    Y_ENSURE(!Pinned);
    Y_ENSURE(!ToLoad);

    Y_ENSURE(!Stats.NewlyPinnedCount);
    Y_ENSURE(!Stats.NewlyPinnedSize);
    Y_ENSURE(!Stats.ToLoadCount);
    Y_ENSURE(!Stats.ToLoadSize);

    Y_ENSURE(pinned);
    Pinned = pinned;
}

void TPrivatePageCache::EndTransaction() {
    Pinned = nullptr;
    ToLoad.clear();

    Stats.NewlyPinnedCount = 0;
    Stats.NewlyPinnedSize = 0;
    Stats.ToLoadCount = 0;
    Stats.ToLoadSize = 0;
}

void TPrivatePageCache::DropPage(TPageId pageId, TInfo *info) {
    if (info->DropPage(pageId)) {
        Stats.TotalSharedBody -= info->GetPageSize(pageId);
    }
}

void TPrivatePageCache::AddPage(TPageId pageId, TSharedPageRef sharedBody, TInfo *info)
{
    if (info->AddPage(pageId, std::move(sharedBody))) {
        Stats.TotalSharedBody += info->GetPageSize(pageId);
    }
}

THashMap<TLogoBlobID, TVector<TPageId>> TPrivatePageCache::GetToLoad() {
    THashMap<TLogoBlobID, TVector<TPageId>> result;
    for (auto& [pageCollectionId, pages] : ToLoad) {
        // TODO: support THashSet in TEvRequest 
        result.emplace(pageCollectionId, TVector<TPageId>(pages.begin(), pages.end()));
    }
    ToLoad.clear();
    return result;
}

void TPrivatePageCache::TranslatePinnedToSharedCacheTouches(ui64 &pinnedMemory) {
    Y_ENSURE(Pinned, "can be called only in a transaction context");

    for (const auto& [pageCollectionId, pages] : *Pinned) {
        if (auto *info = Info(pageCollectionId)) {
            auto& touches = SharedCacheTouches[pageCollectionId];
            for (const auto& [pageId, pinnedPageRef] : pages) {
                if (!info->IsStickyPage(pageId)) {
                    pinnedMemory += pinnedPageRef->size();
                }
                touches.insert(pageId);
            }
        }
    }
}

THashMap<TLogoBlobID, THashSet<TPageId>> TPrivatePageCache::GetSharedCacheTouches() {
    return std::move(SharedCacheTouches);
}

THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TInfo>> TPrivatePageCache::DetachPrivatePageCache() {
    THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TInfo>> ret;

    for (const auto &xpair : PageCollections) {
        TIntrusivePtr<TInfo> info(new TInfo(*xpair.second));
        ret.insert(std::make_pair(xpair.first, info));
    }

    return ret;
}

}}
