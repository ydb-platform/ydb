#include "flat_sausagecache.h"
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

TPrivatePageCache::TPage::TPage(size_t size, TPageId pageId, TInfo* info)
    : LoadState(LoadStateNo)
    , Sticky(false)
    , SharedPending(false)
    , Id(pageId)
    , Size(size)
    , Info(info)
{}

TPrivatePageCache::TInfo::TInfo(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection)
    : Id(pageCollection->Label())
    , PageCollection(std::move(pageCollection))
    , Users(0)
{
    PageMap.resize(PageCollection->Total());
}

TPrivatePageCache::TInfo::TInfo(const TInfo &info)
    : Id(info.Id)
    , PageCollection(info.PageCollection)
    , Users(info.Users)
{
    PageMap.resize(info.PageMap.size());
    for (const auto& kv : info.PageMap) {
        auto* src = kv.second.Get();
        Y_DEBUG_ABORT_UNLESS(src);
        if (src->LoadState == TPage::LoadStateLoaded) {
            auto* dst = EnsurePage(src->Id);
            dst->LoadState = TPage::LoadStateLoaded;
            dst->SharedBody = src->SharedBody;
            dst->PinnedBody = src->PinnedBody;
            dst->Sticky = src->Sticky;
        }
    }
}

void TPrivatePageCache::RegisterPageCollection(TIntrusivePtr<TInfo> info) {
    auto itpair = PageCollections.insert(decltype(PageCollections)::value_type(info->Id, info));
    Y_ABORT_UNLESS(itpair.second, "double registration of page collection is forbidden. logic flaw?");
    ++Stats.TotalCollections;

    for (const auto& kv : info->PageMap) {
        auto* page = kv.second.Get();
        Y_DEBUG_ABORT_UNLESS(page);
        
        if (page->SharedBody)
            Stats.TotalSharedBody += page->Size;
        if (page->PinnedBody)
            Stats.TotalPinnedBody += page->Size;
        if (page->PinnedBody && !page->SharedBody)
            Stats.TotalExclusive += page->Size;
        if (page->Sticky)
            Stats.TotalSticky += page->Size;

        Y_DEBUG_ABORT_UNLESS(!page->SharedPending, "New page shouldn't be shared pending");
        TryShareBody(page);

        TryUnload(page);
        Y_DEBUG_ABORT_UNLESS(!page->IsUnnecessary());
    }

    ++info->Users;
}

TPrivatePageCache::TPage::TWaitQueuePtr TPrivatePageCache::ForgetPageCollection(TLogoBlobID id) {
    // todo: amortize destruction cost (how?)
    auto it = PageCollections.find(id);
    Y_ABORT_UNLESS(it != PageCollections.end(), "trying to forget unknown page collection. logic flaw?");
    TIntrusivePtr<TInfo> info = it->second;

    TPage::TWaitQueuePtr ret;
    for (const auto& kv : info->PageMap) {
        auto* page = kv.second.Get();
        Y_DEBUG_ABORT_UNLESS(page);

        if (page->LoadState == TPage::LoadStateRequested) {
            while (TPrivatePageCacheWaitPad *x = page->WaitQueue->Pop()) {
                if (0 == x->Dec()) {
                    if (!ret)
                        ret = new TPrivatePageCache::TPage::TWaitQueue;
                    ret->Push(x);
                }
            }
            page->WaitQueue.Destroy();
        }

        if (page->PinPad) {
            page->PinPad.Drop();
            Stats.PinnedSetSize -= page->Size;

            if (page->LoadState != TPage::LoadStateLoaded)
                Stats.PinnedLoadSize -= page->Size;
        }
    }

    UnlockPageCollection(id);

    return ret;
}

void TPrivatePageCache::LockPageCollection(TLogoBlobID id) {
    auto it = PageCollections.find(id);
    Y_ABORT_UNLESS(it != PageCollections.end(), "trying to lock unknown page collection. logic flaw?");
    ++it->second->Users;
}

bool TPrivatePageCache::UnlockPageCollection(TLogoBlobID id) {
    auto it = PageCollections.find(id);
    Y_ABORT_UNLESS(it != PageCollections.end(), "trying to unlock unknown page collection. logic flaw?");
    TIntrusivePtr<TInfo> info = it->second;

    --info->Users;

    // Completely forget page collection if no users remain.
    if (!info->Users) {
        for (const auto& kv : info->PageMap) {
            auto* page = kv.second.Get();
            Y_DEBUG_ABORT_UNLESS(page);

            Y_ABORT_UNLESS(!page->WaitQueue, "non-empty wait queue in forgotten page.");
            Y_ABORT_UNLESS(!page->PinPad, "non-empty pin pad in forgotten page.");

            if (page->SharedBody)
                Stats.TotalSharedBody -= page->Size;
            if (page->PinnedBody)
                Stats.TotalPinnedBody -= page->Size;
            if (page->PinnedBody && !page->SharedBody)
                Stats.TotalExclusive -= page->Size;
            if (page->SharedPending)
                Stats.TotalSharedPending -= page->Size;
            if (page->Sticky)
                Stats.TotalSticky -= page->Size;
        }

        info->PageMap.clear();
        PageCollections.erase(it);
        ToTouchShared.erase(id);
        --Stats.TotalCollections;
    }

    return !info->Users;
}

TPrivatePageCache::TInfo* TPrivatePageCache::Info(TLogoBlobID id) {
    auto *x = PageCollections.FindPtr(id);
    if (x)
        return x->Get();
    else
        return nullptr;
}

void TPrivatePageCache::MarkSticky(TPageId pageId, TInfo *collectionInfo) {
    TPage *page = collectionInfo->EnsurePage(pageId);
    if (Y_LIKELY(!page->Sticky)) {
        // N.B. the call site that marks pages as sticky starts to load them
        // asynchronously later, so sticky pages may not be loaded yet.
        page->Sticky = 1;
        Stats.TotalSticky += page->Size;
    }
}

TIntrusivePtr<TPrivatePageCachePinPad> TPrivatePageCache::Pin(TPage *page) {
    Y_DEBUG_ABORT_UNLESS(page);
    if (page && !page->PinPad) {
        page->PinPad = new TPrivatePageCachePinPad();
        Stats.PinnedSetSize += page->Size;

        TryLoad(page);

        if (page->LoadState != TPage::LoadStateLoaded)
            Stats.PinnedLoadSize += page->Size;
    }

    return page->PinPad;
}

void TPrivatePageCache::Unpin(TPage *page, TPrivatePageCachePinPad *pad) {
    if (page && page->PinPad.Get() == pad) {
        if (page->PinPad.RefCount() == 1) {
            page->PinPad.Drop();
            Stats.PinnedSetSize -= page->Size;
            if (page->LoadState != TPage::LoadStateLoaded)
                Stats.PinnedLoadSize -= page->Size;

            TryUnload(page);
        }
    }
}

std::pair<ui32, ui64> TPrivatePageCache::Request(TVector<ui32> &pages, TPrivatePageCacheWaitPad *waitPad, TInfo *info) {
    ui32 blocksToRequest = 0;
    ui64 bytesToRequest = 0;

    auto it = pages.begin();
    auto end = pages.end();

    while (it != end) {
        TPage *page = info->EnsurePage(*it);
        switch (page->LoadState) {
        case TPage::LoadStateNo:
            Y_DEBUG_ABORT_UNLESS(!page->SharedPending, "Trying to load a page that may be restored");
            [[fallthrough]];
        case TPage::LoadStateRequestedAsync:
            page->LoadState = TPage::LoadStateRequested;
            bytesToRequest += page->Size;

            Y_ABORT_UNLESS(!page->WaitQueue);
            page->WaitQueue = new TPage::TWaitQueue();
            page->WaitQueue->Push(waitPad);
            waitPad->Inc();

            ++blocksToRequest;
            ++it;
            break;
        case TPage::LoadStateLoaded:
            Y_ABORT("must not request already loaded pages");
        case TPage::LoadStateRequested:
            if (!page->WaitQueue)
                page->WaitQueue = new TPage::TWaitQueue();

            page->WaitQueue->Push(waitPad);
            waitPad->Inc();

            --end;
            if (end != it)
                *it = *end;
            break;
        }
    }
    pages.erase(end, pages.end());
    return std::make_pair(blocksToRequest, bytesToRequest);
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
        if (!page->SharedPending && !page->PinPad && !page->Sticky) {
            ToTouchShared[page->Info->Id][page->Id];
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
        Y_ABORT_UNLESS(info->PageMap.erase(pageId));
    }
}

void TPrivatePageCache::TPrivatePageCache::TryShareBody(TPage *page) {
    if (page->LoadState == TPage::LoadStateLoaded) {
        auto &x = ToTouchShared[page->Info->Id][page->Id];
        if (!page->SharedPending && !page->SharedBody && page->PinnedBody) {
            // We keep pinned body around until it's either
            // accepted or dropped by the shared cache
            page->SharedPending = true;
            Stats.TotalSharedPending += page->Size;
            x = page->PinnedBody;
        }
    }
}

const TSharedData* TPrivatePageCache::Lookup(TPageId pageId, TInfo *info) {
    TPage *page = info->EnsurePage(pageId);
    
    TryLoad(page);

    if (page->LoadState == TPage::LoadStateLoaded) {
        if (page->Empty()) {
            Touches.PushBack(page);
            Stats.CurrentCacheHits++;
            if (!page->Sticky) {
                Stats.CurrentCacheHitSize += page->Size;
            }
        }
        return &page->PinnedBody;
    }

    if (page->Empty()) {
        ToLoad.PushBack(page);

        // Note: we mark flat index pages sticky before we load them
        if (!page->Sticky && info->GetPageType(page->Id) == EPage::FlatIndex) {
            MarkSticky(page->Id, info);
        }

        Stats.CurrentCacheMisses++;
    }
    return nullptr;
}

TSharedPageRef TPrivatePageCache::LookupShared(TPageId pageId, TInfo *info) {
    TPage *page = info->GetPage(pageId);
    if (!page)
        return { };

    if (page->LoadState == TPage::LoadStateLoaded) {
        if (page->SharedBody) {
            Y_DEBUG_ABORT_UNLESS(page->SharedBody.IsUsed(), "Loaded page should have used body");
            return page->SharedBody;
        } else {
            return TSharedPageRef::MakePrivate(page->PinnedBody);
        }
    }

    if (page->LoadState == TPage::LoadStateNo) {
        if (page->SharedBody) {
            auto copy = page->SharedBody;
            if (copy.Use()) {
                return copy;
            }

            page->SharedBody.Drop();
            Stats.TotalSharedBody -= page->Size;
            if (Y_UNLIKELY(page->PinnedBody)) {
                Stats.TotalExclusive += page->Size;
            }
        }
    }

    TryEraseIfUnnecessary(page);
    return { };
}

void TPrivatePageCache::CountTouches(TPinned &pinned, ui32 &newPages, ui64 &newMemory, ui64 &pinnedMemory) {
    if (pinned.empty()) {
        newPages += Stats.CurrentCacheHits;
        newMemory += Stats.CurrentCacheHitSize;
        return;
    }

    for (auto &page : Touches) {
        bool isPinned = pinned[page.Info->Id].contains(page.Id);

        if (!isPinned) {
            newPages++;
        }

        // Note: it seems useless to count sticky pages in tx usage
        // also we want to read index from Env
        if (!page.Sticky) {
            if (isPinned) {
                pinnedMemory += page.Size;
            } else {
                newMemory += page.Size;
            }
        }
    }
}

void TPrivatePageCache::PinTouches(TPinned &pinned, ui32 &touchedPages, ui32 &pinnedPages, ui64 &pinnedMemory) {
    for (auto &page : Touches) {
        auto &pinnedCollection = pinned[page.Info->Id];
        
        // would insert only if first seen
        if (pinnedCollection.insert(std::make_pair(page.Id, Pin(&page))).second) {
            pinnedPages++;
            // Note: it seems useless to count sticky pages in tx usage
            // also we want to read index from Env
            if (!page.Sticky) {
                pinnedMemory += page.Size;
            }
        }
        touchedPages++;
    }
}

void TPrivatePageCache::PinToLoad(TPinned &pinned, ui32 &pinnedPages, ui64 &pinnedMemory) {
    for (auto &page : ToLoad) {
        auto &pinnedCollection = pinned[page.Info->Id];

        // would insert only if first seen
        if (pinnedCollection.insert(std::make_pair(page.Id, Pin(&page))).second) {
            pinnedPages++;
            // Note: it seems useless to count sticky pages in tx usage
            // also we want to read index from Env
            if (!page.Sticky) {
                pinnedMemory += page.Size;
            }
        }
    }
}

void TPrivatePageCache::RepinPages(TPinned &newPinned, TPinned &oldPinned, size_t &pinnedPages) {
    auto repinTouched = [&](TPage* page) {
        auto& newPinnedCollection = newPinned[page->Info->Id];
        
        if (auto* oldPinnedCollection = oldPinned.FindPtr(page->Info->Id)) {
            // We had previously pinned pages from this page collection
            // Create new or move used old pins to the new map
            if (auto it = oldPinnedCollection->find(page->Id); it != oldPinnedCollection->end()) {
                Y_DEBUG_ABORT_UNLESS(it->second);
                newPinnedCollection[page->Id] = std::move(it->second);
                oldPinnedCollection->erase(it);
            } else {
                newPinnedCollection[page->Id] = Pin(page);
                pinnedPages++;
            }
        } else {
            newPinnedCollection[page->Id] = Pin(page);
            pinnedPages++;
        }
    };

    // Everything touched during this read iteration must be pinned
    for (auto& page : Touches) {
        repinTouched(&page);
    }
    for (auto& page : ToLoad) {
        repinTouched(&page);
    }
}

void TPrivatePageCache::UnpinPages(TPinned &pinned, size_t &unpinnedPages) {
    for (auto &xinfoid : pinned) {
        if (TPrivatePageCache::TInfo *info = Info(xinfoid.first)) {
            for (auto &x : xinfoid.second) {
                TPageId pageId = x.first;
                TPrivatePageCachePinPad *pad = x.second.Get();
                x.second.Reset();
                TPage *page = info->GetPage(pageId);
                Unpin(page, pad);
                unpinnedPages++;
            }
        }
    }
}

// todo: do we really need that grouping by page collection?
THashMap<TPrivatePageCache::TInfo*, TVector<ui32>> TPrivatePageCache::GetToLoad() const {
    THashMap<TPrivatePageCache::TInfo*, TVector<ui32>> result;
    for (auto &page : ToLoad) {
        result[page.Info].push_back(page.Id);
    }
    return result;
}

void TPrivatePageCache::ResetTouchesAndToLoad(bool verifyEmpty) {
    if (verifyEmpty) {
        Y_ABORT_UNLESS(!Touches);
        Y_ABORT_UNLESS(!Stats.CurrentCacheHits);
        Y_ABORT_UNLESS(!Stats.CurrentCacheHitSize);
        Y_ABORT_UNLESS(!ToLoad);
        Y_ABORT_UNLESS(!Stats.CurrentCacheMisses);
    }

    while (Touches) {
        TPage *page = Touches.PopBack();
        TryUnload(page);
    }
    Stats.CurrentCacheHits = 0;
    Stats.CurrentCacheHitSize = 0;

    while (ToLoad) {
        TPage *page = ToLoad.PopBack();
        TryEraseIfUnnecessary(page);
    }
    Stats.CurrentCacheMisses = 0;
}

void TPrivatePageCache::UpdateSharedBody(TInfo *info, TPageId pageId, TSharedPageRef shared) {
    TPage *page = info->GetPage(pageId);
    if (!page)
        return;

    // Note: shared cache may accept a pending page if it is used by multiple private caches
    // (for example, used by tablet and its follower)
    if (Y_UNLIKELY(!page->SharedPending)) {
        return;
    }
    Y_DEBUG_ABORT_UNLESS(page->LoadState == TPage::LoadStateLoaded, "Shared pending page should be loaded");
    
    // Shared cache accepted our page and provided its shared reference
    Stats.TotalSharedPending -= page->Size;
    page->SharedPending = false;
    if (Y_LIKELY(!page->SharedBody)) {
        Stats.TotalSharedBody += page->Size;
        if (Y_LIKELY(page->PinnedBody)) {
            Stats.TotalExclusive -= page->Size;
        }
    }
    page->SharedBody = std::move(shared);
    TryUnload(page);
}

void TPrivatePageCache::DropSharedBody(TInfo *info, TPageId pageId) {
    TPage *page = info->GetPage(pageId);
    if (!page)
        return;

    // Note: shared cache may drop a pending page if it is used by multiple private caches
    // (for example, used by tablet and its follower)
    if (Y_UNLIKELY(page->SharedPending)) {
        // Shared cache rejected our page so we should drop it too
        Stats.TotalSharedPending -= page->Size;
        page->SharedPending = false;
        TryUnload(page);
    }

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

TPrivatePageCache::TPage::TWaitQueuePtr TPrivatePageCache::ProvideBlock(
        NSharedCache::TEvResult::TLoaded&& loaded, TInfo *info)
{
    Y_DEBUG_ABORT_UNLESS(loaded.Page && loaded.Page.IsUsed());
    TPage *page = info->EnsurePage(loaded.PageId);

    if (page->LoadState != TPage::LoadStateLoaded && page->PinPad)
        Stats.PinnedLoadSize -= page->Size;

    if (Y_UNLIKELY(page->SharedBody))
        Stats.TotalSharedBody -= page->Size;
    if (Y_UNLIKELY(page->PinnedBody))
        Stats.TotalPinnedBody -= page->Size;
    if (Y_UNLIKELY(page->PinnedBody && !page->SharedBody))
        Stats.TotalExclusive -= page->Size;
    if (Y_UNLIKELY(page->SharedPending))
        Stats.TotalSharedPending -= page->Size;

    // Note: we must be careful not to accidentally drop the sticky bit
    page->Fill(std::move(loaded.Page), page->Sticky);
    Stats.TotalSharedBody += page->Size;
    Stats.TotalPinnedBody += page->Size;
    TryUnload(page);

    TPage::TWaitQueuePtr ret;
    if (page->WaitQueue) {
        while (TPrivatePageCacheWaitPad *x = page->WaitQueue->Pop()) {
            if (0 == x->Dec()) {
                if (!ret)
                    ret = new TPage::TWaitQueue();
                ret->Push(x);
            }
        }
        page->WaitQueue.Destroy();
    }

    return ret;
}

THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TInfo>> TPrivatePageCache::DetachPrivatePageCache() {
    THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TInfo>> ret;

    for (const auto &xpair : PageCollections) {
        TIntrusivePtr<TInfo> info(new TInfo(*xpair.second));
        ret.insert(std::make_pair(xpair.first, info));
    }

    return ret;
}

THashMap<TLogoBlobID, THashMap<TPrivatePageCache::TPageId, TSharedData>> TPrivatePageCache::GetPrepareSharedTouched() {
    return std::move(ToTouchShared);
}

}}
