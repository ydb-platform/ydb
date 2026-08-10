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
        }
    }
}

TIntrusivePtr<TPrivatePageCache::TInfo> TPrivatePageCache::GetPageCollection(TLogoBlobID id) const {
    auto it = PageCollections.find(id);
    Y_ABORT_UNLESS(it != PageCollections.end(), "trying to get unknown page collection. logic flaw?");
    return it->second;
}

void TPrivatePageCache::RegisterPageCollection(TIntrusivePtr<TInfo> info) {
    auto itpair = PageCollections.insert(decltype(PageCollections)::value_type(info->Id, info));
    Y_ABORT_UNLESS(itpair.second, "double registration of page collection is forbidden. logic flaw?");
    ++Stats.TotalCollections;

    for (const auto& kv : info->PageMap) {
        auto* page = kv.second.Get();
        Y_ABORT_UNLESS(page);
        Y_ABORT_UNLESS(page->SharedBody, "New filled pages can't be without a shared body");

        Stats.TotalSharedBody += page->Size;
        if (page->PinnedBody)
            Stats.TotalPinnedBody += page->Size;

        TryUnload(page);
        // notify shared cache that we have a page handle
        ToTouchShared[page->Info->Id].insert(page->Id);
        Y_DEBUG_ABORT_UNLESS(!page->IsUnnecessary());
    }

    ++info->Users;
}

TPrivatePageCache::TPage::TWaitQueuePtr TPrivatePageCache::ForgetPageCollection(TIntrusivePtr<TInfo> info) {
    // todo: amortize destruction cost (how?)

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

    UnlockPageCollection(info->Id);

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

std::pair<ui32, ui64> TPrivatePageCache::Request(TVector<TPageId> &pages, TPrivatePageCacheWaitPad *waitPad, TInfo *info) {
    ui32 blocksToRequest = 0;
    ui64 bytesToRequest = 0;

    auto it = pages.begin();
    auto end = pages.end();

    while (it != end) {
        TPage *page = info->EnsurePage(*it);
        switch (page->LoadState) {
        case TPage::LoadStateNo:
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
        if (!page->PinPad) {
            ToTouchShared[page->Info->Id].insert(page->Id);
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
        Y_DEBUG_ABORT_UNLESS(info->GetPageType(page->Id) != EPage::FlatIndex, "Flat index pages should have been sticked and preloaded");
        ToLoad.PushBack(page);
        Stats.CurrentCacheMisses++;
    }
    return nullptr;
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
        if (!page.IsSticky()) {
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
            if (!page.IsSticky()) {
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
            if (!page.IsSticky()) {
                pinnedMemory += page.Size;
            }
        }
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

THashMap<TPrivatePageCache::TInfo*, TVector<TPageId>> TPrivatePageCache::GetToLoad() const {
    THashMap<TPrivatePageCache::TInfo*, TVector<TPageId>> result;
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

void TPrivatePageCache::DropSharedBody(TInfo *info, TPageId pageId) {
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

    page->Fill(std::move(loaded.Page));
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

THashMap<TLogoBlobID, THashSet<TPrivatePageCache::TPageId>> TPrivatePageCache::GetPrepareSharedTouched() {
    return std::move(ToTouchShared);
}

}}
