#include "flat_sausagecache.h"
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

TPrivatePageCache::TPage::TPage(ui32 size, ui32 pageId, TInfo* info)
    : LoadState(LoadStateNo)
    , CacheGeneration(TCacheCacheConfig::CacheGenNone)
    , Sticky(false)
    , SharedPending(false)
    , Padding(0)
    , Size(size)
    , Id(pageId)
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
        Y_VERIFY_DEBUG(src);
        if (src->LoadState == TPage::LoadStateLoaded) {
            auto* dst = EnsurePage(src->Id);
            dst->LoadState = TPage::LoadStateLoaded;
            dst->SharedBody = src->SharedBody;
            dst->PinnedBody = src->PinnedBody;
            dst->Sticky = src->Sticky;
        }
    }
}

TPrivatePageCache::TPrivatePageCache(const TCacheCacheConfig &cacheConfig)
    : Cache(cacheConfig)
{
}

void TPrivatePageCache::RegisterPageCollection(TIntrusivePtr<TInfo> info) {
    auto itpair = PageCollections.insert(decltype(PageCollections)::value_type(info->Id, info));
    Y_VERIFY(itpair.second, "double registration of page collection is forbidden. logic flaw?");
    ++Stats.TotalCollections;

    for (const auto& kv : info->PageMap) {
        auto* page = kv.second.Get();
        Y_VERIFY_DEBUG(page);
        if (page->LoadState == TPage::LoadStateLoaded && !page->Sticky) {
            auto &x = ToTouchShared[page->Info->Id][page->Id];
            if (!page->SharedPending && !page->SharedBody && page->PinnedBody) {
                // We keep pinned body around until it's either
                // accepted or dropped by the shared cache
                page->SharedPending = true;
                x = page->PinnedBody;
            }

            if (!page->PinPad) {
                page->LoadState = TPage::LoadStateNo;
                if (!page->SharedPending && page->PinnedBody) {
                    page->PinnedBody = { };
                }
                page->SharedBody.UnUse();
            }

            Y_VERIFY_DEBUG(!page->IsUnnecessary());
        }
        if (page->SharedBody)
            Stats.TotalSharedBody += page->Size;
        if (page->PinnedBody)
            Stats.TotalPinnedBody += page->Size;
        if (page->PinnedBody && !page->SharedBody)
            Stats.TotalExclusive += page->Size;
        if (page->SharedPending)
            Stats.TotalSharedPending += page->Size;
        if (page->Sticky)
            Stats.TotalSticky += page->Size;
    }

    ++info->Users;
}

TPrivatePageCache::TPage::TWaitQueuePtr TPrivatePageCache::ForgetPageCollection(TLogoBlobID id) {
    // todo: amortize destruction cost (how?)
    auto it = PageCollections.find(id);
    Y_VERIFY(it != PageCollections.end(), "trying to forget unknown page collection. logic flaw?");
    TIntrusivePtr<TInfo> info = it->second;

    TPage::TWaitQueuePtr ret;
    for (const auto& kv : info->PageMap) {
        auto* page = kv.second.Get();
        Y_VERIFY_DEBUG(page);

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
    Y_VERIFY(it != PageCollections.end(), "trying to lock unknown page collection. logic flaw?");
    ++it->second->Users;
}

bool TPrivatePageCache::UnlockPageCollection(TLogoBlobID id) {
    auto it = PageCollections.find(id);
    Y_VERIFY(it != PageCollections.end(), "trying to unlock unknown page collection. logic flaw?");
    TIntrusivePtr<TInfo> info = it->second;

    --info->Users;

    // Completely forget page collection if no users remain.
    if (!info->Users) {
        for (const auto& kv : info->PageMap) {
            auto* page = kv.second.Get();
            Y_VERIFY_DEBUG(page);
            if (page->CacheGeneration != TCacheCacheConfig::CacheGenNone)
                Cache.Evict(page);

            Y_VERIFY(!page->WaitQueue, "non-empty wait queue in forgotten page.");
            Y_VERIFY(!page->PinPad, "non-empty pin pad in forgotten page.");

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

THashMap<TLogoBlobID, THashMap<ui32, TSharedData>> TPrivatePageCache::GetPrepareSharedTouched() {
    return std::move(ToTouchShared);
}

TPrivatePageCache::TInfo* TPrivatePageCache::Info(TLogoBlobID id) {
    auto *x = PageCollections.FindPtr(id);
    if (x)
        return x->Get();
    else
        return nullptr;
}

void TPrivatePageCache::Touch(TPage *page) {
    if (!page->Sticky) {
        switch (page->LoadState) {
        case TPage::LoadStateNo:
            return;
        case TPage::LoadStateLoaded:
        case TPage::LoadStateRequested:
        case TPage::LoadStateRequestedAsync:
            return Evict(Cache.Touch(page));
        }
    }
}

void TPrivatePageCache::Touch(ui32 pageId, TInfo *info) {
    if (auto* page = info->GetPage(pageId)) {
        Touch(page);
    }
}

void TPrivatePageCache::MarkSticky(ui32 pageId, TInfo *collectionInfo) {
    TPage *page = collectionInfo->EnsurePage(pageId);
    if (Y_LIKELY(!page->Sticky)) {
        // N.B. the call site that marks pages as sticky starts to load them
        // asynchronously later, so sticky pages may not be loaded yet.
        page->Sticky = 1;
        Stats.TotalSticky += page->Size;

        // Sticky pages are not expected to exist in the cache
        if (page->CacheGeneration != TCacheCacheConfig::CacheGenNone) {
            Cache.Evict(page);
            page->CacheGeneration = TCacheCacheConfig::CacheGenNone;
        }
    }
}

TIntrusivePtr<TPrivatePageCachePinPad> TPrivatePageCache::Pin(ui32 pageId, TInfo *info) {
    TPage *page = info->EnsurePage(pageId);
    if (!page->PinPad) {
        page->PinPad = new TPrivatePageCachePinPad();
        Stats.PinnedSetSize += page->Size;

        // N.B. it's ok not to call Touch here, because pinned pages don't have
        // to be part of the cache even when they are loaded.
        Restore(page);

        if (page->LoadState != TPage::LoadStateLoaded)
            Stats.PinnedLoadSize += page->Size;
    }

    return page->PinPad;
}

void TPrivatePageCache::Unpin(ui32 pageId, TPrivatePageCachePinPad *pad, TInfo *info) {
    TPage *page = info->GetPage(pageId);
    if (page && page->PinPad.Get() == pad) {
        if (page->PinPad.RefCount() == 1) {
            page->PinPad.Drop();
            Stats.PinnedSetSize -= page->Size;
            if (page->LoadState != TPage::LoadStateLoaded)
                Stats.PinnedLoadSize -= page->Size;

            if (page->CacheGeneration != TCacheCacheConfig::CacheGenNone || page->Sticky)
                return;

            if (page->LoadState == TPage::LoadStateLoaded) {
                page->LoadState = TPage::LoadStateNo;
                if (!page->SharedPending) {
                    if (Y_LIKELY(page->PinnedBody)) {
                        Stats.TotalPinnedBody -= page->Size;
                        if (!page->SharedBody) {
                            Stats.TotalExclusive -= page->Size;
                        }
                        page->PinnedBody = { };
                    }
                }
                page->SharedBody.UnUse();
            }
        }
    }
}

std::pair<ui32, ui64> TPrivatePageCache::Load(TVector<ui32> &pages, TPrivatePageCacheWaitPad *waitPad, TInfo *info) {
    ui32 blocksToRequest = 0;
    ui64 bytesToRequest = 0;

    auto it = pages.begin();
    auto end = pages.end();

    while (it != end) {
        TPage *page = info->EnsurePage(*it);
        switch (page->LoadState) {
        case TPage::LoadStateNo:
            Y_VERIFY_DEBUG(!page->SharedPending, "Trying to load a page that may be restored");
            [[fallthrough]];
        case TPage::LoadStateRequestedAsync:
            page->LoadState = TPage::LoadStateRequested;
            bytesToRequest += page->Size;

            Y_VERIFY(!page->WaitQueue);
            page->WaitQueue = new TPage::TWaitQueue();
            page->WaitQueue->Push(waitPad);
            waitPad->Inc();

            ++blocksToRequest;
            ++it;
            break;
        case TPage::LoadStateLoaded:
            Y_FAIL("must not request already loaded pages");
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

bool TPrivatePageCache::Restore(TPage *page) {
    if (page->LoadState == TPage::LoadStateNo && page->SharedPending) {
        page->LoadState = TPage::LoadStateLoaded;
        return true;
    }

    if (page->LoadState == TPage::LoadStateNo && page->SharedBody) {
        if (page->SharedBody.Use()) {
            if (Y_LIKELY(!page->PinnedBody))
                Stats.TotalPinnedBody += page->Size;
            page->PinnedBody = TPinnedPageRef(page->SharedBody).GetData();
            page->LoadState = TPage::LoadStateLoaded;
            return true;
        }

        page->SharedBody.Drop();
        Stats.TotalSharedBody -= page->Size;
        if (Y_UNLIKELY(page->PinnedBody)) {
            Stats.TotalExclusive += page->Size;
        }
    }

    return false;
}

const TSharedData* TPrivatePageCache::Lookup(ui32 pageId, TInfo *info) {
    TPage *page = info->GetPage(pageId);
    if (!page)
        return { };

    if (Restore(page)) {
        Touch(page);
    }

    return page->GetBody();
}

TSharedPageRef TPrivatePageCache::LookupShared(ui32 pageId, TInfo *info) {
    TPage *page = info->GetPage(pageId);
    if (!page)
        return { };

    if (Restore(page)) {
        Touch(page);
    }

    return page->GetShared();
}

TPrivatePageCache::TPage::TWaitQueuePtr TPrivatePageCache::ProvideBlock(
        NSharedCache::TEvResult::TLoaded&& loaded, TInfo *info)
{
    Y_VERIFY_DEBUG(loaded.Page && loaded.Page.IsUsed());
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

    // N.B. we must be careful not to accidentally drop the sticky bit
    page->Fill(std::move(loaded.Page), page->Sticky);

    Stats.TotalSharedBody += page->Size;
    Stats.TotalPinnedBody += page->Size;

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

    if (page->CacheGeneration == TCacheCacheConfig::CacheGenNone
        && !page->Sticky && !page->PinPad)
    { // should be cache refresh by provided profile
        Touch(page);
    }

    return ret;
}

void TPrivatePageCache::UpdateCacheSize(ui64 cacheSize) {
    Cache.UpdateCacheSize(cacheSize);
}

THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TInfo>> TPrivatePageCache::DetachPrivatePageCache() {
    THashMap<TLogoBlobID, TIntrusivePtr<TPrivatePageCache::TInfo>> ret;

    for (const auto &xpair : PageCollections) {
        TIntrusivePtr<TInfo> info(new TInfo(*xpair.second));
        ret.insert(std::make_pair(xpair.first, info));
    }

    return ret;
}

void TPrivatePageCache::Evict(TPage *pages) {
    if (pages == nullptr)
        return;

    TPage *page = pages;
    for (;;) {
        Y_VERIFY(page->CacheGeneration == TCacheCacheConfig::CacheGenEvicted, "evicting non-evicted page with gen %" PRIu32, (ui32)page->CacheGeneration);
        page->CacheGeneration = TCacheCacheConfig::CacheGenNone;

        Y_VERIFY(!page->Sticky, "Unexpected sticky page evicted from cache");

        switch (page->LoadState) {
        case TPage::LoadStateNo:
        case TPage::LoadStateRequested:
        case TPage::LoadStateRequestedAsync:
            break;
        case TPage::LoadStateLoaded: {
            auto &x = ToTouchShared[page->Info->Id][page->Id];
            if (!page->SharedPending && !page->SharedBody && page->PinnedBody) {
                // We keep pinned body around until it's either
                // accepted or dropped by the shared cache
                Stats.TotalSharedPending += page->Size;
                page->SharedPending = true;
                x = page->PinnedBody;
            }

            if (!page->PinPad) {
                page->LoadState = TPage::LoadStateNo;
                if (!page->SharedPending) {
                    if (Y_LIKELY(page->PinnedBody)) {
                        Stats.TotalPinnedBody -= page->Size;
                        if (!page->SharedBody) {
                            Stats.TotalExclusive -= page->Size;
                        }
                        page->PinnedBody = { };
                    }
                }
                page->SharedBody.UnUse();
            }

            break;
        }
        default:
            Y_FAIL("unknown load state");
        }

        TPage *next = page->Next()->Node();
        if (page != next) {
            page->Unlink();
        }

        if (page->IsUnnecessary()) {
            if (Y_UNLIKELY(page->PinnedBody)) {
                Stats.TotalPinnedBody -= page->Size;
                Stats.TotalExclusive -= page->Size;
                page->PinnedBody = { };
            }
            const ui32 pageId = page->Id;
            auto* info = page->Info;
            Y_VERIFY_DEBUG(info->PageMap[pageId].Get() == page);
            Y_VERIFY(info->PageMap.erase(pageId));
        }

        if (page == next)
            break;

        page = next;
    }
}

}}
