#include "flat_bio_actor.h"
#include "flat_bio_events.h"
#include "shared_cache_events.h"
#include "shared_cache_pages.h"
#include "shared_cache_tiered.h"
#include "shared_cache_counters.h"
#include "shared_page.h"
#include "shared_sausagecache.h"
#include "util_fmt_abort.h"
#include <util/stream/format.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/util/page_map.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/set.h>

namespace NKikimr::NSharedCache {

using namespace NTabletFlatExecutor;

::NFormatPrivate::THumanReadableSize HumanReadableBytes(ui64 bytes) {
    return HumanReadableSize(bytes, SF_BYTES);
}

struct TRequest : public TSimpleRefCount<TRequest>, public TIntrusiveListItem<TRequest> {
    bool IsResponded() const {
        return !Sender;
    }

    void MarkResponded() {
        Sender = {};
    }

    TLogoBlobID Label;
    TActorId Sender;
    NBlockIO::EPriority Priority;
    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
    ui64 EventCookie = 0;
    ui64 RequestCookie = 0;
    ui64 PendingBlocks = 0;
    TVector<TEvResult::TLoaded> ReadyPages;
    TDeque<TPageId> QueuePagesToRequest; // FIXME: store first pending page index
    TIntrusivePtr<NPageCollection::TPagesWaitPad> WaitPad;
    NWilson::TTraceId TraceId;
};

// pending request, index in ready blocks for page
using TPendingRequests = THashMap<TIntrusivePtr<TRequest>, ui32>;

struct TCollection {
    TLogoBlobID Id;
    TMap<TActorId, TIntrusiveConstPtr<NPageCollection::IPageCollection>> InMemoryOwners;
    TSet<TActorId> Owners;
    TPageMap<TIntrusivePtr<TPage>> PageMap;
    ui64 TotalSize;
    TMap<TPageId, TPendingRequests> PendingRequests;
    TDeque<TPageId> DroppedPages;

    ECacheMode GetCacheMode() {
        return InMemoryOwners ? ECacheMode::TryKeepInMemory : ECacheMode::Regular;
    }
};

struct TPageTraits {
    struct TPageKey {
        TLogoBlobID LogoBlobID;
        ui32 PageId;
    };
    
    static ui64 GetSize(const TPage* page) {
        return sizeof(TPage) + page->Size;
    }

    static TPageKey GetKey(const TPage* page) {
        return {page->Collection->Id, page->PageId};
    }

    static size_t GetHash(const TPageKey& key) {
        return MultiHash(key.LogoBlobID.Hash(), key.PageId);
    }

    static TString ToString(const TPageKey& key) {
        return TStringBuilder() << "LogoBlobID: " << key.LogoBlobID.ToString() << " PageId: " << key.PageId;
    }

    static TString GetKeyToString(const TPage* page) {
        return ToString(GetKey(page));
    }

    static ES3FIFOPageLocation GetLocation(const TPage* page) {
        return page->Location;
    }

    static void SetLocation(TPage* page, ES3FIFOPageLocation location) {
        page->Location = location;
    }

    static ui32 GetFrequency(const TPage* page) {
        return page->GetFrequency();
    }

    static void SetFrequency(TPage* page, ui32 frequency) {
        page->SetFrequency(frequency);
    }

    static ui32 GetTier(TPage* page) {
        return static_cast<ui32>(page->CacheMode);
    }
};

enum class EBlockIOFetchTypeCookie {
    NoQueue = 1,
    AsyncQueue = 2,
    ScanQueue = 3,
    TryKeepInMemoryPreload = 4,
};

struct TRequestQueue {
    explicit TRequestQueue(EBlockIOFetchTypeCookie cookie)
        : Cookie(cookie)
    {}

    EBlockIOFetchTypeCookie Cookie;

    TMap<TActorId, TDeque<TIntrusivePtr<TRequest>>> Requests;

    ui64 Limit = 0;
    ui64 InFly = 0;

    TActorId NextToRequest;
};

namespace {

static bool DoTraceLog() {
    if (NLog::TSettings *settings = TlsActivationContext->LoggerSettings())
        return settings->Satisfies(NLog::PRI_TRACE, NKikimrServices::TABLET_SAUSAGECACHE);
    else
        return false;
}

class TSharedPageCache : public TActorBootstrapped<TSharedPageCache> {
    using ELnLev = NUtil::ELnLev;

    TActorId Owner;
    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;
    NSharedCache::TSharedCachePages* SharedCachePages;
    TSharedCacheConfig Config;
    TSharedPageCacheCounters Counters;

    THashMap<TLogoBlobID, TCollection> Collections;
    THashMap<TActorId, THashMap<TCollection*, TIntrusiveList<TRequest>>> Owners;
    TRequestQueue AsyncRequests{EBlockIOFetchTypeCookie::AsyncQueue};
    TRequestQueue ScanRequests{EBlockIOFetchTypeCookie::ScanQueue};

    TTieredCache<TPage, TPageTraits> Cache;

    ui64 StatBioReqs = 0;
    ui64 StatActiveBytes = 0;
    ui64 StatPassiveBytes = 0;
    ui64 StatLoadInFlyBytes = 0;

    ui64 MemLimitBytes = 0;
    ui64 TargetInMemoryBytes = 0;

    void ActualizeCacheSizeLimit() {
        ui64 limitBytes = MemLimitBytes;
        if (Config.HasMemoryLimit()) {
            limitBytes = Min(limitBytes, Config.GetMemoryLimit());
            Counters.ConfigLimitBytes->Set(Config.GetMemoryLimit());
        } else {
            Counters.ConfigLimitBytes->Set(0);
        }

        // limit of cache depends only on config and mem because passive pages may go in and out arbitrary
        // we may have some passive bytes, so if we fully fill this Cache we may exceed the limit
        // because of that DoGC should be called to ensure limits
        Cache.UpdateLimit(limitBytes, TargetInMemoryBytes);
        Counters.ActiveLimitBytes->Set(limitBytes);
    }

    void DoGC() {
        // maybe we already have enough useless pages
        // update StatActiveBytes + StatPassiveBytes
        ProcessGCList();

        // TODO: get rid of active pages reservation
        ui64 configActiveReservedBytes = (Config.HasMemoryLimit() ? Config.GetMemoryLimit() : 0)
            * Config.GetActivePagesReservationPercent() / 100;

        THashSet<TCollection*> recheck;
        ui64 evictedInMemoryBytes = 0;
        while (GetStatAllBytes() > MemLimitBytes
            || GetStatAllBytes() > (Config.HasMemoryLimit() ? Config.GetMemoryLimit() : Max<ui64>()) && StatActiveBytes > configActiveReservedBytes)
        {
            if (TPage* evictedPage = Cache.EvictNext()) {
                if (evictedPage->CacheMode == ECacheMode::TryKeepInMemory) {
                    evictedInMemoryBytes += TPageTraits::GetSize(evictedPage);
                }
                EvictNow(evictedPage, recheck);
            } else {
                break;
            }
        }
        if (recheck) {
            CheckExpiredCollections(std::move(recheck));
        }

        if (MemoryConsumer) {
            MemoryConsumer->SetConsumption(GetStatAllBytes());
        }

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "GC has finished with"
            << " Limit: " << HumanReadableBytes(Cache.GetLimit())
            << " Active: " << HumanReadableBytes(StatActiveBytes)
            << " Passive: " << HumanReadableBytes(StatPassiveBytes)
            << " LoadInFly: " << HumanReadableBytes(StatLoadInFlyBytes)
            << " EvictedInMemoryBytes: " << HumanReadableBytes(evictedInMemoryBytes)
        );
    }

    void Handle(NMemory::TEvConsumerRegistered::TPtr &ev, const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Register memory consumer");

        auto *msg = ev->Get();
        MemoryConsumer = std::move(msg->Consumer);
    }

    void Handle(NMemory::TEvConsumerLimit::TPtr &ev, const TActorContext& ctx) {
        auto *msg = ev->Get();

        LOG_INFO_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Limit memory consumer"
            << " with " << HumanReadableBytes(msg->LimitBytes));

        MemLimitBytes = msg->LimitBytes;
        Counters.MemLimitBytes->Set(MemLimitBytes);

        ActualizeCacheSizeLimit();
    }

    void Registered(TActorSystem *sys, const TActorId &owner) {
        NActors::TActorBootstrapped<TSharedPageCache>::Registered(sys, owner);
        Owner = owner;

        SharedCachePages = sys->AppData<TAppData>()->SharedCachePages.Get();
    }

    void TakePoison(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Poison"
            << " cache serviced " << StatBioReqs << " reqs"
            << " hit {" << Counters.CacheHitPages->Val() << " " << Counters.CacheHitBytes->Val() << "b}"
            << " miss {" << Counters.CacheMissPages->Val() << " " << Counters.CacheMissBytes->Val() << "b}");

        if (auto owner = std::exchange(Owner, { }))
            Send(owner, new TEvents::TEvGone);

        PassAway();
    }

    TCollection& AttachCollection(const TLogoBlobID &pageCollectionId, const NPageCollection::IPageCollection &pageCollection, const TActorId &owner) {
        TCollection &collection = EnsureCollection(pageCollectionId, pageCollection, owner);

        if (collection.Owners.insert(owner).second) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Add page collection " << pageCollectionId
                << " owner " << owner);
            auto ownerIt = Owners.find(owner);
            if (ownerIt == Owners.end()) {
                ownerIt = Owners.emplace(owner, THashMap<TCollection*, TIntrusiveList<TRequest>>()).first;
                Counters.Owners->Inc();
            }
            auto emplaced = ownerIt->second.emplace(&collection, TIntrusiveList<TRequest>()).second;
            Y_ENSURE(emplaced);
            Counters.PageCollectionOwners->Inc();
        }

        return collection;
    }

    void Handle(NSharedCache::TEvAttach::TPtr &ev, const TActorContext& ctx) {
        NSharedCache::TEvAttach *msg = ev->Get();
        const auto &pageCollection = *msg->PageCollection;
        const TLogoBlobID pageCollectionId = pageCollection.Label();

        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Attach page collection " << pageCollectionId
            << " owner " << ev->Sender
            << " cache mode " << msg->CacheMode);

        TCollection& collection = AttachCollection(pageCollectionId, pageCollection, ev->Sender);
        switch (msg->CacheMode) {
        case ECacheMode::Regular:
            TryMoveToRegularCache(collection, ev->Sender);
            break;
        case ECacheMode::TryKeepInMemory:
            TryMoveToTryKeepInMemoryCache(collection, std::move(msg->PageCollection), ev->Sender);
            break;
        }
    }

    void Handle(NSharedCache::TEvSaveCompactedPages::TPtr &ev, const TActorContext& ctx) {
        NSharedCache::TEvSaveCompactedPages *msg = ev->Get();
        const auto &pageCollection = *msg->PageCollection;
        const TLogoBlobID pageCollectionId = pageCollection.Label();

        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Save page collection " << pageCollectionId
            << " owner " << ev->Sender
            << " compacted pages " << msg->Pages);

        Y_ENSURE(pageCollectionId);
        Y_ENSURE(!Collections.contains(pageCollectionId), "Only new collections can save compacted pages");
        auto& collection = EnsureCollection(pageCollectionId, pageCollection, ev->Sender);

        for (auto &page : msg->Pages) {
            Y_ENSURE(page->PageId < collection.PageMap.size());

            auto emplaced = collection.PageMap.emplace(page->PageId, page);
            Y_ENSURE(emplaced, "Pages should be unique");

            page->Collection = &collection;
            BodyProvided(collection, page.Get());
        }
    }

    void Handle(NSharedCache::TEvRequest::TPtr &ev, const TActorContext& ctx) {
        NSharedCache::TEvRequest *msg = ev->Get();
        const auto &pageCollection = *msg->PageCollection;
        const TLogoBlobID pageCollectionId = pageCollection.Label();
        const bool doTraceLog = DoTraceLog();

        TCollection &collection = AttachCollection(pageCollectionId, pageCollection, ev->Sender);
        ECacheMode cacheMode = collection.GetCacheMode();

        TStackVec<std::pair<TPageId, ui32>> pendingPages; // pageId, reqIdx
        ui32 pagesToRequestCount = 0;

        TVector<TEvResult::TLoaded> readyPages(::Reserve(msg->Pages.size()));
        TVector<TPageId> pagesFromCacheTraceLog;

        TRequestQueue *queue = nullptr;
        switch (msg->Priority) {
            case NBlockIO::EPriority::None:
            case NBlockIO::EPriority::Fast:
                break;
            case NBlockIO::EPriority::Bkgr:
                queue = &AsyncRequests;
                break;
            case NBlockIO::EPriority::Bulk:
            case NBlockIO::EPriority::Low:
                queue = &ScanRequests;
                break;
        }

        for (const ui32 reqIdx : xrange(msg->Pages.size())) {
            const TPageId pageId = msg->Pages[reqIdx];
            auto* page = EnsurePage(pageCollection, collection, pageId, cacheMode);

            Counters.RequestedPages->Inc();
            Counters.RequestedBytes->Add(page->Size);

            bool wasEvicted = page->State == PageStateEvicted;
            if (wasEvicted) {
                Y_ENSURE(page->Use()); // still in PageMap, guaranteed to be alive
                page->State = PageStateLoaded;
                RemovePassivePage(page);
                AddActivePage(page);
            }

            switch (page->State) {
            case PageStateLoaded:
                Counters.CacheHitPages->Inc();
                Counters.CacheHitBytes->Add(page->Size);
                if (!wasEvicted) {
                    page->IncrementFrequency();
                }
                if (doTraceLog) {
                    pagesFromCacheTraceLog.push_back(pageId);
                }
                readyPages.emplace_back(pageId, TSharedPageRef::MakeUsed(page, SharedCachePages->GCList));
                break;
            case PageStateNo:
                ++pagesToRequestCount;
                [[fallthrough]];
            case PageStateRequested:
            case PageStateRequestedAsync:
            case PageStatePending:
                Counters.CacheMissPages->Inc();
                Counters.CacheMissBytes->Add(page->Size);
                readyPages.emplace_back(pageId, TSharedPageRef());
                pendingPages.emplace_back(pageId, reqIdx);
                break;
            case PageStateEvicted:
                Y_TABLET_ERROR("must not happens");
            }

            if (wasEvicted) {
                // call insert here because reloaded evicted page may be evicted again
                Evict(Cache.Insert(page));
            }
        }

        auto request = MakeIntrusive<TRequest>();
        request->Label = msg->PageCollection->Label();
        request->PageCollection = std::move(msg->PageCollection);
        request->Sender = ev->Sender;
        request->Priority = msg->Priority;
        request->EventCookie = ev->Cookie;
        request->RequestCookie = msg->Cookie;
        request->ReadyPages = std::move(readyPages);
        request->WaitPad = std::move(msg->WaitPad);
        request->TraceId = std::move(msg->TraceId);
        Counters.PendingRequests->Inc();

        if (pendingPages) {
            TVector<TPageId> pagesToRequest(::Reserve(pagesToRequestCount));
            TVector<TPageId> pagesToWaitTraceLog;
            ui64 pagesToRequestBytes = 0;
            if (doTraceLog) {
                pagesToWaitTraceLog.reserve(pendingPages.size() - pagesToRequestCount);
            }

            if (queue) {
                // register for loading regardless of pending state, to simplify actor deregister logic
                // would be filtered on actual request
                queue->Requests[ev->Sender].push_back(request);
            }

            for (auto [pageId, reqIdx] : pendingPages) {
                collection.PendingRequests[pageId].emplace(request, reqIdx);
                ++request->PendingBlocks;
                auto* page = collection.PageMap[pageId].Get();
                Y_ENSURE(page);

                if (queue) {
                    request->QueuePagesToRequest.push_back(pageId);
                }

                switch (page->State) {
                case PageStateNo:
                    pagesToRequest.push_back(pageId);
                    pagesToRequestBytes += page->Size;

                    if (queue)
                        page->State = PageStatePending;
                    else
                        page->State = PageStateRequested;

                    break;
                case PageStateRequested:
                    if (doTraceLog) {
                        pagesToWaitTraceLog.emplace_back(pageId);
                    }
                    break;
                case PageStateRequestedAsync:
                case PageStatePending:
                    if (!queue) {
                        pagesToRequest.push_back(pageId);
                        pagesToRequestBytes += page->Size;
                        page->State = PageStateRequested;
                    } else if (doTraceLog) {
                        pagesToWaitTraceLog.emplace_back(pageId);
                    }
                    break;
                default:
                    Y_TABLET_ERROR("must not happens");
                }
            }

            LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Request page collection " << pageCollectionId
                << " owner " << ev->Sender
                << " cookie " << ev->Cookie
                << " class " << request->Priority
                << " from cache " << pagesFromCacheTraceLog
                << " already requested " << pagesToWaitTraceLog
                << " to request " << pagesToRequest);
            
            Owners[ev->Sender][&collection].PushBack(request.Get());

            if (pagesToRequest) {
                if (queue) {
                    RequestFromQueue(*queue);
                } else {
                    SendRequest(*request, std::move(pagesToRequest), pagesToRequestBytes, EBlockIOFetchTypeCookie::NoQueue);
                }
            }
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Request page collection " << pageCollectionId
                << " owner " << ev->Sender
                << " cookie " << ev->Cookie
                << " class " << msg->Priority
                << " from cache " << msg->Pages);
            SendResult(*request);
        }
    }

    void RequestFromQueue(TRequestQueue &queue) {
        if (queue.Requests.empty()) {
            return;
        }

        auto it = queue.Requests.begin();
        if (queue.NextToRequest) {
            it = queue.Requests.find(queue.NextToRequest);
        }

        while (queue.Requests && queue.InFly <= queue.Limit) { // on limit == 0 would request pages one by one
            // request whole limit from one page collection for better locality (if possible)
            if (it == queue.Requests.end()) {
                it = queue.Requests.begin();
            }
            Y_ENSURE(!it->second.empty());

            ui32 nthToRequest = 0;
            ui32 nthToLoad = 0;
            ui64 sizeToLoad = 0;

            auto& request_ = it->second.front();
            auto& request = *request_;

            if (!request.IsResponded()) {
                auto *collection = Collections.FindPtr(request.Label);
                Y_ENSURE(collection);

                for (TPageId pageId : request.QueuePagesToRequest) {
                    ++nthToRequest;

                    auto* page = collection->PageMap[pageId].Get();
                    if (!page || page->State != PageStatePending)
                        continue;

                    ++nthToLoad;
                    queue.InFly += page->Size;
                    sizeToLoad += page->Size;
                    if (queue.InFly > queue.Limit)
                        break;
                }

                if (nthToLoad != 0) {
                    TVector<ui32> toLoad;
                    toLoad.reserve(nthToLoad);
                    for (TPageId pageId : request.QueuePagesToRequest) {
                        auto* page = collection->PageMap[pageId].Get();
                        if (!page || page->State != PageStatePending)
                            continue;

                        toLoad.push_back(pageId);
                        page->State = PageStateRequestedAsync;
                        if (--nthToLoad == 0)
                            break;
                    }

                    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Request page collection " << request.Label
                        << (&queue == &AsyncRequests ? " async" : " scan") << " queue"
                        << " pages " << toLoad);

                    SendRequest(request, std::move(toLoad), sizeToLoad, queue.Cookie);
                }
            }

            // cleanup
            if (request.IsResponded() || nthToRequest == request.QueuePagesToRequest.size()) {
                if (request.IsResponded()) {
                    DropPendingRequest(request_);
                }

                it->second.pop_front();

                if (it->second.empty()) {
                    it = queue.Requests.erase(it);
                }
                else {
                    // FIXME(kungasc): this is really strange, I think we should just handle requests in their original order one by one
                    ++it;
                }
            } else {
                request.QueuePagesToRequest.erase(request.QueuePagesToRequest.begin(), request.QueuePagesToRequest.begin() + nthToRequest);
                ++it;
            }
        }

        if (it == queue.Requests.end()) {
            queue.NextToRequest = TActorId();
        } else {
            queue.NextToRequest = it->first;
        }
    }

    void DropPendingRequest(TIntrusivePtr<TRequest>& request) {
        // Note: pending requests that were responded during Unregister and Detach
        // should be removed from PendingRequests manually
        Y_ASSERT(request->IsResponded());
        if (request.RefCount() == 1) {
            // already no PendingRequests
            return;
        }

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Drop pending page collection request " << request->Label
            << " class " << request->Priority
            << " cookie " << request->EventCookie);

        auto *collection = Collections.FindPtr(request->Label);
        Y_ENSURE(collection);

        for (TPageId pageId : request->QueuePagesToRequest) {
            auto pageRequestsIt = collection->PendingRequests.find(pageId);
            if (pageRequestsIt != collection->PendingRequests.end()) {
                if (pageRequestsIt->second.erase(request) && pageRequestsIt->second.empty()) {
                    collection->PendingRequests.erase(pageRequestsIt);
                }
            }
        }

        TryDropExpiredCollection(*collection);

        // Note: sent request pages will be kept in PendingRequests until their pages are loaded
    }

    void Handle(NSharedCache::TEvSync::TPtr &ev, const TActorContext& ctx) {
        NSharedCache::TEvSync *msg = ev->Get();
        THashMap<TLogoBlobID, THashSet<TPageId>> droppedPages;

        for (auto &[pageCollectionId, pages] : msg->Pages) {
            LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Sync page collection " << pageCollectionId
                << " owner " << ev->Sender
                << " pages " << pages);

            auto collection = Collections.FindPtr(pageCollectionId);
            if (!collection) {
                droppedPages[pageCollectionId].insert(pages.begin(), pages.end());
                continue;
            }

            for (auto pageId : pages) {
                Y_ENSURE(pageId < collection->PageMap.size());
                auto* page = collection->PageMap[pageId].Get();
                if (!page) {
                    droppedPages[pageCollectionId].insert(pageId);
                }
            }
        }

        if (droppedPages) {
            SendDroppedPages(ev->Sender, std::move(droppedPages));
        }
    }

    void Handle(NSharedCache::TEvUnregister::TPtr &ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Unregister"
            << " owner " << ev->Sender);

        auto ownerIt = Owners.find(ev->Sender);
        if (ownerIt == Owners.end()) {
            return;
        }

        for (auto& [collection, requests] : ownerIt->second) {
            for (auto& request : requests) {
                SendError(request, NKikimrProto::RACE);
            }

            LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Remove page collection " << collection->Id
                << " owner " << ev->Sender);
            bool erased = collection->Owners.erase(ev->Sender);
            Y_ENSURE(erased);
            Counters.PageCollectionOwners->Dec();

            TryMoveToRegularCache(*collection, ev->Sender);

            TryDropExpiredCollection(*collection);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Remove owner " << ev->Sender);
        Owners.erase(ownerIt);
        Counters.Owners->Dec();
    }

    void Handle(NSharedCache::TEvDetach::TPtr &ev, const TActorContext& ctx) {
        const TLogoBlobID pageCollectionId = ev->Get()->PageCollectionId;
        auto collection = Collections.FindPtr(pageCollectionId);

        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Detach page collection " << pageCollectionId
            << " owner " << ev->Sender);

        if (!collection || !collection->Owners.erase(ev->Sender)) {
            return;
        }

        auto ownerIt = Owners.find(ev->Sender);
        Y_ENSURE(ownerIt != Owners.end());

        auto collectionIt = ownerIt->second.find(collection);
        Y_ENSURE(collectionIt != ownerIt->second.end());

        // Note: sent request will be kept in PendingRequests until their pages are loaded
        // while queued requests will be handled in RequestFromQueue
        for (auto& request : collectionIt->second) {
            SendError(request, NKikimrProto::RACE);
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Remove page collection " << collection->Id
            << " owner " << ev->Sender);
        ownerIt->second.erase(collectionIt);
        Counters.PageCollectionOwners->Dec();

        TryMoveToRegularCache(*collection, ev->Sender);

        TryDropExpiredCollection(*collection);
    }

    void Handle(NBlockIO::TEvData::TPtr &ev, const TActorContext& ctx) {
        auto *msg = ev->Get();

        LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Receive page collection " << msg->PageCollection->Label()
            << " status " << msg->Status
            << " pages " << msg->Pages);

        RemoveInFlyPages(msg->Pages.size(), msg->Cookie);

        auto fetchType = static_cast<EBlockIOFetchTypeCookie>(ev->Cookie);
        TRequestQueue *queue = nullptr;
        switch (fetchType) {
            case EBlockIOFetchTypeCookie::NoQueue:
                break;
            case EBlockIOFetchTypeCookie::AsyncQueue:
                queue = &AsyncRequests;
                break;
            case EBlockIOFetchTypeCookie::ScanQueue:
                queue = &ScanRequests;
                break;
            case EBlockIOFetchTypeCookie::TryKeepInMemoryPreload:
                break;
        }
        if (queue) {
            Y_ENSURE(queue->InFly >= msg->Cookie);
            queue->InFly -= msg->Cookie;
        }

        auto collection = Collections.FindPtr(msg->PageCollection->Label());
        if (!collection) {
            if (queue) {
                RequestFromQueue(*queue);
            }
            return;
        }

        if (msg->Status != NKikimrProto::OK) {
            DropCollection(*collection, msg->Status);
        } else {
            bool needNotifyOwners = fetchType == EBlockIOFetchTypeCookie::TryKeepInMemoryPreload && collection->InMemoryOwners;
            auto loadedPages = needNotifyOwners ? TVector<TPage*>(::Reserve(msg->Pages.size())) : TVector<TPage*>();
            for (auto &paged : msg->Pages) {
                Y_ENSURE(paged.PageId < collection->PageMap.size());
                auto* page = collection->PageMap[paged.PageId].Get();
                if (!page || !page->HasMissingBody()) {
                    continue;
                }

                page->ProvideBody(std::move(paged.Data));
                BodyProvided(*collection, page);
                if (needNotifyOwners) {
                    loadedPages.push_back(page);
                }
            }

            if (loadedPages) {
                for (const auto& [owner, pageCollection] : collection->InMemoryOwners) {
                    NotifyOwners(pageCollection, loadedPages, owner);
                }
            }
        }

        if (queue) {
            RequestFromQueue(*queue);
        }
    }

    TPage* EnsurePage(const NPageCollection::IPageCollection& pageCollection, TCollection& collection, const TPageId pageId, ECacheMode initialMode) {
        Y_ENSURE(pageId < collection.PageMap.size(),
            "Page collection " << pageCollection.Label()
            << " requested page " << pageId
            << " out of " << collection.PageMap.size() << " pages");
        auto* page = collection.PageMap[pageId].Get();

        if (!page) {
            Y_ENSURE(collection.PageMap.emplace(pageId, (page = new TPage(pageId, pageCollection.Page(pageId).Size, &collection))));
            page->CacheMode = initialMode;
        }

        return page;
    }

    void ReloadEvictedPage(TPage* page) {
        Y_ASSERT(page->State == PageStateEvicted);
        Y_ENSURE(page->Use()); // still in PageMap, guaranteed to be alive
        page->State = PageStateLoaded;
        RemovePassivePage(page);
        AddActivePage(page);
        Evict(Cache.Insert(page));
    }

    TCollection& EnsureCollection(const TLogoBlobID& pageCollectionId, const NPageCollection::IPageCollection& pageCollection, const TActorId& owner) {
        TCollection &collection = Collections[pageCollectionId];
        if (!collection.Id) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Add page collection " << pageCollectionId);
            Counters.PageCollections->Inc();
            Y_ENSURE(pageCollectionId);
            collection.Id = pageCollectionId;
            collection.PageMap.resize(pageCollection.Total());
            collection.TotalSize = sizeof(TPage) * pageCollection.Total() + pageCollection.BackingSize();
        } else {
            Y_DEBUG_ABORT_UNLESS(collection.Id == pageCollectionId);
            Y_ENSURE(collection.PageMap.size() == pageCollection.Total(),
                "Page collection " << pageCollectionId
                << " changed number of pages from " << collection.PageMap.size()
                << " to " << pageCollection.Total() << " by " << owner);
        }
        return collection;
    }

    void TryDropExpiredCollection(TCollection& collection) {
        // Drop unnecessary collections from memory
        if (!collection.Owners &&
            !collection.PendingRequests &&
            collection.PageMap.used() == 0)
        {
            auto pageCollectionId = collection.Id;
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Drop expired page collection " << pageCollectionId);
            Collections.erase(pageCollectionId);
            Counters.PageCollections->Dec();
        }
    }

    void Wakeup(TKikimrEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
        LOG_INFO_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Wakeup " << tag);

        switch (tag) {
        case EWakeupTag::DoGCScheduled:
            ScheduleGC();
            [[fallthrough]];
        case EWakeupTag::DoGCManual:
            // DoGC will be called at the end of StateFunc
            break;
        }
    }

    void ProcessGCList() {
        THashSet<TCollection*> recheck;

        while (auto rawPage = SharedCachePages->GCList->PopGC()) {
            auto* page = static_cast<TPage*>(rawPage.Get());
            if (page->State == PageStateEvicted && page->GetFrequency() > 0) {
                // page was accessed while being passive, load it back
                ReloadEvictedPage(page);
            }
            // load evicted page may be evicted again
            TryDrop(page, recheck);
        }

        if (recheck) {
            CheckExpiredCollections(std::move(recheck));
        }
    }

    void TryDrop(TPage* page, THashSet<TCollection*>& recheck) {
        if (page->TryDrop()) {
            // We have successfully dropped the page
            // We are guaranteed no new uses for this page are possible
            Y_ENSURE(page->State == PageStateEvicted);
            RemovePassivePage(page);

            Y_VERIFY_DEBUG_S(page->Collection, "Evicted pages are expected to have collection");
            if (auto* collection = page->Collection) {
                auto pageId = page->PageId;
                Y_DEBUG_ABORT_UNLESS(collection->PageMap[pageId].Get() == page);
                Y_ENSURE(collection->PageMap.erase(pageId));
                // Note: don't use page after erase as it may be deleted
                if (collection->Owners) {
                    collection->DroppedPages.push_back(pageId);
                }
                recheck.insert(collection);
            }
        }
    }

    void ScheduleGC() {
        TActivationContext::AsActorContext().Schedule(TDuration::Seconds(15), new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::DoGCScheduled)));
    }

    void CheckExpiredCollections(THashSet<TCollection*> recheck) {
        THashMap<TActorId, THashMap<TLogoBlobID, THashSet<TPageId>>> droppedPages;

        for (TCollection *collection : recheck) {
            if (collection->DroppedPages) {
                // N.B. usually there is a single owner
                for (TActorId owner : collection->Owners) {
                    droppedPages[owner][collection->Id].insert(collection->DroppedPages.begin(), collection->DroppedPages.end());
                }
                collection->DroppedPages.clear();
            }

            TryDropExpiredCollection(*collection);
        }

        for (auto& kv : droppedPages) {
            SendDroppedPages(kv.first, std::move(kv.second));
        }
    }

    void SendDroppedPages(TActorId owner, THashMap<TLogoBlobID, THashSet<TPageId>>&& droppedPages_) {
        auto msg = MakeHolder<NSharedCache::TEvUpdated>();
        msg->DroppedPages = std::move(droppedPages_);
        for (auto& [pageCollectionId, droppedPages] : msg->DroppedPages) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Drop page collection " << pageCollectionId
                << " pages " << droppedPages
                << " owner " << owner);
        }
        Send(owner, msg.Release());
    }

    void BodyProvided(TCollection &collection, TPage *page) {
        AddActivePage(page);
        auto pendingRequestsIt = collection.PendingRequests.find(page->PageId);
        if (pendingRequestsIt == collection.PendingRequests.end()) {
            Evict(Cache.Insert(page));
            return;
        }
        for (auto &[request, index] : pendingRequestsIt->second) {
            if (request->IsResponded()) {
                continue;
            }

            auto &readyPage = request->ReadyPages[index];
            Y_ENSURE(readyPage.PageId == page->PageId);
            readyPage.Page = TSharedPageRef::MakeUsed(page, SharedCachePages->GCList);

            if (--request->PendingBlocks == 0) {
                SendResult(*request);
            }
        }
        collection.PendingRequests.erase(pendingRequestsIt);
        Evict(Cache.Insert(page));
    }

    void SendResult(TRequest &request) {
        if (request.IsResponded()) {
            return;
        }

        TAutoPtr<NSharedCache::TEvResult> result = new NSharedCache::TEvResult(std::move(request.PageCollection), NKikimrProto::OK, request.RequestCookie);
        result->Pages = std::move(request.ReadyPages);
        result->WaitPad = std::move(request.WaitPad);

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Send page collection result " << result->PageCollection->Label()
            << " owner " << request.Sender
            << " class " << request.Priority
            << " pages " << result->Pages
            << " cookie " << request.EventCookie);

        Send(request.Sender, result.Release(), 0, request.EventCookie);
        Counters.PendingRequests->Dec();
        Counters.SucceedRequests->Inc();
        StatBioReqs += 1;

        request.MarkResponded();
    }

    void SendError(TRequest &request, NKikimrProto::EReplyStatus error) {
        if (request.IsResponded()) {
            return;
        }

        TAutoPtr<NSharedCache::TEvResult> result = new NSharedCache::TEvResult(std::move(request.PageCollection), error, request.RequestCookie);
        result->WaitPad = std::move(request.WaitPad);

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Send page collection error " << result->PageCollection->Label()
            << " owner " << request.Sender
            << " class " << request.Priority
            << " error " << error
            << " cookie " << request.EventCookie);

        Send(request.Sender, result.Release(), 0, request.EventCookie);
        Counters.PendingRequests->Dec();
        Counters.FailedRequests->Inc();
        StatBioReqs += 1;

        request.MarkResponded();
    }

    void NotifyOwners(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TVector<TPage*>& readyPages, const TActorId& owner) {
        TVector<TEvResult::TLoaded> readyLoadedPages;

        for (auto* page : readyPages) {
            if (page->State == PageStateLoaded) { // page may be evicted before NotifyOwners call
                readyLoadedPages.emplace_back(page->PageId, TSharedPageRef::MakeUsed(page, SharedCachePages->GCList));
            }
        }

        if (readyLoadedPages) {
            TAutoPtr<NSharedCache::TEvResult> result = new NSharedCache::TEvResult(std::move(pageCollection), NKikimrProto::OK, 0);
            result->Pages = std::move(readyLoadedPages);
            Send(owner, result.Release(), 0, static_cast<ui64>(ERequestTypeCookie::TryKeepInMemPages));
        }
    }

    void SendRequest(TRequest& request, TVector<TPageId>&& pages, ui64 bytes, EBlockIOFetchTypeCookie cookie) {
        AddInFlyPages(pages.size(), bytes);

        // fetch cookie -> requested size
        // event cookie -> queue type
        auto *fetch = new NBlockIO::TEvFetch(request.Priority, request.PageCollection, std::move(pages), bytes);
        if (cookie == EBlockIOFetchTypeCookie::AsyncQueue || cookie == EBlockIOFetchTypeCookie::ScanQueue) {
            // Note: queued requests can fetch multiple times, so copy trace id
            fetch->TraceId = request.TraceId.GetTraceId();
        } else {
            fetch->TraceId = std::move(request.TraceId);            
        }
        NBlockIO::Start(this, request.Sender, static_cast<ui64>(cookie), fetch);
    }

    void DropCollection(TCollection &collection, NKikimrProto::EReplyStatus blobStorageError) {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Drop page collection " << collection.Id
            << " error " << blobStorageError);

        // decline all pending requests
        for (auto &[_, requests] : collection.PendingRequests) {
            for (auto &[request, _] : requests) {
                SendError(*request, blobStorageError);
            }
        }
        collection.PendingRequests.clear();

        bool haveValidPages = false;
        size_t droppedPagesCount = 0;
        for (const auto &kv : collection.PageMap) {
            auto* page = kv.second.Get();

            Cache.Erase(page);
            page->EnsureNoCacheFlags();

            if (page->State == PageStateLoaded) {
                page->State = PageStateEvicted;
                RemoveActivePage(page);
                AddPassivePage(page);
                if (page->UnUse()) {
                    SharedCachePages->GCList->PushGC(page);
                }
            }

            if (page->State == PageStateEvicted) {
                // Evicted pages are either still used or scheduled for gc
                haveValidPages = true;
                continue;
            }

            page->Collection = nullptr;
            ++droppedPagesCount;
        }

        if (haveValidPages) {
            TVector<ui32> dropped(Reserve(droppedPagesCount));
            for (const auto &kv : collection.PageMap) {
                auto* page = kv.second.Get();
                if (!page->Collection) {
                    dropped.push_back(page->PageId);
                }
            }
            for (ui32 pageId : dropped) {
                collection.PageMap.erase(pageId);
            }
        } else {
            collection.PageMap.clear();
        }

        //TODO: delete ownership of dropping page collection

        TryDropExpiredCollection(collection);
    }

    void TryMoveToRegularCache(TCollection& collection, const TActorId& owner) {
        if (!collection.InMemoryOwners.erase(owner)) {
            return;
        }
        if (collection.InMemoryOwners) {
            return;
        }

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Change mode of page collection " << collection.Id
            << " to " << ECacheMode::Regular);
        Y_ENSURE(TargetInMemoryBytes >= collection.TotalSize);
        TargetInMemoryBytes -= collection.TotalSize;
        Counters.TargetInMemoryBytes->Set(TargetInMemoryBytes);
        ActualizeCacheSizeLimit();
        // TODO: move pages async and batched
        for (const auto& kv : collection.PageMap) {
            auto* page = kv.second.Get();
            TryChangeCacheMode(page, ECacheMode::Regular);
        }
    }

    void TryMoveToTryKeepInMemoryCache(TCollection& collection, TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
        if (!collection.InMemoryOwners.emplace(owner, pageCollection).second) {
            return;
        }

        if (collection.InMemoryOwners.size() == 1) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Change mode of page collection " << collection.Id
                << " to " << ECacheMode::TryKeepInMemory);
            TargetInMemoryBytes += collection.TotalSize;
            Counters.TargetInMemoryBytes->Set(TargetInMemoryBytes);
            ActualizeCacheSizeLimit();
        }

        // TODO: pages async and batched and re-request when evicted
        TVector<TPageId> pagesToRequest(::Reserve(pageCollection->Total()));
        TVector<TPage*> loadedPages;
        ui64 pagesToRequestBytes = 0;
        ui64 remainBytes = Cache.GetLimit();
        for (const auto& pageId : xrange(pageCollection->Total())) {
            auto* page = EnsurePage(*pageCollection, collection, pageId, ECacheMode::TryKeepInMemory);
            TryChangeCacheMode(page, ECacheMode::TryKeepInMemory);

            switch (page->State) {
            case PageStateLoaded:
                loadedPages.push_back(page);
                break;
            case PageStateEvicted:
                ReloadEvictedPage(page);
                loadedPages.push_back(page);
                break;
            case PageStateNo:
                // Prevent loading out-of-memory pages. TODO: Load the remaining pages when there is enough memory available.
                if (TPageTraits::GetSize(page) > remainBytes) {
                    collection.PageMap.erase(pageId);
                    continue;
                }
                remainBytes -= TPageTraits::GetSize(page);
                page->State = PageStateRequestedAsync;
                pagesToRequest.push_back(pageId);
                pagesToRequestBytes += page->Size;
            }
        }

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Try move collection " << collection.Id
                << " in memory, total pages: " << pageCollection->Total() << " (" << HumanReadableBytes(collection.TotalSize) << "), "
                << "pages already loaded: " << loadedPages.size() << " , "
                << "pages to request: " << pagesToRequest.size() << " (" << HumanReadableBytes(pagesToRequestBytes) << "), "
                << "pages out of memory limit: " << (pageCollection->Total() - loadedPages.size() - pagesToRequest.size()));

        if (loadedPages) {
            NotifyOwners(pageCollection, loadedPages, owner);
        }

        if (pagesToRequest) {
            TRequest request;
            request.PageCollection = std::move(pageCollection);
            request.Sender = owner;
            request.Priority = NBlockIO::EPriority::Bulk;

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Request page collection " << request.PageCollection->Label()
                << " owner " << owner
                << " class " << request.Priority
                << " pages " << pagesToRequest);

            // TODO: add some counters for these fetches?
            SendRequest(request, std::move(pagesToRequest), pagesToRequestBytes, EBlockIOFetchTypeCookie::TryKeepInMemoryPreload);
        }
    }

    void TryChangeCacheMode(TPage* page, ECacheMode targetMode) {
        if (page->CacheMode != targetMode) {
            switch (page->State) {
            case PageStateLoaded:
                Cache.Erase(page);
                page->EnsureNoCacheFlags();
                RemoveActivePage(page);
                page->CacheMode = targetMode;
                AddActivePage(page);
                Evict(Cache.Insert(page));
                break;
            default:
                page->CacheMode = targetMode;
                break;
            }
        }
    }

    void Evict(TIntrusiveList<TPage>&& pages) {
        while (!pages.Empty()) {
            TPage* page = pages.PopFront();

            page->EnsureNoCacheFlags();

            Y_ENSURE(page->State == PageStateLoaded, "unexpected " << page->State << " page state");
            page->State = PageStateEvicted;

            RemoveActivePage(page);
            AddPassivePage(page);
            if (page->UnUse()) {
                SharedCachePages->GCList->PushGC(page);
            }
        }
    }

    void EvictNow(TPage* page, THashSet<TCollection*>& recheck) {
        page->EnsureNoCacheFlags();

        Y_ENSURE(page->State == PageStateLoaded, "unexpected " << page->State << " page state");
        page->State = PageStateEvicted;

        RemoveActivePage(page);
        AddPassivePage(page);
        if (page->UnUse()) {
            TryDrop(page, recheck);
        }
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        {
            auto* appData = AppData(ctx);
            NKikimrSharedCache::TSharedCacheConfig config;
            if (record.GetConfig().HasBootstrapConfig()) {
                if (record.GetConfig().GetBootstrapConfig().HasSharedCacheConfig()) {
                    config.MergeFrom(record.GetConfig().GetBootstrapConfig().GetSharedCacheConfig());
                }
            } else if (appData->BootstrapConfig.HasSharedCacheConfig()) {
                config.MergeFrom(appData->BootstrapConfig.GetSharedCacheConfig());
            }
            if (record.GetConfig().HasSharedCacheConfig()) {
                config.MergeFrom(record.GetConfig().GetSharedCacheConfig());
            } else {
                config.MergeFrom(appData->SharedCacheConfig);
            }
            LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Update config " << config.ShortDebugString());
            Config.Swap(&config);
        }

        ActualizeCacheSizeLimit();

        AsyncRequests.Limit = Config.GetAsyncQueueInFlyLimit();
        ScanRequests.Limit = Config.GetScanQueueInFlyLimit();
    }

    inline ui64 GetStatAllBytes() const {
        return StatActiveBytes + StatPassiveBytes + StatLoadInFlyBytes;
    }

    inline void AddActivePage(const TPage* page) {
        auto pageSize = TPageTraits::GetSize(page);
        StatActiveBytes += pageSize;
        Counters.ActivePages->Inc();
        Counters.ActiveBytes->Add(pageSize);
        if (page->CacheMode == ECacheMode::TryKeepInMemory) {
            Counters.ActiveInMemoryBytes->Add(pageSize);
        }
    }

    inline void RemoveActivePage(const TPage* page) {
        auto pageSize = TPageTraits::GetSize(page);
        Y_DEBUG_ABORT_UNLESS(StatActiveBytes >= pageSize);
        StatActiveBytes -= pageSize;
        Counters.ActivePages->Dec();
        Counters.ActiveBytes->Sub(pageSize);
        if (page->CacheMode == ECacheMode::TryKeepInMemory) {
            Counters.ActiveInMemoryBytes->Sub(pageSize);
        }
    }

    inline void AddPassivePage(const TPage* page) {
        auto pageSize = TPageTraits::GetSize(page);
        StatPassiveBytes += pageSize;
        Counters.PassivePages->Inc();
        Counters.PassiveBytes->Add(pageSize);
    }

    inline void RemovePassivePage(const TPage* page) {
        auto pageSize = TPageTraits::GetSize(page);
        Y_DEBUG_ABORT_UNLESS(StatPassiveBytes >= pageSize);
        StatPassiveBytes -= pageSize;
        Counters.PassivePages->Dec();
        Counters.PassiveBytes->Sub(pageSize);
    }

    inline void AddInFlyPages(ui64 count, ui64 size) {
        ui64 totalBytes = size + sizeof(TPage) * count;
        StatLoadInFlyBytes += totalBytes;
        Counters.LoadInFlyPages->Add(count);
        Counters.LoadInFlyBytes->Add(totalBytes);
    }

    inline void RemoveInFlyPages(ui64 count, ui64 size) {
        ui64 totalBytes = size + sizeof(TPage) * count;
        Y_ENSURE(StatLoadInFlyBytes >= totalBytes);
        StatLoadInFlyBytes -= totalBytes;
        Counters.LoadInFlyPages->Sub(count);
        Counters.LoadInFlyBytes->Sub(totalBytes);
    }

public:
    TSharedPageCache(const TSharedCacheConfig& config, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Config(config)
        , Counters(counters)
        , Cache(config.GetMemoryLimit())
    {
        AsyncRequests.Limit = Config.GetAsyncQueueInFlyLimit();
        ScanRequests.Limit = Config.GetScanQueueInFlyLimit();
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Bootstrap with config " << Config.ShortDebugString());

        MemLimitBytes = Config.HasMemoryLimit()
            ? Config.GetMemoryLimit()
            : 128_MB; // soon will be updated by MemoryController
        ActualizeCacheSizeLimit();
        
        Send(NMemory::MakeMemoryControllerId(), new NMemory::TEvConsumerRegister(NMemory::EMemoryConsumerKind::SharedCache));

        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
                NKikimrConsole::TConfigItem::BootstrapConfigItem, NKikimrConsole::TConfigItem::SharedCacheConfigItem}));

        Become(&TThis::StateFunc);

        ScheduleGC();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSharedCache::TEvAttach, Handle);
            HFunc(NSharedCache::TEvSaveCompactedPages, Handle);
            HFunc(NSharedCache::TEvRequest, Handle);
            HFunc(NSharedCache::TEvSync, Handle);
            HFunc(NSharedCache::TEvUnregister, Handle);
            HFunc(NSharedCache::TEvDetach, Handle);

            HFunc(NBlockIO::TEvData, Handle);
            HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
            HFunc(TKikimrEvents::TEvWakeup, Wakeup);
            CFunc(TEvents::TSystem::PoisonPill, TakePoison);

            HFunc(NMemory::TEvConsumerRegistered, Handle);
            HFunc(NMemory::TEvConsumerLimit, Handle);
        }

        DoGC();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SAUSAGE_CACHE;
    }
};

}

IActor* CreateSharedPageCache(
    const TSharedCacheConfig& config,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters
) {
    return new TSharedPageCache(config,
        GetServiceCounters(counters, "tablets")->GetSubgroup("type", "S_CACHE"));
}

}

template<> inline
void Out<TVector<ui32>>(IOutputStream& o, const TVector<ui32> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}

template<> inline
void Out<TDeque<ui32>>(IOutputStream& o, const TDeque<ui32> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}

template<> inline
void Out<THashSet<ui32>>(IOutputStream& o, const THashSet<ui32> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}

template<> inline
void Out<TVector<NKikimr::NSharedCache::TEvResult::TLoaded>>(IOutputStream& o, const TVector<NKikimr::NSharedCache::TEvResult::TLoaded> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x.PageId << ' ';
    o << "]";
}

template<> inline
void Out<TVector<NKikimr::NPageCollection::TLoadedPage>>(IOutputStream& o, const TVector<NKikimr::NPageCollection::TLoadedPage> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x.PageId << ' ';
    o << "]";
}

template<> inline
void Out<TVector<TIntrusivePtr<NKikimr::NSharedCache::TPage>>>(IOutputStream& o, const TVector<TIntrusivePtr<NKikimr::NSharedCache::TPage>> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x->PageId << ' ';
    o << "]";
}
