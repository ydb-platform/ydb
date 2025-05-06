#include "flat_bio_actor.h"
#include "flat_bio_events.h"
#include "shared_cache_clock_pro.h"
#include "shared_cache_events.h"
#include "shared_cache_pages.h"
#include "shared_cache_s3fifo.h"
#include "shared_cache_switchable.h"
#include "shared_page.h"
#include "shared_sausagecache.h"
#include "util_fmt_abort.h"
#include <util/stream/format.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/util/cache_cache.h>
#include <ydb/core/util/page_map.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/set.h>

namespace NKikimr::NSharedCache {

using namespace NTabletFlatExecutor;

TSharedPageCacheCounters::TSharedPageCacheCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
    : Counters(counters)
    // lru cache counters:
    , FreshBytes(counters->GetCounter("fresh"))
    , StagingBytes(counters->GetCounter("staging"))
    , WarmBytes(counters->GetCounter("warm"))
    // page counters:
    , MemLimitBytes(counters->GetCounter("MemLimitBytes"))
    , ConfigLimitBytes(counters->GetCounter("ConfigLimitBytes"))
    , ActivePages(counters->GetCounter("ActivePages"))
    , ActiveBytes(counters->GetCounter("ActiveBytes"))
    , ActiveLimitBytes(counters->GetCounter("ActiveLimitBytes"))
    , PassivePages(counters->GetCounter("PassivePages"))
    , PassiveBytes(counters->GetCounter("PassiveBytes"))
    , RequestedPages(counters->GetCounter("RequestedPages", true))
    , RequestedBytes(counters->GetCounter("RequestedBytes", true))
    , CacheHitPages(counters->GetCounter("CacheHitPages", true))
    , CacheHitBytes(counters->GetCounter("CacheHitBytes", true))
    , CacheMissPages(counters->GetCounter("CacheMissPages", true))
    , CacheMissBytes(counters->GetCounter("CacheMissBytes", true))
    , LoadInFlyPages(counters->GetCounter("LoadInFlyPages"))
    , LoadInFlyBytes(counters->GetCounter("LoadInFlyBytes"))
    // page collection counters:
    , PageCollections(counters->GetCounter("PageCollections"))
    , Owners(counters->GetCounter("Owners"))
    , PageCollectionOwners(counters->GetCounter("PageCollectionOwners"))
    // request counters:
    , PendingRequests(counters->GetCounter("PendingRequests"))
    , SucceedRequests(counters->GetCounter("SucceedRequests", true))
    , FailedRequests(counters->GetCounter("FailedRequests", true))
{ }

TSharedPageCacheCounters::TCounterPtr TSharedPageCacheCounters::ReplacementPolicySize(TReplacementPolicy policy) {
    return Counters->GetCounter(TStringBuilder() << "ReplacementPolicySize/" << policy);
}

struct TRequest : public TSimpleRefCount<TRequest>, public TIntrusiveListItem<TRequest> {
    TRequest(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, NWilson::TTraceId &&traceId)
        : Label(pageCollection->Label())
        , PageCollection(std::move(pageCollection))
        , TraceId(std::move(traceId))
    {
    }

    bool IsResponded() const {
        return !Sender;
    }

    void MarkResponded() {
        Sender = {};
    }

    const TLogoBlobID Label;
    TActorId Sender;
    NBlockIO::EPriority Priority;
    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
    ui64 EventCookie = 0;
    ui64 RequestCookie = 0;
    ui64 PendingBlocks = 0;
    TVector<TEvResult::TLoaded> ReadyPages;
    TDeque<TPageId> QueuePagesToRequest; // FIXME: store first pending page index
    NWilson::TTraceId TraceId;
};

// pending request, index in ready blocks for page
using TPendingRequests = THashMap<TIntrusivePtr<TRequest>, ui32>;

struct TCollection {
    TLogoBlobID Id;
    TSet<TActorId> Owners;
    TPageMap<TIntrusivePtr<TPage>> PageMap;
    TMap<TPageId, TPendingRequests> PendingRequests;
    TDeque<TPageId> DroppedPages;
};

struct TRequestQueue {
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

    static const ui64 DO_GC_TAG = 1;

    static const ui64 NO_QUEUE_COOKIE = 1;
    static const ui64 ASYNC_QUEUE_COOKIE = 2;
    static const ui64 SCAN_QUEUE_COOKIE = 3;

    struct TCacheCachePageTraits {
        static ui64 GetWeight(const TPage* page) {
            return sizeof(TPage) + page->Size;
        }

        static ECacheCacheGeneration GetGeneration(const TPage *page) {
            return static_cast<ECacheCacheGeneration>(page->CacheFlags1);
        }

        static void SetGeneration(TPage *page, ECacheCacheGeneration generation) {
            ui32 generation_ = static_cast<ui32>(generation);
            Y_ENSURE(generation_ < (1 << 4));
            page->CacheFlags1 = generation_;
        }
    };

    struct TS3FIFOPageTraits {
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
            return static_cast<ES3FIFOPageLocation>(page->CacheFlags1);
        }

        static void SetLocation(TPage* page, ES3FIFOPageLocation location) {
            ui32 location_ = static_cast<ui32>(location);
            Y_ENSURE(location_ < (1 << 4));
            page->CacheFlags1 = location_;
        }

        static ui32 GetFrequency(const TPage* page) {
            return page->CacheFlags2;
        }

        static void SetFrequency(TPage* page, ui32 frequency) {
            Y_ENSURE(frequency < (1 << 4));
            page->CacheFlags2 = frequency;
        }
    };

    struct TClockProPageTraits {
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

        static bool Equals(const TPageKey& left, const TPageKey& right) {
            return left.PageId == right.PageId && left.LogoBlobID == right.LogoBlobID;
        }

        static TString ToString(const TPageKey& key) {
            return TStringBuilder() << "LogoBlobID: " << key.LogoBlobID.ToString() << " PageId: " << key.PageId;
        }

        static TString GetKeyToString(const TPage* page) {
            return ToString(GetKey(page));
        }

        static EClockProPageLocation GetLocation(const TPage* page) {
            return static_cast<EClockProPageLocation>(page->CacheFlags1);
        }

        static void SetLocation(TPage* page, EClockProPageLocation location) {
            ui32 location_ = static_cast<ui32>(location);
            Y_ENSURE(location_ < (1 << 4));
            page->CacheFlags1 = location_;
        }

        static bool GetReferenced(const TPage* page) {
            return page->CacheFlags2;
        }

        static void SetReferenced(TPage* page, bool referenced) {
            page->CacheFlags2 = static_cast<ui32>(referenced);
        }
    };

    struct TCompositeCachePageTraits {
        static ui64 GetSize(const TPage* page) {
            return sizeof(TPage) + page->Size;
        }

        static ui32 GetCacheId(const TPage* page) {
            return page->CacheId;
        }

        static void SetCacheId(TPage* page, ui32 id) {
            Y_ENSURE(id < (1 << 4));
            page->CacheId = id;
        }
    };

    TActorId Owner;
    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;
    NSharedCache::TSharedCachePages* SharedCachePages;
    TSharedCacheConfig Config;
    TSharedPageCacheCounters Counters;

    THashMap<TLogoBlobID, TCollection> Collections;
    THashMap<TActorId, THashMap<TCollection*, TIntrusiveList<TRequest>>> Owners;
    TRequestQueue AsyncRequests;
    TRequestQueue ScanRequests;

    TSwitchableCache<TPage, TCompositeCachePageTraits> Cache;

    ui64 StatBioReqs = 0;
    ui64 StatActiveBytes = 0;
    ui64 StatPassiveBytes = 0;
    ui64 StatLoadInFlyBytes = 0;

    bool GCScheduled = false;

    ui64 MemLimitBytes;

    THolder<ICacheCache<TPage>> CreateCache() {
        // TODO: pass actual limit to cache config
        // now it will be fixed by ActualizeCacheSizeLimit call

        switch (Config.GetReplacementPolicy()) {
            case NKikimrSharedCache::S3FIFO:
                return MakeHolder<TS3FIFOCache<TPage, TS3FIFOPageTraits>>(1);
            case NKikimrSharedCache::ClockPro:
                return MakeHolder<TClockProCache<TPage, TClockProPageTraits>>(1);
            case NKikimrSharedCache::ThreeLeveledLRU:
            default: {
                TCacheCacheConfig cacheCacheConfig(1, Counters.FreshBytes, Counters.StagingBytes, Counters.WarmBytes);
                return MakeHolder<TCacheCache<TPage, TCacheCachePageTraits>>(std::move(cacheCacheConfig));
            }
        }
    }

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
        Cache.UpdateLimit(limitBytes);
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
        while (GetStatAllBytes() > MemLimitBytes
            || GetStatAllBytes() > (Config.HasMemoryLimit() ? Config.GetMemoryLimit() : Max<ui64>()) && StatActiveBytes > configActiveReservedBytes)
        {
            TIntrusiveList<TPage> pages = Cache.EvictNext();
            if (pages.Empty()) {
                break;
            }
            while (!pages.Empty()) {
                TPage* page = pages.PopFront();
                EvictNow(page, recheck);
            }
        }
        if (recheck) {
            CheckExpiredCollections(std::move(recheck));
        }

        if (MemoryConsumer) {
            MemoryConsumer->SetConsumption(GetStatAllBytes());
        }
    }

    void Handle(NMemory::TEvConsumerRegistered::TPtr &ev, const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Register memory consumer");

        auto *msg = ev->Get();
        MemoryConsumer = std::move(msg->Consumer);
    }

    void Handle(NMemory::TEvConsumerLimit::TPtr &ev, const TActorContext& ctx) {
        auto *msg = ev->Get();

        LOG_INFO_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Limit memory consumer"
            << " with " << HumanReadableSize(msg->LimitBytes, SF_BYTES));

        MemLimitBytes = msg->LimitBytes;
        Counters.MemLimitBytes->Set(MemLimitBytes);

        ActualizeCacheSizeLimit();

        DoGC();
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
        TCollection &collection = Collections[pageCollectionId];
        if (!collection.Id) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Add page collection " << pageCollectionId);
            Counters.PageCollections->Inc();
            Y_ENSURE(pageCollectionId);
            collection.Id = pageCollectionId;
            collection.PageMap.resize(pageCollection.Total());
        } else {
            Y_DEBUG_ABORT_UNLESS(collection.Id == pageCollectionId);
            Y_ENSURE(collection.PageMap.size() == pageCollection.Total(),
                "Page collection " << pageCollectionId
                << " changed number of pages from " << collection.PageMap.size()
                << " to " << pageCollection.Total() << " by " << owner);
        }

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
            << " owner " << ev->Sender);

        AttachCollection(pageCollectionId, pageCollection, ev->Sender);
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
        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Add page collection " << pageCollectionId);
        Counters.PageCollections->Inc();
        TCollection &collection = Collections[pageCollectionId];
        collection.Id = pageCollectionId;
        collection.PageMap.resize(pageCollection.Total());

        for (auto &page : msg->Pages) {
            Y_ENSURE(page->PageId < collection.PageMap.size());

            auto emplaced = collection.PageMap.emplace(page->PageId, page);
            Y_ENSURE(emplaced, "Pages should be unique");

            page->Collection = &collection;
            BodyProvided(collection, page.Get());
            Evict(Cache.Touch(page.Get()));
        }

        DoGC();
    }

    void Handle(NSharedCache::TEvRequest::TPtr &ev, const TActorContext& ctx) {
        NSharedCache::TEvRequest *msg = ev->Get();
        const auto &pageCollection = *msg->Fetch->PageCollection;
        const TLogoBlobID pageCollectionId = pageCollection.Label();
        const bool doTraceLog = DoTraceLog();

        TCollection &collection = AttachCollection(pageCollectionId, pageCollection, ev->Sender);

        TStackVec<std::pair<TPageId, ui32>> pendingPages; // pageId, reqIdx
        ui32 pagesToRequestCount = 0;

        TVector<TEvResult::TLoaded> readyPages(::Reserve(msg->Fetch->Pages.size()));
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

        for (const ui32 reqIdx : xrange(msg->Fetch->Pages.size())) {
            const ui32 pageId = msg->Fetch->Pages[reqIdx];
            Y_ENSURE(pageId < collection.PageMap.size(),
                "Page collection " << pageCollectionId
                << " requested page " << pageId
                << " out of " << collection.PageMap.size() << " pages");
            auto* page = collection.PageMap[pageId].Get();
            if (!page) {
                Y_ENSURE(collection.PageMap.emplace(pageId, (page = new TPage(pageId, pageCollection.Page(pageId).Size, &collection))));
            }

            Counters.RequestedPages->Inc();
            Counters.RequestedBytes->Add(page->Size);

            switch (page->State) {
            case PageStateEvicted:
                Y_ENSURE(page->Use()); // still in PageMap, guaranteed to be alive
                page->State = PageStateLoaded;
                RemovePassivePage(page);
                AddActivePage(page);
                [[fallthrough]];
            case PageStateLoaded:
                Counters.CacheHitPages->Inc();
                Counters.CacheHitBytes->Add(page->Size);
                readyPages.emplace_back(pageId, TSharedPageRef::MakeUsed(page, SharedCachePages->GCList));
                if (doTraceLog) {
                    pagesFromCacheTraceLog.push_back(pageId);
                }
                Evict(Cache.Touch(page));
                break;
            case PageStateNo:
                ++pagesToRequestCount;
                [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
            case PageStateRequested:
            case PageStateRequestedAsync:
            case PageStatePending:
                Counters.CacheMissPages->Inc();
                Counters.CacheMissBytes->Add(page->Size);
                readyPages.emplace_back(pageId, TSharedPageRef());
                pendingPages.emplace_back(pageId, reqIdx);
                break;
            }
        }

        auto request = MakeIntrusive<TRequest>(std::move(msg->Fetch->PageCollection), std::move(msg->Fetch->TraceId));
        request->Sender = ev->Sender;
        request->Priority = msg->Priority;
        request->EventCookie = ev->Cookie;
        request->RequestCookie = msg->Fetch->Cookie;
        request->ReadyPages = std::move(readyPages);
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
                    AddInFlyPages(pagesToRequest.size(), pagesToRequestBytes);
                    // fetch cookie -> requested size
                    auto *fetch = new NPageCollection::TFetch(pagesToRequestBytes, request->PageCollection, std::move(pagesToRequest), std::move(request->TraceId));
                    NBlockIO::Start(this, request->Sender, NO_QUEUE_COOKIE, request->Priority, fetch);
                }
            }
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Request page collection " << pageCollectionId
                << " owner " << ev->Sender
                << " cookie " << ev->Cookie
                << " class " << msg->Priority
                <<  " from cache " << msg->Fetch->Pages);
            SendResult(request);
        }

        DoGC();
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
                    for (ui32 pageId : request.QueuePagesToRequest) {
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

                    AddInFlyPages(toLoad.size(), sizeToLoad);
                    // fetch cookie -> requested size;
                    // event cookie -> queue type
                    auto *fetch = new NPageCollection::TFetch(sizeToLoad, request.PageCollection, std::move(toLoad), request.TraceId.GetTraceId());
                    NBlockIO::Start(this, request.Sender, (&queue == &AsyncRequests ? ASYNC_QUEUE_COOKIE : SCAN_QUEUE_COOKIE), request.Priority, fetch);
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

        Y_ENSURE(request.RefCount() == 1, "Should not be in PendingRequests");
    }

    void Handle(NSharedCache::TEvTouch::TPtr &ev, const TActorContext& ctx) {
        NSharedCache::TEvTouch *msg = ev->Get();
        THashMap<TLogoBlobID, THashSet<TPageId>> droppedPages;

        for (auto &[pageCollectionId, touchedPages] : msg->Touched) {
            LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Touch page collection " << pageCollectionId
                << " owner " << ev->Sender
                << " pages " << touchedPages);

            auto collection = Collections.FindPtr(pageCollectionId);
            if (!collection) {
                droppedPages[pageCollectionId].insert(touchedPages.begin(), touchedPages.end());
                continue;
            }

            for (auto pageId : touchedPages) {
                Y_ENSURE(pageId < collection->PageMap.size());
                auto* page = collection->PageMap[pageId].Get();
                if (!page) {
                    droppedPages[pageCollectionId].insert(pageId);
                    continue;
                }

                switch (page->State) {
                case PageStateNo:
                    Y_TABLET_ERROR("unexpected uninitialized page found");
                case PageStateRequested:
                case PageStateRequestedAsync:
                case PageStatePending:
                    break;
                case PageStateEvicted:
                    Y_ENSURE(page->Use());
                    page->State = PageStateLoaded;
                    RemovePassivePage(page);
                    AddActivePage(page);
                    [[fallthrough]];
                case PageStateLoaded:
                    Evict(Cache.Touch(page));
                    break;
                default:
                    Y_TABLET_ERROR("unknown load state");
                }
            }
        }

        if (droppedPages) {
            SendDroppedPages(ev->Sender, std::move(droppedPages));
        }

        DoGC();
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

            TryDropExpiredCollection(*collection);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Remove owner " << ev->Sender);
        Owners.erase(ownerIt);
        Counters.Owners->Dec();

        ProcessGCList();
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

        TryDropExpiredCollection(*collection);

        ProcessGCList();
    }

    void Handle(NBlockIO::TEvData::TPtr &ev, const TActorContext& ctx) {
        auto *msg = ev->Get();

        LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Receive page collection " << msg->Fetch->PageCollection->Label()
            << " status " << msg->Status
            << " pages " << msg->Fetch->Pages);

        RemoveInFlyPages(msg->Fetch->Pages.size(), msg->Fetch->Cookie);

        TRequestQueue *queue = nullptr;
        if (ev->Cookie == ASYNC_QUEUE_COOKIE) {
            queue = &AsyncRequests;
        } else if (ev->Cookie == SCAN_QUEUE_COOKIE) {
            queue = &ScanRequests;
        } else {
            Y_ENSURE(ev->Cookie == NO_QUEUE_COOKIE);
        }
        if (queue) {
            Y_ENSURE(queue->InFly >= msg->Fetch->Cookie);
            queue->InFly -= msg->Fetch->Cookie;
        }

        auto collection = Collections.FindPtr(msg->Fetch->PageCollection->Label());
        if (!collection) {
            if (queue) {
                RequestFromQueue(*queue);
            }
            DoGC();
            return;
        }

        if (msg->Status != NKikimrProto::OK) {
            DropCollection(*collection, msg->Status);
        } else {
            for (auto &paged : msg->Blocks) {
                Y_ENSURE(paged.PageId < collection->PageMap.size());
                auto* page = collection->PageMap[paged.PageId].Get();
                if (!page || !page->HasMissingBody()) {
                    continue;
                }

                page->Initialize(std::move(paged.Data));
                BodyProvided(*collection, page);
                Evict(Cache.Touch(page));
            }
        }

        if (queue) {
            RequestFromQueue(*queue);
        }

        DoGC();
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
        LOG_INFO_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Wakeup " << ev->Get()->Tag);

        switch (ev->Get()->Tag) {
        case DO_GC_TAG: {
            GCScheduled = false;
            ProcessGCList();
            break;
        }
        default:
            Y_TABLET_ERROR("Unknown wakeup tag: " << ev->Get()->Tag);
        }
    }

    void ProcessGCList() {
        THashSet<TCollection*> recheck;

        while (auto rawPage = SharedCachePages->GCList->PopGC()) {
            auto* page = static_cast<TPage*>(rawPage.Get());
            TryDrop(page, recheck);
        }

        if (recheck) {
            CheckExpiredCollections(std::move(recheck));
        }

        TryScheduleGC();
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

    void TryScheduleGC() {
        if (!GCScheduled) {
            TActivationContext::AsActorContext().Schedule(TDuration::Seconds(15), new TKikimrEvents::TEvWakeup(DO_GC_TAG));
            GCScheduled = true;
        }
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
            return;
        }
        for (auto &[request, index] : pendingRequestsIt->second) {
            if (request->IsResponded()) {
                continue;
            }
            auto &readyPage = request->ReadyPages[index];
            Y_ENSURE(readyPage.PageId == page->PageId);
            readyPage.Page = TSharedPageRef::MakeUsed(page, SharedCachePages->GCList);

            if (--request->PendingBlocks == 0)
                SendResult(request);
        }
        collection.PendingRequests.erase(pendingRequestsIt);
    }

    void SendResult(const TIntrusivePtr<TRequest> &request) {
        if (request->IsResponded()) {
            return;
        }

        TAutoPtr<NSharedCache::TEvResult> result =
            new NSharedCache::TEvResult(std::move(request->PageCollection), request->RequestCookie, NKikimrProto::OK);
        result->Pages = std::move(request->ReadyPages);

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Send page collection result " << result->PageCollection->Label()
            << " owner " << request->Sender
            << " class " << request->Priority
            << " pages " << result->Pages
            << " cookie " << request->EventCookie);

        Send(request->Sender, result.Release(), 0, request->EventCookie);
        Counters.PendingRequests->Dec();
        Counters.SucceedRequests->Inc();
        StatBioReqs += 1;

        request->MarkResponded();
    }

    void SendError(TRequest &request, NKikimrProto::EReplyStatus error) {
        if (request.IsResponded()) {
            return;
        }

        TAutoPtr<NSharedCache::TEvResult> result =
            new NSharedCache::TEvResult(std::move(request.PageCollection), request.RequestCookie, error);

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

        auto currentReplacementPolicy = Config.GetReplacementPolicy();

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

        if (currentReplacementPolicy != Config.GetReplacementPolicy()) {
            LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Switch replacement policy "
                << "from " << currentReplacementPolicy << " to " << Config.GetReplacementPolicy());
            Evict(Cache.Switch(CreateCache(), Counters.ReplacementPolicySize(Config.GetReplacementPolicy())));
            LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Switch replacement policy done "
                << " from " << currentReplacementPolicy << " to " << Config.GetReplacementPolicy());
        }

        DoGC();
    }

    inline ui64 GetStatAllBytes() const {
        return StatActiveBytes + StatPassiveBytes + StatLoadInFlyBytes;
    }

    inline void AddActivePage(const TPage* page) {
        StatActiveBytes += sizeof(TPage) + page->Size;
        Counters.ActivePages->Inc();
        Counters.ActiveBytes->Add(sizeof(TPage) + page->Size);
    }

    inline void RemoveActivePage(const TPage* page) {
        Y_DEBUG_ABORT_UNLESS(StatActiveBytes >= sizeof(TPage) + page->Size);
        StatActiveBytes -= sizeof(TPage) + page->Size;
        Counters.ActivePages->Dec();
        Counters.ActiveBytes->Sub(sizeof(TPage) + page->Size);
    }

    inline void AddPassivePage(const TPage* page) {
        StatPassiveBytes += sizeof(TPage) + page->Size;
        Counters.PassivePages->Inc();
        Counters.PassiveBytes->Add(sizeof(TPage) + page->Size);
    }

    inline void RemovePassivePage(const TPage* page) {
        Y_DEBUG_ABORT_UNLESS(StatPassiveBytes >= sizeof(TPage) + page->Size);
        StatPassiveBytes -= sizeof(TPage) + page->Size;
        Counters.PassivePages->Dec();
        Counters.PassiveBytes->Sub(sizeof(TPage) + page->Size);
    }

    inline void AddInFlyPages(ui64 count, ui64 size) {
        StatLoadInFlyBytes += size;
        Counters.LoadInFlyPages->Add(count);
        Counters.LoadInFlyBytes->Add(size);
    }

    inline void RemoveInFlyPages(ui64 count, ui64 size) {
        Y_ENSURE(StatLoadInFlyBytes >= size);
        StatLoadInFlyBytes -= size;
        Counters.LoadInFlyPages->Sub(count);
        Counters.LoadInFlyBytes->Sub(size);
    }

public:
    TSharedPageCache(const TSharedCacheConfig& config, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Config(config)
        , Counters(counters)
        , Cache(1, CreateCache(), Counters.ReplacementPolicySize(Config.GetReplacementPolicy()))
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
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSharedCache::TEvAttach, Handle);
            HFunc(NSharedCache::TEvSaveCompactedPages, Handle);
            HFunc(NSharedCache::TEvRequest, Handle);
            HFunc(NSharedCache::TEvTouch, Handle);
            HFunc(NSharedCache::TEvUnregister, Handle);
            HFunc(NSharedCache::TEvDetach, Handle);

            HFunc(NBlockIO::TEvData, Handle);
            HFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
            HFunc(TKikimrEvents::TEvWakeup, Wakeup);
            CFunc(TEvents::TSystem::PoisonPill, TakePoison);

            HFunc(NMemory::TEvConsumerRegistered, Handle);
            HFunc(NMemory::TEvConsumerLimit, Handle);
        }
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
void Out<TVector<TIntrusivePtr<NKikimr::NSharedCache::TPage>>>(IOutputStream& o, const TVector<TIntrusivePtr<NKikimr::NSharedCache::TPage>> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x->PageId << ' ';
    o << "]";
}
