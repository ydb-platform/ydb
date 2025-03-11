#include "flat_bio_actor.h"
#include "flat_bio_events.h"
#include "shared_cache_clock_pro.h"
#include "shared_cache_events.h"
#include "shared_cache_pages.h"
#include "shared_cache_s3fifo.h"
#include "shared_cache_switchable.h"
#include "shared_page.h"
#include "shared_sausagecache.h"
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
    , FreshBytes(counters->GetCounter("fresh"))
    , StagingBytes(counters->GetCounter("staging"))
    , WarmBytes(counters->GetCounter("warm"))
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
{ }

TSharedPageCacheCounters::TCounterPtr TSharedPageCacheCounters::ReplacementPolicySize(TReplacementPolicy policy) {
    return Counters->GetCounter(TStringBuilder() << "ReplacementPolicySize/" << policy);
}

struct TRequest : public TSimpleRefCount<TRequest> {
    TRequest(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, NWilson::TTraceId &&traceId)
        : Label(pageCollection->Label())
        , PageCollection(std::move(pageCollection))
        , TraceId(std::move(traceId))
    {
    }

    const TLogoBlobID Label;
    TActorId Source;    /* receiver of read results     */
    TActorId Owner;     /* receiver of NBlockIO::TEvStat*/
    NBlockIO::EPriority Priority;
    TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
    ui64 EventCookie = 0;
    ui64 RequestCookie = 0;
    ui64 PendingBlocks = 0;
    TVector<TEvResult::TLoaded> ReadyPages;
    TDeque<ui32> PagesToRequest;
    NWilson::TTraceId TraceId;
};

struct TExpectant {
    TDeque<std::pair<TIntrusivePtr<TRequest>, ui32>> SourceRequests; // waiting request, index in ready blocks for page
};

struct TCollection {
    TLogoBlobID Id;
    TSet<TActorId> Owners;
    TPageMap<TIntrusivePtr<TPage>> PageMap;
    TMap<ui32, TExpectant> Expectants;
    TDeque<ui32> DroppedPages;
};

struct TCollectionsOwner {
    THashSet<TCollection*> Collections;
};

struct TRequestQueue {
    struct TPagesToRequest : public TIntrusiveListItem<TPagesToRequest> {
        TIntrusivePtr<TRequest> Request;
    };

    struct TByActorRequest {
        TIntrusiveList<TPagesToRequest> Listed;
        THashMap<TLogoBlobID, TDeque<TPagesToRequest>> Index;
    };

    TMap<TActorId, TByActorRequest> Requests;

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

    struct TCacheCachePageTraits {
        static ui64 GetWeight(const TPage* page) {
            return sizeof(TPage) + page->Size;
        }

        static ECacheCacheGeneration GetGeneration(const TPage *page) {
            return static_cast<ECacheCacheGeneration>(page->CacheFlags1);
        }

        static void SetGeneration(TPage *page, ECacheCacheGeneration generation) {
            ui32 generation_ = static_cast<ui32>(generation);
            Y_ABORT_UNLESS(generation_ < (1 << 4));
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
            Y_ABORT_UNLESS(location_ < (1 << 4));
            page->CacheFlags1 = location_;
        }

        static ui32 GetFrequency(const TPage* page) {
            return page->CacheFlags2;
        }

        static void SetFrequency(TPage* page, ui32 frequency) {
            Y_ABORT_UNLESS(frequency < (1 << 4));
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
            Y_ABORT_UNLESS(location_ < (1 << 4));
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
            Y_ABORT_UNLESS(id < (1 << 4));
            page->CacheId = id;
        }
    };

    TActorId Owner;
    THashMap<TLogoBlobID, TCollection> Collections;
    THashMap<TActorId, TCollectionsOwner> CollectionsOwners;
    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;
    NSharedCache::TSharedCachePages* SharedCachePages;

    TRequestQueue AsyncRequests;
    TRequestQueue ScanRequests;

    TSharedCacheConfig Config;
    TSharedPageCacheCounters Counters;
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

    void Registered(TActorSystem *sys, const TActorId &owner)
    {
        NActors::TActorBootstrapped<TSharedPageCache>::Registered(sys, owner);
        Owner = owner;

        SharedCachePages = sys->AppData<TAppData>()->SharedCachePages.Get();
    }

    void TakePoison(const TActorContext& ctx)
    {
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
            Y_ABORT_UNLESS(pageCollectionId);
            collection.Id = pageCollectionId;
            collection.PageMap.resize(pageCollection.Total());
        } else {
            Y_DEBUG_ABORT_UNLESS(collection.Id == pageCollectionId);
            Y_ABORT_UNLESS(collection.PageMap.size() == pageCollection.Total(),
                "Page collection %s changed number of pages from %" PRISZT " to %" PRIu32 " by %s",
                pageCollectionId.ToString().c_str(), collection.PageMap.size(), pageCollection.Total(), owner.ToString().c_str());
        }

        if (collection.Owners.insert(owner).second) {
            CollectionsOwners[owner].Collections.insert(&collection);
        }

        return collection;
    }

    void Handle(NSharedCache::TEvAttach::TPtr &ev, const TActorContext& ctx) {
        NSharedCache::TEvAttach *msg = ev->Get();
        const auto &pageCollection = *msg->PageCollection;
        const TLogoBlobID pageCollectionId = pageCollection.Label();

        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Attach page collection " << pageCollectionId
            << " owner " << msg->Owner);

        AttachCollection(pageCollectionId, pageCollection, msg->Owner);
    }

    void Handle(NSharedCache::TEvSaveCompactedPages::TPtr &ev, const TActorContext& ctx) {
        NSharedCache::TEvSaveCompactedPages *msg = ev->Get();
        const auto &pageCollection = *msg->PageCollection;
        const TLogoBlobID pageCollectionId = pageCollection.Label();

        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Save page collection " << pageCollectionId << 
            " compacted pages " << msg->Pages);

        Y_ABORT_UNLESS(pageCollectionId);
        Y_ABORT_IF(Collections.contains(pageCollectionId), "Only new collections can save compacted pages");
        TCollection &collection = Collections[pageCollectionId];
        collection.Id = pageCollectionId;
        collection.PageMap.resize(pageCollection.Total());

        for (auto &page : msg->Pages) {
            Y_ABORT_UNLESS(page->PageId < collection.PageMap.size());

            auto emplaced = collection.PageMap.emplace(page->PageId, page);
            Y_ABORT_UNLESS(emplaced, "Pages should be unique");

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

        TCollection &collection = AttachCollection(pageCollectionId, pageCollection, msg->Owner);

        TStackVec<std::pair<ui32, ui32>> pendingPages; // pageId, reqIdx
        ui32 pagesToLoad = 0;

        TVector<TEvResult::TLoaded> readyPages(::Reserve(msg->Fetch->Pages.size()));
        TVector<ui32> traceLogPagesToWait;
        if (doTraceLog) {
            traceLogPagesToWait.reserve(msg->Fetch->Pages.size());
        }

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
            Y_ABORT_UNLESS(pageId < collection.PageMap.size(),
                "Page collection %s requested page %" PRIu32 " out of %" PRISZT " pages",
                pageCollectionId.ToString().c_str(), pageId, collection.PageMap.size());
            auto* page = collection.PageMap[pageId].Get();
            if (!page) {
                Y_ABORT_UNLESS(collection.PageMap.emplace(pageId, (page = new TPage(pageId, pageCollection.Page(pageId).Size, &collection))));
            }

            Counters.RequestedPages->Inc();
            Counters.RequestedBytes->Add(page->Size);

            switch (page->State) {
            case PageStateEvicted:
                Y_ABORT_UNLESS(page->Use()); // still in PageMap, guaranteed to be alive
                page->State = PageStateLoaded;
                RemovePassivePage(page);
                AddActivePage(page);
                [[fallthrough]];
            case PageStateLoaded:
                Counters.CacheHitPages->Inc();
                Counters.CacheHitBytes->Add(page->Size);
                readyPages.emplace_back(pageId, TSharedPageRef::MakeUsed(page, SharedCachePages->GCList));
                if (doTraceLog) {
                    traceLogPagesToWait.emplace_back(pageId);
                }
                Evict(Cache.Touch(page));
                break;
            case PageStateNo:
                ++pagesToLoad;
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

        auto waitingRequest = MakeIntrusive<TRequest>(std::move(msg->Fetch->PageCollection), std::move(msg->Fetch->TraceId));

        waitingRequest->Source = ev->Sender;
        waitingRequest->Owner = msg->Owner;
        waitingRequest->Priority = msg->Priority;
        waitingRequest->EventCookie = ev->Cookie;
        waitingRequest->RequestCookie = msg->Fetch->Cookie;
        waitingRequest->ReadyPages = std::move(readyPages);

        if (pendingPages) {
            TVector<ui32> pagesToKeep;
            TVector<ui32> pagesToRequest;
            ui64 pagesToRequestBytes = 0;
            pagesToRequest.reserve(pagesToLoad);
            if (doTraceLog) {
                traceLogPagesToWait.reserve(pendingPages.size() - pagesToLoad);
            }

            TRequestQueue::TPagesToRequest *qpages = nullptr;

            if (queue) {
            // register for loading regardless of pending state, to simplify actor deregister logic
            // would be filtered on actual request
                auto &owner = queue->Requests[msg->Owner];
                auto &list = owner.Index[pageCollectionId];

                qpages = &list.emplace_back();

                qpages->Request = waitingRequest;
                owner.Listed.PushBack(qpages);
            }

            for (auto xpair : pendingPages) {
                const ui32 pageId = xpair.first;
                const ui32 reqIdx = xpair.second;

                collection.Expectants[pageId].SourceRequests.emplace_back(waitingRequest, reqIdx);
                ++waitingRequest->PendingBlocks;
                auto* page = collection.PageMap[pageId].Get();
                Y_ABORT_UNLESS(page);

                if (qpages)
                    qpages->Request->PagesToRequest.push_back(pageId);

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
                        traceLogPagesToWait.emplace_back(pageId);
                    }
                    break;
                case PageStateRequestedAsync:
                case PageStatePending:
                    if (!queue) {
                        pagesToRequest.push_back(pageId);
                        pagesToRequestBytes += page->Size;
                        page->State = PageStateRequested;
                    } else if (doTraceLog) {
                        traceLogPagesToWait.emplace_back(pageId);
                    }
                    break;
                default:
                    Y_ABORT("must not happens");
                }
            }

            LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Request page collection " << pageCollectionId
                << " owner " << msg->Owner
                << " class " << waitingRequest->Priority
                << " from cache " << pagesToKeep
                << " already requested " << traceLogPagesToWait
                << " to request " << pagesToRequest);

            if (pagesToRequest) {
                if (queue) {
                    RequestFromQueue(*queue);
                } else {
                    AddInFlyPages(pagesToRequest.size(), pagesToRequestBytes);
                    // fetch cookie -> requested size
                    auto *fetch = new NPageCollection::TFetch(pagesToRequestBytes, waitingRequest->PageCollection, std::move(pagesToRequest), std::move(waitingRequest->TraceId));
                    NBlockIO::Start(this, waitingRequest->Owner, 0, waitingRequest->Priority, fetch);
                }
            }
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Request page collection " << pageCollectionId
                << " owner " << msg->Owner
                << " class " << msg->Priority
                <<  " from cache " << msg->Fetch->Pages);
            SendReadyBlocks(*waitingRequest);
        }

        DoGC();
    }

    void RequestFromQueue(TRequestQueue &queue) {
        if (queue.Requests.empty())
            return;

        TMap<TActorId, TRequestQueue::TByActorRequest>::iterator it;
        if (queue.NextToRequest) {
            it = queue.Requests.find(queue.NextToRequest);
            if (it == queue.Requests.end())
                it = queue.Requests.begin();
        } else {
            it = queue.Requests.begin();
        }

        while (queue.InFly <= queue.Limit) { // on limit == 0 would request pages one by one
            // request whole limit from one page collection for better locality (if possible)
            // should be 'request from one logoblobid
            auto &owner = it->second;
            Y_ABORT_UNLESS(!owner.Listed.Empty());

            ui32 nthToRequest = 0;
            ui32 nthToLoad = 0;
            ui64 sizeToLoad = 0;

            auto &wa = *owner.Listed.Front()->Request;

            if (wa.Source) { // is request already served?
                auto *collection = Collections.FindPtr(wa.Label);
                Y_ABORT_UNLESS(collection);

                for (ui32 pageId : wa.PagesToRequest) {
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

                if (nthToRequest != 0) {
                    if (nthToLoad != 0) {
                        TVector<ui32> toLoad;
                        toLoad.reserve(nthToLoad);
                        for (ui32 pageId : wa.PagesToRequest) {
                            auto* page = collection->PageMap[pageId].Get();
                            if (!page || page->State != PageStatePending)
                                continue;

                            toLoad.push_back(pageId);
                            page->State = PageStateRequestedAsync;
                            if (--nthToLoad == 0)
                                break;
                        }

                        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Request page collection " << wa.Label
                            << (&queue == &AsyncRequests ? " async" : " scan") << " queue"
                            << " pages " << toLoad);

                        AddInFlyPages(toLoad.size(), sizeToLoad);
                        // fetch cookie -> requested size;
                        // event cookie -> ptr to queue
                        auto *fetch = new NPageCollection::TFetch(sizeToLoad, wa.PageCollection, std::move(toLoad), std::move(wa.TraceId));
                        NBlockIO::Start(this, wa.Owner, (ui64)&queue, wa.Priority, fetch);
                    }
                }
            }

            // cleanup
            if (!wa.Source || nthToRequest == wa.PagesToRequest.size()) {
                {
                    auto reqit = owner.Index.find(wa.Label);
                    Y_ABORT_UNLESS(reqit != owner.Index.end());
                    reqit->second.pop_front();

                    if (reqit->second.empty())
                        owner.Index.erase(reqit);
                }

                Y_ABORT_UNLESS(bool(owner.Listed) == bool(owner.Index));

                if (owner.Listed.Empty())
                    it = queue.Requests.erase(it);
                else
                    ++it;
            } else {
                wa.PagesToRequest.erase(wa.PagesToRequest.begin(), wa.PagesToRequest.begin() + nthToRequest);
                ++it;
            }

            if (it == queue.Requests.end())
                it = queue.Requests.begin();

            if (it == queue.Requests.end()) {
                queue.NextToRequest = TActorId();
                break;
            }

            queue.NextToRequest = it->first;
        }
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
                Y_ABORT_UNLESS(pageId < collection->PageMap.size());
                auto* page = collection->PageMap[pageId].Get();
                if (!page) {
                    droppedPages[pageCollectionId].insert(pageId);
                    continue;
                }

                switch (page->State) {
                case PageStateNo:
                    Y_ABORT("unexpected uninitialized page found");
                case PageStateRequested:
                case PageStateRequestedAsync:
                case PageStatePending:
                    break;
                case PageStateEvicted:
                    Y_ABORT_UNLESS(page->Use());
                    page->State = PageStateLoaded;
                    RemovePassivePage(page);
                    AddActivePage(page);
                    [[fallthrough]];
                case PageStateLoaded:
                    Evict(Cache.Touch(page));
                    break;
                default:
                    Y_ABORT("unknown load state");
                }
            }
        }

        if (droppedPages) {
            SendDroppedPages(ev->Sender, std::move(droppedPages));
        }

        DoGC();
    }

    void Handle(NSharedCache::TEvUnregister::TPtr &ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Unregister " << ev->Sender);

        DropFromQueue(ScanRequests, ev->Sender, ctx);
        DropFromQueue(AsyncRequests, ev->Sender, ctx);

        RequestFromQueue(AsyncRequests);
        RequestFromQueue(ScanRequests);

        auto ownerIt = CollectionsOwners.find(ev->Sender);
        if (ownerIt != CollectionsOwners.end()) {
            for (auto* collection : ownerIt->second.Collections) {
                collection->Owners.erase(ev->Sender);
                TryDropExpiredCollection(collection);
            }
            CollectionsOwners.erase(ownerIt);
        }

        ProcessGCList();
    }

    void Handle(NSharedCache::TEvInvalidate::TPtr &ev, const TActorContext& ctx) {
        const TLogoBlobID pageCollectionId = ev->Get()->PageCollectionId;
        auto collectionIt = Collections.find(pageCollectionId);

        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Invalidate page collection " << pageCollectionId
            << (collectionIt == Collections.end() ? " unknown" : ""));

        if (collectionIt == Collections.end()) {
            return;
        }

        auto &collection = collectionIt->second;

        DropRequestsFor(ev->Sender, pageCollectionId);

        if (collection.Owners.erase(ev->Sender)) {
            auto ownerIt = CollectionsOwners.find(ev->Sender);
            if (ownerIt != CollectionsOwners.end() &&
                ownerIt->second.Collections.erase(&collection) &&
                ownerIt->second.Collections.empty())
            {
                CollectionsOwners.erase(ownerIt);
            }

            if (collection.Owners.empty()) {
                DropCollection(collectionIt, NKikimrProto::RACE);
            }
        }

        ProcessGCList();
    }

    void Handle(NBlockIO::TEvData::TPtr &ev, const TActorContext& ctx) {
        auto *msg = ev->Get();

        LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Loaded page collection " << msg->Fetch->PageCollection->Label()
            << " status " << msg->Status
            << " pages " << msg->Fetch->Pages);

        RemoveInFlyPages(msg->Fetch->Pages.size(), msg->Fetch->Cookie);

        if (TRequestQueue *queue = (TRequestQueue *)ev->Cookie) {
            Y_ABORT_UNLESS(queue == &ScanRequests || queue == &AsyncRequests);
            Y_ABORT_UNLESS(queue->InFly >= msg->Fetch->Cookie);
            queue->InFly -= msg->Fetch->Cookie;
            RequestFromQueue(*queue);
        }

        auto collectionIt = Collections.find(msg->Fetch->PageCollection->Label());
        if (collectionIt == Collections.end())
            return;

        if (msg->Status != NKikimrProto::OK) {
            DropCollection(collectionIt, msg->Status);
        } else {
            TCollection &collection = collectionIt->second;
            for (auto &paged : msg->Blocks) {
                Y_ABORT_UNLESS(paged.PageId < collection.PageMap.size());
                auto* page = collection.PageMap[paged.PageId].Get();
                if (!page || !page->HasMissingBody()) {
                    continue;
                }

                page->Initialize(std::move(paged.Data));
                BodyProvided(collection, page);
                Evict(Cache.Touch(page));
            }
        }

        DoGC();
    }

    void TryDropExpiredCollection(TCollection* pageCollection) {
        if (!pageCollection->Owners &&
            !pageCollection->Expectants &&
            pageCollection->PageMap.used() == 0)
        {
            // Drop unnecessary collections from memory
            auto pageCollectionId = pageCollection->Id;
            Collections.erase(pageCollectionId);
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
            Y_ABORT("Unknown wakeup tag: %lu", ev->Get()->Tag);
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
            Y_ABORT_UNLESS(page->State == PageStateEvicted);
            RemovePassivePage(page);

            Y_VERIFY_DEBUG_S(page->Collection, "Evicted pages are expected to have collection");
            if (auto* collection = page->Collection) {
                auto pageId = page->PageId;
                Y_DEBUG_ABORT_UNLESS(collection->PageMap[pageId].Get() == page);
                Y_ABORT_UNLESS(collection->PageMap.erase(pageId));
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

            TryDropExpiredCollection(collection);
        }

        for (auto& kv : droppedPages) {
            SendDroppedPages(kv.first, std::move(kv.second));
        }
    }

    void SendDroppedPages(TActorId owner, THashMap<TLogoBlobID, THashSet<TPageId>>&& droppedPages_) {
        auto msg = MakeHolder<NSharedCache::TEvUpdated>();
        msg->DroppedPages = std::move(droppedPages_);
        for (auto& [pageCollectionId, droppedPages] : msg->DroppedPages) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Dropping page collection " << pageCollectionId
                << " pages " << droppedPages
                << " owner " << owner);
        }
        Send(owner, msg.Release());
    }

    void BodyProvided(TCollection &collection, TPage *page) {
        AddActivePage(page);
        auto expectantIt = collection.Expectants.find(page->PageId);
        if (expectantIt == collection.Expectants.end()) {
            return;
        }
        for (auto &xpair : expectantIt->second.SourceRequests) {
            auto &r = xpair.first;
            auto &rblock = r->ReadyPages[xpair.second];
            Y_ABORT_UNLESS(rblock.PageId == page->PageId);
            rblock.Page = TSharedPageRef::MakeUsed(page, SharedCachePages->GCList);

            if (--r->PendingBlocks == 0)
                SendReadyBlocks(*r);
        }
        collection.Expectants.erase(expectantIt);
    }

    void SendReadyBlocks(TRequest &wa) {
        /* Do not hold my NPageCollection::IPageCollection, leave std::move(wa.PageCollection) */

        TAutoPtr<NSharedCache::TEvResult> result =
            new NSharedCache::TEvResult(std::move(wa.PageCollection), wa.RequestCookie, NKikimrProto::OK);
        result->Loaded = std::move(wa.ReadyPages);

        Send(wa.Source, result.Release(), 0, wa.EventCookie);
        wa.Source = TActorId();
        StatBioReqs += 1;
    }

    void DropCollection(THashMap<TLogoBlobID, TCollection>::iterator collectionIt, NKikimrProto::EReplyStatus blobStorageError) {
        // decline all pending requests
        TCollection &collection = collectionIt->second;
        const TLogoBlobID &pageCollectionId = collectionIt->first;

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Drop page collection " << pageCollectionId);

        for (auto &expe : collection.Expectants) {
            for (auto &xpair : expe.second.SourceRequests) {
                auto &x = xpair.first;
                if (!x->Source)
                    continue;

                Send(x->Source, new NSharedCache::TEvResult(std::move(x->PageCollection), x->RequestCookie, blobStorageError), 0, x->EventCookie);
                x->Source = TActorId();
            }
        }
        collection.Expectants.clear();

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

        for (TActorId owner : collection.Owners) {
            DropRequestsFor(owner, pageCollectionId);
        }

        if (!collection.Owners && !haveValidPages) {
            // This collection no longer has anything useful
            Collections.erase(collectionIt);
        }
    }

    void DropRequestsFor(TActorId owner, const TLogoBlobID &pageCollectionId) {
        DropFromQueue(ScanRequests, owner, pageCollectionId);
        DropFromQueue(AsyncRequests, owner, pageCollectionId);
    }

    void DropFromQueue(TRequestQueue &queue, TActorId ownerId, const TLogoBlobID &pageCollectionId) {
        auto ownerIt = queue.Requests.find(ownerId);
        if (ownerIt == queue.Requests.end())
            return;
        auto &reqsByOwner = ownerIt->second;
        auto reqsIt = reqsByOwner.Index.find(pageCollectionId);
        if (reqsIt == reqsByOwner.Index.end())
            return;

        if (reqsByOwner.Index.size() == 1) {
            queue.Requests.erase(ownerIt);
        } else {
            for (auto &x : reqsIt->second)
                x.Unlink();
            reqsByOwner.Index.erase(reqsIt);
        }
    }

    void DropFromQueue(TRequestQueue &queue, TActorId ownerId, const TActorContext& ctx) {
        auto it = queue.Requests.find(ownerId);
        if (it != queue.Requests.end()) {
            LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Drop queues"
                << " owner " << ownerId
                << " page collections " << it->second.Index.size());

            queue.Requests.erase(it);
        }
    }

    void Evict(TIntrusiveList<TPage>&& pages) {
        while (!pages.Empty()) {
            TPage* page = pages.PopFront();

            page->EnsureNoCacheFlags();

            Y_VERIFY_S(page->State == PageStateLoaded, "unexpected " << page->State << " page state");
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

        Y_VERIFY_S(page->State == PageStateLoaded, "unexpected " << page->State << " page state");
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
        Y_ABORT_UNLESS(StatLoadInFlyBytes >= size);
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
            HFunc(NSharedCache::TEvInvalidate, Handle);

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
void Out<THashSet<ui32>>(IOutputStream& o, const THashSet<ui32> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}

template<> inline
void Out<TVector<TIntrusivePtr<NKikimr::NSharedCache::TPage>>>(IOutputStream& o, const TVector<TIntrusivePtr<NKikimr::NSharedCache::TPage>> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x->PageId << ' ';
    o << "]";
}
