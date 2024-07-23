#include "shared_sausagecache.h"
#include "shared_cache_events.h"
#include "flat_bio_events.h"
#include "flat_bio_actor.h"
#include "util_fmt_logger.h"
#include <ydb/core/util/cache_cache.h>
#include <ydb/core/util/page_map.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/set.h>

namespace NKikimr {

TSharedPageCacheCounters::TSharedPageCacheCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters> &group)
    : FreshBytes(group->GetCounter("fresh"))
    , StagingBytes(group->GetCounter("staging"))
    , WarmBytes(group->GetCounter("warm"))
    , MemLimitBytes(group->GetCounter("MemLimitBytes"))
    , ConfigLimitBytes(group->GetCounter("ConfigLimitBytes"))
    , ActivePages(group->GetCounter("ActivePages"))
    , ActiveBytes(group->GetCounter("ActiveBytes"))
    , ActiveLimitBytes(group->GetCounter("ActiveLimitBytes"))
    , PassivePages(group->GetCounter("PassivePages"))
    , PassiveBytes(group->GetCounter("PassiveBytes"))
    , RequestedPages(group->GetCounter("RequestedPages", true))
    , RequestedBytes(group->GetCounter("RequestedBytes", true))
    , CacheHitPages(group->GetCounter("CacheHitPages", true))
    , CacheHitBytes(group->GetCounter("CacheHitBytes", true))
    , CacheMissPages(group->GetCounter("CacheMissPages", true))
    , CacheMissBytes(group->GetCounter("CacheMissBytes", true))
    , LoadInFlyPages(group->GetCounter("LoadInFlyPages"))
    , LoadInFlyBytes(group->GetCounter("LoadInFlyBytes"))
{ }

}

namespace NKikimr {
namespace NTabletFlatExecutor {

static bool Satisfies(NLog::EPriority priority = NLog::PRI_DEBUG) {
    if (NLog::TSettings *settings = TlsActivationContext->LoggerSettings())
        return settings->Satisfies(priority, NKikimrServices::TABLET_SAUSAGECACHE);
    else
        return false;
}

class TSharedPageCache : public TActorBootstrapped<TSharedPageCache> {
    using ELnLev = NUtil::ELnLev;
    using TBlocks = TVector<NSharedCache::TEvResult::TLoaded>;

    enum EPageState {
        PageStateNo,
        PageStateLoaded,
        PageStateRequested,
        PageStateRequestedAsync,
        PageStatePending,
        PageStateEvicted,
    };

    static const ui64 DO_GC_TAG = 1;

    struct TCollection;

    struct TPage
        : public TSharedPageHandle
        , public TIntrusiveListItem<TPage>
    {
        ui32 State : 4;
        ui32 CacheGeneration : 3;
        ui32 InMemory : 1;

        const ui32 PageId;
        const size_t Size;

        TCollection* Collection;

        TPage(ui32 pageId, size_t size, TCollection* collection)
            : State(PageStateNo)
            , CacheGeneration(TCacheCacheConfig::CacheGenNone)
            , InMemory(false)
            , PageId(pageId)
            , Size(size)
            , Collection(collection)
        {}

        bool HasMissingBody() const {
            switch (State) {
                case PageStateNo:
                case PageStateRequested:
                case PageStateRequestedAsync:
                case PageStatePending:
                    return true;

                default:
                    return false;
            }
        }

        void Initialize(TSharedData data) {
            Y_DEBUG_ABORT_UNLESS(HasMissingBody());
            TSharedPageHandle::Initialize(std::move(data));
            State = PageStateLoaded;
        }

        struct TWeight {
            static ui64 Get(TPage *x) {
                return sizeof(TPage) + (x->State == PageStateLoaded ? x->Size : 0);
            }
        };
    };

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
        TBlocks ReadyBlocks;
        TDeque<ui32> PagesToRequest;
        NWilson::TTraceId TraceId;
    };

    struct TExpectant {
        TDeque<std::pair<TIntrusivePtr<TRequest>, ui32>> SourceRequests; // waiting request, index in ready blocks for page
    };

    struct TCollection {
        TLogoBlobID MetaId;
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

    TIntrusivePtr<TSharedPageGCList> GCList = new TSharedPageGCList;

    TActorId Owner;
    TAutoPtr<NUtil::ILogger> Logger;
    THashMap<TLogoBlobID, TCollection> Collections;
    THashMap<TActorId, TCollectionsOwner> CollectionsOwners;
    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;

    TRequestQueue AsyncRequests;
    TRequestQueue ScanRequests;

    THolder<TSharedPageCacheConfig> Config;
    TCacheCache<TPage, TPage::TWeight, TCacheCacheConfig::TDefaultGeneration<TPage>> Cache;

    ui64 StatBioReqs = 0;
    ui64 StatHitPages = 0;
    ui64 StatHitBytes = 0;
    ui64 StatMissPages = 0;
    ui64 StatMissBytes = 0;
    ui64 StatActiveBytes = 0;
    ui64 StatPassiveBytes = 0;
    ui64 StatLoadInFlyBytes = 0;

    bool GCScheduled = false;

    ui64 MemLimitBytes;

    void ActualizeCacheSizeLimit() {
        ui64 limitBytes = Min(Config->LimitBytes.value_or(Max<ui64>()), MemLimitBytes);

        // limit of cache depends only on config and mem because passive pages may go in and out arbitrary
        // we may have some passive bytes, so if we fully fill this Cache we may exceed the limit
        // because of that DoGC should be called to ensure limits
        // unlimited cache is banned
        Cache.UpdateCacheSize(Max<ui64>(1, limitBytes));

        if (Config->Counters) {
            Config->Counters->ConfigLimitBytes->Set(Config->LimitBytes.value_or(0));
            Config->Counters->ActiveLimitBytes->Set(limitBytes);
        }
    }

    void DoGC() {
        // maybe we already have enough useless pages
        // update StatActiveBytes + StatPassiveBytes
        ProcessGCList();

        // TODO: get rid of active pages reservation
        ui64 configActiveReservedBytes = Config->LimitBytes.value_or(0) * Config->ActivePagesReservationPercent / 100;

        THashSet<TCollection*> recheck;
        while (GetStatAllBytes() > MemLimitBytes
                || GetStatAllBytes() > Config->LimitBytes.value_or(Max<ui64>()) && StatActiveBytes > configActiveReservedBytes) {
            auto page = Cache.EvictNext();
            if (!page) {
                break;
            }
            EvictNow(page, recheck);
        }
        if (recheck) {
            CheckExpiredCollections(std::move(recheck));
        }

        if (MemoryConsumer) {
            MemoryConsumer->SetConsumption(GetStatAllBytes());
        }
    }

    void Handle(NMemory::TEvConsumerRegistered::TPtr &ev) {
        auto *msg = ev->Get();
        MemoryConsumer = std::move(msg->Consumer);
    }

    void Handle(NMemory::TEvConsumerLimit::TPtr &ev) {
        auto *msg = ev->Get();

        MemLimitBytes = msg->LimitBytes;
        if (Config->Counters) {
            Config->Counters->MemLimitBytes->Set(MemLimitBytes);
        }

        ActualizeCacheSizeLimit();

        DoGC();
    }

    void Registered(TActorSystem *sys, const TActorId &owner)
    {
        NActors::TActorBootstrapped<TSharedPageCache>::Registered(sys, owner);
        Owner = owner;

        Logger = new NUtil::TLogger(sys, NKikimrServices::TABLET_SAUSAGECACHE);
    }

    void TakePoison()
    {
        if (auto logl = Logger->Log(ELnLev::Info)) {
            logl
                << "Page collection cache gone, serviced " << StatBioReqs << " reqs"
                << " hit {" << StatHitPages << " " << StatHitBytes << "b}"
                << " miss {" << StatMissPages << " " << StatMissBytes << "b}";
        }

        if (auto owner = std::exchange(Owner, { }))
            Send(owner, new TEvents::TEvGone);

        PassAway();
    }

    TCollection& AttachCollection(const TLogoBlobID &metaId, const NPageCollection::IPageCollection &pageCollection, const TActorId &owner) {
        TCollection &collection = Collections[metaId];
        if (!collection.MetaId) {
            Y_ABORT_UNLESS(metaId);
            collection.MetaId = metaId;
            collection.PageMap.resize(pageCollection.Total());
        } else {
            Y_DEBUG_ABORT_UNLESS(collection.MetaId == metaId);
            Y_ABORT_UNLESS(collection.PageMap.size() == pageCollection.Total(),
                "Page collection %s changed number of pages from %" PRISZT " to %" PRIu32 " by %s",
                metaId.ToString().c_str(), collection.PageMap.size(), pageCollection.Total(), owner.ToString().c_str());
        }

        if (collection.Owners.insert(owner).second) {
            CollectionsOwners[owner].Collections.insert(&collection);
        }

        return collection;
    }

    void Handle(NSharedCache::TEvAttach::TPtr &ev) {
        NSharedCache::TEvAttach *msg = ev->Get();
        const auto &pageCollection = *msg->PageCollection;
        const TLogoBlobID metaId = pageCollection.Label();

        AttachCollection(metaId, pageCollection, msg->Owner);
    }

    void Handle(NSharedCache::TEvRequest::TPtr &ev) {
        NSharedCache::TEvRequest *msg = ev->Get();
        const auto &pageCollection = *msg->Fetch->PageCollection;
        const TLogoBlobID metaId = pageCollection.Label();
        const bool logsat = Satisfies();

        TCollection &collection = AttachCollection(metaId, pageCollection, msg->Owner);

        TStackVec<std::pair<ui32, ui32>> pendingPages; // pageId, reqIdx
        ui32 pagesToLoad = 0;

        TBlocks readyBlocks;
        readyBlocks.reserve(msg->Fetch->Pages.size());
        TVector<ui32> pagesToWait;
        if (logsat)
            pagesToWait.reserve(msg->Fetch->Pages.size());

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
                metaId.ToString().c_str(), pageId, collection.PageMap.size());
            auto* page = collection.PageMap[pageId].Get();
            if (!page) {
                Y_ABORT_UNLESS(collection.PageMap.emplace(pageId, (page = new TPage(pageId, pageCollection.Page(pageId).Size, &collection))));
            }

            if (Config->Counters) {
                ++*Config->Counters->RequestedPages;
                *Config->Counters->RequestedBytes += page->Size;
            }

            switch (page->State) {
            case PageStateEvicted:
                Y_ABORT_UNLESS(page->Use()); // still in PageMap, guaranteed to be alive
                page->State = PageStateLoaded;
                RemovePassivePage(page);
                AddActivePage(page);
                [[fallthrough]];
            case PageStateLoaded:
                StatHitPages += 1;
                StatHitBytes += page->Size;
                readyBlocks.emplace_back(pageId, TSharedPageRef::MakeUsed(page, GCList));
                if (logsat)
                    pagesToWait.emplace_back(pageId);
                if (Config->Counters) {
                    ++*Config->Counters->CacheHitPages;
                    *Config->Counters->CacheHitBytes += page->Size;
                }
                Evict(Cache.Touch(page));
                break;
            case PageStateNo:
                ++pagesToLoad;
                [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
            case PageStateRequested:
            case PageStateRequestedAsync:
            case PageStatePending:
                if (Config->Counters) {
                    ++*Config->Counters->CacheMissPages;
                    *Config->Counters->CacheMissBytes += page->Size;
                }
                readyBlocks.emplace_back(pageId, TSharedPageRef());
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
        waitingRequest->ReadyBlocks = std::move(readyBlocks);

        if (pendingPages) {
            TVector<ui32> pagesToKeep;
            TVector<ui32> pagesToRequest;
            ui64 pagesToRequestBytes = 0;
            pagesToRequest.reserve(pagesToLoad);
            if (logsat)
                pagesToWait.reserve(pendingPages.size() - pagesToLoad);

            TRequestQueue::TPagesToRequest *qpages = nullptr;

            if (queue) {
            // register for loading regardless of pending state, to simplify actor deregister logic
            // would be filtered on actual request
                auto &owner = queue->Requests[msg->Owner];
                auto &list = owner.Index[metaId];

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
                    if (logsat)
                        pagesToWait.emplace_back(pageId);
                    break;
                case PageStateRequestedAsync:
                case PageStatePending:
                    if (!queue) {
                        pagesToRequest.push_back(pageId);
                        pagesToRequestBytes += page->Size;
                        page->State = PageStateRequested;
                    } else {
                        pagesToWait.emplace_back(pageId);
                    }
                    break;
                default:
                    Y_ABORT("must not happens");
                }
            }

            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << "PageCollection " << metaId << " class " << waitingRequest->Priority
                    << " from cache " << pagesToKeep
                    << " already requested " << pagesToWait
                    << " to request " << pagesToRequest;
            }

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
            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << "PageCollection " << metaId << " class " << msg->Priority
                    <<  " from cache " << msg->Fetch->Pages;
            }
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

                        if (auto logl = Logger->Log(ELnLev::Debug)) {
                            logl
                                << "queue pageCollection " << wa.Label << " q: "
                                << (&queue == &AsyncRequests ? "async" : "scan")
                                << " pages " << toLoad;
                        }

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

    void Handle(NSharedCache::TEvTouch::TPtr &ev) {
        NSharedCache::TEvTouch *msg = ev->Get();
        THashMap<TLogoBlobID, NSharedCache::TEvUpdated::TActions> actions;
        for (auto &xpair : msg->Touched) {
            auto collectionIt = Collections.find(xpair.first);
            if (collectionIt == Collections.end()) {
                for (auto &x : xpair.second) {
                    if (x.second) {
                        actions[xpair.first].Dropped.insert(x.first);
                        x.second = { };
                    }
                }
                continue;
            }
            auto &collection = collectionIt->second;
            for (auto &x : xpair.second) {
                const ui32 pageId = x.first;
                Y_ABORT_UNLESS(pageId < collection.PageMap.size());
                auto* page = collection.PageMap[pageId].Get();
                if (!page) {
                    if (x.second) {
                        Y_ABORT_UNLESS(collection.PageMap.emplace(pageId, (page = new TPage(pageId, x.second.size(), &collection))));
                    } else {
                        continue;
                    }
                }
                Y_ABORT_UNLESS(page);

                if (auto body = std::move(x.second)) {
                    if (page->HasMissingBody()) {
                        page->Initialize(std::move(body));
                        BodyProvided(collection, x.first, page);
                    }

                    auto ref = TSharedPageRef::MakeUsed(page, GCList);
                    Y_ABORT_UNLESS(ref.IsUsed(), "Unexpected failure to grab a cached page");
                    actions[xpair.first].Accepted[pageId] = std::move(ref);
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

        if (actions) {
            auto msg = MakeHolder<NSharedCache::TEvUpdated>();
            msg->Actions = std::move(actions);
            Send(ev->Sender, msg.Release());
        }

        DoGC();
    }

    void Handle(NSharedCache::TEvUnregister::TPtr &ev) {
        DropFromQueue(ScanRequests, ev->Sender);
        DropFromQueue(AsyncRequests, ev->Sender);

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

    void Handle(NSharedCache::TEvInvalidate::TPtr &ev) {
        const TLogoBlobID pageCollectionId = ev->Get()->PageCollectionId;
        auto collectionIt = Collections.find(pageCollectionId);

        if (auto logl = Logger->Log(ELnLev::Debug)) {
            logl
                << "invalidate pageCollection " << pageCollectionId
                << (collectionIt == Collections.end() ? " unknown" : "");
        }

        if (collectionIt == Collections.end())
            return;

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

    void Handle(NBlockIO::TEvData::TPtr &ev) {
        auto *msg = ev->Get();

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
                StatMissPages += 1;
                StatMissBytes += paged.Data.size();

                Y_ABORT_UNLESS(paged.PageId < collection.PageMap.size());
                auto* page = collection.PageMap[paged.PageId].Get();
                if (!page || !page->HasMissingBody())
                    continue;

                page->Initialize(std::move(paged.Data));
                BodyProvided(collection, paged.PageId, page);
                Evict(Cache.Touch(page));
            }
        }

        DoGC();
    }

    void TryDropExpiredCollection(TCollection* collection) {
        if (!collection->Owners &&
            !collection->Expectants &&
            collection->PageMap.used() == 0)
        {
            // Drop unnecessary collections from memory
            auto metaId = collection->MetaId;
            Collections.erase(metaId);
        }
    }

    void Wakeup(TKikimrEvents::TEvWakeup::TPtr& ev) {
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

        while (auto rawPage = GCList->PopGC()) {
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
        THashMap<TActorId, THashMap<TLogoBlobID, NSharedCache::TEvUpdated::TActions>> toSend;

        for (TCollection *collection : recheck) {
            if (collection->DroppedPages) {
                // N.B. usually there is a single owner
                for (TActorId owner : collection->Owners) {
                    auto& actions = toSend[owner][collection->MetaId];
                    for (ui32 pageId : collection->DroppedPages) {
                        actions.Dropped.insert(pageId);
                    }
                }
                collection->DroppedPages.clear();
            }

            TryDropExpiredCollection(collection);
        }

        for (auto& kv : toSend) {
            auto msg = MakeHolder<NSharedCache::TEvUpdated>();
            msg->Actions = std::move(kv.second);
            Send(kv.first, msg.Release());
        }
    }

    void BodyProvided(TCollection &collection, ui32 pageId, TPage *page) {
        AddActivePage(page);
        auto expectantIt = collection.Expectants.find(pageId);
        if (expectantIt == collection.Expectants.end())
            return;
        for (auto &xpair : expectantIt->second.SourceRequests) {
            auto &r = xpair.first;
            auto &rblock = r->ReadyBlocks[xpair.second];
            Y_ABORT_UNLESS(rblock.PageId == pageId);
            rblock.Page = TSharedPageRef::MakeUsed(page, GCList);

            if (--r->PendingBlocks == 0)
                SendReadyBlocks(*r);
        }
        collection.Expectants.erase(expectantIt);
    }

    void SendReadyBlocks(TRequest &wa) {
        /* Do not hold my NPageCollection::IPageCollection, leave std::move(wa.PageCollection) */

        TAutoPtr<NSharedCache::TEvResult> result =
            new NSharedCache::TEvResult(std::move(wa.PageCollection), wa.RequestCookie, NKikimrProto::OK);
        result->Loaded = std::move(wa.ReadyBlocks);

        Send(wa.Source, result.Release(), 0, wa.EventCookie);
        wa.Source = TActorId();
        StatBioReqs += 1;
    }

    void DropCollection(THashMap<TLogoBlobID, TCollection>::iterator collectionIt, NKikimrProto::EReplyStatus blobStorageError) {
        // decline all pending requests
        TCollection &collection = collectionIt->second;
        const TLogoBlobID &pageCollectionId = collectionIt->first;

        if (auto logl = Logger->Log(ELnLev::Debug))
            logl << "droping pageCollection " << pageCollectionId;

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

            Cache.Evict(page);
            page->CacheGeneration = TCacheCacheConfig::CacheGenNone;

            if (page->State == PageStateLoaded) {
                page->State = PageStateEvicted;
                RemoveActivePage(page);
                AddPassivePage(page);
                if (page->UnUse()) {
                    GCList->PushGC(page);
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

    void DropRequestsFor(TActorId owner, const TLogoBlobID &metaId) {
        DropFromQueue(ScanRequests, owner, metaId);
        DropFromQueue(AsyncRequests, owner, metaId);
    }

    void DropFromQueue(TRequestQueue &queue, TActorId ownerId, const TLogoBlobID &metaId) {
        auto ownerIt = queue.Requests.find(ownerId);
        if (ownerIt == queue.Requests.end())
            return;
        auto &reqsByOwner = ownerIt->second;
        auto reqsIt = reqsByOwner.Index.find(metaId);
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

    void DropFromQueue(TRequestQueue &queue, TActorId ownerId) {
        auto it = queue.Requests.find(ownerId);
        if (it != queue.Requests.end()) {
            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << " drop from queue for " << ownerId
                    << " pageCollections " << it->second.Index.size();
            }

            queue.Requests.erase(it);
        }
    }

    void Evict(TIntrusiveList<TPage>&& pages) {
        while (!pages.Empty()) {
            TPage* page = pages.PopFront();

            Y_VERIFY_S(page->CacheGeneration == TCacheCacheConfig::CacheGenEvicted, "unexpected " << page->CacheGeneration << " page cache generation");
            page->CacheGeneration = TCacheCacheConfig::CacheGenNone;

            Y_VERIFY_S(page->State == PageStateLoaded, "unexpected " << page->State << " page state");
            page->State = PageStateEvicted;

            RemoveActivePage(page);
            AddPassivePage(page);
            if (page->UnUse()) {
                GCList->PushGC(page);
            }
        }
    }

    void EvictNow(TPage* page, THashSet<TCollection*>& recheck) {
        Y_VERIFY_S(page->CacheGeneration == TCacheCacheConfig::CacheGenEvicted, "unexpected " << page->CacheGeneration << " page cache generation");
        page->CacheGeneration = TCacheCacheConfig::CacheGenNone;

        Y_VERIFY_S(page->State == PageStateLoaded, "unexpected " << page->State << " page state");
        page->State = PageStateEvicted;

        RemoveActivePage(page);
        AddPassivePage(page);
        if (page->UnUse()) {
            TryDrop(page, recheck);
        }
    }

    void Handle(TEvSharedPageCache::TEvConfigure::TPtr& ev) {
        const auto* msg = ev->Get();

        if (msg->Record.HasMemoryLimit() && msg->Record.GetMemoryLimit() != 0) {
            Config->LimitBytes = msg->Record.GetMemoryLimit();
        } else {
            Config->LimitBytes = {};
        }
        ActualizeCacheSizeLimit();

        if (msg->Record.HasActivePagesReservationPercent()) {
            Config->ActivePagesReservationPercent = msg->Record.GetActivePagesReservationPercent();
        }

        if (msg->Record.GetAsyncQueueInFlyLimit() != 0) {
            AsyncRequests.Limit = msg->Record.GetAsyncQueueInFlyLimit();
            RequestFromQueue(AsyncRequests);
        }

        if (msg->Record.GetScanQueueInFlyLimit() != 0) {
            ScanRequests.Limit = msg->Record.GetScanQueueInFlyLimit();
            RequestFromQueue(ScanRequests);
        }
    }

    inline ui64 GetStatAllBytes() const {
        return StatActiveBytes + StatPassiveBytes + StatLoadInFlyBytes;
    }

    inline void AddActivePage(const TPage* page) {
        StatActiveBytes += sizeof(TPage) + page->Size;
        if (Config->Counters) {
            ++*Config->Counters->ActivePages;
            *Config->Counters->ActiveBytes += sizeof(TPage) + page->Size;
        }
    }

    inline void AddPassivePage(const TPage* page) {
        StatPassiveBytes += sizeof(TPage) + page->Size;
        if (Config->Counters) {
            ++*Config->Counters->PassivePages;
            *Config->Counters->PassiveBytes += sizeof(TPage) + page->Size;
        }
    }

    inline void AddInFlyPages(ui64 count, ui64 size) {
        StatLoadInFlyBytes += size;
        if (Config->Counters) {
            *Config->Counters->LoadInFlyPages += count;
            *Config->Counters->LoadInFlyBytes += size;
        }
    }

    inline void RemoveActivePage(const TPage* page) {
        Y_DEBUG_ABORT_UNLESS(StatActiveBytes >= sizeof(TPage) + page->Size);
        StatActiveBytes -= sizeof(TPage) + page->Size;
        if (Config->Counters) {
            --*Config->Counters->ActivePages;
            *Config->Counters->ActiveBytes -= sizeof(TPage) + page->Size;
        }
    }

    inline void RemovePassivePage(const TPage* page) {
        Y_DEBUG_ABORT_UNLESS(StatPassiveBytes >= sizeof(TPage) + page->Size);
        StatPassiveBytes -= sizeof(TPage) + page->Size;
        if (Config->Counters) {
            --*Config->Counters->PassivePages;
            *Config->Counters->PassiveBytes -= sizeof(TPage) + page->Size;
        }
    }

    inline void RemoveInFlyPages(ui64 count, ui64 size) {
        Y_ABORT_UNLESS(StatLoadInFlyBytes >= size);
        StatLoadInFlyBytes -= size;
        if (Config->Counters) {
            *Config->Counters->LoadInFlyPages -= count;
            *Config->Counters->LoadInFlyBytes -= size;
        }
    }

public:
    TSharedPageCache(THolder<TSharedPageCacheConfig> config)
        : Config(std::move(config))
        , Cache(TCacheCacheConfig(1, Config->Counters->FreshBytes, Config->Counters->StagingBytes, Config->Counters->WarmBytes))
    {
        AsyncRequests.Limit = Config->TotalAsyncQueueInFlyLimit;
        ScanRequests.Limit = Config->TotalScanQueueInFlyLimit;
    }

    void Bootstrap() {
        MemLimitBytes = Config->LimitBytes.value_or(10_MB); // soon will be updated by MemoryController
        ActualizeCacheSizeLimit();
        
        auto consumerRegister = MakeHolder<NMemory::TEvConsumerRegister>(NMemory::EMemoryConsumerKind::SharedCache);
        consumerRegister->ConfigLimit = Config->LimitBytes;
        Send(NMemory::MakeMemoryControllerId(), consumerRegister.Release());

        Become(&TThis::StateFunc);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSharedCache::TEvAttach, Handle);
            hFunc(NSharedCache::TEvRequest, Handle);
            hFunc(NSharedCache::TEvTouch, Handle);
            hFunc(NSharedCache::TEvUnregister, Handle);
            hFunc(NSharedCache::TEvInvalidate, Handle);

            hFunc(NBlockIO::TEvData, Handle);
            hFunc(TEvSharedPageCache::TEvConfigure, Handle);
            hFunc(TKikimrEvents::TEvWakeup, Wakeup);
            cFunc(TEvents::TSystem::PoisonPill, TakePoison);

            hFunc(NMemory::TEvConsumerRegistered, Handle);
            hFunc(NMemory::TEvConsumerLimit, Handle);
        }
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SAUSAGE_CACHE;
    }
};

} // NTabletFlatExecutor

IActor* CreateSharedPageCache(THolder<TSharedPageCacheConfig> config) {
    return new NTabletFlatExecutor::TSharedPageCache(std::move(config));
}

}

template<> inline
void Out<TVector<ui32>>(IOutputStream& o, const TVector<ui32> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}


