#include "shared_sausagecache.h"
#include "shared_cache_events.h"
#include "flat_bio_events.h"
#include "flat_bio_actor.h"
#include "util_fmt_logger.h"
#include <ydb/core/util/cache_cache.h>
#include <ydb/core/util/page_map.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/set.h>

namespace NKikimr {

TSharedPageCacheCounters::TSharedPageCacheCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters> &group)
    : MemLimitBytes(group->GetCounter("MemLimitBytes"))
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
    , MemTableTotalBytes(group->GetCounter("MemTableTotalBytes"))
    , MemTableCompactingBytes(group->GetCounter("MemTableCompactingBytes"))
    , MemTableCompactedBytes(group->GetCounter("MemTableCompactedBytes", true))
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

class TSharedPageCacheMemTableTracker;

class TSharedPageCacheMemTableRegistration : public NSharedCache::ISharedPageCacheMemTableRegistration {
public:
   TSharedPageCacheMemTableRegistration(TActorId owner, ui32 table, std::weak_ptr<TSharedPageCacheMemTableTracker> tracker)
        : Tracker(std::move(tracker))
        , Owner(owner)
        , Table(table)
    {}

    ui64 GetConsumption() const {
        return Consumption.load();
    }

    void SetConsumption(ui64 newConsumption);

private:
    std::weak_ptr<TSharedPageCacheMemTableTracker> Tracker;
    std::atomic<ui64> Consumption = 0;

public:
    const TActorId Owner;
    const ui32 Table;
};

class TSharedPageCacheMemTableTracker : public std::enable_shared_from_this<TSharedPageCacheMemTableTracker> {
    friend TSharedPageCacheMemTableRegistration;

public:
    TSharedPageCacheMemTableTracker(TIntrusivePtr<TSharedPageCacheCounters> counters)
        : Counters(counters)
    {}

    ui64 GetTotalConsumption() {
        return TotalConsumption.load();
    }

    ui64 GetTotalCompacting() {
        return TotalCompacting;
    }

    TIntrusivePtr<TSharedPageCacheMemTableRegistration> Register(TActorId owner, ui32 table) {
        auto &ret = Registrations[{owner, table}];
        if (!ret) {
            ret = MakeIntrusive<TSharedPageCacheMemTableRegistration>(owner, table, weak_from_this());
            NonCompacting.insert(ret);
        }
        return ret;
    }

    void Unregister(TActorId owner, ui32 table) {
        auto it = Registrations.find({owner, table});
        if (it != Registrations.end()) {
            auto& registration = it->second;
            CompactionComplete(registration);
            registration->SetConsumption(0);
            Y_DEBUG_ABORT_UNLESS(NonCompacting.contains(registration));
            NonCompacting.erase(registration);
            Registrations.erase(it);
        }
    }

    void CompactionComplete(TIntrusivePtr<TSharedPageCacheMemTableRegistration> registration) {
        auto it = Compacting.find(registration);
        if (it != Compacting.end()) {
            if (Counters) {
                Counters->MemTableCompactedBytes->Add(it->second);
            }
            ChangeTotalCompacting(-it->second);
            NonCompacting.insert(it->first);
            Compacting.erase(it);
        }
    }

    /**
     * @return registrations and sizes that should be compacted
     */
    TVector<std::pair<TIntrusivePtr<TSharedPageCacheMemTableRegistration>, ui64>> SelectForCompaction(ui64 toCompact) {
        TVector<std::pair<TIntrusivePtr<TSharedPageCacheMemTableRegistration>, ui64>> ret;

        if (toCompact <= TotalCompacting) {
            // nothing to compact more
            return ret;
        }

        for (const auto &r : NonCompacting) {
            ui64 consumption = r->GetConsumption();
            if (consumption) {
                ret.emplace_back(r, consumption);
            }
        }

        Sort(ret, [](const auto &x, const auto &y) { return x.second > y.second; });

        size_t take = 0;
        for (auto it = ret.begin(); it != ret.end() && toCompact > TotalCompacting; it++) {
            auto reg = it->first;

            Compacting[reg] = it->second;
            Y_ABORT_UNLESS(NonCompacting.erase(reg));

            ChangeTotalCompacting(it->second);

            take++;
        }

        ret.resize(take);
        return ret;
    }

private:
    void ChangeTotalConsumption(ui64 delta) {
        TotalConsumption.fetch_add(delta);
        if (Counters) {
            Counters->MemTableTotalBytes->Add(delta);
        }
    }

    void ChangeTotalCompacting(ui64 delta) {
        TotalCompacting += delta;
        if (Counters) {
            Counters->MemTableCompactingBytes->Add(delta);
        }
    }

private:
    TIntrusivePtr<TSharedPageCacheCounters> Counters;
    TMap<std::pair<TActorId, ui32>, TIntrusivePtr<TSharedPageCacheMemTableRegistration>> Registrations;
    THashSet<TIntrusivePtr<TSharedPageCacheMemTableRegistration>> NonCompacting;
    THashMap<TIntrusivePtr<TSharedPageCacheMemTableRegistration>, ui64> Compacting;
    std::atomic<ui64> TotalConsumption = 0;
    // Approximate value, updates only on compaction start/stop.
    //
    // Counts only Shared Cache triggered compactions.
    ui64 TotalCompacting = 0;
};

void TSharedPageCacheMemTableRegistration::SetConsumption(ui64 newConsumption) {
    ui64 before = Consumption.exchange(newConsumption);
    if (auto t = Tracker.lock()) {
        t->ChangeTotalConsumption(newConsumption - before);
    }
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
    static const ui64 UPDATE_WHITEBOARD_TAG = 2;

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
    TIntrusivePtr<TMemObserver> MemObserver;
    std::shared_ptr<TSharedPageCacheMemTableTracker> MemTableTracker;

    TRequestQueue AsyncRequests;
    TRequestQueue ScanRequests;

    THolder<TSharedPageCacheConfig> Config;
    TCacheCache<TPage, TPage::TWeight, TCacheCacheConfig::TDefaultGeneration<TPage>> Cache;

    TControlWrapper SizeOverride;

    ui64 StatBioReqs = 0;
    ui64 StatHitPages = 0;
    ui64 StatHitBytes = 0;
    ui64 StatMissPages = 0;
    ui64 StatMissBytes = 0;
    ui64 StatActiveBytes = 0;
    ui64 StatPassiveBytes = 0;
    ui64 StatLoadInFlyBytes = 0;

    bool GCScheduled = false;

    // 0 means unlimited
    ui64 MemLimitBytes = 0;
    ui64 ConfigLimitBytes;

    void ActualizeCacheSizeLimit() {
        if ((ui64)SizeOverride != Config->CacheConfig->Limit) {
            Config->CacheConfig->SetLimit(SizeOverride);
        }

        ConfigLimitBytes = Config->CacheConfig->Limit;

        ui64 limit = ConfigLimitBytes;
        if (MemLimitBytes && ConfigLimitBytes > MemLimitBytes) {
            limit = MemLimitBytes;
        }

        // limit of cache depends only on config and mem because passive pages may go in and out arbitrary
        // we may have some passive bytes, so if we fully fill this Cache we may exceed the limit
        // because of that DoGC should be called to ensure limits
        Cache.UpdateCacheSize(limit);

        if (Config->Counters) {
            Config->Counters->ConfigLimitBytes->Set(ConfigLimitBytes);
            Config->Counters->ActiveLimitBytes->Set(limit);
        }
    }

    void DoGC() {
        // maybe we already have enough useless pages
        // update StatActiveBytes + StatPassiveBytes
        ProcessGCList();

        ui64 configActiveReservedBytes = ConfigLimitBytes * Config->ActivePagesReservationPercent / 100;

        THashSet<TCollection*> recheck;
        while (MemLimitBytes && GetStatAllBytes() > MemLimitBytes
                || GetStatAllBytes() > ConfigLimitBytes && StatActiveBytes > configActiveReservedBytes) {
            auto page = Cache.EvictNext();
            if (!page) {
                break;
            }
            EvictNow(page, recheck);
        }
        if (recheck) {
            CheckExpiredCollections(std::move(recheck));
        }
    }

    void Handle(NSharedCache::TEvMem::TPtr &) {
        // always get the latest value
        auto mem = MemObserver->GetStat();

        if (mem.SoftLimit) {
            ui64 usedExternal = 0;
            // we have: mem.Used = usedExternal + StatAllBytes
            if (mem.Used > GetStatAllBytes()) {
                usedExternal = mem.Used - GetStatAllBytes();
            }

            // we want: MemLimitBytes + externalUsage <= mem.SoftLimit
            MemLimitBytes = mem.SoftLimit > usedExternal
                ? mem.SoftLimit - usedExternal
                : 1;
        } else {
            MemLimitBytes = 0;
        }

        if (Config->Counters) {
            Config->Counters->MemLimitBytes->Set(MemLimitBytes);
        }

        ActualizeCacheSizeLimit();

        DoGC();

        if (MemLimitBytes && MemLimitBytes < ConfigLimitBytes) {
            // in normal scenario we expect that we can fill the whole shared cache
            ui64 memTableReservedBytes = ConfigLimitBytes * Config->MemTableReservationPercent / 100;
            ui64 memTableTotal = MemTableTracker->GetTotalConsumption();
            if (memTableTotal > memTableReservedBytes) {
                ui64 toCompact = Min(ConfigLimitBytes - MemLimitBytes, memTableTotal - memTableReservedBytes);
                auto registrations = MemTableTracker->SelectForCompaction(toCompact);
                for (auto registration : registrations) {
                    Send(registration.first->Owner, new NSharedCache::TEvMemTableCompact(registration.first->Table, registration.second));
                }
                if (auto logl = Logger->Log(ELnLev::Debug)) {
                    logl
                        << "MemTable compactions triggered for " << toCompact << " bytes "
                        << "(out of " << memTableTotal << ") "
                        << registrations.size() << " tables ";
                }
            }
        }
    }

    void Handle(NSharedCache::TEvMemTableRegister::TPtr &ev) {
        const auto *msg = ev->Get();
        auto registration = MemTableTracker->Register(ev->Sender, msg->Table);
        Send(ev->Sender, new NSharedCache::TEvMemTableRegistered(msg->Table, std::move(registration)));
    }

    void Handle(NSharedCache::TEvMemTableUnregister::TPtr &ev) {
        const auto *msg = ev->Get();
        MemTableTracker->Unregister(ev->Sender, msg->Table);
    }

    void Handle(NSharedCache::TEvMemTableCompacted::TPtr &ev) {
        const auto *msg = ev->Get();
        if (auto registration = dynamic_cast<TSharedPageCacheMemTableRegistration*>(msg->Registration.Get())) {
            MemTableTracker->CompactionComplete(registration);
        }
    }

    void Registered(TActorSystem *sys, const TActorId &owner)
    {
        NActors::TActorBootstrapped<TSharedPageCache>::Registered(sys, owner);
        Owner = owner;

        Logger = new NUtil::TLogger(sys, NKikimrServices::TABLET_SAUSAGECACHE);
        sys->AppData<TAppData>()->Icb->RegisterSharedControl(SizeOverride, Config->CacheName + "_Size");
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
        ActualizeCacheSizeLimit();

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
        ActualizeCacheSizeLimit();

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
        ActualizeCacheSizeLimit();

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
        case UPDATE_WHITEBOARD_TAG: {
            SendWhiteboardStats();
            Schedule(TDuration::Seconds(1), new TKikimrEvents::TEvWakeup(UPDATE_WHITEBOARD_TAG));
            break;
        }
        default:
            Y_ABORT("Unknown wakeup tag: %lu", ev->Get()->Tag);
        }
    }

    void SendWhiteboardStats() {
        TActorId whiteboardId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());
        Send(whiteboardId, NNodeWhiteboard::TEvWhiteboard::CreateSharedCacheStatsUpdateRequest(GetStatAllBytes(), MemLimitBytes));
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
            TVector<ui32> dropped(::Reserve(droppedPagesCount));
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

        if (msg->Record.GetMemoryLimit() != 0) {
            Config->CacheConfig->SetLimit(msg->Record.GetMemoryLimit());
            SizeOverride = Config->CacheConfig->Limit;
            // limit will be updated with ActualizeCacheSizeLimit call
        }

        if (msg->Record.HasActivePagesReservationPercent()) {
            Config->ActivePagesReservationPercent = msg->Record.GetActivePagesReservationPercent();
        }
        if (msg->Record.HasMemTableReservationPercent()) {
            Config->MemTableReservationPercent = msg->Record.GetMemTableReservationPercent();
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
    TSharedPageCache(THolder<TSharedPageCacheConfig> config, TIntrusivePtr<TMemObserver> memObserver)
        : MemObserver(std::move(memObserver))
        , MemTableTracker(std::make_shared<TSharedPageCacheMemTableTracker>(config->Counters))
        , Config(std::move(config))
        , Cache(*Config->CacheConfig)
        , SizeOverride(Config->CacheConfig->Limit, 1, Max<i64>())
        , ConfigLimitBytes(Config->CacheConfig->Limit)
    {
        AsyncRequests.Limit = Config->TotalAsyncQueueInFlyLimit;
        ScanRequests.Limit = Config->TotalScanQueueInFlyLimit;
    }

    void Bootstrap() {
        MemObserver->Subscribe([actorSystem = NActors::TActivationContext::ActorSystem(), selfId = SelfId()] () {
            actorSystem->Send(selfId, new NSharedCache::TEvMem());
        });

        Become(&TThis::StateFunc);
        Schedule(TDuration::Seconds(1), new TKikimrEvents::TEvWakeup(UPDATE_WHITEBOARD_TAG));
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSharedCache::TEvAttach, Handle);
            hFunc(NSharedCache::TEvRequest, Handle);
            hFunc(NSharedCache::TEvTouch, Handle);
            hFunc(NSharedCache::TEvUnregister, Handle);
            hFunc(NSharedCache::TEvInvalidate, Handle);
            hFunc(NSharedCache::TEvMem, Handle);
            hFunc(NSharedCache::TEvMemTableRegister, Handle);
            hFunc(NSharedCache::TEvMemTableUnregister, Handle);
            hFunc(NSharedCache::TEvMemTableCompacted, Handle);

            hFunc(NBlockIO::TEvData, Handle);
            hFunc(TEvSharedPageCache::TEvConfigure, Handle);
            hFunc(TKikimrEvents::TEvWakeup, Wakeup);
            cFunc(TEvents::TSystem::PoisonPill, TakePoison);
        }
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SAUSAGE_CACHE;
    }
};

} // NTabletFlatExecutor

IActor* CreateSharedPageCache(THolder<TSharedPageCacheConfig> config, TIntrusivePtr<TMemObserver> memObserver) {
    return new NTabletFlatExecutor::TSharedPageCache(std::move(config), std::move(memObserver));
}

}

template<> inline
void Out<TVector<ui32>>(IOutputStream& o, const TVector<ui32> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}


