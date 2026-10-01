#include "flat_bio_actor.h"
#include "flat_bio_events.h"
#include "shared_cache_btree_walk.h"
#include "shared_cache_events.h"
#include "shared_sausagecache_state.h"
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
using TPageLocation = NTable::NPage::TPageLocation;

::NFormatPrivate::THumanReadableSize HumanReadableBytes(ui64 bytes) {
    return HumanReadableSize(bytes, SF_BYTES);
}

struct TPageTraits {
    struct TPageKey {
        TLogoBlobID LogoBlobID;
        TPageOffset Offset;
    };

    static ui64 GetSize(const TPage* page) {
        return sizeof(TPage) + page->Size;
    }

    static TPageKey GetKey(const TPage* page) {
        return {page->Collection->Id, page->Offset};
    }

    static size_t GetHash(const TPageKey& key) {
        return MultiHash(key.LogoBlobID.Hash(), static_cast<size_t>(key.Offset));
    }

    static TString ToString(const TPageKey& key) {
        return TStringBuilder() << "LogoBlobID: " << key.LogoBlobID.ToString() << " Offset: " << key.Offset;
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

class TSharedPageCache : public TActorBootstrapped<TSharedPageCache>, private ICacheBTreeWalkHost {
    using ELnLev = NUtil::ELnLev;

    TActorId Owner;
    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;
    NSharedCache::TSharedCachePages* SharedCachePages;
    TSharedCacheConfig Config;
    TSharedPageCacheCounters Counters;

    THashMap<TLogoBlobID, TCollection> Collections;
    TCacheBTreeWalkController Walks{ *this };
    // Keyed by the per-fetch TBlockIO actor returned by Start; removed when its TEvData arrives.
    // A fetch that dies without reporting leaves both this entry and its run waiting for completion.
    THashMap<TActorId, TLogoBlobID> WalkFetches;
    TPendingInMemoryPages PendingInMemoryPages;
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
    ui64 AliveInMemoryBytes = 0; // Bytes of pages marked in-memory (Active + Passive + InFly)
    ui64 ActiveInMemoryBytes = 0;
    ui64 InFlyInMemoryBytes = 0;
    bool WalkContinuationScheduled = false;

    TCollection* FindWalkCollection(const TLogoBlobID& id) override {
        return Collections.FindPtr(id);
    }

    TPendingInMemoryPages& PendingWalkPages() override {
        return PendingInMemoryPages;
    }

    void ScheduleWalkContinuation() override {
        if (!std::exchange(WalkContinuationScheduled, true)) {
            Send(SelfId(), new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::ContinueBTreeWalk)));
        }
    }

    TSharedCachePages* WalkCachePages() override {
        return SharedCachePages;
    }

    void FetchWalkIndexLevel(
        TCollection& collection, TVector<TPageLocation>&& locations, const TLogoBlobID& walkCollectionId) override {
        Y_DEBUG_ABORT_UNLESS(collection.GetCacheMode() == ECacheMode::Regular);
        FetchIndexLevelInQueue(collection, std::move(locations), walkCollectionId);
    }

    void SendWalkStickyPages(
        TCollection& collection, const TActorId& owner, const TVector<TPageLocation>& locations) override {
        SendStickyCollectionPages(collection, owner, locations);
    }

    void CancelQueuedWalkRequestsAndPump(const TLogoBlobID& walkCollectionId) override {
        CancelQueuedWalkRequests(walkCollectionId);
        RequestFromQueue(AsyncRequests);
        RequestFromQueue(ScanRequests);
    }

    // True if after a large drop in target cache limit, we couldn't decrease it right away
    // and yielded to process other events.
    bool LimitDecreaseScheduled = false;

    ui64 GetTargetCacheLimitBytes() const {
        if (Config.HasMemoryLimit()) {
            return Min(MemLimitBytes, Config.GetMemoryLimit());
        }
        return MemLimitBytes;
    }

    ui64 GetInMemoryLimitBytes() {
        ui64 remainInFlyLimit = Config.GetInMemoryInFlyLimit() > InFlyInMemoryBytes ? Config.GetInMemoryInFlyLimit() - InFlyInMemoryBytes : 0;
        return Min(AliveInMemoryBytes + remainInFlyLimit, TargetInMemoryBytes);
    }

    // Reloading into a full tier would evict another page, and the two would trade places forever.
    bool InMemoryTierHasRoomFor(ui64 pageSize) const {
        const ui32 tier = static_cast<ui32>(NTable::NPage::ECacheMode::TryKeepInMemory);
        return Cache.GetTierSize(tier) + pageSize <= Cache.GetTierLimit(tier);
    }

    void ActualizeCacheSizeLimit() {
        Counters.ConfigLimitBytes->Set(Config.HasMemoryLimit() ? Config.GetMemoryLimit() : 0);

        const ui64 currentLimit = Cache.GetLimit();
        const ui64 targetLimit = GetTargetCacheLimitBytes();
        ui64 newLimit;
        if (targetLimit >= currentLimit) {
            // Limit increased, no problem, do it right away.
            newLimit = targetLimit;
        } else if (LimitDecreaseScheduled) {
            // Limit decreased, but we can't update it yet.
            newLimit = currentLimit;
        } else if (targetLimit + Config.GetMaxLimitDecreaseStepBytes() >= currentLimit) {
            // Limit decreased by a small amount, do it right away.
            newLimit = targetLimit;
        } else {
            // A large decrease in target limit, decrease the current limit for a bit
            // and schedule another decrease after we process the current event queue.
            newLimit = currentLimit - Config.GetMaxLimitDecreaseStepBytes();
            LimitDecreaseScheduled = true;
            Send(SelfId(), new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::DoLimitDecrease)));
        }

        // limit of cache depends only on config and mem because passive pages may go in and out arbitrary
        // we may have some passive bytes, so if we fully fill this Cache we may exceed the limit
        // because of that DoGC should be called to ensure limits
        Cache.UpdateLimit(newLimit, GetInMemoryLimitBytes());
        Counters.ActiveLimitBytes->Set(newLimit);
    }

    void DoGC() {
        // maybe we already have enough useless pages
        // update StatActiveBytes + StatPassiveBytes
        ProcessGCList();

        const ui64 cacheLimit = Cache.GetLimit();
        // TODO: get rid of active pages reservation
        const ui64 configActiveReservedBytes = cacheLimit * Config.GetActivePagesReservationPercent() / 100;

        THashSet<TCollection*> recheck;
        ui64 evictedBytes = 0;
        ui64 evictedInMemoryBytes = 0;
        // Evict pages from the cache until *all* used bytes get under the cache limit,
        // but stop if *active* bytes get under configActiveReservedBytes to avoid the case
        // where we just make all pages passive without actually freeing any memory.
        while (GetStatAllBytes() > cacheLimit && StatActiveBytes > configActiveReservedBytes) {
            if (TPage* evictedPage = Cache.EvictNext()) {
                auto pageSize = TPageTraits::GetSize(evictedPage);
                evictedBytes += pageSize;
                if (evictedPage->CacheMode == ECacheMode::TryKeepInMemory) {
                    evictedInMemoryBytes += pageSize;
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

        Counters.S3FIFOEvictOps->Set(Cache.GetEvictOpsCounter());

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "GC has finished with"
            << " Limit: " << HumanReadableBytes(Cache.GetLimit())
            << " TargetLimit: " << HumanReadableBytes(GetTargetCacheLimitBytes())
            << " Active: " << HumanReadableBytes(StatActiveBytes)
            << " Passive: " << HumanReadableBytes(StatPassiveBytes)
            << " LoadInFly: " << HumanReadableBytes(StatLoadInFlyBytes)
            << " EvictedBytes: " << HumanReadableBytes(evictedBytes)
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

    void Registered(TActorSystem* sys, const TActorId& owner) override {
        NActors::TActorBootstrapped<TSharedPageCache>::Registered(sys, owner);
        Owner = owner;

        SharedCachePages = sys->AppData<TAppData>()->SharedCachePages.Get();
    }

    void TakePoison(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Poison"
            << " cache serviced " << StatBioReqs << " reqs"
            << " hit {" << Counters.CacheHitPages->Val() << " " << Counters.CacheHitBytes->Val() << "b}"
            << " miss {" << Counters.CacheMissPages->Val() << " " << Counters.CacheMissBytes->Val() << "b}"
            << " in-memory miss {" << Counters.CacheMissInMemoryPages->Val() << " " << Counters.CacheMissInMemoryBytes->Val() << "b}");

        if (auto owner = std::exchange(Owner, { }))
            Send(owner, new TEvents::TEvGone);

        PassAway();
    }

    TCollection& AttachCollection(const TLogoBlobID& pageCollectionId,
        TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
        TCollection& collection = EnsureCollection(pageCollectionId, *pageCollection, owner);
        collection.PageCollection = std::move(pageCollection);

        if (collection.Owners.insert(owner).second) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE,
                "Add page collection " << pageCollectionId << " owner " << owner);
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

    void CancelQueuedWalkRequests(const TLogoBlobID& walkCollectionId) {
        for (auto& [_, requests] : AsyncRequests.Requests) {
            for (auto& request : requests) {
                if (request->WalkCollectionId != walkCollectionId || request->IsResponded()) {
                    continue;
                }

                if (auto* collection = Collections.FindPtr(request->Label)) {
                    for (TPageOffset offset : request->QueuePagesToRequest) {
                        if (auto* page = collection->PageSet.FindPage(offset);
                            page && page->State == PageStatePending) {
                            // Walk requests register no waiter. A live waiter here belongs to another
                            // queued request; Fast/None requests promote Pending to Requested immediately.
                            const auto* pendingRequests = collection->PendingRequests.FindPtr(offset);
                            const bool hasLiveRequest =
                                pendingRequests && AnyOf(*pendingRequests, [](const auto& item) {
                                    return !item.first->IsResponded();
                                });
                            if (!hasLiveRequest) {
                                RemoveAlivePage(page);
                                const bool erased = collection->PageSet.ErasePage(offset);
                                Y_ENSURE(erased);
                            }
                        }
                    }
                }
                request->MarkResponded();
            }
        }
    }

    void Handle(NSharedCache::TEvAttach::TPtr &ev, const TActorContext& ctx) {
        NSharedCache::TEvAttach *msg = ev->Get();
        const auto &pageCollection = *msg->PageCollection;
        const TLogoBlobID pageCollectionId = pageCollection.Label();

        LOG_DEBUG_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Attach page collection " << pageCollectionId
            << " owner " << ev->Sender
            << " cache mode " << msg->CacheMode);

        TCollection& collection = AttachCollection(pageCollectionId, msg->PageCollection, ev->Sender);
        switch (msg->CacheMode) {
        case ECacheMode::Regular:
            TryMoveToRegularCache(collection, ev->Sender);
            break;
        case ECacheMode::TryKeepInMemory:
            TryMoveToTryKeepInMemoryCache(collection, std::move(msg->PageCollection), ev->Sender);
            break;
        }
        Walks.UpdateSeeds(collection, ev->Sender, std::move(msg->BtreeSeeds), msg->ReplayStickyWalk);
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
            auto emplaced = collection.PageSet.insert(page);
            Y_ENSURE(emplaced.second, "Pages should be unique");

            page->Collection = &collection;
            AddAlivePage(page.Get());
            BodyProvided(collection, page.Get());
        }

        Y_ENSURE(collection.PageSet.size() <= collection.TotalPages,
            "Saved more pages " << collection.PageSet.size()
            << " than collection " << pageCollectionId << " declares " << collection.TotalPages);
    }

    void Handle(NSharedCache::TEvRequest::TPtr &ev, const TActorContext& ctx) {
        NSharedCache::TEvRequest *msg = ev->Get();
        const auto &pageCollection = *msg->PageCollection;
        const TLogoBlobID pageCollectionId = pageCollection.Label();
        const bool doTraceLog = DoTraceLog();

        TCollection &collection = AttachCollection(pageCollectionId, msg->PageCollection, ev->Sender);
        ECacheMode cacheMode = collection.GetCacheMode();

        TStackVec<std::pair<TPageOffset, ui32>> pendingPages; // offset, reqIdx
        ui32 pagesToRequestCount = 0;

        TVector<TEvResult::TLoaded> readyPages(::Reserve(msg->Pages.size()));
        TVector<TPageOffset> pagesFromCacheTraceLog;

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
            const auto& location = msg->Pages[reqIdx];
            auto* page = EnsurePage(collection, location, cacheMode);

            Counters.RequestedPages->Inc();
            Counters.RequestedBytes->Add(page->Size);

            bool wasEvicted = page->State == PageStateEvicted;
            if (wasEvicted) {
                const bool used = page->Use(); // still in PageSet, guaranteed to be alive
                Y_ENSURE(used);
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
                    pagesFromCacheTraceLog.push_back(page->Offset);
                }
                readyPages.emplace_back(page->Offset, page->Size, TSharedPageRef::MakeUsed(page, SharedCachePages->GCList, page->Type));
                break;
            case PageStateNo:
                ++pagesToRequestCount;
                [[fallthrough]];
            case PageStateRequested:
            case PageStateRequestedAsync:
            case PageStatePending:
                Counters.CacheMissPages->Inc();
                Counters.CacheMissBytes->Add(page->Size);
                if (page->CacheMode == NTable::NPage::ECacheMode::TryKeepInMemory) {
                    Counters.CacheMissInMemoryPages->Inc();
                    Counters.CacheMissInMemoryBytes->Add(page->Size);
                }
                readyPages.emplace_back(page->Offset, page->Size, TSharedPageRef());
                pendingPages.emplace_back(page->Offset, reqIdx);
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
            TVector<TPageLocation> pagesToRequest(::Reserve(pagesToRequestCount));
            TVector<TPageOffset> pagesToWaitTraceLog;
            ui64 pagesToRequestBytes = 0;
            if (doTraceLog) {
                pagesToWaitTraceLog.reserve(pendingPages.size() - pagesToRequestCount);
            }

            if (queue) {
                // register for loading regardless of pending state, to simplify actor deregister logic
                // would be filtered on actual request
                queue->Requests[ev->Sender].push_back(request);
            }

            for (auto [offset, reqIdx] : pendingPages) {
                collection.PendingRequests[offset].emplace(request, reqIdx);
                ++request->PendingBlocks;
                auto* page = collection.PageSet.FindPage(offset);
                Y_ENSURE(page);

                if (queue) {
                    request->QueuePagesToRequest.push_back(offset);
                }

                switch (page->State) {
                case PageStateNo:
                    pagesToRequest.emplace_back(offset, page->Size, page->Type, page->Crc32);
                    pagesToRequestBytes += page->Size;

                    if (queue)
                        page->State = PageStatePending;
                    else
                        page->State = PageStateRequested;

                    break;
                case PageStateRequested:
                    if (doTraceLog) {
                        pagesToWaitTraceLog.emplace_back(offset);
                    }
                    break;
                case PageStateRequestedAsync:
                case PageStatePending:
                    if (!queue) {
                        pagesToRequest.emplace_back(offset, page->Size, page->Type, page->Crc32);
                        pagesToRequestBytes += page->Size;
                        page->State = PageStateRequested;
                    } else if (doTraceLog) {
                        pagesToWaitTraceLog.emplace_back(offset);
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

                for (TPageOffset offset : request.QueuePagesToRequest) {
                    ++nthToRequest;

                    auto* page = collection->PageSet.FindPage(offset);
                    if (!page || page->State != PageStatePending)
                        continue;

                    ++nthToLoad;
                    queue.InFly += page->Size;
                    sizeToLoad += page->Size;
                    if (queue.InFly > queue.Limit)
                        break;
                }

                if (nthToLoad != 0) {
                    TVector<TPageLocation> toLoad;
                    toLoad.reserve(nthToLoad);
                    for (TPageOffset offset : request.QueuePagesToRequest) {
                        auto* page = collection->PageSet.FindPage(offset);
                        if (!page || page->State != PageStatePending)
                            continue;

                        toLoad.emplace_back(offset, page->Size, page->Type, page->Crc32);
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

        for (TPageOffset offset : request->QueuePagesToRequest) {
            auto pageRequestsIt = collection->PendingRequests.find(offset);
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
        THashMap<TLogoBlobID, THashSet<TPageOffset>> droppedPages;

        for (auto &[pageCollectionId, pages] : msg->Pages) {
            LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Sync page collection " << pageCollectionId
                << " owner " << ev->Sender
                << " pages " << pages);

            auto collection = Collections.FindPtr(pageCollectionId);
            if (!collection) {
                droppedPages[pageCollectionId].insert(pages.begin(), pages.end());
                continue;
            }

            for (auto offset : pages) {
                bool found = collection->PageSet.contains(offset);
                if (!found) {
                    droppedPages[pageCollectionId].insert(offset);
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
            const TLogoBlobID pageCollectionId = collection->Id;
            Walks.UpdateSeeds(*collection, ev->Sender, {});

            // Cancelling the last walk may already have expired the collection.
            // Its owner-map key is not dereferenced again before the owner entry is erased below.
            if (auto* current = Collections.FindPtr(pageCollectionId)) {
                TryDropExpiredCollection(*current);
            }
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
        Walks.UpdateSeeds(*collection, ev->Sender, {});

        // Cancelling the last walk may already have expired the collection.
        if (auto* current = Collections.FindPtr(pageCollectionId)) {
            TryDropExpiredCollection(*current);
        }
    }

    void Handle(NBlockIO::TEvData::TPtr &ev, const TActorContext& ctx) {
        auto *msg = ev->Get();
        TLogoBlobID walkCollectionId;
        if (auto it = WalkFetches.find(ev->Sender); it != WalkFetches.end()) {
            walkCollectionId = it->second;
            WalkFetches.erase(it);
        }

        LOG_TRACE_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Receive page collection " << msg->PageCollection->Label()
            << " status " << msg->Status
            << " pages " << msg->Pages);

        auto fetchType = static_cast<EBlockIOFetchTypeCookie>(ev->Cookie);

        RemoveInFlyPages(msg->Pages.size(), msg->Cookie);

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
                ui64 totalBytes = msg->Cookie + sizeof(TPage) * msg->Pages.size();
                Y_ENSURE(InFlyInMemoryBytes >= totalBytes);
                InFlyInMemoryBytes -= totalBytes;
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
            Walks.FinishFetch(walkCollectionId);
            return;
        }

        if (msg->Status != NKikimrProto::OK) {
            if (walkCollectionId) {
                Walks.InvalidateRun(walkCollectionId);
            } else if (fetchType == EBlockIOFetchTypeCookie::TryKeepInMemoryPreload) {
                // DropCollection also invalidates walks that use this collection for index pages.
                Walks.InvalidateDataCollection(collection->Id);
            }
            DropCollection(*collection, msg->Status, fetchType);
        } else {
            bool needNotifyOwners = fetchType == EBlockIOFetchTypeCookie::TryKeepInMemoryPreload && collection->InMemoryOwners;
            auto loadedPages = needNotifyOwners ? TVector<TPage*>(::Reserve(msg->Pages.size())) : TVector<TPage*>();
            for (auto &paged : msg->Pages) {
                auto* page = collection->PageSet.FindPage(paged.Offset);
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
                for (const auto& owner : collection->InMemoryOwners) {
                    NotifyInMemOwner(collection->PageCollection, loadedPages, owner);
                }
                ActualizeCacheSizeLimit();
                Evict(Cache.EnsureLimits());
            }
        }

        if (queue) {
            RequestFromQueue(*queue);
        }
        Walks.FinishFetch(walkCollectionId);
    }

    TPage* EnsurePage(TCollection& collection,
        const TPageLocation& location, ECacheMode initialMode) {
        TPage* page = collection.PageSet.FindPage(location.Offset);

        if (!page) {
            Y_ENSURE(collection.PageSet.size() < collection.TotalPages);
            page = new TPage(location.Offset, location.Size, location.Type, location.Crc32, &collection);
            const bool inserted = collection.PageSet.emplace(page).second;
            Y_DEBUG_ABORT_UNLESS(inserted);
            page->CacheMode = initialMode;
            AddAlivePage(page);
        } else {
            Y_ENSURE(page->Size == location.Size && page->Crc32 == location.Crc32 && page->Type == location.Type);
        }

        return page;
    }

    void ReloadEvictedPage(TPage* page, bool touched = true) {
        Y_ASSERT(page->State == PageStateEvicted);
        const bool used = page->Use(); // still in PageSet, guaranteed to be alive
        Y_ENSURE(used);
        page->State = PageStateLoaded;
        RemovePassivePage(page);
        AddActivePage(page);
        if (touched) {
            Evict(Cache.Insert(page));
        } else {
            Evict(Cache.InsertUntouched(page));
        }
    }

    TCollection& EnsureCollection(const TLogoBlobID& pageCollectionId, const NPageCollection::IPageCollection& pageCollection, const TActorId& owner) {
        TCollection &collection = Collections[pageCollectionId];
        if (!collection.Id) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Add page collection " << pageCollectionId);
            Counters.PageCollections->Inc();
            Y_ENSURE(pageCollectionId);
            collection.Id = pageCollectionId;
            // Do not call reserve(): TotalPages counts all disk pages; PageSet grows with tracked pages.
            collection.TotalPages = pageCollection.Total();
            collection.TotalSize = sizeof(TPage) * collection.TotalPages + pageCollection.BackingSize();
        } else {
            Y_DEBUG_ABORT_UNLESS(collection.Id == pageCollectionId);
            Y_ENSURE(collection.TotalPages == pageCollection.Total(),
                "Page collection " << pageCollectionId
                << " changed number of pages from " << collection.TotalPages
                << " to " << pageCollection.Total() << " by " << owner);
        }
        return collection;
    }

    void TryDropExpiredCollection(TCollection& collection) override {
        // Drop unnecessary collections from memory
        if (!collection.Owners &&
            !collection.PendingRequests &&
            Walks.IsIdle(collection.Id) &&
            collection.PageSet.empty())
        {
            Y_DEBUG_ABORT_UNLESS(collection.InMemoryOwners.empty());
            auto pageCollectionId = collection.Id;
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Drop expired page collection " << pageCollectionId);
            Walks.EraseCollection(pageCollectionId);
            Collections.erase(pageCollectionId);
            Counters.PageCollections->Dec();
        }
    }

    void Wakeup(TKikimrEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
        if (tag != EWakeupTag::ContinueBTreeWalk) {
            LOG_INFO_S(ctx, NKikimrServices::TABLET_SAUSAGECACHE, "Wakeup " << tag);
        }

        switch (tag) {
        case EWakeupTag::DoGCScheduled:
            ScheduleGC();
            break;
        case EWakeupTag::DoGCManual:
            break;
        case EWakeupTag::DoLimitDecrease:
            LimitDecreaseScheduled = false;
            ActualizeCacheSizeLimit();
            break;
        case EWakeupTag::ContinueBTreeWalk:
            WalkContinuationScheduled = false;
            break;
        }

        // DoGC will be called at the end of StateFunc
    }

    void ProcessGCList() {
        THashSet<TCollection*> recheck;

        while (auto rawPage = SharedCachePages->GCList->PopGC()) {
            auto* page = static_cast<TPage*>(rawPage.Get());
            if (page->State == PageStateEvicted) {
                if (page->GetFrequency() > 0) {
                    // page was accessed while being passive, load it back
                    ReloadEvictedPage(page);
                } else if (page->CacheMode == NTable::NPage::ECacheMode::TryKeepInMemory
                    && ActiveInMemoryBytes + TPageTraits::GetSize(page) <= GetInMemoryLimitBytes()
                    && InMemoryTierHasRoomFor(TPageTraits::GetSize(page)))
                {
                    ReloadEvictedPage(page, false);
                }
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
            Y_DEBUG_ABORT_UNLESS(page->State == PageStateEvicted);
            RemovePassivePage(page);

            Y_VERIFY_DEBUG_S(page->Collection, "Evicted pages are expected to have collection");
            if (auto* collection = page->Collection) {
                auto offset = page->Offset;
                Y_DEBUG_ABORT_UNLESS(collection->PageSet.FindPage(page->Offset) == page);
                if (page->CacheMode == NTable::NPage::ECacheMode::TryKeepInMemory) {
                    PendingInMemoryPages[collection->Id].emplace(
                        TPageLocation(page->Offset, page->Size, page->Type, page->Crc32));
                }
                RemoveAlivePage(page);
                const bool erased = collection->PageSet.ErasePage(offset);
                Y_ENSURE(erased);
                // Note: don't use page after erase as it may be deleted
                if (collection->Owners) {
                    collection->DroppedPages.push_back(offset);
                }
                recheck.insert(collection);
            }
        }
    }

    void ScheduleGC() {
        TActivationContext::AsActorContext().Schedule(TDuration::Seconds(15), new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::DoGCScheduled)));
    }

    void CheckExpiredCollections(THashSet<TCollection*> recheck) {
        THashMap<TActorId, THashMap<TLogoBlobID, THashSet<TPageOffset>>> droppedPages;

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

    void SendDroppedPages(TActorId owner, THashMap<TLogoBlobID, THashSet<TPageOffset>>&& droppedPages_) {
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
        if (page->Type == NTable::NPage::EPage::BTreeIndexV2) {
            Walks.IndexPagesChanged(collection.Id);
        }
        AddActivePage(page);
        auto pendingRequestsIt = collection.PendingRequests.find(page->Offset);
        if (pendingRequestsIt == collection.PendingRequests.end()) {
            Evict(Cache.Insert(page));
            return;
        }
        for (auto &[request, index] : pendingRequestsIt->second) {
            if (request->IsResponded()) {
                continue;
            }

            auto &readyPage = request->ReadyPages[index];
            Y_DEBUG_ABORT_UNLESS(readyPage.Offset == page->Offset);
            readyPage.Page = TSharedPageRef::MakeUsed(page, SharedCachePages->GCList, page->Type);

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

    void NotifyInMemOwner(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TVector<TPage*>& readyPages, const TActorId& owner) {
        TVector<TEvResult::TLoaded> readyLoadedPages;

        for (auto* page : readyPages) {
            if (page->State == PageStateLoaded) { // page may be evicted before NotifyInMemOwner call
                readyLoadedPages.emplace_back(
                    page->Offset, page->Size,
                    TSharedPageRef::MakeUsed(page, SharedCachePages->GCList, page->Type));
            }
        }

        if (readyLoadedPages) {
            TAutoPtr<NSharedCache::TEvResult> result = new NSharedCache::TEvResult(std::move(pageCollection), NKikimrProto::OK, 0);
            result->Pages = std::move(readyLoadedPages);
            Send(owner, result.Release(), 0, static_cast<ui64>(ERequestTypeCookie::TryKeepInMemPages));
        }
    }

    void NotifyInMemOwnerAboutError(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, NKikimrProto::EReplyStatus error, const TActorId& owner) {
        TAutoPtr<NSharedCache::TEvResult> result = new NSharedCache::TEvResult(std::move(pageCollection), error, 0);

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Send page collection error " << result->PageCollection->Label()
            << " owner " << owner
            << " class " << NBlockIO::EPriority::Bulk
            << " error " << error
            << " cookie " << static_cast<ui64>(ERequestTypeCookie::TryKeepInMemPages)
            << " (in-memory preload)");

        Send(owner, result.Release(), 0, static_cast<ui64>(ERequestTypeCookie::TryKeepInMemPages));
    }

    void SendRequest(TRequest& request, TVector<TPageLocation>&& pages, ui64 bytes, EBlockIOFetchTypeCookie cookie) {
        if (request.WalkCollectionId) {
            Walks.FetchStarted(request.WalkCollectionId);
        }
        AddInFlyPages(pages.size(), bytes);

        // fetch cookie -> requested size
        // event cookie -> queue type
        auto *fetch = new NBlockIO::TEvFetch(request.Priority, request.PageCollection,
            std::move(pages), bytes);
        if (cookie == EBlockIOFetchTypeCookie::AsyncQueue || cookie == EBlockIOFetchTypeCookie::ScanQueue) {
            // Note: queued requests can fetch multiple times, so copy trace id
            fetch->TraceId = NWilson::TTraceId(request.TraceId);
        } else {
            fetch->TraceId = std::move(request.TraceId);            
        }
        const TActorId bioActor = NBlockIO::Start(this, request.Sender, static_cast<ui64>(cookie), fetch);
        if (request.WalkCollectionId) {
            const bool inserted = WalkFetches.emplace(bioActor, request.WalkCollectionId).second;
            Y_ENSURE(inserted);
        }
    }

    void DropCollection(TCollection &collection, NKikimrProto::EReplyStatus blobStorageError, EBlockIOFetchTypeCookie fetchType) {
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
        for (const auto &kv : collection.PageSet) {
            auto* page = kv.Get();

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

            RemoveAlivePage(page);
            page->Collection = nullptr;
            ++droppedPagesCount;
        }

        if (haveValidPages) {
            TVector<TPageOffset> dropped(Reserve(droppedPagesCount));
            for (const auto &ptr : collection.PageSet) {
                auto* page = ptr.Get();
                if (!page->Collection) {
                    dropped.push_back(page->Offset);
                }
            }
            for (auto offset : dropped) {
                collection.PageSet.ErasePage(offset);
            }
        } else {
            collection.PageSet.clear();
        }

        ActualizeCacheSizeLimit();

        if (fetchType == EBlockIOFetchTypeCookie::TryKeepInMemoryPreload) {
            for (const auto& owner : collection.InMemoryOwners) {
                // InMemoryOwners and Owners will be cleared on TEvUnregister response from tablet
                NotifyInMemOwnerAboutError(collection.PageCollection, blobStorageError, owner);
            }
        }

        Walks.DropForIndexCollection(collection.Id);

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

        ui64 bytesToMove = 0;
        for (const auto& ptr : collection.PageSet) {
            if (ptr->CacheMode == ECacheMode::TryKeepInMemory) {
                bytesToMove += TPageTraits::GetSize(ptr.Get());
            }
        }
        Y_ENSURE(AliveInMemoryBytes >= bytesToMove);
        AliveInMemoryBytes -= bytesToMove;
        ActualizeCacheSizeLimit();

        PendingInMemoryPages.erase(collection.Id);
        Walks.DropIndexOnlyWalks(collection.Id);
        // TODO: move pages async and batched
        for (const auto& ptr : collection.PageSet) {
            TryChangeCacheMode(ptr.Get(), ECacheMode::Regular);
        }

        Evict(Cache.EnsureLimits());
    }

    void TryMoveToTryKeepInMemoryCache(TCollection& collection, TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, const TActorId& owner) {
        if (collection.InMemoryOwners) {
            if (collection.InMemoryOwners.insert(owner).second) {
                // new owner for already in-memory collection
                TVector<TPage*> loadedPages;
                for (const auto& ptr : collection.PageSet) {
                    auto* page = ptr.Get();
                    if (page->CacheMode != ECacheMode::TryKeepInMemory) {
                        continue;
                    }
                    switch (page->State) {
                    case PageStateLoaded:
                        loadedPages.push_back(page);
                        break;
                    case PageStateEvicted:
                        // also we need to notify about pages that can be reloaded
                        if (ActiveInMemoryBytes + TPageTraits::GetSize(page) <= GetInMemoryLimitBytes()) {
                            ReloadEvictedPage(page, false);
                            loadedPages.push_back(page);
                        }
                        break;
                    }
                }

                if (loadedPages) {
                    NotifyInMemOwner(pageCollection, loadedPages, owner);
                }
            }
            return;
        }

        const bool inserted = collection.InMemoryOwners.insert(owner).second;
        Y_ENSURE(inserted);

        Walks.RestartForIndexCollection(collection.Id);

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Change mode of page collection " << collection.Id
            << " to " << ECacheMode::TryKeepInMemory);
        TargetInMemoryBytes += collection.TotalSize;
        Counters.TargetInMemoryBytes->Set(TargetInMemoryBytes);
        auto skipBTreeIndexV1Shadow = pageCollection->SkipBTreeIndexV1Shadow();
        auto skipPageType = [skipBTreeIndexV1Shadow](NTable::NPage::EPage type) {
            return NPageCollection::IsDeadPage(type, skipBTreeIndexV1Shadow);
        };

        // Reserve only the pages that will change tiers; resident V1 shadows remain regular.
        for (const auto& ptr : collection.PageSet) {
            if (ptr->CacheMode != ECacheMode::TryKeepInMemory && !skipPageType(ptr->Type)) {
                AliveInMemoryBytes += TPageTraits::GetSize(ptr.Get());
            }
        }
        ActualizeCacheSizeLimit();

        TVector<TPage*> loadedPages;
        auto& pagesToLoad = PendingInMemoryPages[collection.Id];
        auto processPage = [&](TPage* page) {
            TryChangeCacheMode(page, ECacheMode::TryKeepInMemory);

            switch (page->State) {
            case PageStateLoaded:
                loadedPages.push_back(page);
                break;
            case PageStateEvicted:
                ReloadEvictedPage(page);
                loadedPages.push_back(page);
                break;
            }
        };

        bool hasSkippedPages = false;
        for (const auto& pageId : xrange(pageCollection->MetaPages())) {
            auto type = static_cast<NTable::NPage::EPage>(pageCollection->Page(pageId).Type);
            hasSkippedPages |= type == NTable::NPage::EPage::Skip;
            if (skipPageType(type)) {
                continue;
            }

            auto location = pageCollection->GetLocation(pageId);
            if (auto* page = collection.PageSet.FindPage(location.Offset)) {
                processPage(page);
            } else {
                pagesToLoad.emplace(location);
            }
        }

        // Hidden pages use Skip entries, including collections where Total() > MetaPages().
        if (hasSkippedPages) {
            // A Skip entry can hide even one page, when Total() still equals MetaPages().
            for (const auto& ptr : collection.PageSet) {
                auto* page = ptr.Get();
                if (page->CacheMode != ECacheMode::TryKeepInMemory && !skipPageType(page->Type)) {
                    processPage(page);
                }
            }
        }

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Try move collection " << collection.Id
                << " in memory, total pages: " << pageCollection->Total() << " (" << HumanReadableBytes(collection.TotalSize) << "), "
                << "pages already loaded: " << loadedPages.size());

        if (loadedPages) {
            NotifyInMemOwner(pageCollection, loadedPages, owner);
        }

        Evict(Cache.EnsureLimits());
    }

    // Ordinary background pages of a regular collection, sent through the async queue.
    void FetchIndexLevelInQueue(
        TCollection& collection, TVector<TPageLocation>&& locations, const TLogoBlobID& walkCollectionId) {
        auto request = MakeIntrusive<TRequest>();
        request->Label = collection.Id;
        request->PageCollection = collection.PageCollection;
        request->Priority = NBlockIO::EPriority::Bkgr;
        request->WalkCollectionId = walkCollectionId;
        if (collection.InMemoryOwners) {
            request->Sender = *collection.InMemoryOwners.begin();
        } else if (collection.Owners) {
            request->Sender = *collection.Owners.begin();
        }

        // No result is expected: the queue only sends the pages and bounds what is in flight.
        for (const auto& location : locations) {
            auto* page = EnsurePage(collection, location, ECacheMode::Regular);
            page->State = PageStatePending;
            request->QueuePagesToRequest.push_back(location.Offset);
        }

        AsyncRequests.Requests[request->Sender].push_back(request);
        RequestFromQueue(AsyncRequests);
    }

    // The cache only enumerates the pages of a sticky collection, the owner fetches and keeps them.
    void SendStickyCollectionPages(
        TCollection& collection, const TActorId& owner, const TVector<TPageLocation>& locations) {
        if (!collection.Owners.contains(owner)) {
            return;
        }

        auto send = [&](auto first, auto last) {
            if (first == last) {
                return;
            }

            TVector<TPageLocation> batch(first, last);
            Send(owner, new NSharedCache::TEvStickyCollectionPages(collection.Id, batch));
        };

        for (auto it = locations.begin(); it != locations.end(); ) {
            auto last = it + Min<size_t>(TEvStickyCollectionPages::MaxBatchLocations, locations.end() - it);
            send(it, last);
            it = last;
        }
    }

    void TryLoadInMemoryCollections() {
        if (PendingInMemoryPages.empty()) {
            return;
        }

        ui64 remainBytes = Min(
            Cache.GetLimit() > GetStatAllBytes() ? Cache.GetLimit() - GetStatAllBytes() : 0,                               // available in cache
            Config.GetInMemoryInFlyLimit() > InFlyInMemoryBytes ? Config.GetInMemoryInFlyLimit() - InFlyInMemoryBytes : 0  // available in in-fly limit
        );

        while (PendingInMemoryPages) {
            auto it = PendingInMemoryPages.begin();

            const TLogoBlobID collectionId = it->first;
            auto& pagesToLoad = it->second;

            auto* collection = Collections.FindPtr(collectionId);
            if (!collection) {
                PendingInMemoryPages.erase(it);
                continue;
            }

            // any owner is suitable for reporting BIO stats
            auto ownersIt = collection->InMemoryOwners.begin();
            Y_ENSURE(ownersIt != collection->InMemoryOwners.end());
            const auto& owner = *ownersIt;

            TVector<TPageLocation> pagesToRequest;
            ui64 pagesToRequestBytes = 0;
            while (pagesToLoad) {
                auto locationIt = pagesToLoad.begin();
                const auto& location = *locationIt;

                auto* page = EnsurePage(*collection, location, ECacheMode::TryKeepInMemory);
                if (page->State == PageStateNo) {
                    if (TPageTraits::GetSize(page) > remainBytes) {
                        RemoveAlivePage(page);
                        collection->PageSet.ErasePage(page->Offset);
                        break;
                    }
                    remainBytes -= TPageTraits::GetSize(page);
                    page->State = PageStateRequestedAsync;
                    pagesToRequest.push_back(location);
                    pagesToRequestBytes += page->Size;
                    InFlyInMemoryBytes += TPageTraits::GetSize(page);
                }

                pagesToLoad.erase(locationIt);
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE,
                "Try load in-memory collection " << collection->Id << ", "
                                                 << "total pages: " << collection->PageCollection->Total() << " ("
                                                 << HumanReadableBytes(collection->TotalSize) << "), "
                                                 << "pages to request: " << pagesToRequest.size() << " ("
                                                 << HumanReadableBytes(pagesToRequestBytes) << ", "
                                                 << "remain pages in queue: " << pagesToLoad.size());

            if (pagesToRequest) {
                TRequest request;
                request.PageCollection = collection->PageCollection;
                request.Sender = owner;
                request.Priority = NBlockIO::EPriority::Bulk;
                LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Request page collection " << request.PageCollection->Label()
                    << " owner " << owner
                    << " class " << request.Priority
                    << " pages " << pagesToRequest);

                SendRequest(request, std::move(pagesToRequest), pagesToRequestBytes, EBlockIOFetchTypeCookie::TryKeepInMemoryPreload);
            }

            // some pages out of limit, try continue loading on next call
            if (pagesToLoad) {
                break;
            }

            PendingInMemoryPages.erase(it);
        }

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_SAUSAGECACHE, "Remain in-memory collections to load: " << PendingInMemoryPages.size());
    }

    // Callers adjust AliveInMemoryBytes before moving pages to reserve capacity for the new tier.
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
        size_t evictedPages = 0;
        ui64 evictedBytes = 0;
        while (!pages.Empty()) {
            TPage* page = pages.PopFront();
            auto pageSize = TPageTraits::GetSize(page);

            page->EnsureNoCacheFlags();

            Y_DEBUG_ABORT_UNLESS(page->State == PageStateLoaded);
            page->State = PageStateEvicted;

            RemoveActivePage(page);
            AddPassivePage(page);
            if (page->UnUse()) {
                SharedCachePages->GCList->PushGC(page);
            }

            ++evictedPages;
            evictedBytes += pageSize;
        }

        Counters.EvictedPages->Add(evictedPages);
        Counters.EvictedBytes->Add(evictedBytes);
    }

    void EvictNow(TPage* page, THashSet<TCollection*>& recheck) {
        auto pageSize = TPageTraits::GetSize(page);
        page->EnsureNoCacheFlags();

        Y_DEBUG_ABORT_UNLESS(page->State == PageStateLoaded);
        page->State = PageStateEvicted;

        RemoveActivePage(page);
        AddPassivePage(page);
        if (page->UnUse()) {
            TryDrop(page, recheck);
        }

        Counters.EvictedPages->Inc();
        Counters.EvictedBytes->Add(pageSize);
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        {
            auto* appData = AppData(ctx);
            NKikimrSharedCache::TSharedCacheConfig config;
            if (record.GetConfig().HasBootstrapConfig() && record.GetConfig().GetBootstrapConfig().HasSharedCacheConfig()) {
                config.MergeFrom(record.GetConfig().GetBootstrapConfig().GetSharedCacheConfig());
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
            ActiveInMemoryBytes += pageSize;
            Counters.ActiveInMemoryBytes->Add(pageSize);
        }
    }

    inline void RemoveActivePage(const TPage* page) {
        auto pageSize = TPageTraits::GetSize(page);
        Y_ENSURE(StatActiveBytes >= pageSize);
        StatActiveBytes -= pageSize;
        Counters.ActivePages->Dec();
        Counters.ActiveBytes->Sub(pageSize);
        if (page->CacheMode == ECacheMode::TryKeepInMemory) {
            Y_ENSURE(ActiveInMemoryBytes >= pageSize);
            ActiveInMemoryBytes -= pageSize;
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
        Y_ENSURE(StatPassiveBytes >= pageSize);
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

    inline void AddAlivePage(const TPage* page) {
        auto pageSize = TPageTraits::GetSize(page);
        Y_ENSURE(page->Collection);
        page->Collection->AliveBytes += pageSize;
        if (page->CacheMode == NTable::NPage::ECacheMode::TryKeepInMemory) {
            AliveInMemoryBytes += pageSize;
        }
    }

    inline void RemoveAlivePage(const TPage* page) {
        auto pageSize = TPageTraits::GetSize(page);
        Y_ENSURE(page->Collection);
        Y_ENSURE(page->Collection->AliveBytes >= pageSize);
        page->Collection->AliveBytes -= pageSize;
        if (page->CacheMode == NTable::NPage::ECacheMode::TryKeepInMemory) {
            Y_ENSURE(AliveInMemoryBytes >= pageSize);
            AliveInMemoryBytes -= pageSize;
        }
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

        // Advance is a no-op without new work; FinishReady only checks runs whose state changed.
        // This ordering is load-bearing: walks parse arrivals before GC and may become Draining
        // after queueing data pages; the loader must submit those pages before completion is checked.
        if (Walks.HasActiveWalks()) {
            Walks.Advance();
        }
        DoGC();
        TryLoadInMemoryCollections();
        if (Walks.HasActiveWalks()) {
            Walks.FinishReady();
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
void Out<TVector<NKikimr::NSharedCache::TPageOffset>>(IOutputStream& o, const TVector<NKikimr::NSharedCache::TPageOffset> &vec) {
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
        o << x.Offset << ' ';
    o << "]";
}

template<> inline
void Out<TVector<NKikimr::NPageCollection::TLoadedPage>>(IOutputStream& o, const TVector<NKikimr::NPageCollection::TLoadedPage> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x.Location << ' ';
    o << "]";
}

template<> inline
void Out<TVector<NKikimr::NPageCollection::TLoadedPageData>>(IOutputStream& o, const TVector<NKikimr::NPageCollection::TLoadedPageData> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x.Offset << ' ';
    o << "]";
}

template<> inline
void Out<TVector<TIntrusivePtr<NKikimr::NSharedCache::TPage>>>(IOutputStream& o, const TVector<TIntrusivePtr<NKikimr::NSharedCache::TPage>> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x->Offset << ' ';
    o << "]";
}
