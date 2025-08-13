#include <shared_cache_counters.h>
#include <shared_cache_events.h>
#include <shared_sausagecache.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/wait_events.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSharedCache {
using namespace NActors;
using namespace NTabletFlatExecutor;
using namespace NPageCollection;

static const ui64 NO_QUEUE_COOKIE = 1;
static const ui64 ASYNC_QUEUE_COOKIE = 2;

static const ui64 MemoryLimit = 4 * (104 + 10); // sizeof(TPage) = 104

struct TFetch {
    ui64 Cookie;
    TIntrusiveConstPtr<IPageCollection> PageCollection;
    TVector<TPageId> Pages;

    TString DebugString() {
        TStringBuilder result;
        result << "PageCollection: " << PageCollection->Label();
        result << " Cookie: " << Cookie;
        result << " Pages: [";
        for (auto page : Pages) {
            result << " " << page;
        }
        result << " ]";
        return result;
    }
};

struct TPageCollectionMock : public IPageCollection {
    TPageCollectionMock(ui64 id, ui32 totalPages)
        : Id(1, 1, id)
        , TotalPages(totalPages)
    {}

    const TLogoBlobID& Label() const noexcept override {
        return Id;
    }

    ui32 Total() const noexcept override {
        return TotalPages;
    }

    TInfo Page(ui32 page) const override {
        Y_UNUSED(page);
        return { 10, ui32(NTable::NPage::EPage::Undef) };
    }

    TBorder Bounds(ui32) const override {
        Y_TABLET_ERROR("Unexpected Bounds(...) call");
    }

    TGlobId Glob(ui32) const override {
        Y_TABLET_ERROR("Unexpected Glob(...) call");
    }

    bool Verify(ui32, TArrayRef<const char>) const override {
        Y_TABLET_ERROR("Unexpected Verify(...) call");
    }

    size_t BackingSize() const noexcept override {
        return 10 * TotalPages;
    }

private:
    TLogoBlobID Id;
    ui32 TotalPages;
};

struct TExecutorMock : public TActorBootstrapped<TExecutorMock> {
public:
    TExecutorMock(std::deque<NSharedCache::TEvResult::TPtr>& results)
        : Results(results)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSharedCache::TEvResult, Handle);
        }
    }

    void Handle(NSharedCache::TEvResult::TPtr& ev) {
        Results.push_back(ev);
    }

    std::deque<NSharedCache::TEvResult::TPtr>& Results;
};

struct TSharedPageCacheMock {
    TSharedPageCacheMock() {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_TRACE);
        Runtime.SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NLog::PRI_TRACE);

        TSharedCacheConfig config;
        config.SetMemoryLimit(MemoryLimit);
        config.SetAsyncQueueInFlyLimit(19); // 2 in-fly pages
        ActorId = Runtime.Register(CreateSharedPageCache(config, Runtime.GetDynamicCounters()));

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(options);

        Sender1 = Runtime.Register(new TExecutorMock(Results));
        Sender2 = Runtime.Register(new TExecutorMock(Results));
        BlockIoSender = Runtime.AllocateEdgeActor();

        Fetches = MakeHolder<TBlockEvents<NBlockIO::TEvFetch>>(Runtime);

        Counters = MakeHolder<TSharedPageCacheCounters>(GetServiceCounters(Runtime.GetDynamicCounters(), "tablets")->GetSubgroup("type", "S_CACHE"));
    }

    TSharedPageCacheMock& Request(TActorId sender, TIntrusiveConstPtr<TPageCollectionMock> collection, TVector<TPageId> pages, EPriority priority = EPriority::Fast) {
        auto request = new TEvRequest(priority, collection, pages, ++RequestId);
        Send(sender, request, RequestId);

        TWaitForFirstEvent<TEvRequest> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& Provide(TIntrusiveConstPtr<TPageCollectionMock> collection, TVector<TPageId> pages, ui64 eventCookie = NO_QUEUE_COOKIE) { // event cookie -> queue type
        auto data = new NBlockIO::TEvData(NKikimrProto::OK, collection, pages.size() * 10); // fetch cookie -> requested size
        for (auto pageId : pages) {
            data->Pages.push_back(TLoadedPage(pageId, TSharedData::Copy(TString(10, 'x'))));
        }
        Send(BlockIoSender, data, eventCookie);

        // TODO: why this broke everything?
        // TWaitForFirstEvent<NBlockIO::TEvData> waiter(Runtime);
        // waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& Attach(TActorId sender, TIntrusiveConstPtr<TPageCollectionMock> collection, ECacheMode cacheMode = ECacheMode::Regular) {
        auto attach = new TEvAttach(collection, cacheMode);
        Send(sender, attach);

        TWaitForFirstEvent<TEvAttach> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& Detach(TActorId sender, TIntrusiveConstPtr<TPageCollectionMock> collection) {
        auto detach = new TEvDetach(collection->Label());
        Send(sender, detach);

        TWaitForFirstEvent<TEvDetach> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& Unregister(TActorId sender) {
        auto unregister = new TEvUnregister();
        Send(sender, unregister);

        TWaitForFirstEvent<TEvUnregister> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& CheckFetches(const TVector<TFetch>& expected) {
        if (expected.empty()) {
            Runtime.SimulateSleep(TDuration::Seconds(1));
        } else {
            Runtime.WaitFor(TStringBuilder() << "fetches #" << RequestId, 
                [&]{return Fetches->size() >= expected.size();}, TDuration::Seconds(5));
        }
        
        TVector<TFetch> actual;
        for (auto& f : *Fetches) {
            auto &fetch = *f->Get();
            actual.push_back({fetch.Cookie, fetch.PageCollection, fetch.Pages});
        }
        Fetches->clear();
        
        Cerr << "Checking fetches#" << RequestId << Endl;
        CheckFetches(expected, actual);

        return *this;
    }

    THashMap<TPageId, TSharedPageRef> CheckResults(TVector<TFetch> expected, NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
        if (expected.empty()) {
            Runtime.SimulateSleep(TDuration::Seconds(1));
        } else {
            Runtime.WaitFor(TStringBuilder() << "results #" << RequestId, 
                [&]{return Results.size() >= expected.size();}, TDuration::Seconds(5));
        }
        
        TVector<TFetch> actual;
        THashMap<TPageId, TSharedPageRef> pages;
        for (auto& r : Results) {
            UNIT_ASSERT_VALUES_EQUAL(r->Get()->Status, status);
            auto& result = *r->Get();
            actual.push_back(TFetch{result.Cookie, result.PageCollection, {}});
            for (auto p : r->Get()->Pages) {
                actual.back().Pages.push_back(p.PageId);
                pages.emplace(p.PageId, p.Page);
            }
        }
        Results.clear();
        
        Cerr << "Checking results#" << RequestId << Endl;
        CheckFetches(expected, actual);

        return pages;
    }

    void CheckFetches(TVector<TFetch> expected, TVector<TFetch> actual) {
        // blocked results to different senders may be reordered, sort them before check:
        auto cmp = [](const auto& l, const auto& r){
            if (l.PageCollection->Label() != r.PageCollection->Label()) {
                return l.PageCollection->Label() < r.PageCollection->Label();
            }
            if (l.Cookie != r.Cookie) {
                return l.Cookie < r.Cookie;
            }
            return l.Pages < r.Pages;
        };
        Sort(expected, cmp);
        Sort(actual, cmp);

        Cerr << "Expected:" << Endl;
        for (auto f : expected) {
            Cerr << "  " << f.DebugString() << Endl;
        }
        Cerr << "Actual:" << Endl;
        for (auto f : actual) {
            Cerr << "  " << f.DebugString() << Endl;
        }

        UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
        for (auto i : xrange(expected.size())) {
            UNIT_ASSERT_VALUES_EQUAL(actual[i].PageCollection->Label(), expected[i].PageCollection->Label());
            UNIT_ASSERT_VALUES_EQUAL(actual[i].Pages, expected[i].Pages);
            UNIT_ASSERT_VALUES_EQUAL(actual[i].Cookie, expected[i].Cookie);
        }
    }

    void Send(TActorId sender, IEventBase* ev, ui64 cookie = 0) {
        Runtime.Send(new IEventHandle(ActorId, sender, ev, 0, cookie), 0, true);
    }

    TTestActorRuntime Runtime;
    TActorId ActorId;
    ui64 RequestId = 0;
    THolder<TSharedPageCacheCounters> Counters;

    THolder<TBlockEvents<NBlockIO::TEvFetch>> Fetches;
    std::deque<NSharedCache::TEvResult::TPtr> Results;

    TActorId Sender1;
    TActorId Sender2;
    TActorId BlockIoSender;
    TIntrusiveConstPtr<TPageCollectionMock> Collection1 = new TPageCollectionMock(1, 100);
    TIntrusiveConstPtr<TPageCollectionMock> Collection2 = new TPageCollectionMock(2, 100);
};

Y_UNIT_TEST_SUITE(TSharedPageCache_Actor) {

    Y_UNIT_TEST(Request_Basics) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
    }

    Y_UNIT_TEST(Request_Failed) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {4, 5});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {5, 6});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {6, 7});
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            TFetch{20, sharedCache.Collection2, {4, 5}},
            TFetch{20, sharedCache.Collection1, {5, 6}},
            TFetch{20, sharedCache.Collection2, {6, 7}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 9);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 9);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 4);

        auto data = new NBlockIO::TEvData(NKikimrProto::ERROR, sharedCache.Collection1, 30);
        data->Pages = {NPageCollection::TLoadedPage{1, {}}, NPageCollection::TLoadedPage{2, {}}, NPageCollection::TLoadedPage{3, {}}};
        sharedCache.Send(sharedCache.BlockIoSender, data, NO_QUEUE_COOKIE);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {}},
            TFetch{3, sharedCache.Collection1, {}}
        }, NKikimrProto::ERROR);

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 4); // TODO: should be 2?

        sharedCache.Provide(sharedCache.Collection1, {5, 6});
        sharedCache.CheckResults({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2); // TODO: should be 1?
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 4);

        sharedCache.Provide(sharedCache.Collection2, {6, 7});
        sharedCache.Provide(sharedCache.Collection2, {4, 5});
        sharedCache.CheckResults({
            TFetch{4, sharedCache.Collection2, {6, 7}},
            TFetch{2, sharedCache.Collection2, {4, 5}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 4);
    }

    Y_UNIT_TEST(Request_Queue) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3, 4, 5}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {1, 2, 3}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {1, 2}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 8);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {3, 4}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {3, 4}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            // TODO: shouldn't we finish with Collection1 page 5?
            TFetch{20, sharedCache.Collection2, {1, 2}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection2, {1, 2}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection2, {3}},
            TFetch{10, sharedCache.Collection1, {5}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection2, {3});
        sharedCache.Provide(sharedCache.Collection1, {5});
        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection2, {1, 2, 3}},
            TFetch{1, sharedCache.Collection1, {1, 2, 3, 4, 5}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 2);
    }

    Y_UNIT_TEST(Request_Queue_Failed) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {2}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {3}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {4}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {5}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {6}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {1}},
            TFetch{10, sharedCache.Collection1, {2}},
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 4);

        auto data = new NBlockIO::TEvData(NKikimrProto::ERROR, sharedCache.Collection1, 10);
        data->Pages = {NPageCollection::TLoadedPage{1, {}}};
        sharedCache.Send(sharedCache.BlockIoSender, data, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {}},
            TFetch{2, sharedCache.Collection1, {}},
            TFetch{3, sharedCache.Collection1, {}},
            TFetch{5, sharedCache.Collection1, {}},
        }, NKikimrProto::ERROR);
        sharedCache.CheckFetches({
            // page 2 is still in-fly
            TFetch{10, sharedCache.Collection2, {4}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 4); // TODO: should be 2

        sharedCache.Provide(sharedCache.Collection1, {2}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection2, {6}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2); // TODO: should be 1
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 4);

        sharedCache.Provide(sharedCache.Collection2, {6}, ASYNC_QUEUE_COOKIE);
        sharedCache.Provide(sharedCache.Collection2, {4}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({
            TFetch{6, sharedCache.Collection2, {6}},
            TFetch{4, sharedCache.Collection2, {4}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 4);
    }

    Y_UNIT_TEST(Request_Queue_Fast) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3, 4, 5}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {1, 2}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3, 4, 6}, EPriority::Fast);
        sharedCache.CheckFetches({
            TFetch{50, sharedCache.Collection1, {1, 2, 3, 4, 6}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 10);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3, 4, 6});
        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection1, {1, 2, 3, 4, 6}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {1, 2}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {5}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {5}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3, 4, 5}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
    }

    Y_UNIT_TEST(Request_Sequential) {
        TSharedPageCacheMock sharedCache;
        
        {
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
            sharedCache.CheckFetches({
                TFetch{30, sharedCache.Collection1, {1, 2, 3}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 3);
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

            sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
            sharedCache.CheckResults({
                TFetch{1, sharedCache.Collection1, {1, 2, 3}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        }

        {
            sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {1, 2});
            sharedCache.CheckFetches({
                TFetch{20, sharedCache.Collection2, {1, 2}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

            sharedCache.Provide(sharedCache.Collection2, {1, 2});
            sharedCache.CheckResults({
                TFetch{2, sharedCache.Collection2, {1, 2}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        }
    }

    Y_UNIT_TEST(Request_Cached) {
        TSharedPageCacheMock sharedCache;
        
        for (TPageId pageId : xrange(1, 8)) {
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {pageId});
            sharedCache.CheckFetches({
                TFetch{10, sharedCache.Collection1, {pageId}}
            });

            sharedCache.Provide(sharedCache.Collection1, {pageId});
            sharedCache.CheckResults({
                TFetch{pageId, sharedCache.Collection1, {pageId}}
            });
        }

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);

        {
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3, 4, 5, 6, 7});
            sharedCache.CheckFetches({
                TFetch{30, sharedCache.Collection1, {1, 2, 3}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 10);

            sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
            sharedCache.CheckResults({
                TFetch{8, sharedCache.Collection1, {1, 2, 3, 4, 5, 6, 7}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 7);
        }
    }

    Y_UNIT_TEST(Request_Different_Collections) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {1, 2});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            TFetch{20, sharedCache.Collection2, {1, 2}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.Provide(sharedCache.Collection2, {1, 2});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            TFetch{2, sharedCache.Collection2, {1, 2}}
        });
    }

    Y_UNIT_TEST(Request_Different_Pages) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {4, 5});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            TFetch{20, sharedCache.Collection1, {4, 5}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.Provide(sharedCache.Collection1, {4, 5});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            TFetch{2, sharedCache.Collection1, {4, 5}}
        });
    }

    Y_UNIT_TEST(Request_Different_Pages_Reversed) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {4, 5});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            TFetch{20, sharedCache.Collection1, {4, 5}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {4, 5});
        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});

        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection1, {4, 5}},
            TFetch{1, sharedCache.Collection1, {1, 2, 3}}
        });
    }

    Y_UNIT_TEST(Request_Subset) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1, 2});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            TFetch{2, sharedCache.Collection1, {1, 2}}
        });
    }

    Y_UNIT_TEST(Request_Subset_Shuffled) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {3, 1});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            TFetch{2, sharedCache.Collection1, {3, 1}}
        });
    }

    Y_UNIT_TEST(Request_Superset) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1, 2, 3, 4});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            TFetch{10, sharedCache.Collection1, {4}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.Provide(sharedCache.Collection1, {4});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            TFetch{2, sharedCache.Collection1, {1, 2, 3, 4}}
        });
    }

    Y_UNIT_TEST(Request_Superset_Reversed) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1, 2, 3, 4});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            TFetch{10, sharedCache.Collection1, {4}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {4});
        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            TFetch{2, sharedCache.Collection1, {1, 2, 3, 4}}
        });
    }

    Y_UNIT_TEST(Request_Crossing) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {3, 4});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            TFetch{10, sharedCache.Collection1, {4}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.Provide(sharedCache.Collection1, {4});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            TFetch{2, sharedCache.Collection1, {3, 4}}
        });
    }

    Y_UNIT_TEST(Request_Crossing_Reversed) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {3, 4});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            TFetch{10, sharedCache.Collection1, {4}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {4});
        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            TFetch{2, sharedCache.Collection1, {3, 4}}
        });
    }

    Y_UNIT_TEST(Request_Crossing_Shuffled) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {4, 3});

        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            TFetch{10, sharedCache.Collection1, {4}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.Provide(sharedCache.Collection1, {4});

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            TFetch{2, sharedCache.Collection1, {4, 3}}
        });
    }

    Y_UNIT_TEST(Attach_Basics) {
        TSharedPageCacheMock sharedCache;
        
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        // call again
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);

        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 4);
    }

    Y_UNIT_TEST(Attach_Request) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);
    }

    Y_UNIT_TEST(Detach_Basics) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection2);
        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);

        // call again
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Detach(sharedCache.Sender2, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);
    }

    Y_UNIT_TEST(Detach_Cached) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}}
        });
        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);
    }

    Y_UNIT_TEST(Detach_Expired) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}}
        });
        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}}
        });
        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);

        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {1});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection2, {1}}
        });
        sharedCache.Provide(sharedCache.Collection2, {1});
        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection2, {1}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {2, 3, 4, 5, 6});
        sharedCache.CheckFetches({
            TFetch{50, sharedCache.Collection2, {2, 3, 4, 5, 6}}
        });
        sharedCache.Provide(sharedCache.Collection2, {2, 3, 4, 5, 6});
        sharedCache.CheckResults({
            TFetch{3, sharedCache.Collection2, {2, 3, 4, 5, 6}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);
    }

    Y_UNIT_TEST(Detach_InFly) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1});
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {2});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1, 3});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {1}},
            TFetch{10, sharedCache.Collection2, {2}},
            TFetch{10, sharedCache.Collection1, {3}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {}}
        }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1});
        sharedCache.CheckResults({
            TFetch{3, sharedCache.Collection1, {1}}
        });
        sharedCache.Provide(sharedCache.Collection1, {3});
        sharedCache.CheckResults({
            TFetch{4, sharedCache.Collection1, {1, 3}}
        });
        sharedCache.Provide(sharedCache.Collection2, {2});
        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection2, {2}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);
    }

    Y_UNIT_TEST(Detach_Queued) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3, 4, 5}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {6, 7}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {10, 11, 12}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1, 5, 9, 10}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {1, 2}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {}},
            TFetch{2, sharedCache.Collection1, {}}
        }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {5, 9}}
        });
        sharedCache.Provide(sharedCache.Collection1, {5, 9}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {10}},
            TFetch{10, sharedCache.Collection2, {10}},
        });
        sharedCache.Provide(sharedCache.Collection1, {10}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({
            TFetch{4, sharedCache.Collection1, {1, 5, 9, 10}}
        });
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection2, {11}}
        });

        sharedCache.Provide(sharedCache.Collection2, {10}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection2, {12}}
        });
        sharedCache.Provide(sharedCache.Collection2, {11}, ASYNC_QUEUE_COOKIE);
        sharedCache.Provide(sharedCache.Collection2, {12}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({
            TFetch{3, sharedCache.Collection2, {10, 11, 12}},
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);
    }

    Y_UNIT_TEST(Unregister_Basics) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1);
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection2);
        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Unregister(sharedCache.Sender1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        // call again
        sharedCache.Unregister(sharedCache.Sender1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Unregister(sharedCache.Sender2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);
    }

    Y_UNIT_TEST(Unregister_Cached) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}}
        });
        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Unregister(sharedCache.Sender1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);
    }

    Y_UNIT_TEST(Unregister_Expired) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {1, 2, 3}}
        });
        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}}
        });
        sharedCache.Unregister(sharedCache.Sender1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);

        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {1});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection2, {1}}
        });
        sharedCache.Provide(sharedCache.Collection2, {1});
        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection2, {1}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {2, 3, 4, 5, 6});
        sharedCache.CheckFetches({
            TFetch{50, sharedCache.Collection2, {2, 3, 4, 5, 6}}
        });
        sharedCache.Provide(sharedCache.Collection2, {2, 3, 4, 5, 6});
        sharedCache.CheckResults({
            TFetch{3, sharedCache.Collection2, {2, 3, 4, 5, 6}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);
    }

    Y_UNIT_TEST(Unregister_InFly) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1});
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {2});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1, 3});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {1}},
            TFetch{10, sharedCache.Collection2, {2}},
            TFetch{10, sharedCache.Collection1, {3}},
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Unregister(sharedCache.Sender1);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {}},
            TFetch{2, sharedCache.Collection2, {}}
        }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {1});
        sharedCache.Provide(sharedCache.Collection2, {2});
        sharedCache.CheckResults({
            TFetch{3, sharedCache.Collection1, {1}}
        });
        sharedCache.Provide(sharedCache.Collection1, {3});
        sharedCache.CheckResults({
            TFetch{4, sharedCache.Collection1, {1, 3}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);
    }

    Y_UNIT_TEST(Unregister_Queued) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3, 4, 5}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {6, 7}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {10, 11, 12}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1, 5, 9, 10}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {1, 2}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 3);

        sharedCache.Unregister(sharedCache.Sender1);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {}},
            TFetch{2, sharedCache.Collection1, {}},
            TFetch{3, sharedCache.Collection2, {}}
        }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {1, 2}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {5, 9}}
        });
        sharedCache.Provide(sharedCache.Collection1, {5, 9}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {10}},
        });
        sharedCache.Provide(sharedCache.Collection1, {10}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({
            TFetch{4, sharedCache.Collection1, {1, 5, 9, 10}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);
    }

    Y_UNIT_TEST(Unregister_Queued_Pending) {
        TSharedPageCacheMock sharedCache;

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {10, 11}, EPriority::Bkgr);
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {12}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {1}},
            TFetch{10, sharedCache.Collection2, {10}}
        });
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 2);

        sharedCache.Unregister(sharedCache.Sender2);
        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection2, {}},
            TFetch{3, sharedCache.Collection2, {}}
        }, NKikimrProto::RACE);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {1}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1}},
        });
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Provide(sharedCache.Collection2, {10}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->FailedRequests->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);
    }

    Y_UNIT_TEST(InMemory_Basics) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * (104 + 10);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {0, 1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Provide(sharedCache.Collection1, {0, 1, 2, 3});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);

        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_Preemption) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * (104 + 10);

        // request not in-memory page
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {1});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection2, {1}},
        });
        sharedCache.Provide(sharedCache.Collection2, {1});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection2, {1}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        // load in-memory collection
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {0, 1, 2, 3}}
        });
        sharedCache.Provide(sharedCache.Collection1, {0, 1, 2, 3});
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);
        
        // not in-memory page should be loaded again
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {1});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection2, {1}},
        });
        sharedCache.Provide(sharedCache.Collection2, {1});
        sharedCache.CheckResults({
            TFetch{2, sharedCache.Collection2, {1}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 2);
        // in-fly pages can preempt in-memory pages
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 3);
    }

    Y_UNIT_TEST(InMemory_NotEnoughMemory) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 6u);
        ui64 collection1TotalSize = 6 * (104 + 10);

        // only 4 pages should be in memory cache
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{60, sharedCache.Collection1, {0, 1, 2, 3, 4, 5}}
        });
        sharedCache.Provide(sharedCache.Collection1, {0, 1, 2, 3, 4, 5});
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1, 2, 3, 4, 5});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 2);
    }

    Y_UNIT_TEST(InMemory_Enabling) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * (104 + 10);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {2});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {2}}
        });
        sharedCache.Provide(sharedCache.Collection1, {2});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {2}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{30, sharedCache.Collection1, {0, 1, 3}}
        });
        sharedCache.Provide(sharedCache.Collection1, {0, 1, 3});
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 1);
    }

    Y_UNIT_TEST(InMemory_Enabling_AllRequested) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * (104 + 10);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1, 2, 3});
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {0, 1, 2, 3}}
        });
        sharedCache.Provide(sharedCache.Collection1, {0, 1, 2, 3});
        sharedCache.CheckResults({
            TFetch{1, sharedCache.Collection1, {0, 1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 4);
    }

    Y_UNIT_TEST(InMemory_Disabling) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * (104 + 10);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {0, 1, 2, 3}}
        });
        sharedCache.Provide(sharedCache.Collection1, {0, 1, 2, 3});
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular);

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_Detach) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * (104 + 10);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {0, 1, 2, 3}}
        });
        sharedCache.Provide(sharedCache.Collection1, {0, 1, 2, 3});
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Detach(sharedCache.Sender1, sharedCache.Collection1);

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Detach(sharedCache.Sender2, sharedCache.Collection1);

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_Unregister) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 4u);
        ui64 collection1TotalSize = 4 * (104 + 10);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection1, {0, 1, 2, 3}}
        });
        sharedCache.Provide(sharedCache.Collection1, {0, 1, 2, 3});
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Attach(sharedCache.Sender2, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Unregister(sharedCache.Sender1);

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Unregister(sharedCache.Sender2);

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActiveLimitBytes->Val(), MemoryLimit);

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1, 2, 3});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_MoveEvictedToInMemory) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 2u);
        ui64 fetchNo = 1;
        ui64 cacheHits = 0;

        // request and hold collection#1 page refs
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1});
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {0, 1}}
        });
        sharedCache.Provide(sharedCache.Collection1, {0, 1});
        auto collection1Pages = sharedCache.CheckResults({
            TFetch{fetchNo++, sharedCache.Collection1, {0, 1}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), cacheHits);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 2);

        auto ensurePageInCache = [&](auto pageId) {
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {pageId});
            sharedCache.CheckFetches({
                TFetch{10, sharedCache.Collection2, {pageId}}
            });
            sharedCache.Provide(sharedCache.Collection2, {pageId});
            sharedCache.CheckResults({
                TFetch{fetchNo++, sharedCache.Collection2, {pageId}}
            });

            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection2, {pageId});
            sharedCache.CheckFetches({});
            sharedCache.CheckResults({
                TFetch{fetchNo++, sharedCache.Collection2, {pageId}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), ++cacheHits);
        };

        // request 4 pages from collection#2 to preempt collection#1 pages
        for (TPageId pageId : xrange(4)) {
            ensurePageInCache(pageId);
        }

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 2); // collection#1
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), cacheHits);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 6);

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({}); // all collection#1 pages already loaded
        collection1Pages.clear(); // release refs and allow collection#1 pages eviction

        // request next 4 pages from collection#2 to try preempt collection#1 pages 
        for (TPageId pageId : xrange(4, 8)) {
            ensurePageInCache(pageId);
        }

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1});
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), cacheHits += 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
    }

    Y_UNIT_TEST(InMemory_MoveEvictedToRegular) {
        TSharedPageCacheMock sharedCache;
        sharedCache.Collection1 = MakeIntrusiveConst<TPageCollectionMock>(1ul, 2u);
        ui64 collection1TotalSize = 2 * (104 + 10);

        sharedCache.Collection2 = MakeIntrusiveConst<TPageCollectionMock>(2ul, 4u);
        ui64 collection2TotalSize = 4 * (104 + 10);

        ui64 fetchNo = 1;
        ui64 cacheHits = 0;

        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{20, sharedCache.Collection1, {0, 1}}
        });
        sharedCache.Provide(sharedCache.Collection1, {0, 1});
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0, 1});
        sharedCache.CheckFetches({});
        auto collection1Pages = sharedCache.CheckResults({
            TFetch{fetchNo++, sharedCache.Collection1, {0, 1}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), cacheHits += 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize);

        // after loading collection#2 to InMemory, all collection#1 pages should be evicted
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection2, ECacheMode::TryKeepInMemory);
        sharedCache.CheckFetches({
            TFetch{40, sharedCache.Collection2, {0, 1, 2, 3}}
        });
        sharedCache.Provide(sharedCache.Collection2, {0, 1, 2, 3});
        sharedCache.CheckResults({});
        sharedCache.CheckFetches({});

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection1TotalSize + collection2TotalSize);

        // all evicted collection#1 pages sould be moved to Regular
        sharedCache.Attach(sharedCache.Sender1, sharedCache.Collection1, ECacheMode::Regular);
        sharedCache.CheckFetches({});

        collection1Pages.clear();

        // collection#1 page#0 should be preserved in PassivePages, collection#1 page#1 should be GC'ed
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {0});
        sharedCache.CheckFetches({});
        sharedCache.CheckResults({
            TFetch{fetchNo++, sharedCache.Collection1, {0}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), ++cacheHits);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->TryKeepInMemoryBytes->Val(), collection2TotalSize);

        // collection#1 page#1 should be loaded again
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1});
        sharedCache.CheckFetches({
            TFetch{10, sharedCache.Collection1, {1}}
        });
        sharedCache.Provide(sharedCache.Collection1, {1});
        sharedCache.CheckResults({
            TFetch{fetchNo++, sharedCache.Collection1, {1}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheHitPages->Val(), cacheHits);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 1);
    }
}
}
