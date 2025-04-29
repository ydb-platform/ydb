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

static const ui64 ASYNC_QUEUE_COOKIE = 1;

struct TPageCollectionMock : public NPageCollection::IPageCollection {
    TPageCollectionMock(ui64 id)
        : Id(1, 1, id)
    {}

    const TLogoBlobID& Label() const noexcept override {
        return Id;
    }

    ui32 Total() const noexcept override {
        return 100;
    }

    NPageCollection::TInfo Page(ui32 page) const override {
        Y_UNUSED(page);
        return { 10, ui32(NTable::NPage::EPage::Undef) };
    }

    NPageCollection::TBorder Bounds(ui32) const override {
        Y_TABLET_ERROR("Unexpected Bounds(...) call");
    }

    NPageCollection::TGlobId Glob(ui32) const override {
        Y_TABLET_ERROR("Unexpected Glob(...) call");
    }

    bool Verify(ui32, TArrayRef<const char>) const override {
        Y_TABLET_ERROR("Unexpected Verify(...) call");
    }

    size_t BackingSize() const noexcept override {
        return 1000;
    }

private:
    TLogoBlobID Id;
};

struct TSharedPageCacheMock {
    TSharedPageCacheMock() {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NLog::PRI_TRACE);

        TSharedCacheConfig config;
        config.SetMemoryLimit(4 * (104 + 10)); // sizeof(TPage) = 104
        config.SetAsyncQueueInFlyLimit(19); // 2 in-fly pages
        ActorId = Runtime.Register(CreateSharedPageCache(config, Runtime.GetDynamicCounters()));

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(options);

        Sender1 = Runtime.AllocateEdgeActor();
        Sender2 = Runtime.AllocateEdgeActor();
        BlockIoSender = Runtime.AllocateEdgeActor();

        Fetches = MakeHolder<TBlockEvents<NBlockIO::TEvFetch>>(Runtime);
        Results = MakeHolder<TBlockEvents<NSharedCache::TEvResult>>(Runtime);

        Counters = MakeHolder<TSharedPageCacheCounters>(GetServiceCounters(Runtime.GetDynamicCounters(), "tablets")->GetSubgroup("type", "S_CACHE"));
    }

    TSharedPageCacheMock& Request(TActorId sender, TIntrusiveConstPtr<TPageCollectionMock> collection, TVector<TPageId> pages, EPriority priority = EPriority::Fast) {
        auto fetch = new NPageCollection::TFetch(++RequestId, collection, pages);
        auto request = new TEvRequest(priority, fetch);
        Send(sender, request);

        TWaitForFirstEvent<TEvRequest> waiter(Runtime);
        waiter.Wait();

        return *this;
    }

    TSharedPageCacheMock& Provide(TIntrusiveConstPtr<TPageCollectionMock> collection, TVector<TPageId> pages, ui64 eventCookie = 0) { // event cookie -> queue type
        auto fetch = new NPageCollection::TFetch(pages.size() * 10, collection, pages); // fetch cookie -> requested size
        auto data = new NBlockIO::TEvData(fetch, NKikimrProto::OK);
        for (auto pageId : pages) {
            data->Blocks.push_back(NPageCollection::TLoadedPage(pageId, TSharedData::Copy(TString(10, 'x'))));
        }
        Send(BlockIoSender, data, eventCookie);

        return *this;
    }

    TSharedPageCacheMock& Attach(TActorId sender, TIntrusiveConstPtr<TPageCollectionMock> collection) {
        auto attach = new TEvAttach(collection);
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

    TSharedPageCacheMock& CheckFetches(const TVector<NPageCollection::TFetch>& expected) {
        Runtime.WaitFor(TStringBuilder() << "fetches #" << RequestId, 
            [&]{return Fetches->size() >= expected.size();}, TDuration::Seconds(5));
        
        TVector<NPageCollection::TFetch> actual;
        for (auto& f : *Fetches) {
            auto &fetch = *f->Get();
            actual.push_back(*fetch.Fetch);
        }
        Fetches->clear();
        
        CheckFetches(expected, actual);

        return *this;
    }

    TSharedPageCacheMock& CheckResults(const TVector<NPageCollection::TFetch>& expected) {
        Runtime.WaitFor(TStringBuilder() << "results #" << RequestId, 
            [&]{return Results->size() >= expected.size();}, TDuration::Seconds(5));
        
        TVector<NPageCollection::TFetch> actual;
        for (auto& r : *Results) {
            auto& result = *r->Get();
            actual.push_back(NPageCollection::TFetch{result.Cookie, result.PageCollection, {}});
            for (auto p : r->Get()->Pages) {
                actual.back().Pages.push_back(p.PageId);
            }
        }
        Results->clear();
        
        Cerr << "Checking results#" << RequestId << Endl;
        CheckFetches(expected, actual);

        return *this;
    }

    void CheckFetches(const TVector<NPageCollection::TFetch>& expected, const TVector<NPageCollection::TFetch>& actual) {
        Cerr << "Expected:" << Endl;
        for (auto f : expected) {
            Cerr << "  " << f.DebugString(true) << Endl;
        }
        Cerr << "Actual:" << Endl;
        for (auto f : actual) {
            Cerr << "  " << f.DebugString(true) << Endl;
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
    THolder<TBlockEvents<NSharedCache::TEvResult>> Results;

    TActorId Sender1;
    TActorId Sender2;
    TActorId BlockIoSender;
    TIntrusiveConstPtr<TPageCollectionMock> Collection1 = new TPageCollectionMock(1);
    TIntrusiveConstPtr<TPageCollectionMock> Collection2 = new TPageCollectionMock(2);
};

Y_UNIT_TEST_SUITE(TSharedPageCache_Actor) {

    Y_UNIT_TEST(Request_Basics) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckFetches({
            NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.CheckResults({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
    }

    Y_UNIT_TEST(Request_Queue) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3, 4, 5}, EPriority::Bkgr);
        sharedCache.CheckFetches({
            NPageCollection::TFetch{20, sharedCache.Collection1, {1, 2}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {1, 2}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            NPageCollection::TFetch{20, sharedCache.Collection1, {3, 4}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {3, 4}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckFetches({
            NPageCollection::TFetch{10, sharedCache.Collection1, {5}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

        sharedCache.Provide(sharedCache.Collection1, {5}, ASYNC_QUEUE_COOKIE);
        sharedCache.CheckResults({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3, 4, 5}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->LoadInFlyPages->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->SucceedRequests->Val(), 1);
    }

    Y_UNIT_TEST(Request_Sequential) {
        TSharedPageCacheMock sharedCache;
        
        {
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
            sharedCache.CheckFetches({
                NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 3);
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

            sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
            sharedCache.CheckResults({
                NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        }

        {
            sharedCache.Request(sharedCache.Sender2, sharedCache.Collection2, {1, 2});
            sharedCache.CheckFetches({
                NPageCollection::TFetch{20, sharedCache.Collection2, {1, 2}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 1);

            sharedCache.Provide(sharedCache.Collection2, {1, 2});
            sharedCache.CheckResults({
                NPageCollection::TFetch{2, sharedCache.Collection2, {1, 2}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 0);
        }
    }

    Y_UNIT_TEST(Request_Cached) {
        TSharedPageCacheMock sharedCache;
        
        for (TPageId pageId : xrange(1, 8)) {
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {pageId});
            sharedCache.CheckFetches({
                NPageCollection::TFetch{10, sharedCache.Collection1, {pageId}}
            });

            sharedCache.Provide(sharedCache.Collection1, {pageId});
            sharedCache.CheckResults({
                NPageCollection::TFetch{pageId, sharedCache.Collection1, {pageId}}
            });
        }

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->ActivePages->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PassivePages->Val(), 0);

        {
            sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3, 4, 5, 6, 7});
            sharedCache.CheckFetches({
                NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}}
            });

            UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 10);

            sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
            sharedCache.CheckResults({
                NPageCollection::TFetch{8, sharedCache.Collection1, {1, 2, 3, 4, 5, 6, 7}}
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
            NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{20, sharedCache.Collection2, {1, 2}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.Provide(sharedCache.Collection2, {1, 2});

        sharedCache.CheckResults({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{2, sharedCache.Collection2, {1, 2}}
        });
    }

    Y_UNIT_TEST(Request_Different_Pages) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {4, 5});

        sharedCache.CheckFetches({
            NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{20, sharedCache.Collection1, {4, 5}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.Provide(sharedCache.Collection1, {4, 5});

        sharedCache.CheckResults({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{2, sharedCache.Collection1, {4, 5}}
        });
    }

    Y_UNIT_TEST(Request_Different_Pages_Reversed) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {4, 5});

        sharedCache.CheckFetches({
            NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{20, sharedCache.Collection1, {4, 5}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {4, 5});
        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});

        sharedCache.CheckResults({
            NPageCollection::TFetch{2, sharedCache.Collection1, {4, 5}},
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}}
        });
    }

    Y_UNIT_TEST(Request_Subset) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1, 2});

        sharedCache.CheckFetches({
            NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});

        sharedCache.CheckResults({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{2, sharedCache.Collection1, {1, 2}}
        });
    }

    Y_UNIT_TEST(Request_Subset_Shuffled) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {3, 1});

        sharedCache.CheckFetches({
            NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});

        sharedCache.CheckResults({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{2, sharedCache.Collection1, {3, 1}}
        });
    }

    Y_UNIT_TEST(Request_Superset) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1, 2, 3, 4});

        sharedCache.CheckFetches({
            NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{10, sharedCache.Collection1, {4}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.Provide(sharedCache.Collection1, {4});

        sharedCache.CheckResults({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{2, sharedCache.Collection1, {1, 2, 3, 4}}
        });
    }

    Y_UNIT_TEST(Request_Superset_Reversed) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {1, 2, 3, 4});

        sharedCache.CheckFetches({
            NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{10, sharedCache.Collection1, {4}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {4});
        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});

        sharedCache.CheckResults({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{2, sharedCache.Collection1, {1, 2, 3, 4}}
        });
    }

    Y_UNIT_TEST(Request_Crossing) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {3, 4});

        sharedCache.CheckFetches({
            NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{10, sharedCache.Collection1, {4}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.Provide(sharedCache.Collection1, {4});

        sharedCache.CheckResults({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{2, sharedCache.Collection1, {3, 4}}
        });
    }

    Y_UNIT_TEST(Request_Crossing_Reversed) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {3, 4});

        sharedCache.CheckFetches({
            NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{10, sharedCache.Collection1, {4}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {4});
        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});

        sharedCache.CheckResults({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{2, sharedCache.Collection1, {3, 4}}
        });
    }

    Y_UNIT_TEST(Request_Crossing_Shuffled) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Request(sharedCache.Sender2, sharedCache.Collection1, {4, 3});

        sharedCache.CheckFetches({
            NPageCollection::TFetch{30, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{10, sharedCache.Collection1, {4}}
        });

        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->CacheMissPages->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PendingRequests->Val(), 2);

        sharedCache.Provide(sharedCache.Collection1, {1, 2, 3});
        sharedCache.Provide(sharedCache.Collection1, {4});

        sharedCache.CheckResults({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}},
            NPageCollection::TFetch{2, sharedCache.Collection1, {4, 3}}
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
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 1);

        sharedCache.Detach(sharedCache.Sender2, sharedCache.Collection2);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollections->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->Owners->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sharedCache.Counters->PageCollectionOwners->Val(), 0);
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

}
}
