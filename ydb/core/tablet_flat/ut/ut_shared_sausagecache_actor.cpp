#include <shared_cache_events.h>
#include <shared_sausagecache.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSharedCache {
using namespace NActors;
using namespace NTabletFlatExecutor;

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
        ActorId = Runtime.Register(CreateSharedPageCache(config, Runtime.GetDynamicCounters()));

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(options);

        Sender1 = Runtime.AllocateEdgeActor();
        Sender2 = Runtime.AllocateEdgeActor();

        fetches = MakeHolder<TBlockEvents<NBlockIO::TEvFetch>>(Runtime);
    }

    TSharedPageCacheMock& Request(TActorId sender, TIntrusiveConstPtr<TPageCollectionMock> collection, TVector<TPageId> pages) {
        auto fetch = new NPageCollection::TFetch(++RequestId, collection, pages);
        auto request = new TEvRequest(EPriority::Fast, fetch);
        Send(sender, request);
        return *this;
    }

    TSharedPageCacheMock& CheckFetches(const TVector<NPageCollection::TFetch>& expected) {
        TVector<NPageCollection::TFetch> actual;
        for (auto& f : *fetches) {
            actual.push_back(*f->Get()->Fetch);
        }
        fetches->clear();

        Cerr << "Checking " << RequestId << " fetches" << Endl;
        Cerr << "Expected:" << Endl;
        for (auto f : expected) {
            Cerr << "  " << f.DebugString() << Endl;
        }
        Cerr << "Actual:" << Endl;
        for (auto f : actual) {
            Cerr << "  " << f.DebugString() << Endl;
        }

        

        return *this;
    } 

    void Send(TActorId sender, IEventBase* ev) {
        Runtime.Send(new IEventHandle(ActorId, sender, ev), 0, true);
    }

    TTestActorRuntime Runtime;
    TActorId ActorId;
    ui64 RequestId = 2;

    THolder<TBlockEvents<NBlockIO::TEvFetch>> fetches;

    TActorId Sender1;
    TActorId Sender2;
    TIntrusiveConstPtr<TPageCollectionMock> Collection1 = new TPageCollectionMock(1);
    TIntrusiveConstPtr<TPageCollectionMock> Collection2 = new TPageCollectionMock(2);
};

Y_UNIT_TEST_SUITE(TSharedPageCache_Actor) {

    Y_UNIT_TEST(Basics) {
        TSharedPageCacheMock sharedCache;
        
        sharedCache.Request(sharedCache.Sender1, sharedCache.Collection1, {1, 2, 3});
        sharedCache.Runtime.SimulateSleep(TDuration::Seconds(1));
        sharedCache.CheckFetches({
            NPageCollection::TFetch{1, sharedCache.Collection1, {1, 2, 3}}
        });
    }

}
}
