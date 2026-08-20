#include "sysview_service.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace NKikimr {
namespace NSysView {

namespace {

constexpr ui64 ProcessorTabletId = 100500;
const TString Database = "/Root/db1";

// TDetailedTableCounters carries no path id - TablePath is the key (see the
// sys_view.proto message, whose OwnerId/PathId fields were dropped in favour of it).
const TString TablePath = "/Root/db1/Table";

// Records every generation it was asked to Pack() and emits one canned
// TDetailedTableCounters entry, so a test can tell which tick produced
// which message without inspecting the real per-table aggregator.
class TStubDetailedCounters : public IDbDetailedCounters {
public:
    TVector<ui64> PackedGenerations;

    void Pack(NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& out, ui64 generation) override {
        PackedGenerations.push_back(generation);

        auto* table = out.Add();
        table->SetTablePath(TablePath);
        table->SetLevel(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable);
    }
};

struct TServiceIds {
    TActorId ServiceId;
    TActorId PipeCacheEdge;
};

// A real fake scheme cache actor (as opposed to an observer): both the service's
// own RequestProcessorId() and the db watcher's TEvWatchPathId land here.
// Replies to every navigate with a valid, non-empty domain key and the
// sys view processor tablet id baked in.
class TFakeSchemeCache : public TActorBootstrapped<TFakeSchemeCache> {
public:
    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
            IgnoreFunc(TEvTxProxySchemeCache::TEvWatchPathId);
        }
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
        THolder<NSchemeCache::TSchemeCacheNavigate> request(ev->Get()->Request.Release());

        if (request->ResultSet.size() == 1) {
            auto& entry = request->ResultSet.back();
            entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
            const TPathId domainKey(72057594046644480ull, 1);
            auto domainInfo = MakeIntrusive<NSchemeCache::TDomainInfo>(domainKey, domainKey);
            domainInfo->Params.SetSysViewProcessor(ProcessorTabletId);
            entry.DomainInfo = domainInfo;
        }

        Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(request.Release()));
    }
};

// Starts the service with the detailed metrics tick enabled (EnableDbCounters is
// deliberately left off, to prove the detailed stream does not need it), and
// wires up the two well-known service ids the send path talks to:
// - MakeSchemeCacheID(): a real fake actor, for both the service's own
//   RequestProcessorId() and the db watcher's navigate.
// - MakePipePerNodeCacheID(false): a plain edge actor, so TEvForward envelopes
//   (carrying TEvSendDbCountersRequest with DetailedCounters) can be grabbed directly.
TServiceIds SetupService(TTestBasicRuntime& runtime) {
    runtime.Initialize(TAppPrepare().Unwrap());
    // The service computes its first tick deadline from the WALL clock
    // (TInstant::Now() in Bootstrap), while the test runtime's virtual clock
    // starts at 0 — line them up so the deadline lands ~5s ahead, not decades.
    runtime.UpdateCurrentTime(TInstant::Now());
    runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);
    // Only the detailed counters tick is wanted here. The query stats interval
    // timer reschedules itself at the same instant and floods the run once the
    // actor is on the schedule white list.
    runtime.GetAppData().FeatureFlags.SetEnablePersistentQueryStats(false);

    TActorId schemeCacheId = runtime.Register(new TFakeSchemeCache());
    runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId);

    TActorId pipeCacheEdge = runtime.AllocateEdgeActor();
    runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCacheEdge);

    TActorId serviceId = runtime.Register(CreateSysViewServiceForTests().Release());
    runtime.RegisterService(MakeSysViewServiceID(runtime.GetNodeId(0)), serviceId);
    // The service sends the detailed counters off its own Schedule() tick, and the
    // test runtime drops scheduled events for actors that are not white-listed
    // (ydb/core/testlib/basics/services.cpp:510 does the same for this very actor).
    runtime.EnableScheduleForActor(serviceId);
    runtime.SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    return {serviceId, pipeCacheEdge};
}

TIntrusivePtr<TStubDetailedCounters> RegisterStream(TTestBasicRuntime& runtime, const TActorId& serviceId,
    NKikimrSysView::EDbCountersService service)
{
    auto stub = MakeIntrusive<TStubDetailedCounters>();
    auto ev = MakeHolder<TEvSysView::TEvRegisterDbDetailedCounters>(Database, service, stub);
    runtime.Send(new IEventHandle(serviceId, runtime.AllocateEdgeActor(), ev.Release()), 0, true);
    return stub;
}

NKikimrSysView::TEvSendDbCountersRequest GrabRequest(TTestBasicRuntime& runtime, const TActorId& pipeCacheEdge) {
    auto ev = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCacheEdge, TDuration::Seconds(30));
    UNIT_ASSERT(ev);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TabletId, ProcessorTabletId);
    auto* req = static_cast<TEvSysView::TEvSendDbCountersRequest*>(ev->Get()->Ev.Get());
    return req->Record;
}

void SendAck(TTestBasicRuntime& runtime, const TActorId& serviceId, ui64 generation)
{
    auto ack = MakeHolder<TEvSysView::TEvSendDbCountersResponse>();
    ack->Record.SetDatabase(Database);
    ack->Record.SetGeneration(generation);
    runtime.Send(new IEventHandle(serviceId, runtime.AllocateEdgeActor(), ack.Release()), 0, true);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(SysViewServiceDetailedCounters) {

    Y_UNIT_TEST(BothRolesRideOneMessage) {
        TTestBasicRuntime runtime(1);
        auto [serviceId, pipeCacheEdge] = SetupService(runtime);

        auto leaderStub = RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS);
        auto followerStub = RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS_FOLLOWERS);

        auto req = GrabRequest(runtime, pipeCacheEdge);

        UNIT_ASSERT_VALUES_EQUAL(req.GetNodeId(), runtime.GetNodeId(0));
        UNIT_ASSERT_VALUES_EQUAL(req.DetailedCountersSize(), 2);

        THashSet<int> services;
        for (const auto& detailed : req.GetDetailedCounters()) {
            services.insert(detailed.GetService());
            UNIT_ASSERT_VALUES_EQUAL(detailed.TablesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(detailed.GetTables(0).GetTablePath(), TablePath);
        }

        UNIT_ASSERT_VALUES_EQUAL(services.size(), 2u);
        UNIT_ASSERT(services.contains(NKikimrSysView::TABLETS));
        UNIT_ASSERT(services.contains(NKikimrSysView::TABLETS_FOLLOWERS));
    }

    Y_UNIT_TEST(GenerationAdvancesOnlyOnAck) {
        TTestBasicRuntime runtime(1);
        auto [serviceId, pipeCacheEdge] = SetupService(runtime);

        auto leaderStub = RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS);
        auto followerStub = RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS_FOLLOWERS);

        auto req1 = GrabRequest(runtime, pipeCacheEdge);
        auto gen1 = req1.GetGeneration();

        // No ack yet - next tick should retry with same generation
        auto req2 = GrabRequest(runtime, pipeCacheEdge);
        UNIT_ASSERT_VALUES_EQUAL(req2.GetGeneration(), gen1);

        // Now send ack - should advance generation
        SendAck(runtime, serviceId, gen1);

        auto req3 = GrabRequest(runtime, pipeCacheEdge);
        auto gen3 = req3.GetGeneration();
        UNIT_ASSERT_VALUES_EQUAL(gen3, gen1 + 1);

        // Verify that both stubs were packed with the new generation
        UNIT_ASSERT(leaderStub->PackedGenerations.size() >= 2);
        UNIT_ASSERT(followerStub->PackedGenerations.size() >= 2);
        // The first packing was with gen1, the second (in req3) should be with gen3
        UNIT_ASSERT_VALUES_EQUAL(leaderStub->PackedGenerations.back(), gen3);
        UNIT_ASSERT_VALUES_EQUAL(followerStub->PackedGenerations.back(), gen3);
    }

    Y_UNIT_TEST(UnackedRetryResendsSameGeneration) {
        TTestBasicRuntime runtime(1);
        auto [serviceId, pipeCacheEdge] = SetupService(runtime);

        auto stub = RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS);

        auto req1 = GrabRequest(runtime, pipeCacheEdge);
        auto req2 = GrabRequest(runtime, pipeCacheEdge);

        UNIT_ASSERT_VALUES_EQUAL(req1.GetGeneration(), req2.GetGeneration());
        // Both packing attempts should have happened with the same generation. Not an
        // equality on the count: the runtime may have dispatched a further unacked tick
        // before the assert runs, and every one of those packs the same generation too.
        UNIT_ASSERT(stub->PackedGenerations.size() >= 2);
        UNIT_ASSERT_VALUES_EQUAL(stub->PackedGenerations[0], stub->PackedGenerations[1]);
        UNIT_ASSERT_VALUES_EQUAL(stub->PackedGenerations.back(), req1.GetGeneration());
    }

    Y_UNIT_TEST(StaleAckIgnored) {
        TTestBasicRuntime runtime(1);
        auto [serviceId, pipeCacheEdge] = SetupService(runtime);

        RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS);

        auto req1 = GrabRequest(runtime, pipeCacheEdge);
        auto gen1 = req1.GetGeneration();

        // Send ack for an old generation (generation before current)
        SendAck(runtime, serviceId, gen1 - 1);

        // Next tick should retry with same generation (ack was ignored)
        auto req2 = GrabRequest(runtime, pipeCacheEdge);
        UNIT_ASSERT_VALUES_EQUAL(req2.GetGeneration(), gen1);
    }

    Y_UNIT_TEST(DetailedOnlyDatabaseStillSends) {
        TTestBasicRuntime runtime(1);
        auto [serviceId, pipeCacheEdge] = SetupService(runtime);

        // Register only detailed counters, not labeled counters
        RegisterStream(runtime, serviceId, NKikimrSysView::TABLETS);

        auto req = GrabRequest(runtime, pipeCacheEdge);

        // Should have DetailedCounters but empty ServiceCounters
        UNIT_ASSERT_VALUES_EQUAL(req.ServiceCountersSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(req.DetailedCountersSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL((int)req.GetDetailedCounters(0).GetService(),
            (int)NKikimrSysView::TABLETS);
        UNIT_ASSERT_VALUES_EQUAL(req.GetDetailedCounters(0).TablesSize(), 1);
    }

} // Y_UNIT_TEST_SUITE(SysViewServiceDetailedCounters)

} // NSysView
} // NKikimr
