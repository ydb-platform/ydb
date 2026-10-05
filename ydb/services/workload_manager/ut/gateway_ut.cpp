#include <library/cpp/testing/unittest/registar.h>

#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/gateway.h>
#include <ydb/services/workload_manager/gateway_internal.h>
#include <ydb/services/workload_manager/actors/workload_manager_state_actor.h>
#include <ydb/services/workload_manager/service/service.h>
#include <ydb/services/workload_manager/ut/common/query_classifier_ut_common.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>


namespace NKikimr::NWorkloadManager {

namespace {

struct TGatewayFixture : public NUnitTest::TBaseFixture {
    TTestBasicRuntime Runtime{1};
    std::shared_ptr<NPrivate::TWorkloadManagerGateway> Gateway = std::make_shared<NPrivate::TWorkloadManagerGateway>();
    TActorId StateActorEdge;

    void SetUp(NUnitTest::TTestContext&) override {
        TAppPrepare app;
        app.SetEnableResourcePools(true);
        Runtime.Initialize(app.Unwrap());
        Runtime.GetAppData().WorkloadManagerGateway = Gateway;
        StateActorEdge = Runtime.AllocateEdgeActor();
    }

    void Publish(THashMap<TString, NPrivate::TDatabaseInfo> databases,
                 NPrivate::EMetadataState metadata = NPrivate::EMetadataState::Ready,
                 bool enableResourcePools = true,
                 THashSet<TString> readyPaths = {}) {
        auto* snapshot = new NPrivate::TSnapshot();
        snapshot->Databases = std::move(databases);
        snapshot->ReadyPaths = std::move(readyPaths);
        snapshot->StateActorId = StateActorEdge;
        snapshot->EnableResourcePools = enableResourcePools;
        snapshot->Metadata = metadata;
        Gateway->PublishSnapshot(NPrivate::TSnapshotPtr(snapshot));
    }

    static NPrivate::TDatabaseInfo DatabaseInfo(NPrivate::EDatabaseState state, bool serverless = false) {
        return NPrivate::TDatabaseInfo{.State = state, .Serverless = serverless};
    }

    static TClassifyContext MakeContext() {
        return TClassifyContext{
            .PoolId = "",
            .AppName = "",
            .UserToken = nullptr,
        };
    }

    std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(const TString& databaseId) {
        return Runtime.RunCall([databaseId] {
            return AppData()->WorkloadManagerGateway->TryCreateQueryClassifier(databaseId, MakeContext());
        });
    }

    void Warmup(const TString& databasePath) {
        Runtime.RunCall([databasePath] {
            AppData()->WorkloadManagerGateway->Warmup(databasePath);
            return 0;
        });
    }

    TReadyInfo EnsureReady(const TString& databaseId) {
        return Runtime.RunCall([databaseId] {
            return AppData()->WorkloadManagerGateway->EnsureReady(databaseId);
        });
    }
};

}

Y_UNIT_TEST_SUITE(WorkloadManagerGateway) {

    // No snapshot published yet: TryCreateQueryClassifier returns nullptr.
    Y_UNIT_TEST_F(TestTryCreateQueryClassifierNullBeforeSnapshot, TGatewayFixture) {
        UNIT_ASSERT(!TryCreateQueryClassifier(TEST_DB));
    }

    // No snapshot (no state actor). The gateway:
    // - returns Disabled from EnsureReady,
    // - replies UNAVAILABLE to a subscriber at once.
    Y_UNIT_TEST_F(TestNoSnapshotMeansNoStateActor, TGatewayFixture) {
        UNIT_ASSERT(EnsureReady(TEST_DB).State == EReadyState::Disabled);

        const TActorId subscriber = Runtime.AllocateEdgeActor();
        Runtime.RunCall([subscriber] {
            AppData()->WorkloadManagerGateway->SubscribeOnReady(TEST_DB, subscriber, /*cookie=*/1);
            return 0;
        });
        auto ready = Runtime.GrabEdgeEvent<TEvWorkloadManagerReady>(subscriber, TDuration::Seconds(10));
        UNIT_ASSERT(ready);
        UNIT_ASSERT_EQUAL(ready->Get()->Status, Ydb::StatusIds::UNAVAILABLE);
    }

    // State actor registered, Bootstrap not processed yet. The published snapshot:
    // - carries the state actor id,
    // - has resource pools enabled from AppData, so EnsureReady waits instead of skipping admission.
    Y_UNIT_TEST_F(TestSnapshotPublishedOnRegister, TGatewayFixture) {
        const TActorId stateActor = Runtime.Register(CreateWorkloadManagerStateActor(Gateway));

        const auto snapshot = Gateway->GetSnapshot();
        UNIT_ASSERT(snapshot);
        UNIT_ASSERT_EQUAL(snapshot->StateActorId, stateActor);
        UNIT_ASSERT(snapshot->EnableResourcePools);
        UNIT_ASSERT(snapshot->Databases.empty());
    }

    // State actor publishes a Ready database: TryCreateQueryClassifier returns a classifier.
    Y_UNIT_TEST_F(TestStateActorPublishesSnapshot, TGatewayFixture) {
        const ui32 nodeId = Runtime.GetNodeId(0);
        const TActorId edge = Runtime.AllocateEdgeActor();
        const TActorId schemeCacheEdge = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(NKqp::MakeKqpSchedulerServiceId(nodeId), edge);
        Runtime.RegisterService(MakeServiceId(nodeId), edge);
        Runtime.RegisterService(MakeSchemeCacheID(), schemeCacheEdge);
        const TActorId stateActor = Runtime.Register(CreateWorkloadManagerStateActor(Gateway));

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(options);

        // Publish a known non-serverless entry so IsResourcePoolsEnabled(TEST_DB) is true.
        const TActorId sender = Runtime.AllocateEdgeActor();
        Runtime.Send(new IEventHandle(
            stateActor, sender,
            new TEvFetchDatabaseResponse(Ydb::StatusIds::SUCCESS, TEST_DB, TEST_DB, /*serverless=*/false, TPathId(1, 1), {})));
        auto watch = Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvWatchPathId>(schemeCacheEdge, TDuration::Seconds(10));
        UNIT_ASSERT(watch);

        UNIT_ASSERT(TryCreateQueryClassifier(TEST_DB));
    }

    // Resource pools disabled: TryCreateQueryClassifier returns nullptr.
    Y_UNIT_TEST_F(TestTryCreateQueryClassifierNullWhenPoolsDisabled, TGatewayFixture) {
        Publish({{TEST_DB, DatabaseInfo(NPrivate::EDatabaseState::Ready)}}, NPrivate::EMetadataState::Ready, /*enableResourcePools=*/false);
        UNIT_ASSERT(!TryCreateQueryClassifier(TEST_DB));
    }

    // Database state mapping, metadata Ready. EnsureReady returns:
    // - Pending for an unknown or Pending database,
    // - Ready for a Ready database, Disabled for a serverless one,
    // - Failed with the fetch status for a Failed database,
    // - Failed with retryable UNAVAILABLE for TimedOut, Disabled for Unsupported.
    Y_UNIT_TEST_F(TestEnsureReadyDatabaseStates, TGatewayFixture) {
        using EState = NPrivate::EDatabaseState;
        auto failed = DatabaseInfo(EState::Failed);
        failed.FailureStatus = Ydb::StatusIds::NOT_FOUND;
        failed.FailureMessage = "fetch failed";
        Publish({
            {"/Root/pending", DatabaseInfo(EState::Pending)},
            {"/Root/ready", DatabaseInfo(EState::Ready)},
            {"/Root/serverless", DatabaseInfo(EState::Ready, /*serverless=*/true)},
            {"/Root/failed", failed},
            {"/Root/timedout", DatabaseInfo(EState::TimedOut)},
            {"/Root/unsupported", DatabaseInfo(EState::Unsupported)},
        });

        UNIT_ASSERT(EnsureReady("/Root/unknown").State == EReadyState::Pending);
        UNIT_ASSERT(EnsureReady("/Root/pending").State == EReadyState::Pending);
        UNIT_ASSERT(EnsureReady("/Root/ready").State == EReadyState::Ready);
        UNIT_ASSERT(EnsureReady("/Root/serverless").State == EReadyState::Disabled);
        const auto timedOut = EnsureReady("/Root/timedout");
        UNIT_ASSERT(timedOut.State == EReadyState::Failed);
        UNIT_ASSERT_VALUES_EQUAL(timedOut.FailureStatus, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT(EnsureReady("/Root/unsupported").State == EReadyState::Disabled);

        const auto info = EnsureReady("/Root/failed");
        UNIT_ASSERT(info.State == EReadyState::Failed);
        UNIT_ASSERT_VALUES_EQUAL(info.FailureStatus, Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(info.FailureMessage, "fetch failed");
    }

    // Ready database, metadata state mapping. EnsureReady returns:
    // - Pending while metadata is Pending,
    // - Failed with retryable UNAVAILABLE once metadata timed out,
    // - Disabled when resource pools are off, whatever the states.
    Y_UNIT_TEST_F(TestEnsureReadyMetadataStates, TGatewayFixture) {
        const THashMap<TString, NPrivate::TDatabaseInfo> databases = {
            {"/Root/ready", DatabaseInfo(NPrivate::EDatabaseState::Ready)},
        };

        Publish(databases, NPrivate::EMetadataState::Pending);
        UNIT_ASSERT(EnsureReady("/Root/ready").State == EReadyState::Pending);

        Publish(databases, NPrivate::EMetadataState::TimedOut);
        const auto timedOut = EnsureReady("/Root/ready");
        UNIT_ASSERT(timedOut.State == EReadyState::Failed);
        UNIT_ASSERT_VALUES_EQUAL(timedOut.FailureStatus, Ydb::StatusIds::UNAVAILABLE);

        Publish(databases, NPrivate::EMetadataState::Ready, /*enableResourcePools=*/false);
        UNIT_ASSERT(EnsureReady("/Root/ready").State == EReadyState::Disabled);
    }

    // Degraded database. EnsureReady:
    // - sends a warmup to the state actor for Failed and TimedOut databases,
    // - sends nothing for a Ready database.
    Y_UNIT_TEST_F(TestEnsureReadyRequestsWarmup, TGatewayFixture) {
        Publish({
            {"/Root/ready", DatabaseInfo(NPrivate::EDatabaseState::Ready)},
            {"/Root/failed", DatabaseInfo(NPrivate::EDatabaseState::Failed)},
            {"/Root/timedout", DatabaseInfo(NPrivate::EDatabaseState::TimedOut)},
        });

        EnsureReady("/Root/ready");
        EnsureReady("/Root/failed");
        auto warmup = Runtime.GrabEdgeEvent<TEvWarmupDatabaseInfo>(StateActorEdge, TDuration::Seconds(10));
        UNIT_ASSERT(warmup);
        UNIT_ASSERT_VALUES_EQUAL(warmup->Get()->DatabasePath, "/Root/failed");

        EnsureReady("/Root/timedout");
        warmup = Runtime.GrabEdgeEvent<TEvWarmupDatabaseInfo>(StateActorEdge, TDuration::Seconds(10));
        UNIT_ASSERT(warmup);
        UNIT_ASSERT_VALUES_EQUAL(warmup->Get()->DatabasePath, "/Root/timedout");
    }

    // Warmup gating. The gateway:
    // - sends nothing for a path whose database is Ready,
    // - sends nothing while resource pools are off,
    // - sends a warmup for any other path.
    Y_UNIT_TEST_F(TestWarmupSkipsReadyDatabase, TGatewayFixture) {
        Publish({}, NPrivate::EMetadataState::Ready, /*enableResourcePools=*/false);
        Warmup("/Root/disabled");

        Publish({}, NPrivate::EMetadataState::Ready, /*enableResourcePools=*/true, {"/Root/ready"});
        Warmup("/Root/ready");
        Warmup("/Root/other");

        auto warmup = Runtime.GrabEdgeEvent<TEvWarmupDatabaseInfo>(StateActorEdge, TDuration::Seconds(10));
        UNIT_ASSERT(warmup);
        UNIT_ASSERT_VALUES_EQUAL(warmup->Get()->DatabasePath, "/Root/other");
    }
}

}
