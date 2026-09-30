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

TClassifyContext MakeContext() {
    return TClassifyContext{
        .PoolId = "",
        .AppName = "",
        .UserToken = nullptr,
    };
}

}

Y_UNIT_TEST_SUITE(WorkloadManagerGateway) {

    Y_UNIT_TEST(TryCreateQueryClassifierNullBeforeSnapshotPublished) {
        TTestBasicRuntime runtime(1);
        runtime.Initialize(TAppPrepare().Unwrap());

        auto gateway = std::make_shared<NPrivate::TWorkloadManagerGateway>();
        runtime.GetAppData().WorkloadManagerGateway = gateway;

        auto classifier = runtime.RunCall([] {
            return AppData()->WorkloadManagerGateway->TryCreateQueryClassifier(TEST_DB, MakeContext());
        });
        UNIT_ASSERT(!classifier);
    }

    Y_UNIT_TEST(CacheActorPublishesSnapshot) {
        TTestBasicRuntime runtime(1);
        TAppPrepare app;
        app.SetEnableResourcePools(true);
        runtime.Initialize(app.Unwrap());
        const ui32 nodeId = runtime.GetNodeId(0);

        auto gateway = std::make_shared<NPrivate::TWorkloadManagerGateway>();
        runtime.GetAppData().WorkloadManagerGateway = gateway;

        const TActorId edge = runtime.AllocateEdgeActor();
        const TActorId schemeCacheEdge = runtime.AllocateEdgeActor();
        runtime.RegisterService(NKqp::MakeKqpSchedulerServiceId(nodeId), edge);
        runtime.RegisterService(MakeServiceId(nodeId), edge);
        runtime.RegisterService(MakeSchemeCacheID(), schemeCacheEdge);
        const TActorId stateActor = runtime.Register(CreateWorkloadManagerStateActor(gateway));

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        // Publish a known non-serverless entry so IsResourcePoolsEnabled(TEST_DB) is true.
        const TActorId sender = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(
            stateActor, sender,
            new TEvFetchDatabaseResponse(Ydb::StatusIds::SUCCESS, TEST_DB, TEST_DB, /*serverless=*/false, TPathId(1, 1), {})));
        auto watch = runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvWatchPathId>(schemeCacheEdge, TDuration::Seconds(10));
        UNIT_ASSERT(watch);

        auto classifier = runtime.RunCall([] {
            return AppData()->WorkloadManagerGateway->TryCreateQueryClassifier(TEST_DB, MakeContext());
        });
        UNIT_ASSERT(classifier);
    }

    Y_UNIT_TEST(TryCreateQueryClassifierNullWhenPoolsDisabled) {
        TTestBasicRuntime runtime(1);
        TAppPrepare app;
        app.SetEnableResourcePools(false);
        runtime.Initialize(app.Unwrap());
        const ui32 nodeId = runtime.GetNodeId(0);

        auto gateway = std::make_shared<NPrivate::TWorkloadManagerGateway>();
        runtime.GetAppData().WorkloadManagerGateway = gateway;

        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.RegisterService(NKqp::MakeKqpSchedulerServiceId(nodeId), edge);
        runtime.RegisterService(MakeServiceId(nodeId), edge);
        runtime.Register(CreateWorkloadManagerStateActor(gateway));

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        auto classifier = runtime.RunCall([] {
            return AppData()->WorkloadManagerGateway->TryCreateQueryClassifier(TEST_DB, MakeContext());
        });
        UNIT_ASSERT(!classifier);
    }
}

}
