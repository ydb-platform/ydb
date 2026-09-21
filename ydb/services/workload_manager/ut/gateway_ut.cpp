#include <library/cpp/testing/unittest/registar.h>

#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/gateway.h>
#include <ydb/services/workload_manager/gateway/resource_pools_cache_actor.h>
#include <ydb/services/workload_manager/service/service.h>
#include <ydb/services/workload_manager/ut/common/query_classifier_ut_common.h>

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>


namespace NKikimr::NWorkloadManager {

Y_UNIT_TEST_SUITE(WorkloadManagerGateway) {

    Y_UNIT_TEST(TryGetGatewayReturnsNullBeforeRegistration) {
        TTestBasicRuntime runtime(1);
        runtime.Initialize(TAppPrepare().Unwrap());
        UNIT_ASSERT(!TryGetGateway(runtime.GetNodeId(0)));
    }

    Y_UNIT_TEST(RegistersGatewayOnBootstrap) {
        TTestBasicRuntime runtime(1);
        TAppPrepare app;
        app.SetEnableResourcePools(true);
        runtime.Initialize(app.Unwrap());
        const ui32 nodeId = runtime.GetNodeId(0);

        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeServiceId(nodeId), edge);
        runtime.RegisterService(NKqp::MakeKqpSchedulerServiceId(nodeId), edge);
        runtime.Register(CreateResourcePoolsCacheActor(MakeServiceId(nodeId)));

        // Wait resource pool cache actor done bootstrap
        TDispatchOptions dispatchOptions;
        dispatchOptions.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(dispatchOptions);

        auto gateway = TryGetGateway(nodeId);
        UNIT_ASSERT(gateway);
    }

    Y_UNIT_TEST(TryCreateQueryClassifierNullWhenPoolsDisabled) {
        TTestBasicRuntime runtime(1);
        TAppPrepare app;
        app.SetEnableResourcePools(false);
        runtime.Initialize(app.Unwrap());
        const ui32 nodeId = runtime.GetNodeId(0);

        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeServiceId(nodeId), edge);
        runtime.RegisterService(NKqp::MakeKqpSchedulerServiceId(nodeId), edge);
        runtime.Register(CreateResourcePoolsCacheActor(MakeServiceId(nodeId)));

        // Wait resource pool cache actor done bootstrap
        TDispatchOptions dispatchOptions;
        dispatchOptions.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(dispatchOptions);

        auto gateway = TryGetGateway(nodeId);
        UNIT_ASSERT(gateway);

        TClassifyContext ctx{
            .PoolId = "",
            .AppName = "",
            .UserToken = nullptr,
        };
        UNIT_ASSERT(!gateway->TryCreateQueryClassifier(TEST_DB, std::move(ctx)));
    }
}

}
