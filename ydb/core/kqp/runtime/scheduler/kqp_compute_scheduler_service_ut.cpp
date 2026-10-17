#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/node_service/kqp_query_control_plane.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <ydb/library/testlib/helpers.h>

namespace NKikimr::NKqp {

using namespace NWorkloadManager;

Y_UNIT_TEST_SUITE(KqpComputeSchedulerService) {

    /* Scenario:
        - Cancel a remote Start while waiting for QueryResponse.
        - Release only this Start's registration; preserve other owners and the canonical pointer.
        - A nullptr response must not produce RemoveQuery.
     */
    Y_UNIT_TEST_TWIN(CancelledStartReleasesOnlyItsRegistration, Enabled) {
        for (const ui32 otherOwners : {0u, 1u, 2u}) {
            NActors::TTestActorRuntime runtime(2);
            auto names = MakeIntrusive<NActors::TTableNameserverSetup>();
            for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
                names->StaticNodeTable[runtime.GetNodeId(node)] = std::make_pair(TString("localhost"), 10000u + node);
            }
            for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
                runtime.AddLocalService(NActors::GetNameserviceActorId(),
                    NActors::TActorSetupCmd(NActors::CreateNameserverTable(names), NActors::TMailboxType::ReadAsFilled,
                        runtime.InterconnectPoolId()), node);
            }
            runtime.Initialize(TAppPrepare().Unwrap());
            runtime.SetScheduledEventFilter([](auto&, auto&, auto, auto&) { return false; });
            const auto executer = runtime.AllocateEdgeActor(1);
            const auto schedulerActor = runtime.AllocateEdgeActor();
            runtime.RegisterService(MakeKqpSchedulerServiceId(runtime.GetNodeId(0)), schedulerActor);
            auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
            NScheduler::TComputeScheduler scheduler(counters, {});
            scheduler.AddOrUpdateDatabase("database", {});
            scheduler.AddOrUpdatePool("database", "pool", {});
            NScheduler::NHdrf::NDynamic::TQueryPtr canonical;
            if (Enabled) {
                for (ui32 i = 0; i < otherOwners; ++i) {
                    canonical = scheduler.AddOrUpdateQuery("database", "pool", 42, {});
                }
            }
            auto state = std::make_shared<TNodeState>();
            std::shared_ptr<NRm::IKqpResourceManager> resourceManager;
            std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> factory;
            const auto manager = runtime.Register(CreateKqpQueryManager(counters, state, resourceManager, factory, false, false));
            bool cancelled = false;
            TActorId registeredManager;
            UNIT_ASSERT(state->AddRequest(executer, manager, cancelled, registeredManager));
            if (otherOwners == 2 && Enabled) {
                std::vector<ui64> tasks{1};
                ui64 count = 0;
                UNIT_ASSERT(state->UpdateRequest(executer, 42, canonical, TInstant::Now(), {}, tasks, count));
            }

            auto start = MakeHolder<TEvKqpNode::TEvStartKqpTasksRequest>();
            start->Record.SetTxId(42);
            start->Record.SetDatabaseId("database");
            start->Record.SetPoolId("pool");
            start->Record.SetStartAllOrFail(true);
            start->Record.AddTasks()->SetId(2);
            runtime.Send(manager, executer, start.Release(), 1, true);
            auto request = runtime.GrabEdgeEvent<NScheduler::TEvAddQuery>(schedulerActor);
            state->MarkRequestAsCancelled(executer);
            auto response = MakeHolder<NScheduler::TEvQueryResponse>();
            if (Enabled) {
                response->Query = scheduler.AddOrUpdateQuery("database", "pool", 42, {});
                if (canonical) {
                    UNIT_ASSERT(response->Query == canonical);
                } else {
                    canonical = response->Query;
                }
            }
            runtime.Send(new IEventHandle(manager, schedulerActor, response.Release(), 0, request->Cookie));
            runtime.Schedule(new IEventHandle(executer, {}, new TEvents::TEvWakeup()), TDuration::Seconds(2), 1);
            auto reply = runtime.GrabEdgeEvent<TEvKqpNode::TEvStartKqpTasksResponse>(executer, TDuration::Seconds(1));
            UNIT_ASSERT_C(reply, "Cancelled Start did not reply to the remote executor");
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.NotStartedTasksSize(), 1);
            if (Enabled) {
                auto remove = runtime.GrabEdgeEvent<NScheduler::TEvRemoveQuery>(schedulerActor);
                UNIT_ASSERT_VALUES_EQUAL(remove->Get()->QueryId, 42);
                UNIT_ASSERT(!remove->Get()->IsForceRemove);
                UNIT_ASSERT(scheduler.RemoveQuery(remove->Get()->QueryId, remove->Get()->IsForceRemove));
                if (otherOwners) {
                    UNIT_ASSERT(canonical->GetParent()->GetQuery(42) == canonical);
                    for (ui32 i = 0; i < otherOwners; ++i) {
                        UNIT_ASSERT(scheduler.RemoveQuery(42));
                    }
                }
                UNIT_ASSERT(!canonical->GetParent()->GetQuery(42));
                UNIT_ASSERT(!scheduler.RemoveQuery(42));
            }
            runtime.Schedule(new IEventHandle(schedulerActor, {}, new TEvents::TEvWakeup()), TDuration::MilliSeconds(2));
            UNIT_ASSERT(!runtime.GrabEdgeEvent<NScheduler::TEvRemoveQuery>(schedulerActor, TDuration::MilliSeconds(1)));
        }
    }

    /* Scenario:
        - Keep multiple registrations while local tasks are running.
        - The last finished task sends force-remove and releases all registrations.
     */
    Y_UNIT_TEST(LastFinishedTaskForceRemovesSchedulerQuery) {
        NActors::TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto executer = runtime.AllocateEdgeActor();
        const auto schedulerActor = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeKqpSchedulerServiceId(runtime.GetNodeId(0)), schedulerActor);
        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        NScheduler::TComputeScheduler scheduler(counters, {});
        scheduler.AddOrUpdateDatabase("database", {});
        scheduler.AddOrUpdatePool("database", "pool", {});
        auto query = scheduler.AddOrUpdateQuery("database", "pool", 42, {});
        scheduler.AddOrUpdateQuery("database", "pool", 42, {});
        TNodeState state;
        bool cancelled = false;
        TActorId manager;
        UNIT_ASSERT(state.AddRequest(executer, {}, cancelled, manager));
        std::vector<ui64> tasks{1, 2};
        ui64 count = 0;
        UNIT_ASSERT(state.UpdateRequest(executer, 42, query, TInstant::Now(), {}, tasks, count));
        runtime.RunCall([&] { state.OnTaskFinished(42, executer, 1, true); return true; });
        runtime.Schedule(new IEventHandle(schedulerActor, {}, new TEvents::TEvWakeup()), TDuration::MilliSeconds(2));
        UNIT_ASSERT(!runtime.GrabEdgeEvent<NScheduler::TEvRemoveQuery>(schedulerActor, TDuration::MilliSeconds(1)));
        runtime.RunCall([&] { state.OnTaskFinished(42, executer, 2, true); return true; });
        auto remove = runtime.GrabEdgeEvent<NScheduler::TEvRemoveQuery>(schedulerActor);
        UNIT_ASSERT(remove->Get()->IsForceRemove);
        UNIT_ASSERT(scheduler.RemoveQuery(remove->Get()->QueryId, remove->Get()->IsForceRemove));
        UNIT_ASSERT(!scheduler.RemoveQuery(42));
        UNIT_ASSERT(!query->GetParent()->GetQuery(42));
    }

    /* Scenario:
        - Create resource pool with zero CPU.
        - Enable or disable Scheduler on start.
        - Run query inside this pool.
        - Query shouldn't timeout or shouldn't be cancelled by timeout.
     */
    Y_UNIT_TEST_TWIN(FeatureFlagOnStart, Enabled) {
        auto ydb = TYdbSetupSettings()
            .EnableResourcePools(true)
            .EnableResourcePoolsScheduler(Enabled)
            .Create();

        const TString& poolId = "zero_pool";
        NResourcePool::TPoolSettings poolSettings;
        poolSettings.TotalCpuLimitPercentPerNode = 0;
        poolSettings.QueryCancelAfter = TDuration::Seconds(10);
        ydb->CreateResourcePool(poolId, poolSettings);

        auto request = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId(poolId));
        const auto& result = request.GetResult();
        UNIT_ASSERT_EQUAL(result.Response.GetResponse().GetEffectivePoolId(), poolId);
        if (!Enabled) {
            TSampleQueries::TSelect42::CheckResult(result);
        } else {
            TSampleQueries::CheckCancelled(result);
        }
    }

    /* Scenario:
        - Create resource pool with zero CPU.
        - Enable Scheduler on start, but disable Workload Manager.
        - Run query inside this pool - it should actually go to default pool.
        - Query shouldn't timeout.
     */
    Y_UNIT_TEST(EnabledSchedulerWithDisabledWorkloadManager) {
        auto ydb = TYdbSetupSettings()
            .EnableResourcePools(false)
            .EnableResourcePoolsScheduler(true)
            .Create();

        const TString& poolId = "zero_pool";
        NResourcePool::TPoolSettings poolSettings;
        poolSettings.TotalCpuLimitPercentPerNode = 0;
        poolSettings.QueryCancelAfter = TDuration::Seconds(10);
        ydb->CreateResourcePool(poolId, poolSettings);

        auto request = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId(poolId));
        auto result = request.GetResult();
        UNIT_ASSERT_EQUAL(result.Response.GetResponse().GetEffectivePoolId(), NResourcePool::DEFAULT_POOL_ID);
        TSampleQueries::TSelect42::CheckResult(result);
    }

}

} // namespace NKikimr::NKqp
