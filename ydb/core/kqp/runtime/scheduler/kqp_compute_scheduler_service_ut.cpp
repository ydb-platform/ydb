#include "kqp_compute_scheduler_service.h"
#include "kqp_schedulable_memory.h"

#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/node_service/kqp_query_control_plane.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/testlib/helpers.h>

#include <util/generic/size_literals.h>

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

    /* Scenario:
        - The service registers the query execution consumer in the memory controller on start.
        - The memory of the tracked queries is reported as the consumption on every fair-share update.
     */
    Y_UNIT_TEST(ReportMemoryToMemoryController) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());

        auto scheduler = std::make_shared<NScheduler::TComputeScheduler>(
            MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            NScheduler::TOptions{
                .DelayParams = NScheduler::TDelayParams{
                    .MaxDelay = TDuration::MicroSeconds(3'000'000),
                    .MinDelay = TDuration::MicroSeconds(10),
                    .AttemptBonus = TDuration::MicroSeconds(5),
                    .MaxRandomDelay = TDuration::MicroSeconds(100),
                },
            });
        runtime.GetAppData().KqpComputeScheduler = scheduler;

        const TActorId memoryController = runtime.AllocateEdgeActor();
        runtime.RegisterService(NMemory::MakeMemoryControllerId(), memoryController);

        const TActorId service = runtime.Register(CreateKqpComputeSchedulerService(TDuration::MilliSeconds(100)));
        runtime.EnableScheduleForActor(service);

        auto registerEvent = runtime.GrabEdgeEvent<NMemory::TEvConsumerRegister>(memoryController);
        UNIT_ASSERT_EQUAL(registerEvent->Get()->Kind, NMemory::EMemoryConsumerKind::QueryExecution);

        struct TRecorder : public NMemory::IMemoryConsumer {
            void SetReport(NMemory::TConsumerReport report) override {
                Used = report.Used;
                Demand = report.Demand;
            }

            std::atomic<ui64> Used = 0;
            std::atomic<ui64> Demand = 0;
        };

        auto recorder = MakeIntrusive<TRecorder>();
        runtime.Send(new IEventHandle(service, memoryController, new NMemory::TEvConsumerRegistered(recorder)));

        NScheduler::TSchedulableMemory memory(scheduler->GetOrCreateMemoryPool("db", "pool"));

        memory.IncreaseUsage(10_MB);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(recorder->Used.load(), 10_MB);
        UNIT_ASSERT_VALUES_EQUAL(recorder->Demand.load(), 10_MB);

        memory.DecreaseUsage(10_MB);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(recorder->Used.load(), 0);
    }

    /* Scenario:
        - The limit of the memory consumer is the total memory limit of the scheduler.
        - The memory limits of the pools are the shares of it, and follow it.
     */
    Y_UNIT_TEST(ConsumerLimitIsTotalMemoryLimit) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());

        auto scheduler = std::make_shared<NScheduler::TComputeScheduler>(
            MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            NScheduler::TOptions{
                .DelayParams = NScheduler::TDelayParams{
                    .MaxDelay = TDuration::MicroSeconds(3'000'000),
                    .MinDelay = TDuration::MicroSeconds(10),
                    .AttemptBonus = TDuration::MicroSeconds(5),
                    .MaxRandomDelay = TDuration::MicroSeconds(100),
                },
            });
        scheduler->SetTotalMemoryLimit(1000_MB);
        runtime.GetAppData().KqpComputeScheduler = scheduler;

        const TActorId memoryController = runtime.AllocateEdgeActor();
        runtime.RegisterService(NMemory::MakeMemoryControllerId(), memoryController);

        const TActorId service = runtime.Register(CreateKqpComputeSchedulerService(TDuration::MilliSeconds(100)));
        runtime.GrabEdgeEvent<NMemory::TEvConsumerRegister>(memoryController);

        NResourcePool::TPoolSettings poolSettings;
        poolSettings.TotalMemoryLimitPercentPerNode = 30;
        const TActorId sender = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(service, sender, new NScheduler::TEvAddPool("db", "pool", poolSettings)));
        runtime.SimulateSleep(TDuration::MilliSeconds(10));

        auto pool = scheduler->GetOrCreateMemoryPool("db", "pool");
        UNIT_ASSERT_VALUES_EQUAL(pool->GetMemoryLimit(), 300_MB);

        runtime.Send(new IEventHandle(service, memoryController, new NMemory::TEvConsumerLimit(500_MB)));
        runtime.SimulateSleep(TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryLimit(), 500_MB);
        UNIT_ASSERT_VALUES_EQUAL(pool->GetMemoryLimit(), 150_MB);
    }

    /* Scenario:
        - The service pushes the limit and the usage of the query memory to the resource manager, which publishes
          the free memory to the other nodes.
        - It's pushed on the fair-share update once they change.
     */
    Y_UNIT_TEST(PublishQueryMemoryStateToResourceManager) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());

        auto scheduler = std::make_shared<NScheduler::TComputeScheduler>(
            MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            NScheduler::TOptions{
                .DelayParams = NScheduler::TDelayParams{
                    .MaxDelay = TDuration::MicroSeconds(3'000'000),
                    .MinDelay = TDuration::MicroSeconds(10),
                    .AttemptBonus = TDuration::MicroSeconds(5),
                    .MaxRandomDelay = TDuration::MicroSeconds(100),
                },
            });
        scheduler->SetTotalMemoryLimit(1000_MB);
        runtime.GetAppData().KqpComputeScheduler = scheduler;

        const TActorId resourceManager = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeKqpRmServiceID(runtime.GetNodeId()), resourceManager);

        const TActorId service = runtime.Register(CreateKqpComputeSchedulerService(TDuration::MilliSeconds(100)));
        runtime.EnableScheduleForActor(service);

        auto state = runtime.GrabEdgeEvent<NRm::TEvQueryMemoryState>(resourceManager);
        UNIT_ASSERT_VALUES_EQUAL(state->Get()->Limit, 1000_MB);
        UNIT_ASSERT_VALUES_EQUAL(state->Get()->Usage, 0);

        NScheduler::TSchedulableMemory memory(scheduler->GetOrCreateMemoryPool("db", "pool"));
        memory.IncreaseUsage(10_MB);

        state = runtime.GrabEdgeEvent<NRm::TEvQueryMemoryState>(resourceManager);
        UNIT_ASSERT_VALUES_EQUAL(state->Get()->Limit, 1000_MB);
        UNIT_ASSERT_VALUES_EQUAL(state->Get()->Usage, 10_MB);

        memory.DecreaseUsage(10_MB);
    }

}

} // namespace NKikimr::NKqp
