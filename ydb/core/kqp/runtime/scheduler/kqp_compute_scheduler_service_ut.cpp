#include "kqp_compute_scheduler_service.h"
#include "kqp_schedulable_memory.h"
#include "kqp_schedulable_task.h"
#include "tree/snapshot.h"

#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/service/service.h>
#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/node_service/kqp_query_control_plane.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/executor_pool_basic.h>

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/testlib/helpers.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NKqp {

using namespace NWorkloadManager;

namespace {

class TCpuLimitPool final : public NActors::TBasicExecutorPool {
public:
    // UserPoolId is fixed to 0 by the stub test runtime; actor execution stays in the stub pool.
    TCpuLimitPool() : TBasicExecutorPool(0, 1, 0, "User") {}

    float GetThreadCount() const override {
        return Cpu.load(std::memory_order_relaxed);
    }

    // The fake pool exposes only its thread count; it must not create real executor threads.
    void Initialize() override {}
    void Prepare(TActorSystem*, NSchedulerQueue::TReader**, ui32*) override {}
    void Start() override {}
    void PrepareStop() override {}
    void Shutdown() override {}
    bool Cleanup() override { return true; }

    std::atomic<float> Cpu = 8;
};

struct TCpuLimitFixture {
    TTestBasicRuntime Runtime;
    TCpuLimitPool* UserPool = nullptr;
    NScheduler::TComputeSchedulerPtr Scheduler;
    TActorId Service;
    TActorId Sender;
    TVector<NScheduler::TSchedulableTaskPtr> Tasks;

    TCpuLimitFixture() {
        // Keep actor execution in the stub pool; expose a separate basic User pool to the service.
        Runtime.SetupNodeSubSystems = [this](ui32, NActors::TActorSystemSetup* setup) {
            auto* executors = new TAutoPtr<NActors::IExecutorPool>[2];
            executors[0].Reset(setup->Executors[0].Release());
            UserPool = new TCpuLimitPool;
            executors[1].Reset(UserPool);
            setup->Executors.Reset(executors);
            setup->ExecutorsCount = 2;
        };
        Runtime.Initialize(TAppPrepare().Unwrap());
        Scheduler = std::make_shared<NScheduler::TComputeScheduler>(
            MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            NScheduler::TOptions{
                .DelayParams = NScheduler::TDelayParams{
                    .MaxDelay = TDuration::MicroSeconds(3'000'000),
                    .MinDelay = TDuration::MicroSeconds(10),
                    .AttemptBonus = TDuration::MicroSeconds(5),
                    .MaxRandomDelay = TDuration::MicroSeconds(100),
                },
            });
        Scheduler->SetTotalMemoryLimit(1000_MB);
        Runtime.GetAppData().KqpComputeScheduler = Scheduler;
        const auto memoryController = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(NMemory::MakeMemoryControllerId(), memoryController);
        Service = Runtime.Register(CreateKqpComputeSchedulerService(TDuration::Hours(1)));
        Runtime.GrabEdgeEvent<NMemory::TEvConsumerRegister>(memoryController);
        Sender = Runtime.AllocateEdgeActor();
        UNIT_ASSERT_VALUES_EQUAL(Scheduler->GetTotalCpuLimit(), 8);
    }

    void AddPool(const TString& poolId, double limitPercent, double guaranteePercent) {
        NResourcePool::TPoolSettings settings;
        settings.TotalCpuLimitPercentPerNode = limitPercent;
        settings.TotalCpuGuaranteePercentPerNode = guaranteePercent;
        settings.TotalMemoryLimitPercentPerNode = 30;
        Runtime.Send(new IEventHandle(Service, Sender, new NScheduler::TEvAddPool("db", poolId, settings)));
        Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    }

    void Tick(float cpu) {
        UserPool->Cpu.store(cpu, std::memory_order_relaxed);
        Runtime.Send(new IEventHandle(Service, Sender, new NActors::TEvents::TEvWakeup()));
        Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    }

    NScheduler::NHdrf::NDynamic::TQueryPtr AddDemand(const TString& poolId, ui64 queryId, ui64 demand, ui64 guarantee = 0) {
        auto query = Scheduler->AddOrUpdateQuery("db", poolId, queryId, {.CpuGuarantee = guarantee});
        for (ui64 i = 0; i < demand; ++i) {
            Tasks.push_back(std::make_shared<NScheduler::TSchedulableTask>(query));
            Tasks.back()->IncreaseThrottle();
        }
        return query;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpComputeSchedulerService) {

    Y_UNIT_TEST(InvalidPolicyEventsDoNotPoisonLaterUpdates) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        auto scheduler = std::make_shared<NScheduler::TComputeScheduler>(
            MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            NScheduler::TOptions{.MemoryEnabled = true});
        runtime.GetAppData().KqpComputeScheduler = scheduler;
        const auto memory = runtime.AllocateEdgeActor();
        runtime.RegisterService(NMemory::MakeMemoryControllerId(), memory);
        runtime.RegisterService(NWorkloadManager::MakeServiceId(runtime.GetNodeId()), runtime.AllocateEdgeActor());
        const auto service = runtime.Register(CreateKqpComputeSchedulerService(TDuration::Hours(1)));
        runtime.GrabEdgeEvent<NMemory::TEvConsumerRegister>(memory);
        const auto sender = runtime.AllocateEdgeActor();

        const auto initial = scheduler->GetStaticTree();
        auto* badDb = new NScheduler::TEvAddDatabase("invalid");
        badDb->Weight = -1;
        runtime.Send(new IEventHandle(service, sender, badDb));
        auto* badPool = new NScheduler::TEvAddPool("db", "pool");
        badPool->Weight = 0;
        runtime.Send(new IEventHandle(service, sender, badPool));
        UNIT_ASSERT(scheduler->GetStaticTree() == initial);

        NResourcePool::TPoolSettings settings;
        settings.TotalCpuLimitPercentPerNode = 50;
        settings.TotalMemoryLimitPercentPerNode = 30;
        runtime.Send(new IEventHandle(service, sender, new NScheduler::TEvAddPool("db", "pool", settings)));
        const auto accepted = scheduler->GetStaticTree();
        UNIT_ASSERT(!accepted->GetDatabase("invalid"));
        const auto pool = scheduler->GetOrCreateMemoryPool("db", "pool");
        UNIT_ASSERT_VALUES_EQUAL(pool->GetConfiguredAttributes().GetReadLimit(), TDuration::MilliSeconds(500));

        settings.TotalCpuLimitPercentPerNode = 101;
        settings.TotalMemoryLimitPercentPerNode = 75;
        runtime.Send(new IEventHandle(service, sender,
            new NWorkloadManager::TEvUpdatePoolInfo("db", "pool", settings, std::nullopt)));
        UNIT_ASSERT(scheduler->GetStaticTree() == accepted);
        runtime.Send(new IEventHandle(service, memory, new NMemory::TEvConsumerLimit(1000_MB)));
        UNIT_ASSERT_VALUES_EQUAL(pool->GetConfiguredAttributes().GetMemoryLimit(), 300_MB);
        UNIT_ASSERT_VALUES_EQUAL(pool->GetConfiguredAttributes().GetReadLimit(), TDuration::MilliSeconds(500));
    }

    Y_UNIT_TEST(OnlyChangedDatabaseWeightsPublishPolicy) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        auto scheduler = std::make_shared<NScheduler::TComputeScheduler>(
            MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()), NScheduler::TOptions{});
        runtime.GetAppData().KqpComputeScheduler = scheduler;
        const auto memory = runtime.AllocateEdgeActor();
        runtime.RegisterService(NMemory::MakeMemoryControllerId(), memory);
        const auto service = runtime.Register(CreateKqpComputeSchedulerService(TDuration::Hours(1)));
        runtime.GrabEdgeEvent<NMemory::TEvConsumerRegister>(memory);
        const auto sender = runtime.AllocateEdgeActor();
        auto add = [&](double weight) {
            auto ev = MakeHolder<NScheduler::TEvAddDatabase>("db");
            ev->Weight = weight;
            runtime.Send(new IEventHandle(service, sender, ev.Release()));
            return scheduler->GetStaticTree();
        };
        const auto first = add(1);
        UNIT_ASSERT(first->GetDatabase("db"));
        UNIT_ASSERT(add(1) == first);
        const auto changed = add(2);
        UNIT_ASSERT(changed != first);
        UNIT_ASSERT_VALUES_EQUAL(changed->GetDatabase("db")->GetWeight(), 2);
        UNIT_ASSERT(add(2) == changed);
    }

    Y_UNIT_TEST_TWIN(QueryRegistersPoolAfterRemoval, DefaultPool) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        auto scheduler = std::make_shared<NScheduler::TComputeScheduler>(
            MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            NScheduler::TOptions{.MemoryEnabled = false});
        runtime.GetAppData().KqpComputeScheduler = scheduler;
        const auto wm = runtime.AllocateEdgeActor();
        runtime.RegisterService(NWorkloadManager::MakeServiceId(runtime.GetNodeId()), wm);
        const auto service = runtime.Register(CreateKqpComputeSchedulerService(TDuration::Hours(1)));
        const auto sender = runtime.AllocateEdgeActor();
        const TString poolId = DefaultPool ? NResourcePool::DEFAULT_POOL_ID : "pool";
        auto expectSubscription = [&] {
            auto ev = runtime.GrabEdgeEvent<NWorkloadManager::TEvSubscribeOnPoolChanges>(wm);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->DatabaseId, "db");
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->PoolId, poolId);
        };
        runtime.Send(new IEventHandle(service, sender, new NScheduler::TEvAddPool("db", poolId)), 0, true);
        expectSubscription();
        auto remove = [&] {
            runtime.Send(new IEventHandle(service, wm,
                new NWorkloadManager::TEvUpdatePoolInfo("db", poolId, std::nullopt, std::nullopt)));
        };
        remove();
        expectSubscription();
        remove();
        UNIT_ASSERT(!scheduler->GetStaticTree()->GetDatabase("db")->GetPool(poolId));

        auto add = MakeHolder<NScheduler::TEvAddQuery>();
        add->DatabaseId = "db";
        if (!DefaultPool) {
            add->PoolId = poolId;
        }
        add->QueryId = 42;
        runtime.Send(new IEventHandle(service, sender, add.Release()), 0, true);
        auto response = runtime.GrabEdgeEvent<NScheduler::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, 42);
        UNIT_ASSERT(response->Get()->Query);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Query->GetFullPoolId().PoolId, poolId);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Query->GetFullPoolId().DatabaseId, "db");
        expectSubscription();
    }

    Y_UNIT_TEST_TWIN(NewPoolSnapshotPrecedesQueryResponse, MemoryEnabled) {
        // The runtime pool may be new, or already created by memory accounting or reads.
        for (ui32 precreated : {0u, 1u, 2u}) {
            TTestBasicRuntime runtime;
            runtime.Initialize(TAppPrepare().Unwrap());
            auto scheduler = std::make_shared<NScheduler::TComputeScheduler>(
                MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
                NScheduler::TOptions{.MemoryEnabled = MemoryEnabled});
            auto configuration = std::make_unique<NScheduler::NHdrf::NStatic::TRoot>(4, 1000);
            auto& database = configuration->GetOrCreateDatabase("db");
            database.GetOrCreatePool("existing");
            database.GetOrCreatePool("pool");
            scheduler->UpdateConfiguration(std::move(configuration));
            auto existing = scheduler->AddOrUpdateQuery("db", "existing", 1);
            const auto previous = existing->GetSnapshot();
            UNIT_ASSERT(previous);
            if (precreated == 1) {
                scheduler->GetOrCreateMemoryPool("db", "pool");
            } else if (precreated == 2) {
                UNIT_ASSERT(scheduler->GetReadQuery("db", "pool"));
            }
            UNIT_ASSERT(existing->GetSnapshot() == previous);

            runtime.GetAppData().KqpComputeScheduler = scheduler;
            runtime.RegisterService(NMemory::MakeMemoryControllerId(), runtime.AllocateEdgeActor());
            runtime.RegisterService(MakeKqpRmServiceID(runtime.GetNodeId()), runtime.AllocateEdgeActor());
            runtime.RegisterService(NWorkloadManager::MakeServiceId(runtime.GetNodeId()), runtime.AllocateEdgeActor());
            const auto service = runtime.Register(CreateKqpComputeSchedulerService(TDuration::Hours(1)));
            const auto sender = runtime.AllocateEdgeActor();
            NResourcePool::TPoolSettings settings;
            settings.TotalMemoryLimitPercentPerNode = 30;
            runtime.Send(new IEventHandle(service, sender, new NScheduler::TEvAddPool("db", "pool", settings)), 0, true);

            auto registerQuery = [&](ui64 id) {
                auto event = MakeHolder<NScheduler::TEvAddQuery>();
                event->DatabaseId = "db";
                event->PoolId = "pool";
                event->QueryId = id;
                runtime.Send(new IEventHandle(service, sender, event.Release(), 0, id), 0, true);
                return runtime.GrabEdgeEvent<NScheduler::TEvQueryResponse>(sender)->Get()->Query;
            };

            auto first = registerQuery(2);
            UNIT_ASSERT(first);
            UNIT_ASSERT(first->GetSnapshot());
            UNIT_ASSERT(first->GetSnapshot()->GetParent());
            UNIT_ASSERT(existing->GetSnapshot() != previous);
            const auto updated = existing->GetSnapshot();
            auto second = registerQuery(3);
            UNIT_ASSERT(second);
            UNIT_ASSERT(existing->GetSnapshot() == updated);
            UNIT_ASSERT(registerQuery(2) == first);
            UNIT_ASSERT(existing->GetSnapshot() == updated);

            if (MemoryEnabled) {
                UNIT_ASSERT_VALUES_EQUAL(first->GetSnapshot()->MemoryFairShare, 300);
                UNIT_ASSERT(second->GetSnapshot());
                UNIT_ASSERT_VALUES_EQUAL(second->GetSnapshot()->MemoryFairShare, 300);
                NScheduler::TSchedulableMemory firstMemory(first);
                NScheduler::TSchedulableMemory secondMemory(second);
                UNIT_ASSERT(!firstMemory.TryIncreaseUsage(301));
                UNIT_ASSERT(firstMemory.TryIncreaseUsage(200));
                UNIT_ASSERT(secondMemory.TryIncreaseUsage(100));
                UNIT_ASSERT(!secondMemory.TryIncreaseUsage(1));
                firstMemory.DecreaseUsage(200);
                secondMemory.DecreaseUsage(100);
            }
        }
    }

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
            auto configuration = std::make_unique<NScheduler::NHdrf::NStatic::TRoot>();
            configuration->GetOrCreateDatabase("database").GetOrCreatePool("pool");
            scheduler.UpdateConfiguration(std::move(configuration));
            NScheduler::NHdrf::NDynamic::TQueryPtr canonical;
            if (Enabled) {
                for (ui32 i = 0; i < otherOwners; ++i) {
                    canonical = scheduler.AddOrUpdateQuery("database", "pool", 42);
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
                response->Query = scheduler.AddOrUpdateQuery("database", "pool", 42);
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
        auto configuration = std::make_unique<NScheduler::NHdrf::NStatic::TRoot>();
        configuration->GetOrCreateDatabase("database").GetOrCreatePool("pool");
        scheduler.UpdateConfiguration(std::move(configuration));
        auto query = scheduler.AddOrUpdateQuery("database", "pool", 42);
        scheduler.AddOrUpdateQuery("database", "pool", 42);
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
        - The memory limit of a pool is removed once the setting is reset in its config, as the CPU one.
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
        scheduler->UpdateConfiguration(std::make_unique<NScheduler::NHdrf::NStatic::TRoot>(NScheduler::NHdrf::Infinity(), 1000_MB));
        runtime.GetAppData().KqpComputeScheduler = scheduler;

        const TActorId memoryController = runtime.AllocateEdgeActor();
        runtime.RegisterService(NMemory::MakeMemoryControllerId(), memoryController);

        const TActorId service = runtime.Register(CreateKqpComputeSchedulerService(TDuration::MilliSeconds(100)));
        runtime.GrabEdgeEvent<NMemory::TEvConsumerRegister>(memoryController);

        NResourcePool::TPoolSettings poolSettings;
        poolSettings.TotalMemoryLimitPercentPerNode = 30;
        poolSettings.TotalCpuLimitPercentPerNode = 50;
        poolSettings.TotalCpuGuaranteePercentPerNode = 25;
        scheduler->SetTotalCpuLimit(8);
        const TActorId sender = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(service, sender, new NScheduler::TEvAddPool("db", "pool", poolSettings)));
        runtime.SimulateSleep(TDuration::MilliSeconds(10));

        auto pool = scheduler->GetOrCreateMemoryPool("db", "pool");
        UNIT_ASSERT_VALUES_EQUAL(pool->GetConfiguredAttributes().GetMemoryLimit(), 300_MB);
        UNIT_ASSERT_LT(pool->GetConfiguredAttributes().GetCpuLimit(), NScheduler::NHdrf::Infinity());
        UNIT_ASSERT_VALUES_EQUAL(pool->GetConfiguredAttributes().GetReadLimit(), TDuration::MilliSeconds(500));

        runtime.Send(new IEventHandle(service, memoryController, new NMemory::TEvConsumerLimit(500_MB)));
        runtime.SimulateSleep(TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryLimit(), 500_MB);
        UNIT_ASSERT_VALUES_EQUAL(pool->GetConfiguredAttributes().GetMemoryLimit(), 150_MB);

        // Adding an already registered pool without settings must retain its limits.
        runtime.Send(new IEventHandle(service, sender, new NScheduler::TEvAddPool("db", "pool")));
        runtime.SimulateSleep(TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(pool->GetConfiguredAttributes().GetMemoryLimit(), 150_MB);
        UNIT_ASSERT_LT(pool->GetConfiguredAttributes().GetCpuLimit(), NScheduler::NHdrf::Infinity());

        // the settings are reset in the config of the pool: the limits are removed, the memory one doesn't follow the
        // total limit anymore
        runtime.Send(new IEventHandle(service, sender,
            new NWorkloadManager::TEvUpdatePoolInfo("db", "pool", NResourcePool::TPoolSettings(), std::nullopt)));
        runtime.SimulateSleep(TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(pool->GetConfiguredAttributes().GetMemoryLimit(), NScheduler::NHdrf::Infinity());
        UNIT_ASSERT_VALUES_EQUAL(pool->GetConfiguredAttributes().GetCpuLimit(), NScheduler::NHdrf::Infinity());
        UNIT_ASSERT_VALUES_EQUAL(pool->GetConfiguredAttributes().GetReadLimit(), TDuration::Seconds(1));

        runtime.Send(new IEventHandle(service, memoryController, new NMemory::TEvConsumerLimit(400_MB)));
        runtime.SimulateSleep(TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryLimit(), 400_MB);
        UNIT_ASSERT_VALUES_EQUAL(pool->GetConfiguredAttributes().GetMemoryLimit(), NScheduler::NHdrf::Infinity());
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
        scheduler->UpdateConfiguration(std::make_unique<NScheduler::NHdrf::NStatic::TRoot>(NScheduler::NHdrf::Infinity(), 1000_MB));
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

    /* Scenario:
        - Without FeatureFlags.EnableComputeSchedulerMemory, or with it but without the CPU scheduled on the start, the
          memory is not scheduled: the service doesn't register in the memory controller and doesn't push the query
          memory state to the resource manager.
        - The memory limits of the pools are not set.
        - The query is added only with the CPU scheduled, as before the scheduler had the memory.
     */
    Y_UNIT_TEST_TWIN(MemoryDisabled, CpuEnabled) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());

        auto scheduler = std::make_shared<NScheduler::TComputeScheduler>(
            MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()),
            NScheduler::TOptions{
                .Enabled = CpuEnabled,
                .DelayParams = NScheduler::TDelayParams{
                    .MaxDelay = TDuration::MicroSeconds(3'000'000),
                    .MinDelay = TDuration::MicroSeconds(10),
                    .AttemptBonus = TDuration::MicroSeconds(5),
                    .MaxRandomDelay = TDuration::MicroSeconds(100),
                },
                // without the CPU the memory is not scheduled even if asked to
                .MemoryEnabled = !CpuEnabled,
            });
        UNIT_ASSERT(!scheduler->IsMemoryEnabled());
        scheduler->UpdateConfiguration(std::make_unique<NScheduler::NHdrf::NStatic::TRoot>(NScheduler::NHdrf::Infinity(), 1000_MB));
        runtime.GetAppData().KqpComputeScheduler = scheduler;

        ui32 registrations = 0;
        auto registerObserver = runtime.AddObserver<NMemory::TEvConsumerRegister>([&](auto&) {
            ++registrations;
        });
        ui32 memoryStates = 0;
        auto stateObserver = runtime.AddObserver<NRm::TEvQueryMemoryState>([&](auto&) {
            ++memoryStates;
        });

        runtime.RegisterService(NMemory::MakeMemoryControllerId(), runtime.AllocateEdgeActor());
        runtime.RegisterService(MakeKqpRmServiceID(runtime.GetNodeId()), runtime.AllocateEdgeActor());

        const TActorId service = runtime.Register(CreateKqpComputeSchedulerService(TDuration::MilliSeconds(100)));
        runtime.EnableScheduleForActor(service);

        NResourcePool::TPoolSettings poolSettings;
        poolSettings.TotalMemoryLimitPercentPerNode = 30;
        const TActorId sender = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(service, sender, new NScheduler::TEvAddPool("db", "pool", poolSettings)), 0, true);

        auto addQuery = MakeHolder<NScheduler::TEvAddQuery>();
        addQuery->DatabaseId = "db";
        addQuery->PoolId = "pool";
        addQuery->QueryId = 1;
        runtime.Send(new IEventHandle(service, sender, addQuery.Release(), 0, 1), 0, true);
        UNIT_ASSERT_VALUES_EQUAL(!!runtime.GrabEdgeEvent<NScheduler::TEvQueryResponse>(sender)->Get()->Query, CpuEnabled);

        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(registrations, 0);
        UNIT_ASSERT_VALUES_EQUAL(memoryStates, 0);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetOrCreateMemoryPool("db", "pool")->GetConfiguredAttributes().GetMemoryLimit(), NScheduler::NHdrf::Infinity());
    }

    /* Scenario:
        - The scheduler is disabled at runtime by FeatureFlags.EnableResourcePoolsScheduler.
        - With the memory scheduled it stays enabled till the restart: the memory is never scheduled without the CPU.
     */
    Y_UNIT_TEST_TWIN(DisableAtRuntime, MemoryEnabled) {
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
                .MemoryEnabled = MemoryEnabled,
            });
        runtime.GetAppData().KqpComputeScheduler = scheduler;

        runtime.RegisterService(NMemory::MakeMemoryControllerId(), runtime.AllocateEdgeActor());
        runtime.RegisterService(MakeKqpRmServiceID(runtime.GetNodeId()), runtime.AllocateEdgeActor());

        const TActorId service = runtime.Register(CreateKqpComputeSchedulerService(TDuration::MilliSeconds(100)));

        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        request->Record.MutableConfig()->MutableFeatureFlags()->SetEnableResourcePoolsScheduler(false);
        const TActorId sender = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(service, sender, request.Release()), 0, true);
        runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(sender);

        UNIT_ASSERT_VALUES_EQUAL(scheduler->IsEnabled(), MemoryEnabled);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->IsMemoryEnabled(), MemoryEnabled);
    }

}

} // namespace NKikimr::NKqp
