#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>
#include <ydb/core/kqp/rm_service/kqp_rm_memory_quota.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/tablet/resource_broker_impl.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/node_service/kqp_query_control_plane.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_memory.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/size_literals.h>

#include <atomic>
#include <limits>

#ifndef NDEBUG
const bool DETAILED_LOG = false;
#else
const bool DETAILED_LOG = true;
#endif

namespace NKikimr {
namespace NKqp {

using namespace NKikimrResourceBroker;
using namespace NResourceBroker;

namespace {

constexpr ui64 KQP_QUEUE_MEMORY_LIMIT = 50'000;
constexpr ui64 TOTAL_MEMORY_LIMIT = 100'000;
// what the resource broker estimates a kqp_query task to take before it has measured any
const TDuration KQP_TASK_DEFAULT_DURATION = TDuration::Seconds(5);

// The compute scheduler of the query quota manager tests: the total limit of the memory of the queries, and the pool
// with a memory limit - its fair-share comes to the queries of the pool with the snapshot
constexpr ui64 QUERY_MEMORY_LIMIT = 1'000;
constexpr ui64 POOL_MEMORY_LIMIT = 500;
// the part of the limits given for the optional memory, like the default SpillingPercent
constexpr double ELASTIC_MEMORY_PERCENT = 80;
const TString DATABASE_ID = "db";
const TString POOL_ID = "pool";

TTenantTestConfig MakeTenantTestConfig() {
    TTenantTestConfig cfg = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{{DOMAIN1_NAME, SCHEME_SHARD1_ID, {{TENANT1_1_NAME, TENANT1_2_NAME}}}}},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        true,
        // FakeSchemeShard
        true,
        // CreateConsole
        false,
        // Nodes
        {{
             // Node0
             {
                 // TenantPoolConfig
                 {
                     // Static slots {tenant, {cpu, memory, network}}
                     {{{DOMAIN1_NAME, {1, 1, 1}}}},
                     "node-type"
                 }
             },
             // Node1
             {
                 // TenantPoolConfig
                 {
                     // Static slots {tenant, {cpu, memory, network}}
                     {{{DOMAIN1_NAME, {1, 1, 1}}}},
                     "node-type"
                 }
             }
         }},
        // DataCenterCount
        1
    };
    return cfg;
}

TResourceBrokerConfig MakeResourceBrokerTestConfig() {
    TResourceBrokerConfig config;

    auto queue = config.AddQueues();
    queue->SetName("queue_default");
    queue->SetWeight(5);
    queue->MutableLimit()->AddResource(4);

    queue = config.AddQueues();
    queue->SetName("queue_kqp_resource_manager");
    queue->SetWeight(20);
    queue->MutableLimit()->AddResource(4);
    queue->MutableLimit()->AddResource(KQP_QUEUE_MEMORY_LIMIT);

    auto task = config.AddTasks();
    task->SetName("unknown");
    task->SetQueueName("queue_default");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::KqpResourceManagerTaskName);
    task->SetQueueName("queue_kqp_resource_manager");
    task->SetDefaultDuration(KQP_TASK_DEFAULT_DURATION.GetValue());

    config.MutableResourceLimit()->AddResource(10);
    config.MutableResourceLimit()->AddResource(TOTAL_MEMORY_LIMIT);

    return config;
}

struct TMockMonRequest : public NMonitoring::IMonHttpRequest {
    IOutputStream& Output() override { Y_ABORT("Not implemented"); }
    HTTP_METHOD GetMethod() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetPath() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetPathInfo() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetUri() const override { Y_ABORT("Not implemented"); }
    const TCgiParameters& GetParams() const override { Y_ABORT("Not implemented"); }
    const TCgiParameters& GetPostParams() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetPostContent() const override { Y_ABORT("Not implemented"); }
    const THttpHeaders& GetHeaders() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetHeader(TStringBuf) const override { Y_ABORT("Not implemented"); }
    TStringBuf GetCookie(TStringBuf) const override { Y_ABORT("Not implemented"); }
    TString GetRemoteAddr() const override { Y_ABORT("Not implemented"); }
    TString GetServiceTitle() const override { Y_ABORT("Not implemented"); }
    NMonitoring::IMonPage* GetPage() const override { Y_ABORT("Not implemented"); }
    NMonitoring::IMonHttpRequest* MakeChild(NMonitoring::IMonPage*, const TString&) const override { Y_ABORT("Not implemented"); }
};

// QueryMemoryLimit is both the initial memory of the queries (until the compute scheduler pushes its state) and the
// initial limit of the memory of the node services (until the resource broker queue config has one)
NKikimrConfig::TTableServiceConfig::TResourceManager MakeKqpResourceManagerConfig() {
    NKikimrConfig::TTableServiceConfig::TResourceManager config;

    config.SetComputeActorsCount(100);
    config.SetPublishStatisticsIntervalSec(0);
    config.SetQueryMemoryLimit(1000);

    auto* infoExchangerRetrySettings = config.MutableInfoExchangerSettings();
    auto* exchangerSettings = infoExchangerRetrySettings->MutableExchangerSettings();
    exchangerSettings->SetStartDelayMs(50);
    exchangerSettings->SetMaxDelayMs(50);

    return config;
}

NScheduler::TComputeSchedulerPtr MakeComputeScheduler() {
    auto scheduler = std::make_shared<NScheduler::TComputeScheduler>(
        MakeIntrusive<TKqpCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>()),
        NScheduler::TOptions{
            .DelayParams = NScheduler::TDelayParams{
                .MaxDelay = TDuration::MicroSeconds(3'000'000),
                .MinDelay = TDuration::MicroSeconds(10),
                .AttemptBonus = TDuration::MicroSeconds(5),
                .MaxRandomDelay = TDuration::MicroSeconds(100),
            },
        });
    scheduler->SetTotalCpuLimit(12);
    scheduler->SetTotalMemoryLimit(QUERY_MEMORY_LIMIT);
    scheduler->AddOrUpdatePool(DATABASE_ID, POOL_ID, {.MemoryLimit = POOL_MEMORY_LIMIT});
    return scheduler;
}

}

class KqpRm : public TTestBase {
public:
    void SetUp() override {
        Runtime = MakeHolder<TTenantTestRuntime>(MakeTenantTestConfig());

        NActors::NLog::EPriority priority = DETAILED_LOG ? NLog::PRI_DEBUG : NLog::PRI_ERROR;
        Runtime->SetLogPriority(NKikimrServices::RESOURCE_BROKER, priority);
        Runtime->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, priority);

        auto now = Now();
        Runtime->UpdateCurrentTime(now);

        Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();

        for (ui32 nodeIndex = 0; nodeIndex < Runtime->GetNodeCount(); ++nodeIndex) {
            auto resourceBrokerConfig = MakeResourceBrokerTestConfig();
            auto broker = CreateResourceBrokerActor(resourceBrokerConfig, Counters);
            auto resourceBrokerActorId = Runtime->Register(broker, nodeIndex);
            // the resource manager talks to the resource broker service of its node: replace the default one
            Runtime->RegisterService(MakeResourceBrokerID(), resourceBrokerActorId, nodeIndex);
            ResourceBrokers.push_back(resourceBrokerActorId);
        }
        WaitForBootstrap();
    }

    void TearDown() override {
        ResourceBrokers.clear();
        ResourceManagers.clear();
        Runtime.Reset();
    }

    void WaitForBootstrap() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(Runtime->DispatchEvents(options));
    }

    void CreateKqpResourceManager(
            const NKikimrConfig::TTableServiceConfig::TResourceManager& config, ui32 nodeInd = 0) {
        auto kqpCounters = MakeIntrusive<TKqpCounters>(Counters);
        auto resman = CreateKqpResourceManagerActor(config, kqpCounters, nullptr, Runtime->GetNodeId(nodeInd));
        // RM creates children during its registration, we need to enable schedule for them
        auto prevObserver = Runtime->SetRegistrationObserverFunc([](TTestActorRuntimeBase& runtime, const TActorId& /*parentId*/, const TActorId& actorId) {
            runtime.EnableScheduleForActor(actorId, true);
        });
        ResourceManagers.push_back(Runtime->Register(resman, nodeInd));
        Runtime->RegisterService(MakeKqpResourceManagerServiceID(
            Runtime->GetNodeId(nodeInd)), ResourceManagers.back(), nodeInd);
        Runtime->SetRegistrationObserverFunc(prevObserver);
    }

    void StartRms(const TVector<NKikimrConfig::TTableServiceConfig::TResourceManager>& configs = {}) {
        for (ui32 nodeIndex = 0; nodeIndex < Runtime->GetNodeCount(); ++nodeIndex) {
            if (configs.empty()) {
                CreateKqpResourceManager(MakeKqpResourceManagerConfig(), nodeIndex);
            } else {
                CreateKqpResourceManager(configs[nodeIndex], nodeIndex);
            }
        }
        WaitForBootstrap();
    }

    void Reconfigure(const NKikimrConfig::TTableServiceConfig::TResourceManager& config) {
        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        request->Record.MutableConfig()->MutableTableServiceConfig()->MutableResourceManager()->CopyFrom(config);

        auto edge = Runtime->AllocateEdgeActor();
        Runtime->Send(new IEventHandle(ResourceManagers.front(), edge, request.Release()), 0, true);
        Runtime->GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(edge);
    }

    void SetSpillingPercent(double spillingPercent) {
        auto config = MakeKqpResourceManagerConfig();
        config.SetSpillingPercent(spillingPercent);
        Reconfigure(config);
    }

    // The handlers below do not reply: the mon page request sent after them is handled after them
    TString RenderRmMonPage(ui32 nodeInd = 0) {
        TMockMonRequest request;
        auto edge = Runtime->AllocateEdgeActor(nodeInd);
        Runtime->Send(new IEventHandle(ResourceManagers[nodeInd], edge, new NMon::TEvHttpInfo(request)), nodeInd, true);

        TAutoPtr<IEventHandle> handle;
        auto* response = Runtime->GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);
        UNIT_ASSERT(response);
        return response->Answer;
    }

    // The resource broker queue config as the resource manager receives it: the limit of the memory of the node services
    TString SetServicesMemoryLimit(ui64 memory) {
        auto response = MakeHolder<TEvResourceBroker::TEvConfigResponse>();
        response->QueueConfig.ConstructInPlace();
        response->QueueConfig->MutableLimit()->SetMemory(memory);
        Runtime->Send(new IEventHandle(ResourceManagers.front(), Runtime->AllocateEdgeActor(), response.Release()), 0, true);
        return RenderRmMonPage();
    }

    // The state of the query memory as the compute scheduler service pushes it
    TString SetQueryMemoryState(ui32 nodeInd, ui64 limit, ui64 usage) {
        Runtime->Send(new IEventHandle(ResourceManagers[nodeInd], Runtime->AllocateEdgeActor(nodeInd),
            new NRm::TEvQueryMemoryState(limit, usage)), nodeInd, true);
        return RenderRmMonPage(nodeInd);
    }

    i64 RmRate(const TString& name) {
        return GetServiceCounters(Counters, "kqp")->GetCounter(name, true)->Val();
    }

    i64 RmGauge(const TString& name) {
        return GetServiceCounters(Counters, "kqp")->GetCounter(name, false)->Val();
    }

    void AssertResourceBrokerSensors(i64 cpu, i64 mem, i64 enqueued, std::optional<i64> finished, i64 infly) {
        auto q = Counters->GetSubgroup("queue", "queue_kqp_resource_manager");
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("CPUConsumption")->Val(), cpu);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("MemoryConsumption")->Val(), mem);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("EnqueuedTasks")->Val(), enqueued);
        if (finished) {
            UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("FinishedTasks")->Val(), *finished);
        }
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("InFlyTasks")->Val(), infly);

        auto t = Counters->GetSubgroup("task", "kqp_query");
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("CPUConsumption")->Val(), cpu);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("MemoryConsumption")->Val(), mem);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("EnqueuedTasks")->Val(), enqueued);
        if (finished) {
            UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("FinishedTasks")->Val(), *finished);
        }
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("InFlyTasks")->Val(), infly);
    }

    TIntrusivePtr<NRm::TTxState> MakeTx(ui64 txId, std::shared_ptr<NRm::IKqpResourceManager> rm,
            const TString& poolId = "", const TString& database = "") {
        return MakeIntrusive<NRm::TTxState>(rm, txId, TInstant::Now(), poolId, database, false);
    }

    // The memory the txs hold (RM/Memory, in the resource manager tests it's only the memory of the node services) and
    // the execution units left on the node. The memory of the node services is not the memory of the queries the
    // resource manager reports (GetLocalResources().Memory, see NRm::TEvQueryMemoryState)
    void AssertResourceManagerStats(
            std::shared_ptr<NRm::IKqpResourceManager> rm, ui64 memory, ui32 executionUnits) {
        UNIT_ASSERT_VALUES_EQUAL(RmGauge("RM/Memory"), static_cast<i64>(memory));
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().ExecutionUnits, executionUnits);
    }

    void Disconnect(ui32 nodeIndexFrom, ui32 nodeIndexTo) {
        const TActorId proxy = Runtime->GetInterconnectProxy(nodeIndexFrom, nodeIndexTo);

        Runtime->Send(
            new IEventHandle(
                proxy,  TActorId(), new TEvInterconnect::TEvDisconnect(), 0, 0),
                nodeIndexFrom, true);

        //Wait for event TEvInterconnect::EvNodeDisconnected
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
        Runtime->DispatchEvents(options);
    }

    struct TCheckedResources {
        ui64 ScanQueryMemory;
        ui32 ExecutionUnits;

        bool operator==(const TCheckedResources& other) const {
            return ScanQueryMemory == other.ScanQueryMemory &&
                ExecutionUnits == other.ExecutionUnits;
        }
    };

    void CheckSnapshot(ui32 nodeIndToCheck, TVector<TCheckedResources> verificationData,
            std::shared_ptr<NRm::IKqpResourceManager> currentRm) {
        TVector<NKikimrKqp::TKqpNodeResources> snapshot;
        std::atomic<int> ready = 0;

        while(true) {
            currentRm->RequestClusterResourcesInfo(
                    [&](TVector<NKikimrKqp::TKqpNodeResources>&& resources) {
                snapshot = std::move(resources);
                ready = 1;
            });

            while (ready.load() != 1) {
                Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
            }

            if (snapshot.size() != verificationData.size()) {
                continue;
            }
            std::sort(snapshot.begin(), snapshot.end(), [](auto first, auto second) {
                return first.GetNodeId() < second.GetNodeId();
            });

            TVector<TCheckedResources> currentData;
            std::transform(snapshot.cbegin(), snapshot.cend(), std::back_inserter(currentData),
                   [](const NKikimrKqp::TKqpNodeResources& cur) {
                        return TCheckedResources{cur.GetMemory()[0].GetAvailable(), cur.GetExecutionUnits()};
            });

            if (verificationData[nodeIndToCheck] == currentData[nodeIndToCheck]) {
                for (ui32 i = 0; i < verificationData.size(); i++) {
                    if (i != nodeIndToCheck) {
                        UNIT_ASSERT_VALUES_EQUAL(verificationData[i].ScanQueryMemory, currentData[i].ScanQueryMemory);
                    }
                }
                break;
            }
        }
    }

    UNIT_TEST_SUITE(KqpRm);
        UNIT_TEST(SingleTask);
        UNIT_TEST(ManyTasks);
        UNIT_TEST(NotEnoughMemory);
        UNIT_TEST(NotEnoughExecutionUnits);
        UNIT_TEST(ResourceBrokerNotEnoughResources);
        UNIT_TEST(SingleSnapshotByExchanger);
        UNIT_TEST(Reduce);
        UNIT_TEST(ConcurrentTasks);
        UNIT_TEST(SpillingPercentReconfigure);
        UNIT_TEST(TotalLimitReconfigure);
        UNIT_TEST(OptionalMemorySpillingThreshold);
        UNIT_TEST(OptionalMemoryConcurrent);
        UNIT_TEST(OptionalMemoryRefusedByBroker);
        UNIT_TEST(ServiceMemoryQuota);
        UNIT_TEST(ConcurrentServiceMemoryQuota);
        UNIT_TEST(QueryMemoryState);
        UNIT_TEST(SnapshotSharingByExchanger);
        UNIT_TEST(NodesMembershipByExchanger);
        UNIT_TEST(DisonnectNodes);
        UNIT_TEST(QueryQuotaManager);
        UNIT_TEST(QueryQuotaManagerDefaultPool);
        UNIT_TEST(QueryQuotaManagerWithoutMemory);
        UNIT_TEST(TaskQuotaManagerOptional);
        UNIT_TEST(ConcurrentChannels);
        UNIT_TEST(TaskElasticMemory);
    UNIT_TEST_SUITE_END();

    void SingleTask();
    void ManyTasks();
    void NotEnoughMemory();
    void NotEnoughExecutionUnits();
    void ResourceBrokerNotEnoughResources();
    void Snapshot();
    void SingleSnapshotByExchanger();
    void Reduce();
    void ConcurrentTasks();
    void SpillingPercentReconfigure();
    void TotalLimitReconfigure();
    void OptionalMemorySpillingThreshold();
    void OptionalMemoryConcurrent();
    void OptionalMemoryRefusedByBroker();
    void ServiceMemoryQuota();
    void ConcurrentServiceMemoryQuota();
    void QueryMemoryState();
    void SnapshotSharing();
    void SnapshotSharingByExchanger();
    void NodesMembership();
    void NodesMembershipByExchanger();
    void DisonnectNodes();
    void QueryQuotaManager();
    void QueryQuotaManagerDefaultPool();
    void QueryQuotaManagerWithoutMemory();
    void TaskQuotaManagerOptional();
    void ConcurrentChannels();
    void TaskElasticMemory();

private:
    THolder<TTestBasicRuntime> Runtime;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TVector<TActorId> ResourceBrokers;
    TVector<TActorId> ResourceManagers;
};
UNIT_TEST_SUITE_REGISTRATION(KqpRm);


void KqpRm::SingleTask() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto stats = rm->GetLocalResources();
    UNIT_ASSERT_VALUES_EQUAL(1000, stats.Memory);

    NRm::TKqpResourcesRequest request{.ExecutionUnits = 1, .Memory = 100};

    {
        auto tx = MakeTx(1, rm);
        auto task = 2;

        bool allocated = rm->AllocateResources(*tx, task, request);
        UNIT_ASSERT(allocated);

        AssertResourceManagerStats(rm, 100, 99);
        AssertResourceBrokerSensors(0, 100, 0, 0, 1);
        // the memory of the node services is not the memory of the queries
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, 1000);

        rm->FreeResources(*tx, task, request);
        AssertResourceManagerStats(rm, 0, 100);
        AssertResourceBrokerSensors(0, 0, 0, 0, 1);
    }

    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
}

// The memory quota of the node services other than the queries: the limit of the resource manager, the spilling
// threshold for the optional memory
void KqpRm::ServiceMemoryQuota() {
    StartRms();
    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    {
        auto quota = NRm::CreateMemoryQuotaManager(rm);
        UNIT_ASSERT(quota->AllocateQuota(600, /* isOptional */ false));
        UNIT_ASSERT(!quota->AllocateQuota(500, /* isOptional */ false));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 1);
        UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 600);
        AssertResourceManagerStats(rm, 600, 100);
        // 400 left, but only 200 below the spilling threshold
        UNIT_ASSERT(!quota->AllocateQuota(300, /* isOptional */ true));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1);
        UNIT_ASSERT(quota->AllocateQuota(200, /* isOptional */ true));
        quota->FreeQuota(400);
        UNIT_ASSERT(quota->AllocateQuota(500, /* isOptional */ false));
        UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 900);
        AssertResourceManagerStats(rm, 900, 100);
        quota->FreeQuota(900);
        AssertResourceManagerStats(rm, 0, 100);
    }
    AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 0);
}

void KqpRm::ConcurrentServiceMemoryQuota() {
    StartRms();
    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    {
        auto quota = NRm::CreateMemoryQuotaManager(rm);
        NPar::LocalExecutor().RunAdditionalThreads(4);
        NPar::LocalExecutor().ExecRange([&](int) {
            for (ui32 i = 0; i < 100; ++i) {
                if (quota->AllocateQuota(300, /* isOptional */ false)) {
                    quota->FreeQuota(300);
                }
            }
        }, 0, 8, NPar::TLocalExecutor::WAIT_COMPLETE);
        UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 0);
        AssertResourceManagerStats(rm, 0, 100);
    }
    AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 0);
}

void KqpRm::ManyTasks() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    NRm::TKqpResourcesRequest request{.ExecutionUnits = 1, .Memory = 100};

    {
        auto tx = MakeTx(1, rm);
        for (ui32 i = 1; i < 10; ++i) {
            auto task = i;
            bool allocated = rm->AllocateResources(*tx, task, request);
            UNIT_ASSERT(allocated);

            AssertResourceManagerStats(rm, 100 * i, 100 - i);
            AssertResourceBrokerSensors(0, 100 * i, 0, i - 1, 1);
        }
    }

    AssertResourceManagerStats(rm, 0, 100);
    AssertResourceBrokerSensors(0, 0, 0, 9, 0);
}

void KqpRm::NotEnoughMemory() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm);
    auto task = 2;

    auto result = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 10, .Memory = 10'000});
    UNIT_ASSERT(!result);
    UNIT_ASSERT(result.GetStatus() == NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 1);
    UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 10'000);

    AssertResourceManagerStats(rm, 0, 100);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
}

void KqpRm::NotEnoughExecutionUnits() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm);
    auto task = 2;

    auto result = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 1000, .Memory = 100});
    UNIT_ASSERT(!result);
    UNIT_ASSERT(result.GetStatus() == NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS);

    AssertResourceManagerStats(rm, 0, 100);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
}

void KqpRm::ResourceBrokerNotEnoughResources() {
    auto config = MakeKqpResourceManagerConfig();
    config.SetQueryMemoryLimit(100000000);

    StartRms({config, MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm);
    auto task = 2;

    bool allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 1'000});
    UNIT_ASSERT(allocated);

    allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 100'000});
    UNIT_ASSERT(!allocated);

    AssertResourceManagerStats(rm, 1000, 99);
    AssertResourceBrokerSensors(0, 1000, 0, 0, 1);
}

// The memory of the node services is not published, only the memory of the queries is
void KqpRm::Snapshot() {
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    NRm::TKqpResourcesRequest request{.ExecutionUnits = 10, .Memory = 100};

    {
        auto tx1 = MakeTx(1, rm);
        auto tx2 = MakeTx(2, rm);

        auto task2 = 2;
        auto task1 = 1;

        bool allocated = rm->AllocateResources(*tx1, task2, request);
        UNIT_ASSERT(allocated);

        allocated &= rm->AllocateResources(*tx2, task1, request);
        UNIT_ASSERT(allocated);

        AssertResourceManagerStats(rm, 200, 80);
        AssertResourceBrokerSensors(0, 200, 0, 0, 2);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(0, {{1000, 80}, {1000, 100}}, rm);

        rm->FreeResources(*tx1, task2, request);
        rm->FreeResources(*tx2, task1, request);

        AssertResourceManagerStats(rm, 0, 100);
        AssertResourceBrokerSensors(0, 0, 0, 0, 2);
    }

    AssertResourceBrokerSensors(0, 0, 0, 2, 0);

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm);
}

void KqpRm::SingleSnapshotByExchanger() {
    Snapshot();
}

void KqpRm::Reduce() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm);
    auto task = 1;

    bool allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 10, .Memory = 100});
    UNIT_ASSERT(allocated);

    AssertResourceManagerStats(rm, 100, 100 - 10);
    AssertResourceBrokerSensors(0, 100, 0, 0, 1);

    rm->FreeResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 7, .Memory = 70});
    AssertResourceManagerStats(rm, 100 - 70, 100 - 10 + 7);
    AssertResourceBrokerSensors(0, 30, 0, 0, 1);
}

void KqpRm::ConcurrentTasks() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);

        NPar::LocalExecutor().RunAdditionalThreads(10);
        std::atomic<ui64> failedAllocations = 0;

        NPar::LocalExecutor().ExecRange([&](int index) {
            const int taskId = index + 1;
            auto count = 0u;
            for (auto n = 0u; n < 20u; n++) {
                for (auto j = 0u; j < 20u; j++) {
                    if (!rm->AllocateResources(*tx, taskId, NRm::TKqpResourcesRequest{.ExecutionUnits = j, .Memory = j * 10u})) {
                        failedAllocations++;
                        Sleep(TDuration::MilliSeconds(j * 10));
                        break;
                    }
                    count += j;
                }
                for (auto j = 20u; j > 0u; j--) {
                    if (count < j) {
                        break;
                    }
                    rm->FreeResources(*tx, taskId, NRm::TKqpResourcesRequest{.ExecutionUnits = j, .Memory = j * 10u});
                    count -= j;
                }
            }
            rm->FreeResources(*tx, taskId, NRm::TKqpResourcesRequest{.ExecutionUnits = count, .Memory = count * 10u});
        }, 0, 10, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);

        UNIT_ASSERT_GT(failedAllocations.load(), 0);
        AssertResourceManagerStats(rm, 0, 100);
        AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 1);
    }

    AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 0);
}

// A runtime SpillingPercent change moves the spilling threshold of the memory of the node services, the limit stays
// as it is (1000)
void KqpRm::SpillingPercentReconfigure() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    // the largest optional request the resource manager grants right now: it's granted, the next byte is not
    auto assertOptionalLimit = [&](auto& tx, ui64 memory) {
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = memory + 1, .Optional = true}));
        if (memory) {
            UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = memory, .Optional = true}));
            rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = memory});
        }
    };

    {
        auto tx = MakeTx(1, rm);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100}));
        assertOptionalLimit(tx, 700); // the default 80%: the threshold at 800 used

        SetSpillingPercent(100); // the threshold moves up to the limit
        assertOptionalLimit(tx, 900);

        SetSpillingPercent(50); // and down to half of it
        assertOptionalLimit(tx, 400);

        // out of range values are clamped: above 100 behaves as 100, below 0 as 0 (no optional memory at all)
        SetSpillingPercent(120);
        assertOptionalLimit(tx, 900);
        SetSpillingPercent(-20);
        assertOptionalLimit(tx, 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 5);

        // the mandatory memory is still given up to the limit
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 900}));
        AssertResourceManagerStats(rm, 1000, 100);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 0);

        rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 900});
        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100});
    }

    AssertResourceManagerStats(rm, 0, 100);
}

// The limit of the memory of the node services follows the memory limit of the resource broker queue
void KqpRm::TotalLimitReconfigure() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 450}));
        UNIT_ASSERT_STRING_CONTAINS(RenderRmMonPage(), "Services memory resource: 450/1000");
        // the threshold at 800 used
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 400, .Optional = true}));

        // the threshold at 1600 used
        UNIT_ASSERT_STRING_CONTAINS(SetServicesMemoryLimit(2000), "Services memory resource: 450/2000");
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 400, .Optional = true}));
        rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 400});
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 1550})); // up to the new limit
        rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 1550});

        // the limit below the usage: nothing more is given
        UNIT_ASSERT_STRING_CONTAINS(SetServicesMemoryLimit(400), "Services memory resource: 450/400");
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 1}));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 1);

        // a queue without the memory limit doesn't change it
        UNIT_ASSERT_STRING_CONTAINS(SetServicesMemoryLimit(0), "Services memory resource: 450/400");

        // the memory of the queries doesn't follow it
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, 1000);

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 450});
    }

    AssertResourceManagerStats(rm, 0, 100);
}

// An optional Memory request is refused once it would take the memory of the node services past the spilling threshold
// (800 used here) although the memory is there, a mandatory one only past the limit. The refusal is quiet: no resource
// broker task, neither RM/NotEnoughMemory nor the failed allocation of the tx see it, and the execution units of the
// request come back
void KqpRm::OptionalMemorySpillingThreshold() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 700}));
        AssertResourceBrokerSensors(0, 700, 0, 0, 1);

        auto result = rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.ExecutionUnits = 5, .Memory = 150, .Optional = true});
        UNIT_ASSERT(!result);
        UNIT_ASSERT(result.GetStatus() == NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY);
        AssertResourceManagerStats(rm, 700, 100); // the units came back
        AssertResourceBrokerSensors(0, 700, 0, 0, 1);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 0);

        // up to the threshold, not past it
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 100, .Optional = true}));
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 1, .Optional = true}));

        // a mandatory request goes past the threshold, an optional one stays refused there
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 150}));
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 1, .Optional = true}));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 3);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 0);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0);
        AssertResourceManagerStats(rm, 950, 100);

        // SpillingPercent = 100: the threshold is the limit, an optional request is refused where a mandatory one is
        SetSpillingPercent(100);
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 51, .Optional = true}));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 4);
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 51}));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 1);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 51);
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 50, .Optional = true}));
        AssertResourceManagerStats(rm, 1000, 100);

        rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 100 + 150 + 50});
        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 700});
    }

    AssertResourceManagerStats(rm, 0, 100);
}

// Concurrent optional requests are decided one at a time under the resource manager lock: together they never take
// the memory of the node services past the spilling threshold
void KqpRm::OptionalMemoryConcurrent() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        // 200 left below the threshold: a single thread is refused already, it asks for 340 and more between its frees
        UNIT_ASSERT(rm->AllocateResources(*tx, 0, NRm::TKqpResourcesRequest{.Memory = 600}));

        // RM/Memory is counted after the grant and uncounted before the release: it never exceeds what is charged
        auto memory = GetServiceCounters(Counters, "kqp")->GetCounter("RM/Memory", false);

        NPar::LocalExecutor().RunAdditionalThreads(10);
        std::atomic<ui64> granted = 0;
        std::atomic<ui64> refused = 0;
        std::atomic<bool> overThreshold = false;

        NPar::LocalExecutor().ExecRange([&](int index) {
            const ui64 taskId = index + 1;
            ui64 held = 0;
            for (auto j = 0u; j < 200u; j++) {
                const ui64 request = (j % 7 + 1) * 10;
                if (rm->AllocateResources(*tx, taskId, NRm::TKqpResourcesRequest{.Memory = request, .Optional = true})) {
                    granted++;
                    held += request;
                    if (memory->Val() > 800) {
                        overThreshold = true;
                    }
                } else {
                    refused++;
                }
                if (j % 10 == 9 && held) {
                    rm->FreeResources(*tx, taskId, NRm::TKqpResourcesRequest{.Memory = held});
                    held = 0;
                }
            }
            if (held) {
                rm->FreeResources(*tx, taskId, NRm::TKqpResourcesRequest{.Memory = held});
            }
        }, 0, 10, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);

        UNIT_ASSERT(!overThreshold.load());
        UNIT_ASSERT_GT(granted.load(), 0);
        UNIT_ASSERT_GT(refused.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), static_cast<i64>(refused.load()));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 0);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0);
        AssertResourceManagerStats(rm, 600, 100);

        rm->FreeResources(*tx, 0, NRm::TKqpResourcesRequest{.Memory = 600});
    }

    AssertResourceManagerStats(rm, 0, 100);
}

// An optional request below the spilling threshold, but which the resource broker refuses (here the kqp queue limit
// of the broker), is a failure like a mandatory one: counted in RM/NotEnoughMemory and recorded as the failed
// allocation of the tx, not taken for a quiet refusal at the threshold. The memory and the execution units are given
// back
void KqpRm::OptionalMemoryRefusedByBroker() {
    auto config = MakeKqpResourceManagerConfig();
    config.SetQueryMemoryLimit(100'000'000); // the limit is far above the kqp queue limit of the broker

    StartRms({config, MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 1'000}));

        auto result = rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 100'000, .Optional = true});
        UNIT_ASSERT(!result);
        UNIT_ASSERT(result.GetStatus() == NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 1);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 100'000);

        AssertResourceManagerStats(rm, 1'000, 100);
        AssertResourceBrokerSensors(0, 1'000, 0, 0, 1);
        UNIT_ASSERT_STRING_CONTAINS(RenderRmMonPage(), "Services memory resource: 1000/100000000");

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 1'000});
    }

    AssertResourceManagerStats(rm, 0, 100);
}

// The memory of the queries comes from the compute scheduler service (NRm::TEvQueryMemoryState): it's the memory the
// resource manager reports locally and publishes to the other nodes
void KqpRm::QueryMemoryState() {
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm_first = GetKqpResourceManager(ResourceManagers[0].NodeId());
    auto rm_second = GetKqpResourceManager(ResourceManagers[1].NodeId());

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // before the first push: QueryMemoryLimit of the config, nothing used
    UNIT_ASSERT_VALUES_EQUAL(rm_first->GetLocalResources().Memory, 1000);
    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm_second);

    UNIT_ASSERT_STRING_CONTAINS(SetQueryMemoryState(0, 1000, 300), "Query memory: 300/1000");
    UNIT_ASSERT_VALUES_EQUAL(rm_first->GetLocalResources().Memory, 700);
    UNIT_ASSERT_VALUES_EQUAL(rm_first->GetLocalResources().ExecutionUnits, 100);
    UNIT_ASSERT_VALUES_EQUAL(rm_second->GetLocalResources().Memory, 1000);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    CheckSnapshot(0, {{700, 100}, {1000, 100}}, rm_second);

    // the usage past the limit (the memory may be taken unconditionally): nothing is available
    SetQueryMemoryState(0, 500, 800);
    UNIT_ASSERT_VALUES_EQUAL(rm_first->GetLocalResources().Memory, 0);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    CheckSnapshot(0, {{0, 100}, {1000, 100}}, rm_second);

    // the limit follows the memory controller
    SetQueryMemoryState(0, 2000, 800);
    UNIT_ASSERT_VALUES_EQUAL(rm_first->GetLocalResources().Memory, 1200);

    // the memory of the node services doesn't change it, the execution units are published as well
    {
        auto tx = MakeTx(1, rm_first);
        UNIT_ASSERT(rm_first->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 10, .Memory = 100}));
        UNIT_ASSERT_VALUES_EQUAL(rm_first->GetLocalResources().Memory, 1200);
        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        CheckSnapshot(0, {{1200, 90}, {1000, 100}}, rm_second);
        rm_first->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 10, .Memory = 100});
    }

    SetQueryMemoryState(1, 1000, 1000);
    UNIT_ASSERT_VALUES_EQUAL(rm_second->GetLocalResources().Memory, 0);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    CheckSnapshot(1, {{1200, 100}, {0, 100}}, rm_first);
}

void KqpRm::SnapshotSharing() {
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm_first = GetKqpResourceManager(ResourceManagers[0].NodeId());
    auto rm_second = GetKqpResourceManager(ResourceManagers[1].NodeId());

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm_first);
    CheckSnapshot(1, {{1000, 100}, {1000, 100}}, rm_second);

    // the memory of the node services is not published, the memory of the queries stays as it is
    NRm::TKqpResourcesRequest request{.ExecutionUnits = 10, .Memory = 100};

    auto tx1Rm1 = MakeTx(1, rm_first);
    auto tx2Rm1 = MakeTx(2, rm_first);
    auto task1Rm1 = 1;
    auto task2Rm1 = 2;

    auto tx1Rm2 = MakeTx(1, rm_second);
    auto tx2Rm2 = MakeTx(2, rm_second);
    auto task1Rm2 = 1;
    auto task2Rm2 = 2;

    {
        bool allocated = rm_first->AllocateResources(*tx1Rm1, task1Rm1, request);
        UNIT_ASSERT(allocated);

        allocated &= rm_first->AllocateResources(*tx2Rm1, task2Rm1, request);
        UNIT_ASSERT(allocated);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(0, {{1000, 80}, {1000, 100}}, rm_second);
    }

    {
        bool allocated = rm_second->AllocateResources(*tx1Rm2, task1Rm2, request);
        UNIT_ASSERT(allocated);

        allocated &= rm_second->AllocateResources(*tx2Rm2, task2Rm2, request);
        UNIT_ASSERT(allocated);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(1, {{1000, 80}, {1000, 80}}, rm_first);
    }

    {
        rm_first->FreeResources(*tx1Rm1, task1Rm1, request);
        rm_first->FreeResources(*tx2Rm1, task2Rm1, request);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(0, {{1000, 100}, {1000, 80}}, rm_second);
    }

    {
        rm_second->FreeResources(*tx1Rm2, task1Rm2, request);
        rm_second->FreeResources(*tx2Rm2, task2Rm2, request);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(1, {{1000, 100}, {1000, 100}}, rm_first);
    }
}

void KqpRm::SnapshotSharingByExchanger() {
    SnapshotSharing();
}

void KqpRm::NodesMembership() {
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm_first = GetKqpResourceManager(ResourceManagers[0].NodeId());
    auto rm_second = GetKqpResourceManager(ResourceManagers[1].NodeId());

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm_first);
    CheckSnapshot(1, {{1000, 100}, {1000, 100}}, rm_second);

    const TActorId edge = Runtime->AllocateEdgeActor(1);
    Runtime->Send(new IEventHandle(
        ResourceManagers[1], edge, new TEvents::TEvPoison, IEventHandle::FlagTrackDelivery, 0),
        1, false);

    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvents::TSystem::Poison, 1);
    UNIT_ASSERT(Runtime->DispatchEvents(options));

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}}, rm_first);
}


void KqpRm::NodesMembershipByExchanger() {
    NodesMembership();
}

void KqpRm::DisonnectNodes() {
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm_first = GetKqpResourceManager(ResourceManagers[0].NodeId());
    auto rm_second = GetKqpResourceManager(ResourceManagers[1].NodeId());

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm_first);
    CheckSnapshot(1, {{1000, 100}, {1000, 100}}, rm_second);

    auto prevObserverFunc = Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case NRm::TEvKqpResourceInfoExchanger::TEvSendResources::EventType: {
                return TTestActorRuntime::EEventAction::DROP;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

    Disconnect(0, 1);

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}}, rm_first);
}

// The query quota manager is the only path between the tx and the node resources: the execution units are taken from
// the resource manager, the memory - from the compute scheduler (the query in the pool with the memory limit 500, the
// optional memory within 80% of the limits). It holds the execution units and the external memory of the started tasks
// (plus the memory of a task per execution unit); the task and channel quota managers start with their part of that
// external memory as the initial limit, take Memory through AllocateQuota and FreeQuota and return, when they die, only
// the Memory they grew by. A terminated compute actor returns the execution unit and the initial limit of its task
// (FreeTasks), the rest is returned when the query quota manager dies. GetCurrentQuota() is the external memory and the
// Memory, without the memory of the tasks
void KqpRm::QueryQuotaManager() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto scheduler = MakeComputeScheduler();
    auto schedulerQuery = scheduler->AddOrUpdateQuery(DATABASE_ID, POOL_ID, 1, {});
    scheduler->UpdateFairShare();
    UNIT_ASSERT_VALUES_EQUAL(schedulerQuery->GetSnapshot()->MemoryFairShare, POOL_MEMORY_LIMIT);

    {
        auto tx = MakeTx(1, rm, POOL_ID, DATABASE_ID);
        auto memory = std::make_shared<NScheduler::TSchedulableMemory>(schedulerQuery, ELASTIC_MEMORY_PERCENT);
        const ui64 taskMemory = 10;
        auto query = CreateQueryQuotaManager(tx, memory, taskMemory);
        auto usage = [&]() -> ui64 {
            return scheduler->GetTotalMemoryUsage();
        };

        // the node service allocates 2 tasks (100 each, expected to grow by 150 each) and the channels (50): the
        // scheduler accounts the external memory and the memory of the tasks, the elastic memory is only the demand
        UNIT_ASSERT(query->AllocateTasks(2, 100 + 100 + 50, 150 + 150));
        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 250);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 250 + 2 * taskMemory);
        UNIT_ASSERT_VALUES_EQUAL(schedulerQuery->MemoryUsage.load(), 270);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryDemand(), 270 + 300);
        UNIT_ASSERT_VALUES_EQUAL(schedulerQuery->MemoryDemand.load(), 570);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxExecutionUnits.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(RmGauge("RM/ExternalMemory"), 250);
        AssertResourceManagerStats(rm, 0, 98);
        // the elastic part of the pool fair-share is 400: 130 left, the elastic part of the total limit (800) is farther
        UNIT_ASSERT_VALUES_EQUAL(query->GetMemoryAvailability(), 130);
        UNIT_ASSERT_VALUES_EQUAL(query->GetMemoryAvailability(), memory->GetAvailability());

        auto t1 = CreateTaskQuotaManager(query, 100, 16);
        auto t2 = CreateTaskQuotaManager(query, 100, 16);
        auto cm = CreateChannelQuotaManager(query, 50, 16);

        // 100 - 50 prepaid = 50, aligned to 64: granted by the scheduler
        UNIT_ASSERT(cm->AllocateQuota(100, /* isOptional = */ false));
        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 314);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 334);

        // a task grows past its initial limit: 150 - 100 = 50, aligned to 64
        UNIT_ASSERT(t1->AllocateQuota(150, /* isOptional = */ false));
        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 378);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 398);
        UNIT_ASSERT_VALUES_EQUAL(RmGauge("RM/Memory"), 128);
        UNIT_ASSERT_VALUES_EQUAL(query->GetMemoryAvailability(), 2);

        // the optional memory is refused quietly past the elastic part of the pool fair-share
        UNIT_ASSERT(!query->AllocateQuota(100, /* isOptional = */ true));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 0);
        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 378);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 398);

        // the mandatory memory is given up to the pool fair-share
        UNIT_ASSERT(query->AllocateQuota(100, /* isOptional = */ false));
        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 478);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 498);
        UNIT_ASSERT_VALUES_EQUAL(query->GetMemoryAvailability(), -98);
        UNIT_ASSERT_VALUES_EQUAL(query->GetMemoryAvailability(), memory->GetAvailability());
        // a negative value dominates the leftover of the task
        UNIT_ASSERT_VALUES_EQUAL(t1->GetMemoryAvailability(), -98);

        // and not past it: a failure
        UNIT_ASSERT(!query->AllocateQuota(10, /* isOptional = */ false));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 1);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1);
        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 478);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 498);
        UNIT_ASSERT_STRING_CONTAINS(query->MemoryConsumptionDetails(), "TxId: 1");

        query->FreeQuota(100);
        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 378);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 398);
        UNIT_ASSERT_VALUES_EQUAL(query->GetMaxMemorySize(), 478); // the peak stays

        // a start request the scheduler refuses gives the execution units back: 200 + 10 doesn't fit into the pool
        auto result = query->AllocateTasks(1, 200);
        UNIT_ASSERT(!result);
        UNIT_ASSERT(result.GetStatus() == NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 2);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxExecutionUnits.load(), 2);
        AssertResourceManagerStats(rm, 128, 98);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 398);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryDemand(), 570);

        // the one the resource manager refuses takes no memory
        result = query->AllocateTasks(1000, 0);
        UNIT_ASSERT(!result);
        UNIT_ASSERT(result.GetStatus() == NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 398);
        AssertResourceManagerStats(rm, 128, 98);

        // the children outlive the owner (the query manager actor dies before the compute actors are destroyed)
        std::weak_ptr<IQueryQuotaManager> weak = query;
        query.reset();
        // the compute actor of task 1 terminates: its execution unit, initial limit and elastic memory come back
        weak.lock()->FreeTasks(1, 100, 150);
        UNIT_ASSERT_VALUES_EQUAL(weak.lock()->GetCurrentQuota(), 278);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 288);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryDemand(), 310);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxExecutionUnits.load(), 1);
        AssertResourceManagerStats(rm, 128, 99);
        t1.reset(); // then its task quota manager dies and returns the 64 it grew by
        UNIT_ASSERT_VALUES_EQUAL(weak.lock()->GetCurrentQuota(), 214);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 224);
        t2.reset(); // no terminated compute actor: the initial limit stays with the query quota manager
        UNIT_ASSERT_VALUES_EQUAL(weak.lock()->GetCurrentQuota(), 214);
        cm->FreeQuota(100); // stays prepaid in the channel quota manager
        UNIT_ASSERT_VALUES_EQUAL(weak.lock()->GetCurrentQuota(), 214);
        UNIT_ASSERT_VALUES_EQUAL(usage(), 224);
        // the last child: the query quota manager dies and returns the rest, the external memory of task 2 and of the
        // channels and the execution unit of task 2
        cm.reset();
        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT_VALUES_EQUAL(usage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryDemand(), 0);
        UNIT_ASSERT_VALUES_EQUAL(schedulerQuery->MemoryUsage.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(schedulerQuery->MemoryDemand.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxExecutionUnits.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(RmGauge("RM/ExternalMemory"), 0);
    }

    AssertResourceManagerStats(rm, 0, 100);
}

// The memory of a tx without the query node (e.g. of the default pool) is limited only by the total limit of the
// scheduler (1000, the optional memory within 800)
void KqpRm::QueryQuotaManagerDefaultPool() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto scheduler = MakeComputeScheduler();
    auto pool = scheduler->GetOrCreateMemoryPool(DATABASE_ID, "");
    scheduler->UpdateFairShare();

    {
        auto tx = MakeTx(1, rm, "", DATABASE_ID);
        auto memory = std::make_shared<NScheduler::TSchedulableMemory>(pool, ELASTIC_MEMORY_PERCENT);
        auto query = CreateQueryQuotaManager(tx, memory, /* taskMemory */ 0);

        UNIT_ASSERT(query->AllocateTasks(1, 700));
        UNIT_ASSERT_VALUES_EQUAL(pool->MemoryUsage.load(), 700);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryUsage(), 700);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryDemand(), 700);
        UNIT_ASSERT_VALUES_EQUAL(query->GetMemoryAvailability(), 100);

        UNIT_ASSERT(!query->AllocateQuota(200, /* isOptional = */ true));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1);
        UNIT_ASSERT(query->AllocateQuota(200, /* isOptional = */ false));
        UNIT_ASSERT_VALUES_EQUAL(query->GetMemoryAvailability(), -100);

        auto result = query->AllocateTasks(1, 200);
        UNIT_ASSERT(!result);
        UNIT_ASSERT(result.GetStatus() == NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 1);
        AssertResourceManagerStats(rm, 200, 99);

        // up to the total limit, not past it
        UNIT_ASSERT(query->AllocateQuota(100, /* isOptional = */ false));
        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryUsage(), 1000);
        UNIT_ASSERT(!query->AllocateQuota(1, /* isOptional = */ false));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 2);
        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 1000);

        query->FreeQuota(300);
    }

    UNIT_ASSERT_VALUES_EQUAL(pool->MemoryUsage.load(), 0);
    UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryUsage(), 0);
    UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryDemand(), 0);
    AssertResourceManagerStats(rm, 0, 100);
}

// Without the compute scheduler the memory is only accounted, not limited; the execution units are still limited
void KqpRm::QueryQuotaManagerWithoutMemory() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        auto query = CreateQueryQuotaManager(tx, nullptr, /* taskMemory */ 10);

        UNIT_ASSERT(query->AllocateTasks(2, 100'000, 50'000));
        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 100'000);
        UNIT_ASSERT_VALUES_EQUAL(RmGauge("RM/ExternalMemory"), 100'000);
        AssertResourceManagerStats(rm, 0, 98);

        UNIT_ASSERT(query->AllocateQuota(1'000'000, /* isOptional = */ true));
        UNIT_ASSERT(query->AllocateQuota(1'000'000, /* isOptional = */ false));
        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 2'100'000);
        UNIT_ASSERT_VALUES_EQUAL(query->GetMemoryAvailability(), std::numeric_limits<i64>::max());
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 0);
        AssertResourceManagerStats(rm, 2'000'000, 98);

        auto result = query->AllocateTasks(1000, 0);
        UNIT_ASSERT(!result);
        UNIT_ASSERT(result.GetStatus() == NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS);

        query->FreeQuota(2'000'000);
        query->FreeTasks(1, 50'000, 25'000);
        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 50'000);
        AssertResourceManagerStats(rm, 0, 99);
    }

    UNIT_ASSERT_VALUES_EQUAL(RmGauge("RM/ExternalMemory"), 0);
    AssertResourceManagerStats(rm, 0, 100);
}

// The task and channel quota managers pass the optional flag on to the scheduler through the query quota manager:
// the optional memory is refused quietly past the elastic part of the limits (the default pool here: the total limit
// 1000, the elastic part 800), RM/OptionalMemoryRefused counts it, RM/NotEnoughMemory does not
void KqpRm::TaskQuotaManagerOptional() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto scheduler = MakeComputeScheduler();
    auto memory = std::make_shared<NScheduler::TSchedulableMemory>(
        scheduler->GetOrCreateMemoryPool(DATABASE_ID, ""), ELASTIC_MEMORY_PERCENT);
    scheduler->UpdateFairShare();
    auto usage = [&]() -> ui64 {
        return scheduler->GetTotalMemoryUsage();
    };

    {
        auto tx = MakeTx(1, rm, "", DATABASE_ID);
        const ui64 initialLimit = 100;
        {
            auto query = CreateQueryQuotaManager(tx, memory, /* taskMemory */ 0);
            // the node service allocates the initial limit as external memory before the task starts
            UNIT_ASSERT(query->AllocateTasks(1, initialLimit));
            UNIT_ASSERT_VALUES_EQUAL(usage(), 100);
            UNIT_ASSERT_VALUES_EQUAL(query->GetMemoryAvailability(), 700);

            // task level manager: 1 MB allocation step, more than the scheduler has
            auto qm = CreateTaskQuotaManager(query, initialLimit);
            UNIT_ASSERT(qm->AllocateQuota(50, /* isOptional = */ true)); // fits in the initial limit
            UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), 700 + 50); // the scheduler value plus the local leftover
            UNIT_ASSERT(!qm->AllocateQuota(1500, /* isOptional = */ true)); // the 1 MB step is past the elastic part
            UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1); // the flag reached the scheduler
            UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 0);
            UNIT_ASSERT_VALUES_EQUAL(usage(), 100);
            UNIT_ASSERT(!qm->AllocateQuota(1500, /* isOptional = */ false)); // the same request, mandatory: a failure
            UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1);
            UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 1);
            UNIT_ASSERT_VALUES_EQUAL(usage(), 100);
            qm->FreeQuota(50);
            qm.reset();
            // the initial limit and the execution unit stay with the query quota manager till the compute actor
            // terminates (FreeTasks) or the query quota manager dies
            UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 100);
            UNIT_ASSERT_VALUES_EQUAL(usage(), 100);
            UNIT_ASSERT_VALUES_EQUAL(tx->TxExecutionUnits.load(), 1);
        }
        // the query quota manager returned them
        UNIT_ASSERT_VALUES_EQUAL(usage(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxExecutionUnits.load(), 0);

        {
            auto query = CreateQueryQuotaManager(tx, memory, /* taskMemory */ 0);
            // channel level manager: 16 byte allocation step
            auto cm = CreateChannelQuotaManager(query, 0, 16);
            UNIT_ASSERT(query->AllocateQuota(600, /* isOptional = */ false)); // availability 200
            UNIT_ASSERT(!cm->AllocateQuota(200, /* isOptional = */ true)); // 208 > 200
            UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 2);
            UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), 200); // nothing prepaid, nothing lost
            UNIT_ASSERT_VALUES_EQUAL(usage(), 600);

            UNIT_ASSERT(cm->AllocateQuota(150, /* isOptional = */ true)); // 160 <= 200: granted
            UNIT_ASSERT_VALUES_EQUAL(query->GetMemoryAvailability(), 40);
            UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), 10 + 40); // 10 bytes of the step prepaid
            UNIT_ASSERT_VALUES_EQUAL(usage(), 600 + 160);
            UNIT_ASSERT(!cm->AllocateQuota(100, /* isOptional = */ true)); // 100 - 10 prepaid = 90, aligned to 96 > 40
            UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 3);
            UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), 10 + 40); // the prepaid quota is restored
            UNIT_ASSERT(cm->AllocateQuota(100, /* isOptional = */ false)); // mandatory: past the elastic part, up to the limit
            UNIT_ASSERT_VALUES_EQUAL(query->GetMemoryAvailability(), -56);
            UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), -56); // a negative value dominates the 6 prepaid
            UNIT_ASSERT_VALUES_EQUAL(usage(), 600 + 160 + 96);

            // a mandatory request beyond the limit is a failure: 300 - 6 prepaid = 294, aligned to 304 > 144
            UNIT_ASSERT(!cm->AllocateQuota(300, /* isOptional = */ false));
            UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 2);
            UNIT_ASSERT_VALUES_EQUAL(usage(), 600 + 160 + 96);

            // a small request (below 10 steps) the scheduler cannot cover: a mandatory one is tolerated as
            // over-quoting, an optional one is refused
            UNIT_ASSERT(query->AllocateQuota(120, /* isOptional = */ false)); // 24 bytes left
            UNIT_ASSERT(!cm->AllocateQuota(100, /* isOptional = */ true)); // 100 - 6 prepaid = 94, aligned to 96 > 24
            UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 4);
            UNIT_ASSERT(cm->AllocateQuota(100, /* isOptional = */ false)); // refused by the scheduler, over-quoted
            UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 3);
            UNIT_ASSERT_VALUES_EQUAL(usage(), 600 + 160 + 96 + 120); // the over-quoted bytes never reached the scheduler
            UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 600 + 160 + 96 + 120);

            cm->FreeQuota(100);
            cm->FreeQuota(100);
            cm->FreeQuota(150);
            UNIT_ASSERT_VALUES_EQUAL(usage(), 600 + 160 + 96 + 120); // the 256 stay prepaid in the channel manager until it dies
            query->FreeQuota(120);
            query->FreeQuota(600);
            UNIT_ASSERT_VALUES_EQUAL(usage(), 160 + 96);
            cm.reset();
            UNIT_ASSERT_VALUES_EQUAL(usage(), 0);
            UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 0);
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryDemand(), 0);
    AssertResourceManagerStats(rm, 0, 100);
}

void KqpRm::ConcurrentChannels() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto scheduler = MakeComputeScheduler();
    auto memory = std::make_shared<NScheduler::TSchedulableMemory>(
        scheduler->GetOrCreateMemoryPool(DATABASE_ID, ""), ELASTIC_MEMORY_PERCENT);
    scheduler->UpdateFairShare();

    {
        auto tx = MakeTx(1, rm, "", DATABASE_ID);
        auto query = CreateQueryQuotaManager(tx, memory, /* taskMemory */ 0);

        {
            auto qm = CreateChannelQuotaManager(query, 0, 16);

            NPar::LocalExecutor().RunAdditionalThreads(10);
            std::atomic<ui64> failedAllocations = 0;

            NPar::LocalExecutor().ExecRange([&](int) {
                auto count = 0u;
                for (auto n = 0u; n < 20u; n++) {
                    for (auto j = 0u; j < 20u; j++) {
                        if (!qm->AllocateQuota(j * 10u, /* isOptional = */ false)) {
                            failedAllocations++;
                            Sleep(TDuration::MilliSeconds(j * 10));
                            break;
                        }
                        count += j;
                    }
                    for (auto j = 20u; j > 0u; j--) {
                        if (count < j) {
                            break;
                        }
                        qm->FreeQuota(j * 10u);
                        count -= j;
                    }
                }
                qm->FreeQuota(count * 10u);
            }, 0, 10, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);

            UNIT_ASSERT_GT(failedAllocations.load(), 0);
            // every grant of the channel quota manager is taken from the scheduler by the query quota manager
            UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), scheduler->GetTotalMemoryUsage());
            UNIT_ASSERT_LE(scheduler->GetTotalMemoryUsage(), QUERY_MEMORY_LIMIT);
            UNIT_ASSERT_GT(query->GetMaxMemorySize(), 0);

            // the channel quota manager exposes the availability of the scheduler memory plus its prepaid quota,
            // DQ channels 2.0 propagate a negative value to the senders as back pressure
            const i64 availability = memory->GetAvailability();
            if (availability < 0) {
                UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), availability);
            } else {
                UNIT_ASSERT_GE(qm->GetMemoryAvailability(), availability);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(query->GetCurrentQuota(), 0);
        UNIT_ASSERT_VALUES_EQUAL(scheduler->GetTotalMemoryUsage(), 0);
    }

    AssertResourceManagerStats(rm, 0, 100);
}

// A heavy program (MapJoin or StateAggregation) is expected to grow up to the heavy limit, a light one - up to the
// light limit, both only beyond their initial limits
void KqpRm::TaskElasticMemory() {
    const ui64 lightLimit = 1'000;
    const ui64 heavyLimit = 30'000;

    NYql::NDqProto::TDqTask light;
    UNIT_ASSERT(!IsHeavyProgram(light));
    UNIT_ASSERT_VALUES_EQUAL(EstimateTaskElasticMemory(light, lightLimit, lightLimit, heavyLimit), 0);
    UNIT_ASSERT_VALUES_EQUAL(EstimateTaskElasticMemory(light, 2 * lightLimit, lightLimit, heavyLimit), 0);
    UNIT_ASSERT_VALUES_EQUAL(EstimateTaskElasticMemory(light, heavyLimit, lightLimit, heavyLimit), 0);
    UNIT_ASSERT_VALUES_EQUAL(EstimateTaskElasticMemory(light, 400, lightLimit, heavyLimit), lightLimit - 400);

    NYql::NDqProto::TDqTask mapJoin;
    mapJoin.MutableProgram()->MutableSettings()->SetHasMapJoin(true);
    NYql::NDqProto::TDqTask stateAggregation;
    stateAggregation.MutableProgram()->MutableSettings()->SetHasStateAggregation(true);

    for (const auto* heavy : {&mapJoin, &stateAggregation}) {
        UNIT_ASSERT(IsHeavyProgram(*heavy));
        UNIT_ASSERT_VALUES_EQUAL(EstimateTaskElasticMemory(*heavy, lightLimit, lightLimit, heavyLimit), heavyLimit - lightLimit);
        UNIT_ASSERT_VALUES_EQUAL(EstimateTaskElasticMemory(*heavy, 0, lightLimit, heavyLimit), heavyLimit);
        UNIT_ASSERT_VALUES_EQUAL(EstimateTaskElasticMemory(*heavy, heavyLimit, lightLimit, heavyLimit), 0);
        UNIT_ASSERT_VALUES_EQUAL(EstimateTaskElasticMemory(*heavy, heavyLimit + 1, lightLimit, heavyLimit), 0);
    }
}

} // namespace NKqp
} // namespace NKikimr
