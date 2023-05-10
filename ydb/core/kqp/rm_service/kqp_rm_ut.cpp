#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/tablet/resource_broker_impl.h>

#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

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
    queue->MutableLimit()->AddResource(50'000);

    auto task = config.AddTasks();
    task->SetName("unknown");
    task->SetQueueName("queue_default");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::KqpResourceManagerTaskName);
    task->SetQueueName("queue_kqp_resource_manager");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    config.MutableResourceLimit()->AddResource(10);
    config.MutableResourceLimit()->AddResource(100'000);

    return config;
}

NKikimrConfig::TTableServiceConfig::TResourceManager MakeKqpResourceManagerConfig() {
    NKikimrConfig::TTableServiceConfig::TResourceManager config;

    config.SetComputeActorsCount(100);
    config.SetPublishStatisticsIntervalSec(0);
    config.SetQueryMemoryLimit(1000);

    return config;
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

        auto resourceBrokerConfig = MakeResourceBrokerTestConfig();
        auto broker = CreateResourceBrokerActor(resourceBrokerConfig, Counters);
        ResourceBrokerActorId = Runtime->Register(broker);
        WaitForBootstrap();
    }

    void WaitForBootstrap() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(Runtime->DispatchEvents(options));
    }

    IActor* CreateKqpResourceManager(const NKikimrConfig::TTableServiceConfig::TResourceManager& config = {}) {
        auto kqpCounters = MakeIntrusive<TKqpCounters>(Counters);
        auto resman = CreateKqpResourceManagerActor(config, kqpCounters, ResourceBrokerActorId);
        ResourceManagerActorId = Runtime->Register(resman);
        Runtime->EnableScheduleForActor(ResourceManagerActorId, true);
        WaitForBootstrap();
        return resman;
    }

    void AssertResourceBrokerSensors(i64 cpu, i64 mem, i64 enqueued, i64 finished, i64 infly) {
        auto q = Counters->GetSubgroup("queue", "queue_kqp_resource_manager");
//        Cerr << "-- queue_kqp_resource_manager\n";
//        q->OutputPlainText(Cerr, " ");
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("CPUConsumption")->Val(), cpu);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("MemoryConsumption")->Val(), mem);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("EnqueuedTasks")->Val(), enqueued);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("FinishedTasks")->Val(), finished);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("InFlyTasks")->Val(), infly);

        auto t = Counters->GetSubgroup("task", "kqp_query");
//        Cerr << "-- kqp_query\n";
//        t->OutputPlainText(Cerr, " ");
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("CPUConsumption")->Val(), cpu);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("MemoryConsumption")->Val(), mem);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("EnqueuedTasks")->Val(), enqueued);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("FinishedTasks")->Val(), finished);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("InFlyTasks")->Val(), infly);
    }

    void AssertResourceManagerStats(std::shared_ptr<NRm::IKqpResourceManager> rm, ui64 scanQueryMemory, ui32 executionUnits) {
        auto stats = rm->GetLocalResources();
        UNIT_ASSERT_VALUES_EQUAL(scanQueryMemory, stats.Memory[NRm::EKqpMemoryPool::ScanQuery]);
        UNIT_ASSERT_VALUES_EQUAL(executionUnits, stats.ExecutionUnits);
    }

    UNIT_TEST_SUITE(KqpRm);
        UNIT_TEST(SingleTask);
        UNIT_TEST(ManyTasks);
        UNIT_TEST(NotEnoughMemory);
        UNIT_TEST(NotEnoughExecutionUnits);
        UNIT_TEST(ResourceBrokerNotEnoughResources);
        UNIT_TEST(Snapshot);
    UNIT_TEST_SUITE_END();

    void SingleTask();
    void ManyTasks();
    void NotEnoughMemory();
    void NotEnoughExecutionUnits();
    void ResourceBrokerNotEnoughResources();
    void Snapshot();

private:
    THolder<TTestBasicRuntime> Runtime;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    // TIntrusivePtr<TKqpCounters> KqpCounters;
    TActorId ResourceBrokerActorId;
    TActorId ResourceManagerActorId;
};
UNIT_TEST_SUITE_REGISTRATION(KqpRm);


void KqpRm::SingleTask() {
    CreateKqpResourceManager(MakeKqpResourceManagerConfig());
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagerActorId.NodeId());

    NRm::TKqpResourcesRequest request;
    request.ExecutionUnits = 10;
    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 100;

    bool allocated = rm->AllocateResources(1, 2, request);
    UNIT_ASSERT(allocated);

    AssertResourceManagerStats(rm, 900, 90);
    AssertResourceBrokerSensors(0, 100, 0, 0, 1);

    rm->FreeResources(1, 2);
    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
}

void KqpRm::ManyTasks() {
    CreateKqpResourceManager(MakeKqpResourceManagerConfig());
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagerActorId.NodeId());

    NRm::TKqpResourcesRequest request;
    request.ExecutionUnits = 10;
    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 100;

    for (ui32 i = 1; i < 10; ++i) {
        bool allocated = rm->AllocateResources(1, i, request);
        UNIT_ASSERT(allocated);

        AssertResourceManagerStats(rm, 1000 - 100 * i, 100 - 10 * i);
        AssertResourceBrokerSensors(0, 100 * i, 0, 0, i);
    }

    // invalid taskId
    rm->FreeResources(1, 0);
    AssertResourceManagerStats(rm, 100, 10);
    AssertResourceBrokerSensors(0, 900, 0, 0, 9);

    // invalid txId
    rm->FreeResources(10, 1);
    AssertResourceManagerStats(rm, 100, 10);
    AssertResourceBrokerSensors(0, 900, 0, 0, 9);

    rm->FreeResources(1, 1);
    AssertResourceManagerStats(rm, 200, 20);
    AssertResourceBrokerSensors(0, 800, 0, 1, 8);

    rm->FreeResources(1);
    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 9, 0);
}

void KqpRm::NotEnoughMemory() {
    CreateKqpResourceManager(MakeKqpResourceManagerConfig());
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagerActorId.NodeId());

    NRm::TKqpResourcesRequest request;
    request.ExecutionUnits = 10;
    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 10'000;

    bool allocated = rm->AllocateResources(1, 2, request);
    UNIT_ASSERT(!allocated);

    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
}

void KqpRm::NotEnoughExecutionUnits() {
    CreateKqpResourceManager(MakeKqpResourceManagerConfig());
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagerActorId.NodeId());

    NRm::TKqpResourcesRequest request;
    request.ExecutionUnits = 1000;
    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 100;

    bool allocated = rm->AllocateResources(1, 2, request);
    UNIT_ASSERT(!allocated);

    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
}

void KqpRm::ResourceBrokerNotEnoughResources() {
    auto config = MakeKqpResourceManagerConfig();
    config.SetQueryMemoryLimit(100000000);
    CreateKqpResourceManager(config);
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagerActorId.NodeId());

    NRm::TKqpResourcesRequest request;
    request.ExecutionUnits = 10;
    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 1'000;

    bool allocated = rm->AllocateResources(1, 2, request);
    UNIT_ASSERT(allocated);

    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 100'000;
    allocated = rm->AllocateResources(1, 2, request);
    UNIT_ASSERT(!allocated);

    AssertResourceManagerStats(rm, config.GetQueryMemoryLimit() - 1000, 90);
    AssertResourceBrokerSensors(0, 1000, 0, 0, 1);
}

void KqpRm::Snapshot() {
    CreateKqpResourceManager(MakeKqpResourceManagerConfig());
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagerActorId.NodeId());

    NRm::TKqpResourcesRequest request;
    request.ExecutionUnits = 10;
    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 100;

    bool allocated = rm->AllocateResources(1, 2, request);
    UNIT_ASSERT(allocated);

    allocated = rm->AllocateResources(2, 1, request);
    UNIT_ASSERT(allocated);

    AssertResourceManagerStats(rm, 800, 80);
    AssertResourceBrokerSensors(0, 200, 0, 0, 2);

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(500));

    {
        TVector<NKikimrKqp::TKqpNodeResources> snapshot;
        std::atomic<int> ready = 0;
        rm->RequestClusterResourcesInfo([&](TVector<NKikimrKqp::TKqpNodeResources>&& resources) {
            snapshot = std::move(resources);
            ready = 1;
        });

        while (ready.load() != 1) {
            Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        }

        UNIT_ASSERT_VALUES_EQUAL(1, snapshot.size());
        UNIT_ASSERT_VALUES_EQUAL(80, snapshot[0].GetExecutionUnits());
        UNIT_ASSERT_VALUES_EQUAL(1, snapshot[0].GetMemory().size());
        UNIT_ASSERT_VALUES_EQUAL(1, snapshot[0].GetMemory()[0].GetPool());
        UNIT_ASSERT_VALUES_EQUAL(800, snapshot[0].GetMemory()[0].GetAvailable());
    }

    rm->FreeResources(1);
    rm->FreeResources(2);
    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 2, 0);

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(500));

    {
        TVector<NKikimrKqp::TKqpNodeResources> snapshot;
        std::atomic<int> ready = 0;
        rm->RequestClusterResourcesInfo([&](TVector<NKikimrKqp::TKqpNodeResources>&& resources) {
            snapshot = std::move(resources);
            ready = 1;
        });

        while (ready.load() != 1) {
            Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        }

        UNIT_ASSERT_VALUES_EQUAL(1, snapshot.size());
        UNIT_ASSERT_VALUES_EQUAL(100, snapshot[0].GetExecutionUnits());
        UNIT_ASSERT_VALUES_EQUAL(1, snapshot[0].GetMemory().size());
        UNIT_ASSERT_VALUES_EQUAL(1, snapshot[0].GetMemory()[0].GetPool());
        UNIT_ASSERT_VALUES_EQUAL(1000, snapshot[0].GetMemory()[0].GetAvailable());
    }
}

} // namespace NKqp
} // namespace NKikimr
