#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/tablet/resource_broker_impl.h>

#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/kqp/common/simple/services.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>

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

NKikimrConfig::TTableServiceConfig::TResourceManager MakeKqpResourceManagerConfig(
        bool EnablePublishResourcesByExchanger = false) {
    NKikimrConfig::TTableServiceConfig::TResourceManager config;

    config.SetComputeActorsCount(100);
    config.SetPublishStatisticsIntervalSec(0);
    config.SetQueryMemoryLimit(1000);
    config.SetEnablePublishResourcesByExchanger(EnablePublishResourcesByExchanger);

    auto* infoExchangerRetrySettings = config.MutableInfoExchangerSettings();
    auto* exchangerSettings = infoExchangerRetrySettings->MutableExchangerSettings();
    exchangerSettings->SetStartDelayMs(50);
    exchangerSettings->SetMaxDelayMs(50);

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

        for (ui32 nodeIndex = 0; nodeIndex < Runtime->GetNodeCount(); ++nodeIndex) {
            auto resourceBrokerConfig = MakeResourceBrokerTestConfig();
            auto broker = CreateResourceBrokerActor(resourceBrokerConfig, Counters);
            auto resourceBrokerActorId = Runtime->Register(broker, nodeIndex);
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
        auto resman = CreateKqpResourceManagerActor(config, kqpCounters, ResourceBrokers[nodeInd]);
        ResourceManagers.push_back(Runtime->Register(resman, nodeInd));
        Runtime->RegisterService(MakeKqpResourceManagerServiceID(
            Runtime->GetNodeId(nodeInd)), ResourceManagers.back(), nodeInd);
        Runtime->EnableScheduleForActor(ResourceManagers.back(), true);
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

    void AssertResourceBrokerSensors(i64 cpu, i64 mem, i64 enqueued, i64 finished, i64 infly) {
        auto q = Counters->GetSubgroup("queue", "queue_kqp_resource_manager");
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("CPUConsumption")->Val(), cpu);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("MemoryConsumption")->Val(), mem);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("EnqueuedTasks")->Val(), enqueued);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("FinishedTasks")->Val(), finished);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("InFlyTasks")->Val(), infly);

        auto t = Counters->GetSubgroup("task", "kqp_query");
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("CPUConsumption")->Val(), cpu);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("MemoryConsumption")->Val(), mem);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("EnqueuedTasks")->Val(), enqueued);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("FinishedTasks")->Val(), finished);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("InFlyTasks")->Val(), infly);
    }

    void AssertResourceManagerStats(
            std::shared_ptr<NRm::IKqpResourceManager> rm, ui64 scanQueryMemory, ui32 executionUnits) {
        Y_UNUSED(executionUnits);
        auto stats = rm->GetLocalResources();
        UNIT_ASSERT_VALUES_EQUAL(scanQueryMemory, stats.Memory[NRm::EKqpMemoryPool::ScanQuery]);
        // UNIT_ASSERT_VALUES_EQUAL(executionUnits, stats.ExecutionUnits);
    }

    void AssertResourceManagerStatsExecutionUnits( std::shared_ptr<NRm::IKqpResourceManager> rm, ui32 executionUnits) {
        auto stats = rm->GetLocalResources();
        UNIT_ASSERT_VALUES_EQUAL(executionUnits, stats.ExecutionUnits);
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
        UNIT_TEST(SingleSnapshotByStateStorage);
        UNIT_TEST(SingleSnapshotByExchanger);
        UNIT_TEST(Reduce);
        UNIT_TEST(SnapshotSharingByStateStorage);
        UNIT_TEST(SnapshotSharingByExchanger);
        UNIT_TEST(NodesMembershipByStateStorage);
        UNIT_TEST(NodesMembershipByExchanger);
        UNIT_TEST(DisonnectNodes);
    UNIT_TEST_SUITE_END();

    void SingleTask();
    void ManyTasks();
    void NotEnoughMemory();
    void NotEnoughExecutionUnits();
    void ResourceBrokerNotEnoughResources();
    void Snapshot(bool byExchanger);
    void SingleSnapshotByStateStorage();
    void SingleSnapshotByExchanger();
    void Reduce();
    void SnapshotSharing(bool byExchanger);
    void SnapshotSharingByStateStorage();
    void SnapshotSharingByExchanger();
    void NodesMembership(bool byExchanger);
    void NodesMembershipByStateStorage();
    void NodesMembershipByExchanger();
    void DisonnectNodes();

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
    UNIT_ASSERT_VALUES_EQUAL(1000, stats.Memory[NRm::EKqpMemoryPool::ScanQuery]);

    NRm::TKqpResourcesRequest request;
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
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    NRm::TKqpResourcesRequest request;
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
}

void KqpRm::NotEnoughMemory() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    NRm::TKqpResourcesRequest request;
    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 10'000;

    bool allocated = rm->AllocateResources(1, 2, request);
    UNIT_ASSERT(!allocated);

    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
}

void KqpRm::NotEnoughExecutionUnits() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    NRm::TKqpResourcesRequest request;
    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 100;
    request.ExecutionUnits = 1000;

    bool allocated = true;
    allocated &= rm->AllocateResources(1, 2, request);
    UNIT_ASSERT(!allocated);

    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
}

void KqpRm::ResourceBrokerNotEnoughResources() {
    auto config = MakeKqpResourceManagerConfig();
    config.SetQueryMemoryLimit(100000000);

    StartRms({config, MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    NRm::TKqpResourcesRequest request;
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

void KqpRm::Snapshot(bool byExchanger) {
    StartRms({MakeKqpResourceManagerConfig(byExchanger), MakeKqpResourceManagerConfig(byExchanger)});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    NRm::TKqpResourcesRequest request;
    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 100;
    request.ExecutionUnits = 10;

    bool allocated = rm->AllocateResources(1, 2, request);
    UNIT_ASSERT(allocated);

    allocated &= rm->AllocateResources(2, 1, request);
    UNIT_ASSERT(allocated);

    AssertResourceManagerStats(rm, 800, 80);
    AssertResourceBrokerSensors(0, 200, 0, 0, 2);

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{800, 80}, {1000, 100}}, rm);

    rm->FreeResources(1, 2);
    rm->FreeResources(2, 1);

    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 2, 0);

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm);
}

void KqpRm::SingleSnapshotByStateStorage() {
    Snapshot(false);
}

void KqpRm::SingleSnapshotByExchanger() {
    Snapshot(true);
}

void KqpRm::Reduce() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    NRm::TKqpResourcesRequest request;
    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 100;

    bool allocated = rm->AllocateResources(1, 1, request);
    UNIT_ASSERT(allocated);

    AssertResourceManagerStats(rm, 1000 - 100, 100 - 10);
    AssertResourceBrokerSensors(0, 100, 0, 0, 1);

    NRm::TKqpResourcesRequest reduceRequest;
    reduceRequest.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    reduceRequest.Memory = 70;

    // invalid taskId
    rm->FreeResources(1, 0);
    AssertResourceManagerStats(rm, 1000 - 100, 100 - 10);
    AssertResourceBrokerSensors(0, 100, 0, 0, 1);

    // invalid txId
    rm->FreeResources(10, 1);
    AssertResourceManagerStats(rm, 1000 - 100, 100 - 10);
    AssertResourceBrokerSensors(0, 100, 0, 0, 1);

    rm->FreeResources(1, 1, reduceRequest);
    AssertResourceManagerStats(rm, 1000 - 30, 100 - 7);
    AssertResourceBrokerSensors(0, 30, 0, 0, 1);
}

void KqpRm::SnapshotSharing(bool byExchanger) {
    StartRms({MakeKqpResourceManagerConfig(byExchanger), MakeKqpResourceManagerConfig(byExchanger)});
    NKikimr::TActorSystemStub stub;

    auto rm_first = GetKqpResourceManager(ResourceManagers[0].NodeId());
    auto rm_second = GetKqpResourceManager(ResourceManagers[1].NodeId());

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm_first);
    CheckSnapshot(1, {{1000, 100}, {1000, 100}}, rm_second);

    NRm::TKqpResourcesRequest request;
    request.MemoryPool = NRm::EKqpMemoryPool::ScanQuery;
    request.Memory = 100;
    request.ExecutionUnits = 10;

    {
        bool allocated = rm_first->AllocateResources(1, 2, request);
        UNIT_ASSERT(allocated);

        allocated &= rm_first->AllocateResources(2, 1, request);
        UNIT_ASSERT(allocated);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(0, {{800, 80}, {1000, 100}}, rm_second);
    }

    {
        bool allocated = rm_second->AllocateResources(1, 2, request);
        UNIT_ASSERT(allocated);

        allocated &= rm_second->AllocateResources(2, 1, request);
        UNIT_ASSERT(allocated);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(1, {{800, 80}, {800, 80}}, rm_first);
    }

    {
        rm_first->FreeResources(1, 2);
        rm_first->FreeResources(2, 1);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(0, {{1000, 100}, {800, 80}}, rm_second);
    }

    {
        rm_second->FreeResources(1, 2);
        rm_second->FreeResources(2, 1);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(1, {{1000, 100}, {1000, 100}}, rm_first);
    }
}

void KqpRm::SnapshotSharingByStateStorage() {
    SnapshotSharing(false);
}

void KqpRm::SnapshotSharingByExchanger() {
    SnapshotSharing(true);
}

void KqpRm::NodesMembership(bool byExchanger) {
    StartRms({MakeKqpResourceManagerConfig(byExchanger), MakeKqpResourceManagerConfig(byExchanger)});
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

void KqpRm::NodesMembershipByStateStorage() {
    NodesMembership(false);
}

void KqpRm::NodesMembershipByExchanger() {
    NodesMembership(true);
}

void KqpRm::DisonnectNodes() {
    StartRms({MakeKqpResourceManagerConfig(true), MakeKqpResourceManagerConfig(true)});
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

} // namespace NKqp
} // namespace NKikimr
