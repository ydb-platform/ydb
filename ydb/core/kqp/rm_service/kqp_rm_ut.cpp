#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/rm_service/kqp_rm_memory_quota.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/tablet/resource_broker_impl.h>

#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/node_service/kqp_query_control_plane.h>

#include <ydb/library/actors/core/interconnect.h>
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
        auto resman = CreateKqpResourceManagerActor(config, kqpCounters, ResourceBrokers[nodeInd], nullptr, Runtime->GetNodeId(nodeInd));
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

    void SetSpillingPercent(double spillingPercent) {
        auto config = MakeKqpResourceManagerConfig();
        config.SetSpillingPercent(spillingPercent);

        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        request->Record.MutableConfig()->MutableTableServiceConfig()->MutableResourceManager()->CopyFrom(config);

        auto edge = Runtime->AllocateEdgeActor();
        Runtime->Send(new IEventHandle(ResourceManagers.front(), edge, request.Release()), 0, true);
        Runtime->GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(edge);
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
            const TString& poolId = "", double memoryPoolPercent = 100) {
        return MakeIntrusive<NRm::TTxState>(rm, txId, TInstant::Now(), poolId, memoryPoolPercent, "", false);
    }

    TIntrusivePtr<NRm::TTxState> MakePoolTx(ui64 txId, std::shared_ptr<NRm::IKqpResourceManager> rm, double memoryPoolPercent) {
        return MakeIntrusive<NRm::TTxState>(rm, txId, TInstant::Now(), "pool", memoryPoolPercent, "db", false);
    }

    void AssertResourceManagerStats(
            std::shared_ptr<NRm::IKqpResourceManager> rm, ui64 scanQueryMemory, ui32 executionUnits) {
        Y_UNUSED(executionUnits);
        auto stats = rm->GetLocalResources();
        UNIT_ASSERT_VALUES_EQUAL(scanQueryMemory, stats.Memory);
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
        UNIT_TEST(SingleSnapshotByExchanger);
        UNIT_TEST(Reduce);
        UNIT_TEST(ConcurrentTasks);
        UNIT_TEST(ConcurrentChannels);
        UNIT_TEST(MemoryAvailability);
        UNIT_TEST(PoolMemoryAvailability);
        UNIT_TEST(PoolMemoryAvailabilityAfterRelease);
        UNIT_TEST(PoolLimitFollowsAllocatingTx);
        UNIT_TEST(PoolReleaseMirrorsAcquire);
        UNIT_TEST(SpillingPercentReconfigure);
        UNIT_TEST(TotalLimitReconfigure);
        UNIT_TEST(TaskQuotaManagerOptional);
        UNIT_TEST(ServiceMemoryQuota);
        UNIT_TEST(ConcurrentServiceMemoryQuota);
        UNIT_TEST(SnapshotSharingByExchanger);
        UNIT_TEST(NodesMembershipByExchanger);
        UNIT_TEST(DisonnectNodes);
        UNIT_TEST(PoolLimitNotReleasedByUnchargedQuery);
        UNIT_TEST(PoolLimitNotReleasedByUnchargedQueryOnRollback);
        UNIT_TEST(PoolLimitNotChargedByHundredPercentQuery);
        UNIT_TEST(PoolLimitNotChargedForDefaultPool);
        UNIT_TEST(PoolLimitIgnoredForSenselessPercents);
        UNIT_TEST(PoolLimitAppliedJustBelowHundredPercent);
        UNIT_TEST(SpillingPercentAppliedWithoutPoolLimit);
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
    void ConcurrentChannels();
    void MemoryAvailability();
    void PoolMemoryAvailability();
    void PoolMemoryAvailabilityAfterRelease();
    void PoolLimitFollowsAllocatingTx();
    void PoolReleaseMirrorsAcquire();
    void SpillingPercentReconfigure();
    void TotalLimitReconfigure();
    void TaskQuotaManagerOptional();
    void ServiceMemoryQuota();
    void ConcurrentServiceMemoryQuota();
    void SnapshotSharing();
    void SnapshotSharingByExchanger();
    void NodesMembership();
    void NodesMembershipByExchanger();
    void DisonnectNodes();
    void PoolLimitNotReleasedByUnchargedQuery();
    void PoolLimitNotReleasedByUnchargedQueryOnRollback();
    void PoolLimitNotChargedByHundredPercentQuery();
    void PoolLimitNotChargedForDefaultPool();
    void PoolLimitIgnoredForSenselessPercents();
    void PoolLimitAppliedJustBelowHundredPercent();
    void SpillingPercentAppliedWithoutPoolLimit();

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

        AssertResourceManagerStats(rm, 900, 99);
        AssertResourceBrokerSensors(0, 100, 0, 0, 1);

        rm->FreeResources(*tx, task, request);
        AssertResourceManagerStats(rm, 1000, 100);
        AssertResourceBrokerSensors(0, 0, 0, 0, 1);
    }

    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
}

void KqpRm::ServiceMemoryQuota() {
    StartRms();
    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    {
        auto quota = NRm::CreateMemoryQuotaManager(rm);
        UNIT_ASSERT(quota->AllocateQuota(600, /* isOptional */ false));
        UNIT_ASSERT(!quota->AllocateQuota(500, /* isOptional */ false));
        UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 600);
        AssertResourceManagerStats(rm, 400, 100);
        quota->FreeQuota(200);
        UNIT_ASSERT(quota->AllocateQuota(500, /* isOptional */ false));
        UNIT_ASSERT_VALUES_EQUAL(quota->GetCurrentQuota(), 900);
        quota->FreeQuota(900);
        AssertResourceManagerStats(rm, 1000, 100);
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
        AssertResourceManagerStats(rm, 1000, 100);
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

            AssertResourceManagerStats(rm, 1000 - 100 * i, 100 - i);
            AssertResourceBrokerSensors(0, 100 * i, 0, i - 1, 1);
        }
    }

    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 9, 0);
}

void KqpRm::NotEnoughMemory() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm);
    auto task = 2;

    bool allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 10, .Memory = 10'000});
    UNIT_ASSERT(!allocated);

    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
}

void KqpRm::NotEnoughExecutionUnits() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm);
    auto task = 2;

    bool allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 1000, .Memory = 100});
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

    auto tx = MakeTx(1, rm);
    auto task = 2;

    bool allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 1'000});
    UNIT_ASSERT(allocated);

    allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 100'000});
    UNIT_ASSERT(!allocated);

    AssertResourceManagerStats(rm, config.GetQueryMemoryLimit() - 1000, 99);
    AssertResourceBrokerSensors(0, 1000, 0, 0, 1);
}

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

        AssertResourceManagerStats(rm, 800, 80);
        AssertResourceBrokerSensors(0, 200, 0, 0, 2);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(0, {{800, 80}, {1000, 100}}, rm);

        rm->FreeResources(*tx1, task2, request);
        rm->FreeResources(*tx2, task1, request);

        AssertResourceManagerStats(rm, 1000, 100);
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

    AssertResourceManagerStats(rm, 1000 - 100, 100 - 10);
    AssertResourceBrokerSensors(0, 100, 0, 0, 1);

    NRm::TKqpResourcesRequest reduceRequest;
    reduceRequest.Memory = 70;

    rm->FreeResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 7, .Memory = 70});
    AssertResourceManagerStats(rm, 1000 - 100 + 70, 100 - 10 + 7);
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
        AssertResourceManagerStats(rm, 1000, 100);
        AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 1);
    }

    AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 0);
}

void KqpRm::ConcurrentChannels() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);

        {
            auto qm = CreateChannelQuotaManager(rm, tx, 0, 16);

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

            // the channel quota manager exposes the node level memory availability of its tx,
            // DQ channels 2.0 propagate a negative value to the senders as back pressure. The load above
            // drives allocations to failure, so the cookie may well be negative already - set it explicitly.
            UNIT_ASSERT(tx->TotalMemoryCookie);
            const i64 saved = tx->TotalMemoryCookie->MemoryAvailability.load();
            tx->TotalMemoryCookie->MemoryAvailability.store(0);
            const i64 base = qm->GetMemoryAvailability(); // the locally prepaid channel quota
            tx->TotalMemoryCookie->MemoryAvailability.store(500);
            UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), base + 500);
            UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 500);
            // a negative node value dominates: prepaid quota must not mask node level memory pressure
            tx->TotalMemoryCookie->MemoryAvailability.store(-1000000);
            UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), -1000000);
            UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -1000000);
            tx->TotalMemoryCookie->MemoryAvailability.store(saved);
        }

        AssertResourceManagerStats(rm, 1000, 100);
        AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 1);
    }

    AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 0);
}

// QueryMemoryLimit = 1000 with the default SpillingPercent = 80: the spilling threshold is at 800 used,
// the availability is 800 - used and turns negative past it (the old SpillingPercentReached signal)
void KqpRm::MemoryAvailability() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        // the cookies are attached at construction: the node availability is known before anything is allocated
        UNIT_ASSERT(tx->TotalMemoryCookie);
        UNIT_ASSERT(!tx->PoolMemoryCookie); // no resource pool
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 800);

        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 700);

        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 700}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 0); // at the threshold: not pressure yet

        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 1}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -1);

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 801});
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 800);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// A tx with a resource pool sees the minimum over the node total and its pool: the pool limit is
// MemoryPoolPercent of the node limit, the spilling threshold applies to each of them
void KqpRm::PoolMemoryAvailability() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakePoolTx(1, rm, /* memoryPoolPercent = */ 50);
        // pool limit 500, pool threshold at 400 used; node threshold at 800 used. The pool resource is created
        // together with the tx, so the pool cookie is there before the first allocation
        UNIT_ASSERT(tx->PoolMemoryCookie);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 400);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100}));
        UNIT_ASSERT_VALUES_EQUAL(tx->TotalMemoryCookie->MemoryAvailability.load(), 700);
        UNIT_ASSERT_VALUES_EQUAL(tx->PoolMemoryCookie->MemoryAvailability.load(), 300);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 300);

        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 350}));
        UNIT_ASSERT_VALUES_EQUAL(tx->TotalMemoryCookie->MemoryAvailability.load(), 350);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -50); // the pool is over its threshold

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 350});
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 300);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// The pool resource survives a release down to zero: a tx keeps the pool cookie it got on its first allocation,
// so that cookie must still be the live one after the pool memory was released and acquired again, and a later
// tx of the same pool must get the very same cookie
void KqpRm::PoolMemoryAvailabilityAfterRelease() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakePoolTx(1, rm, /* memoryPoolPercent = */ 50); // pool limit 500, pool threshold at 400 used
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100}));
        const auto cookie = tx->PoolMemoryCookie;
        UNIT_ASSERT(cookie);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 300);

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100}); // the pool is empty now
        UNIT_ASSERT_VALUES_EQUAL(cookie->MemoryAvailability.load(), 400);

        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 450})); // over the pool threshold
        UNIT_ASSERT(tx->PoolMemoryCookie == cookie);
        UNIT_ASSERT_VALUES_EQUAL(cookie->MemoryAvailability.load(), -50); // the cookie is still the live one
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -50);

        auto tx2 = MakePoolTx(2, rm, /* memoryPoolPercent = */ 50);
        UNIT_ASSERT(rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.Memory = 10}));
        UNIT_ASSERT(tx2->PoolMemoryCookie == cookie); // the same resource, the same cookie
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -60);

        rm->FreeResources(*tx2, 1, NRm::TKqpResourcesRequest{.Memory = 10});
        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 450});
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 400);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// Constructing a tx of a pool with another percent must not move the pool threshold under the running txs:
// the pool limit follows the txs that allocate from the pool, not the ones that merely appear
void KqpRm::PoolLimitFollowsAllocatingTx() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakePoolTx(1, rm, /* memoryPoolPercent = */ 50); // pool limit 500, threshold at 400 used
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 450}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -50); // over the pool threshold

        auto other = MakePoolTx(2, rm, /* memoryPoolPercent = */ 90); // the same pool, another percent: nothing moves
        UNIT_ASSERT(other->PoolMemoryCookie == tx->PoolMemoryCookie);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -50);
        UNIT_ASSERT_VALUES_EQUAL(other->GetMemoryAvailability(), -50);

        // an allocation of the other tx does refresh the pool limit with its percent: limit 900, threshold 720
        UNIT_ASSERT(rm->AllocateResources(*other, 1, NRm::TKqpResourcesRequest{.Memory = 10}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 260); // 900 - 460 - 180, below the node total's 340

        rm->FreeResources(*other, 1, NRm::TKqpResourcesRequest{.Memory = 10});
        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 450});
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// A tx of a pool without a memory limit (percent -1, the default) takes nothing from the pool and must give
// nothing back to it either, whatever the txs with a limit have acquired there
void KqpRm::PoolReleaseMirrorsAcquire() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto limited = MakePoolTx(1, rm, /* memoryPoolPercent = */ 50); // pool limit 500, threshold at 400 used
        auto unlimited = MakePoolTx(2, rm, /* memoryPoolPercent = */ -1); // the same pool, no pool accounting
        UNIT_ASSERT(limited->HasMemoryPoolLimit());
        UNIT_ASSERT(!unlimited->HasMemoryPoolLimit());
        UNIT_ASSERT(!unlimited->PoolMemoryCookie);

        UNIT_ASSERT(rm->AllocateResources(*limited, 1, NRm::TKqpResourcesRequest{.Memory = 300}));
        UNIT_ASSERT_VALUES_EQUAL(limited->GetMemoryAvailability(), 100); // 500 - 300 - 100

        UNIT_ASSERT(rm->AllocateResources(*unlimited, 1, NRm::TKqpResourcesRequest{.Memory = 200})); // the node total only
        UNIT_ASSERT_VALUES_EQUAL(limited->GetMemoryAvailability(), 100); // the pool did not move
        UNIT_ASSERT_VALUES_EQUAL(unlimited->GetMemoryAvailability(), 300); // 1000 - 500 - 200
        rm->FreeResources(*unlimited, 1, NRm::TKqpResourcesRequest{.Memory = 200});
        UNIT_ASSERT_VALUES_EQUAL(limited->GetMemoryAvailability(), 100); // and did not move back either

        rm->FreeResources(*limited, 1, NRm::TKqpResourcesRequest{.Memory = 300});
        UNIT_ASSERT_VALUES_EQUAL(limited->GetMemoryAvailability(), 400);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// A runtime SpillingPercent change moves the spilling threshold of the node total and of every pool at once,
// the limits stay as they are, and the running transactions see it through their cookies
void KqpRm::SpillingPercentReconfigure() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto reconfigure = [&](double spillingPercent) {
        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        auto* config = request->Record.MutableConfig()->MutableTableServiceConfig()->MutableResourceManager();
        config->CopyFrom(MakeKqpResourceManagerConfig());
        config->SetSpillingPercent(spillingPercent);
        const TActorId edge = Runtime->AllocateEdgeActor();
        Runtime->Send(new IEventHandle(ResourceManagers.front(), edge, request.Release()));
        Runtime->GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(edge);
    };

    {
        auto tx = MakeTx(1, rm); // node limit 1000, threshold at 800 used
        auto poolTx = MakePoolTx(2, rm, /* memoryPoolPercent = */ 50); // pool limit 500, threshold at 400 used
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 700);
        UNIT_ASSERT_VALUES_EQUAL(poolTx->GetMemoryAvailability(), 400);

        reconfigure(100); // the thresholds move up to the limits
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 900);
        UNIT_ASSERT_VALUES_EQUAL(poolTx->GetMemoryAvailability(), 500);

        reconfigure(50); // and down to half of them
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 400);
        UNIT_ASSERT_VALUES_EQUAL(poolTx->GetMemoryAvailability(), 250);

        // out of range values are clamped: above 100 behaves as 100, below 0 as 0 (pressure at any usage)
        reconfigure(120);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 900);
        UNIT_ASSERT_VALUES_EQUAL(poolTx->GetMemoryAvailability(), 500);
        reconfigure(-20);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -100); // 1000 - 100 - 1000
        UNIT_ASSERT_VALUES_EQUAL(poolTx->GetMemoryAvailability(), -100); // the pool reads 500 - 0 - 500 = 0, the node total wins

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100});
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// A new node total from the resource broker queue config reaches every pool: a pool is a share of the total,
// so its limit and threshold move with it, and the running txs of the pool see it through their cookies
void KqpRm::TotalLimitReconfigure() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    // the resource broker queue config as the resource manager receives it; the handler does not reply, so
    // dispatch in short slices until the node total shows the new limit
    auto setTotal = [&](ui64 memory, ui64 expectedFree) {
        auto response = MakeHolder<TEvResourceBroker::TEvConfigResponse>();
        response->QueueConfig.ConstructInPlace();
        response->QueueConfig->MutableLimit()->SetMemory(memory);
        Runtime->Send(new IEventHandle(ResourceManagers.front(), Runtime->AllocateEdgeActor(), response.Release()));
        for (int i = 0; i < 100 && rm->GetLocalResources().Memory != expectedFree; ++i) {
            Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, expectedFree);
    };

    {
        auto poolTx = MakePoolTx(1, rm, /* memoryPoolPercent = */ 50); // pool limit 500, threshold at 400 used
        UNIT_ASSERT(rm->AllocateResources(*poolTx, 1, NRm::TKqpResourcesRequest{.Memory = 450}));
        UNIT_ASSERT_VALUES_EQUAL(poolTx->GetMemoryAvailability(), -50); // over the pool threshold

        setTotal(2000, /* expectedFree = */ 2000 - 450); // pool limit 1000, threshold at 800; node threshold at 1600
        UNIT_ASSERT_VALUES_EQUAL(poolTx->GetMemoryAvailability(), 350); // 1000 - 450 - 200, the pool followed

        setTotal(1000, /* expectedFree = */ 1000 - 450); // and back
        UNIT_ASSERT_VALUES_EQUAL(poolTx->GetMemoryAvailability(), -50);

        rm->FreeResources(*poolTx, 1, NRm::TKqpResourcesRequest{.Memory = 450});
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// The quota managers refuse optional requests in advance when the tx availability cannot cover the aligned
// step: no resource manager round trip, even when the resource manager itself would have granted the request.
// The resource manager records every refusal it handles in TxFailedAllocationSize, so a zero there proves it
// was not asked. Their availability follows the sign of the tx value, and an optional request that fits goes
// to the resource manager as usual
void KqpRm::TaskQuotaManagerOptional() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        const ui64 taskId = 1;
        const ui64 initialLimit = 100;
        // the node service prepays the initial limit as external memory before the task starts
        UNIT_ASSERT(rm->AllocateResources(*tx, taskId, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = initialLimit}));
        UNIT_ASSERT(rm->AllocateResources(*tx, taskId, NRm::TKqpResourcesRequest{.Memory = 100}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 700);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0);
        const auto statsBefore = rm->GetLocalResources();

        // task level manager: 1 MB allocation step, more than the tx availability
        auto qm = CreateTaskQuotaManager(rm, tx, taskId, initialLimit);
        UNIT_ASSERT(qm->AllocateQuota(50, /* isOptional = */ true)); // fits in the prepaid limit
        UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), 700 + 50); // tx value plus the local leftover
        UNIT_ASSERT(!qm->AllocateQuota(1500, /* isOptional = */ true)); // 1 MB step > 700: refused in advance
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, statsBefore.Memory);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0); // the resource manager was not asked
        // a negative tx value dominates the local leftover
        tx->TotalMemoryCookie->MemoryAvailability.store(-7);
        UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), -7);
        tx->TotalMemoryCookie->MemoryAvailability.store(700);
        qm->FreeQuota(50);
        qm.reset();

        // channel level manager: 16 byte allocation step. The tx reports less than the aligned request although
        // the resource manager has 700 bytes and would grant it: refused in advance, the resource manager not asked
        auto cm = CreateChannelQuotaManager(rm, tx, 0, 16);
        tx->TotalMemoryCookie->MemoryAvailability.store(100);
        UNIT_ASSERT(!cm->AllocateQuota(200, /* isOptional = */ true)); // 208 > 100: refused in advance
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, statsBefore.Memory);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), 100); // nothing prepaid here
        tx->TotalMemoryCookie->MemoryAvailability.store(700);
        UNIT_ASSERT(cm->AllocateQuota(200, /* isOptional = */ true)); // 208 <= 700: the same request is granted by the resource manager
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, statsBefore.Memory - 208);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 700 - 208); // the cookie follows the allocation
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0);
        cm->FreeQuota(200); // the 208 stay prepaid in the channel manager until it dies
        // a mandatory request beyond the node memory does reach the resource manager and is refused there:
        // 1500 - 208 prepaid = 1292, aligned to 1296 > 692 left on the node
        UNIT_ASSERT(!cm->AllocateQuota(1500, /* isOptional = */ false));
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 1296);

        // the resource manager refuses a small request (below 10 steps) that the pre-check let through: a
        // mandatory one is tolerated as over-quoting, an optional one is refused and the prepaid quota restored
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 600})); // 92 bytes left on the node
        tx->TotalMemoryCookie->MemoryAvailability.store(1'000'000); // the pre-check passes
        UNIT_ASSERT(!cm->AllocateQuota(300, /* isOptional = */ true)); // 300 - 208 prepaid = 92, aligned to 96 > 92
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 96); // refused by the resource manager
        UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), 1'000'000 + 208); // the prepaid quota is intact
        UNIT_ASSERT(cm->AllocateQuota(300, /* isOptional = */ false)); // the mandatory request is over-quoted
        UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), 1'000'000 + 208 - 300);
        cm->FreeQuota(300);
        UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), 1'000'000 + 208);
        tx->TotalMemoryCookie->MemoryAvailability.store(700);
        rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 600});
        cm.reset();
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, statsBefore.Memory);

        rm->FreeResources(*tx, taskId, NRm::TKqpResourcesRequest{.Memory = 100});
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

void KqpRm::SnapshotSharing() {
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm_first = GetKqpResourceManager(ResourceManagers[0].NodeId());
    auto rm_second = GetKqpResourceManager(ResourceManagers[1].NodeId());

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm_first);
    CheckSnapshot(1, {{1000, 100}, {1000, 100}}, rm_second);

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

        CheckSnapshot(0, {{800, 80}, {1000, 100}}, rm_second);
    }

    {
        bool allocated = rm_second->AllocateResources(*tx1Rm2, task1Rm2, request);
        UNIT_ASSERT(allocated);

        allocated &= rm_second->AllocateResources(*tx2Rm2, task2Rm2, request);
        UNIT_ASSERT(allocated);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(1, {{800, 80}, {800, 80}}, rm_first);
    }

    {
        rm_first->FreeResources(*tx1Rm1, task1Rm1, request);
        rm_first->FreeResources(*tx2Rm1, task2Rm1, request);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(0, {{1000, 100}, {800, 80}}, rm_second);
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

void KqpRm::PoolLimitNotReleasedByUnchargedQuery() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    NRm::TKqpResourcesRequest request{.ExecutionUnits = 1, .Memory = 300};

    {
        auto limitedTx = MakeTx(1, rm, "p", 50);
        auto unlimitedTx = MakeTx(2, rm, "p", -1);
        auto anotherLimitedTx = MakeTx(3, rm, "p", 50);

        UNIT_ASSERT(rm->AllocateResources(*limitedTx, 1, request));
        UNIT_ASSERT(rm->AllocateResources(*unlimitedTx, 2, request));

        rm->FreeResources(*unlimitedTx, 2, request);

        UNIT_ASSERT(!rm->AllocateResources(*anotherLimitedTx, 3, request));
        AssertResourceManagerStats(rm, 700, 99);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

void KqpRm::PoolLimitNotReleasedByUnchargedQueryOnRollback() {
    auto config = MakeKqpResourceManagerConfig();
    config.SetQueryMemoryLimit(90'000);

    StartRms({config, MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto limitedTx = MakeTx(1, rm, "p", 50);
        auto unlimitedTx = MakeTx(2, rm, "p", -1);
        auto anotherLimitedTx = MakeTx(3, rm, "p", 50);

        UNIT_ASSERT(rm->AllocateResources(*limitedTx, 1,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 30'000}));
        UNIT_ASSERT(!rm->AllocateResources(*unlimitedTx, 2,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = KQP_QUEUE_MEMORY_LIMIT}));
        UNIT_ASSERT(!rm->AllocateResources(*anotherLimitedTx, 3,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 18'000}));

        AssertResourceManagerStats(rm, 60'000, 99);
    }

    AssertResourceManagerStats(rm, 90'000, 100);
}

void KqpRm::PoolLimitNotChargedByHundredPercentQuery() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto limitedTx = MakeTx(1, rm, "p", 50);
        auto unlimitedTx = MakeTx(2, rm, "p", 100);
        auto anotherLimitedTx = MakeTx(3, rm, "p", 50);
        auto overflowingTx = MakeTx(4, rm, "p", 50);

        UNIT_ASSERT(rm->AllocateResources(*limitedTx, 1,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 400}));
        UNIT_ASSERT(rm->AllocateResources(*unlimitedTx, 2,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 100}));
        UNIT_ASSERT(rm->AllocateResources(*anotherLimitedTx, 3,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 100}));
        UNIT_ASSERT(!rm->AllocateResources(*overflowingTx, 4,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 1}));

        AssertResourceManagerStats(rm, 400, 97);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

void KqpRm::PoolLimitNotChargedForDefaultPool() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto firstTx = MakeTx(1, rm, NResourcePool::DEFAULT_POOL_ID, 50);
        auto secondTx = MakeTx(2, rm, NResourcePool::DEFAULT_POOL_ID, 50);
        auto overflowingTx = MakeTx(3, rm, NResourcePool::DEFAULT_POOL_ID, 50);

        UNIT_ASSERT(rm->AllocateResources(*firstTx, 1,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 600}));
        UNIT_ASSERT(rm->AllocateResources(*secondTx, 2,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 400}));
        UNIT_ASSERT(!rm->AllocateResources(*overflowingTx, 3,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 1}));

        AssertResourceManagerStats(rm, 0, 98);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

void KqpRm::PoolLimitIgnoredForSenselessPercents() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto nanTx = MakeTx(1, rm, "p", std::numeric_limits<double>::quiet_NaN());
        auto overTx = MakeTx(2, rm, "p", 150);

        UNIT_ASSERT(!nanTx->HasMemoryPoolLimit());
        UNIT_ASSERT(!overTx->HasMemoryPoolLimit());

        UNIT_ASSERT(rm->AllocateResources(*nanTx, 1,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 600}));
        UNIT_ASSERT(rm->AllocateResources(*overTx, 2,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 400}));

        AssertResourceManagerStats(rm, 0, 98);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

void KqpRm::PoolLimitAppliedJustBelowHundredPercent() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto firstTx = MakeTx(1, rm, "p", 99);
        auto secondTx = MakeTx(2, rm, "p", 99);
        auto overflowingTx = MakeTx(3, rm, "p", 99);

        UNIT_ASSERT(rm->AllocateResources(*firstTx, 1,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 900}));
        UNIT_ASSERT(rm->AllocateResources(*secondTx, 2,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 50}));
        UNIT_ASSERT(!rm->AllocateResources(*overflowingTx, 3,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 50}));

        AssertResourceManagerStats(rm, 50, 98);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

void KqpRm::SpillingPercentAppliedWithoutPoolLimit() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm, NResourcePool::DEFAULT_POOL_ID, 100);

        UNIT_ASSERT(rm->AllocateResources(*tx, 1,
            NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 600}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 200); // 1000 - 600 - 200, no pressure

        SetSpillingPercent(20);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -400); // 1000 - 600 - 800, past the threshold

        SetSpillingPercent(80);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 200);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

} // namespace NKqp
} // namespace NKikimr
