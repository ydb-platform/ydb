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

    TIntrusivePtr<NRm::TTxState> MakeTx(ui64 txId, std::shared_ptr<NRm::IKqpResourceManager> rm) {
        return MakeIntrusive<NRm::TTxState>(rm, txId, TInstant::Now(), "", (double)100, "", false);
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
        UNIT_TEST(OptionalQuotaDeniedAtYellowZone);
        UNIT_TEST(OptionalQuotaDeniedWhenExceedsPool);
        UNIT_TEST(FreeQuotaSymmetryAfterOptionalGrant);
        UNIT_TEST(SnapshotSharingByExchanger);
        UNIT_TEST(NodesMembershipByExchanger);
        UNIT_TEST(DisonnectNodes);
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
    void OptionalQuotaDeniedAtYellowZone();
    void OptionalQuotaDeniedWhenExceedsPool();
    void FreeQuotaSymmetryAfterOptionalGrant();
    void SnapshotSharing();
    void SnapshotSharingByExchanger();
    void NodesMembership();
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
                        if (!qm->AllocateQuota(j * 10u, false)) {
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

            // the channel quota manager exposes the node level memory pressure signal of its tx,
            // DQ channels 2.0 propagate it to the senders as back pressure. The load above drives
            // allocations to failure, so the cookie may well be set already - toggle it explicitly.
            UNIT_ASSERT(tx->TotalMemoryCookie);
            const bool reached = tx->TotalMemoryCookie->SpillingPercentReached.load();
            tx->TotalMemoryCookie->SpillingPercentReached.store(true);
            UNIT_ASSERT(qm->GetMemoryAvailability() <= 0);
            tx->TotalMemoryCookie->SpillingPercentReached.store(false);
            UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability() <= 0, tx->IsReasonableToStartSpilling());
            tx->TotalMemoryCookie->SpillingPercentReached.store(reached);
        }

        AssertResourceManagerStats(rm, 1000, 100);
        AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 1);
    }

    AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 0);
}

void KqpRm::OptionalQuotaDeniedAtYellowZone() {
    // Drive yellow zone with real AllocateResources calls; verify:
    //   - exact idle availability equals the node budget (green-phase precondition)
    //   - after crossing the spilling threshold, availability drops to 0
    //   - optional allocation is denied with counter deltas
    //   - mandatory allocation still succeeds
    auto config = MakeKqpResourceManagerConfig();
    ui64 nodeLimit = config.GetQueryMemoryLimit();
    double spillPct = config.GetSpillingPercent();
    ui64 overLimit = static_cast<ui64>(static_cast<double>(nodeLimit) / 100.0 * (100.0 - spillPct) + 1e-9);
    ui64 allocBytes = nodeLimit - overLimit + 1;

    StartRms({config, config});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);

    // step=16 so the 10-byte mandatory request is rounded to 16, which fits in the remaining budget
    auto qm = CreateChannelQuotaManager(rm, tx, 0, 16);
    // Green-phase precondition: no allocations yet → full node budget available
    UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), static_cast<i64>(nodeLimit));

    // Cross spilling threshold; RM's UpdateCookie sets SpillingPercentReached
    NRm::TKqpResourcesRequest request{.ExecutionUnits = 1, .Memory = allocBytes};
    bool allocated = rm->AllocateResources(*tx, 1, request);
    UNIT_ASSERT(allocated);
    UNIT_ASSERT(tx->TotalMemoryCookie);
    UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), 0);

    // Optional denied; counters increment by exactly 1 request and 10 bytes
    auto counters = rm->GetCounters();
    i64 deniedBefore = counters->RmOptionalQuotaDenied->Val();
    i64 bytesBefore = counters->RmOptionalQuotaDeniedBytes->Val();

    bool optResult = qm->AllocateQuota(10, /* isOptional */ true);
    UNIT_ASSERT(!optResult);
    UNIT_ASSERT_VALUES_EQUAL(counters->RmOptionalQuotaDenied->Val(), deniedBefore + 1);
    UNIT_ASSERT_VALUES_EQUAL(counters->RmOptionalQuotaDeniedBytes->Val(), bytesBefore + 10);

    // Mandatory ignores yellow zone; RM gets a 16-byte request which fits in overLimit remaining
    bool mandResult = qm->AllocateQuota(10, /* isOptional */ false);
    UNIT_ASSERT(mandResult);
    qm->FreeQuota(10);

    rm->FreeResources(*tx, 1, request);
}

void KqpRm::OptionalQuotaDeniedWhenExceedsPool() {
    // Use a config where pool is smaller than node budget
    auto config = MakeKqpResourceManagerConfig();
    // 1000 bytes node, tx will be in a named pool with 10% = 100 bytes
    StartRms({config, config});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    // Create a tx with a named pool at 10%
    TIntrusivePtr<NRm::TTxState> tx = MakeIntrusive<NRm::TTxState>(
        rm, /*txId=*/42u, TInstant::Now(), /*poolId=*/"testpool", /*memoryPoolPercent=*/10.0, /*database=*/"testdb", false);

    // Allocate pool memory to 60% (pool limit = 10% of 1000 = 100, OverLimit = 20).
    // 60 used → 40 available, which is above OverLimit so SpillingPercentReached stays false.
    NRm::TKqpResourcesRequest request{.ExecutionUnits = 1, .Memory = 60};
    bool allocated = rm->AllocateResources(*tx, 1, request);
    UNIT_ASSERT(allocated);

    // Pool has ~40 bytes left; optional request for 50 should be denied (headroom, not yellow zone)
    auto qm = CreateChannelQuotaManager(rm, tx, 0, 16);
    i64 available = qm->GetMemoryAvailability();
    UNIT_ASSERT_GT(available, 0);  // not in yellow zone (40 >= OverLimit=20)
    UNIT_ASSERT_LT(available, 50); // pool headroom (40) < 50

    auto counters = rm->GetCounters();
    i64 deniedBefore = counters->RmOptionalQuotaDenied->Val();
    i64 bytesBefore = counters->RmOptionalQuotaDeniedBytes->Val();

    bool optResult = qm->AllocateQuota(50, /* isOptional */ true);
    UNIT_ASSERT(!optResult);
    UNIT_ASSERT_VALUES_EQUAL(counters->RmOptionalQuotaDenied->Val(), deniedBefore + 1);
    UNIT_ASSERT_VALUES_EQUAL(counters->RmOptionalQuotaDeniedBytes->Val(), bytesBefore + 50);

    rm->FreeResources(*tx, 1, request);
}

void KqpRm::FreeQuotaSymmetryAfterOptionalGrant() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);

    // step=256: optional grant of 100 triggers a 256-byte RM allocation; AllocatedQuota = 100
    auto qm = CreateChannelQuotaManager(rm, tx, 0, 256);
    UNIT_ASSERT_GT(qm->GetMemoryAvailability(), 0);
    UNIT_ASSERT_VALUES_EQUAL(qm->GetCurrentQuota(), 0u);

    // Counters must not move on a granted optional request
    auto counters = rm->GetCounters();
    i64 deniedBefore = counters->RmOptionalQuotaDenied->Val();
    i64 bytesBefore = counters->RmOptionalQuotaDeniedBytes->Val();

    bool optResult = qm->AllocateQuota(100, /* isOptional */ true);
    UNIT_ASSERT(optResult);
    UNIT_ASSERT_VALUES_EQUAL(counters->RmOptionalQuotaDenied->Val(), deniedBefore);
    UNIT_ASSERT_VALUES_EQUAL(counters->RmOptionalQuotaDeniedBytes->Val(), bytesBefore);
    UNIT_ASSERT_VALUES_EQUAL(qm->GetCurrentQuota(), 100u);

    qm->FreeQuota(100);
    UNIT_ASSERT_VALUES_EQUAL(qm->GetCurrentQuota(), 0u);
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

} // namespace NKqp
} // namespace NKikimr
