#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/rm_service/kqp_rm_memory_quota.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/tablet/resource_broker_impl.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/node_service/kqp_query_control_plane.h>

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

NKikimrConfig::TTableServiceConfig::TResourceManager MakeKqpResourceManagerConfig() {
    NKikimrConfig::TTableServiceConfig::TResourceManager config;

    config.SetComputeActorsCount(100);
    config.SetPublishStatisticsIntervalSec(0);
    config.SetQueryMemoryLimit(1000);
    // no band, so a demand past the memory arena grows it at once, and the execution units take no memory: the
    // tests that do not exercise the arena keep their resource broker expectations
    config.SetExecutionUnitMemory(0);
    config.SetMemoryArenaMinFreeSize(0);
    config.SetMemoryArenaMaxFreeSize(0);

    auto* infoExchangerRetrySettings = config.MutableInfoExchangerSettings();
    auto* exchangerSettings = infoExchangerRetrySettings->MutableExchangerSettings();
    exchangerSettings->SetStartDelayMs(50);
    exchangerSettings->SetMaxDelayMs(50);

    return config;
}

NKikimrConfig::TTableServiceConfig::TResourceManager MakeArenaConfig(ui64 executionUnitMemory, ui64 minFree, ui64 maxFree) {
    auto config = MakeKqpResourceManagerConfig();
    config.SetExecutionUnitMemory(executionUnitMemory);
    config.SetMemoryArenaMinFreeSize(minFree);
    config.SetMemoryArenaMaxFreeSize(maxFree);
    return config;
}

}

class KqpRm : public TTestBase {
public:
    void SetUp() override {
        Runtime = MakeHolder<TTenantTestRuntime>(MakeTenantTestConfig());
        SetPoolsCountersFlag(true);

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

    // the arena gauges of the resource manager: what the resource broker granted, the demand, the refused part
    void AssertArenaSensors(i64 size, i64 used, i64 deficit) {
        auto kqp = GetServiceCounters(Counters, "kqp");
        UNIT_ASSERT_VALUES_EQUAL(kqp->GetCounter("RM/ArenaSize", false)->Val(), size);
        UNIT_ASSERT_VALUES_EQUAL(kqp->GetCounter("RM/ArenaUsed", false)->Val(), used);
        UNIT_ASSERT_VALUES_EQUAL(kqp->GetCounter("RM/ArenaDeficit", false)->Val(), deficit);
    }

    i64 RmRate(const TString& name) {
        return GetServiceCounters(Counters, "kqp")->GetCounter(name, true)->Val();
    }

    TIntrusivePtr<NResourceBroker::IResourceBroker> GetInstantBroker(ui32 nodeInd = 0) {
        auto edge = Runtime->AllocateEdgeActor(nodeInd);
        Runtime->Send(new IEventHandle(ResourceBrokers[nodeInd], edge,
            new TEvResourceBroker::TEvResourceBrokerRequest), nodeInd, true);
        auto response = Runtime->GrabEdgeEvent<TEvResourceBroker::TEvResourceBrokerResponse>(edge);
        UNIT_ASSERT(response);
        return response->Get()->ResourceBroker;
    }

    // runs the periodic arena pass of the resource manager actor: the shrinks, the growths below the burst
    // threshold, the charge of the demand changes the allocation path left to it, and the gauges
    void TickArenaAdjust() {
        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(2));
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
            const TString& poolId = "", double memoryPoolPercent = 100, const TString& database = "") {
        return MakeIntrusive<NRm::TTxState>(rm, txId, TInstant::Now(), poolId, memoryPoolPercent, database, false);
    }

    void SetPoolsCountersFlag(bool value) {
        for (ui32 nodeIndex = 0; nodeIndex < Runtime->GetNodeCount(); ++nodeIndex) {
            Runtime->GetAppData(nodeIndex).FeatureFlags.SetEnableResourcePoolsCounters(value);
        }
    }

    NMonitoring::TDynamicCounterPtr FindPoolSensorGroup(const TString& database, const TString& poolId) {
        auto wm = GetServiceCounters(Counters, "kqp")->FindSubgroup("subsystem", "workload_manager");
        return wm ? wm->FindSubgroup("pool", database + "/" + poolId) : nullptr;
    }

    NMonitoring::TDynamicCounterPtr GetPoolSensorGroup(const TString& database, const TString& poolId) {
        auto group = FindPoolSensorGroup(database, poolId);
        UNIT_ASSERT(group);
        return group;
    }

    TIntrusivePtr<NRm::TTxState> MakePoolTx(ui64 txId, std::shared_ptr<NRm::IKqpResourceManager> rm, double memoryPoolPercent) {
        return MakeIntrusive<NRm::TTxState>(rm, txId, TInstant::Now(), "pool", memoryPoolPercent, "db", false);
    }

    TString RenderBrokerState(ui32 nodeInd = 0) {
        TMockMonRequest request;
        auto edge = Runtime->AllocateEdgeActor(nodeInd);
        Runtime->Send(new IEventHandle(ResourceBrokers[nodeInd], edge, new NMon::TEvHttpInfo(request)), nodeInd, true);

        TAutoPtr<IEventHandle> handle;
        auto* response = Runtime->GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);
        UNIT_ASSERT(response);
        return response->Answer;
    }

    TString RenderRmMonPage() {
        TMockMonRequest request;
        auto edge = Runtime->AllocateEdgeActor();
        Runtime->Send(new IEventHandle(ResourceManagers.front(), edge, new NMon::TEvHttpInfo(request)), 0, true);

        TAutoPtr<IEventHandle> handle;
        auto* response = Runtime->GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);
        UNIT_ASSERT(response);
        return response->Answer;
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
        UNIT_TEST(OptionalMemorySpillingThreshold);
        UNIT_TEST(OptionalMemoryPoolThreshold);
        UNIT_TEST(OptionalMemoryConcurrent);
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
        UNIT_TEST(P09PoolLimitAndAllocated);
        UNIT_TEST(P11PoolDenied);
        UNIT_TEST(P14PoolSensorsPersistAcrossIdle);
        UNIT_TEST(P15PoolSensorsAppearAfterFlagEnabled);
        UNIT_TEST(P16MonPageListsIdlePool);
        UNIT_TEST(ArenaFollowsExternalMemory);
        UNIT_TEST(ArenaHysteresis);
        UNIT_TEST(ArenaExecutionUnitMemory);
        UNIT_TEST(ArenaConfigReloadRepricesUnits);
        UNIT_TEST(ArenaDeficitWhenBrokerRefuses);
        UNIT_TEST(ArenaGrowRefusalLoggedOnChange);
        UNIT_TEST(ArenaGrowRetriesWithDeficitOnly);
        UNIT_TEST(ArenaBrokerNotReady);
        UNIT_TEST(ArenaMixedRequestMemoryFailureLeavesArenaUntouched);
        UNIT_TEST(ArenaConcurrent);
        UNIT_TEST(ArenaSpillingPressure);
        UNIT_TEST(ArenaDisabled);
        UNIT_TEST(ArenaMonPage);
        UNIT_TEST(ArenaStoppedAfterRmPoison);
        UNIT_TEST(ArenaChargePublished);
        UNIT_TEST(ArenaDeficitRetriedOnQueueLimitRaise);
        UNIT_TEST(ArenaConfigReloadMovesThresholds);
        UNIT_TEST(ArenaDemandPastNodeTotal);
        UNIT_TEST(ArenaDoesNotChargePools);
        UNIT_TEST(ArenaGrowthCapLeavesRoomForRunningQueries);
        UNIT_TEST(ArenaMixesExternalMemoryAndUnits);
        UNIT_TEST(ArenaAbsorbsChurn);
        UNIT_TEST(ArenaReleasesIdleReservation);
        UNIT_TEST(ArenaShrinksWhenNodeTotalDrops);
        UNIT_TEST(ArenaReclaimsSurplusWhileGrowthIsWanted);
        UNIT_TEST(ArenaFreePartRefusesMemory);
        UNIT_TEST(ArenaWithheldGrowthWaitsForPass);
        UNIT_TEST(ArenaBurstGrowsOnAllocation);
        UNIT_TEST(ArenaDefaultsAgainstASmallQueue);
        UNIT_TEST(ArenaLifetimeIsNotAQueryDuration);
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
    void OptionalMemorySpillingThreshold();
    void OptionalMemoryPoolThreshold();
    void OptionalMemoryConcurrent();
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
    void P09PoolLimitAndAllocated();
    void P11PoolDenied();
    void P14PoolSensorsPersistAcrossIdle();
    void P15PoolSensorsAppearAfterFlagEnabled();
    void P16MonPageListsIdlePool();
    void ArenaFollowsExternalMemory();
    void ArenaHysteresis();
    void ArenaExecutionUnitMemory();
    void ArenaConfigReloadRepricesUnits();
    void ArenaDeficitWhenBrokerRefuses();
    void ArenaGrowRefusalLoggedOnChange();
    void ArenaGrowRetriesWithDeficitOnly();
    void ArenaBrokerNotReady();
    void ArenaMixedRequestMemoryFailureLeavesArenaUntouched();
    void ArenaConcurrent();
    void ArenaSpillingPressure();
    void ArenaDisabled();
    void ArenaMonPage();
    void ArenaStoppedAfterRmPoison();
    void ArenaChargePublished();
    void ArenaDeficitRetriedOnQueueLimitRaise();
    void ArenaConfigReloadMovesThresholds();
    void ArenaDemandPastNodeTotal();
    void ArenaDoesNotChargePools();
    void ArenaGrowthCapLeavesRoomForRunningQueries();
    void ArenaMixesExternalMemoryAndUnits();
    void ArenaAbsorbsChurn();
    void ArenaReleasesIdleReservation();
    void ArenaShrinksWhenNodeTotalDrops();
    void ArenaReclaimsSurplusWhileGrowthIsWanted();
    void ArenaFreePartRefusesMemory();
    void ArenaWithheldGrowthWaitsForPass();
    void ArenaBurstGrowsOnAllocation();
    void ArenaDefaultsAgainstASmallQueue();
    void ArenaLifetimeIsNotAQueryDuration();

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

// The quota managers pass the optional flag on to the resource manager, which refuses an optional request once it
// would take the node total past the spilling threshold, even with the memory there. The refusal is quiet:
// RM/OptionalMemoryRefused counts it, RM/NotEnoughMemory and the failed allocation of the tx (reported on OOM) do
// not. The decision is taken on the state of the resource manager, the cookies the quota managers report do not
// steer it
void KqpRm::TaskQuotaManagerOptional() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        const ui64 taskId = 1;
        const ui64 initialLimit = 100;
        // the node service prepays the initial limit as external memory before the task starts; the memory arena
        // charges it to the node total (900 left, the spilling threshold at 800) until the task quota manager dies
        UNIT_ASSERT(rm->AllocateResources(*tx, taskId, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = initialLimit}));
        AssertResourceBrokerSensors(0, 100, 0, 0, 1);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 700);
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, 900);

        // task level manager: 1 MB allocation step, more than the node has
        auto qm = CreateTaskQuotaManager(rm, tx, taskId, initialLimit);
        UNIT_ASSERT(qm->AllocateQuota(50, /* isOptional = */ true)); // fits in the prepaid limit
        UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), 700 + 50); // tx value plus the local leftover
        UNIT_ASSERT(!qm->AllocateQuota(1500, /* isOptional = */ true)); // the 1 MB step is past the threshold
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1); // the flag reached the resource manager
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 0);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, 900);
        UNIT_ASSERT(!qm->AllocateQuota(1500, /* isOptional = */ false)); // the same request, mandatory: a failure
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 1);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 1_MB);
        // a negative tx value dominates the local leftover
        const i64 saved = tx->TotalMemoryCookie->MemoryAvailability.load();
        tx->TotalMemoryCookie->MemoryAvailability.store(-7);
        UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), -7);
        tx->TotalMemoryCookie->MemoryAvailability.store(saved);
        qm->FreeQuota(50);
        qm.reset();
        // the task quota manager returned the prepay, and the periodic pass the arena memory to the node total
        TickArenaAdjust();
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, 1000);

        // channel level manager: 16 byte allocation step
        auto cm = CreateChannelQuotaManager(rm, tx, 0, 16);
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 600})); // 400 left, availability 200
        // the cookie reports plenty, the resource manager refuses on its own state: 208 > 200
        tx->TotalMemoryCookie->MemoryAvailability.store(1'000'000);
        UNIT_ASSERT(!cm->AllocateQuota(200, /* isOptional = */ true));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 2);
        UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), 1'000'000); // nothing prepaid, nothing lost
        UNIT_ASSERT_VALUES_EQUAL(tx->TotalMemoryCookie->MemoryAvailability.load(), 1'000'000); // a refusal moves no cookie
        tx->TotalMemoryCookie->MemoryAvailability.store(200);
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, 400);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 1_MB);

        UNIT_ASSERT(cm->AllocateQuota(150, /* isOptional = */ true)); // 160 <= 200: granted
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 40);
        UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), 10 + 40); // 10 bytes of the step prepaid
        UNIT_ASSERT(!cm->AllocateQuota(100, /* isOptional = */ true)); // 100 - 10 prepaid = 90, aligned to 96 > 40
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 3);
        UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), 10 + 40); // the prepaid quota is restored
        UNIT_ASSERT(cm->AllocateQuota(100, /* isOptional = */ false)); // mandatory: past the threshold, up to the limit
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -56);
        UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), -56); // a negative node value dominates the 6 prepaid
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, 144);

        // a mandatory request beyond the node memory is a failure: 300 - 6 prepaid = 294, aligned to 304 > 144
        UNIT_ASSERT(!cm->AllocateQuota(300, /* isOptional = */ false));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 2);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 304);

        // a small request (below 10 steps) the node cannot cover: a mandatory one is tolerated as over-quoting, an
        // optional one is refused
        UNIT_ASSERT(rm->AllocateResources(*tx, 3, NRm::TKqpResourcesRequest{.Memory = 120})); // 24 bytes left on the node
        UNIT_ASSERT(!cm->AllocateQuota(100, /* isOptional = */ true)); // 100 - 6 prepaid = 94, aligned to 96 > 24
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 4);
        UNIT_ASSERT(cm->AllocateQuota(100, /* isOptional = */ false)); // refused by the resource manager, over-quoted
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 3);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 96);
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, 24);

        cm->FreeQuota(100);
        cm->FreeQuota(100);
        cm->FreeQuota(150);
        rm->FreeResources(*tx, 3, NRm::TKqpResourcesRequest{.Memory = 120});
        rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 600});
        cm.reset();
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, 1000);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// An optional Memory request is refused once it would take the node total past the spilling threshold (800 used
// here) although the memory is there, a mandatory one only past the limit. The refusal is quiet: no resource broker
// task, neither RM/NotEnoughMemory nor the failed allocation of the tx see it, and the execution units of the
// request come back. The resource manager decides on its own state, not on the cookies
void KqpRm::OptionalMemorySpillingThreshold() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 700}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 100);
        AssertResourceBrokerSensors(0, 700, 0, 0, 1);

        auto result = rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.ExecutionUnits = 5, .Memory = 150, .Optional = true});
        UNIT_ASSERT(!result);
        UNIT_ASSERT(result.GetStatus() == NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY);
        AssertResourceManagerStats(rm, 300, 100); // the memory is there, the units came back
        AssertResourceBrokerSensors(0, 700, 0, 0, 1);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 100);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 0);

        // up to the threshold, not past it
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 100, .Optional = true}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 0);
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 1, .Optional = true}));

        // the cookie does not steer the decision
        tx->TotalMemoryCookie->MemoryAvailability.store(1'000'000);
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 1, .Optional = true}));
        tx->TotalMemoryCookie->MemoryAvailability.store(0);

        // a mandatory request goes past the threshold, an optional one stays refused there
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 150}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -150);
        tx->TotalMemoryCookie->MemoryAvailability.store(1'000'000);
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 1, .Optional = true}));
        tx->TotalMemoryCookie->MemoryAvailability.store(-150);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 4);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 0);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0);
        AssertResourceManagerStats(rm, 50, 100);

        // SpillingPercent = 100: the threshold is the limit, an optional request is refused where a mandatory one is
        SetSpillingPercent(100);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 50);
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 51, .Optional = true}));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 5);
        UNIT_ASSERT(!rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 51}));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 1);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 51);
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 50, .Optional = true}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 0);

        rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.Memory = 100 + 150 + 50});
        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 700});
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 1000);
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// A tx of a resource pool also meets the threshold of its pool (50%: pool limit 500, threshold at 400 used). A quiet
// optional refusal is no pool denial, a mandatory request past the pool limit still is one
void KqpRm::OptionalMemoryPoolThreshold() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakePoolTx(1, rm, /* memoryPoolPercent = */ 50);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 350}));
        UNIT_ASSERT_VALUES_EQUAL(tx->TotalMemoryCookie->MemoryAvailability.load(), 450);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 50);
        auto denied = GetPoolSensorGroup("db", "pool")->GetCounter("MemoryDeniedRequests", true);

        UNIT_ASSERT(!rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100, .Optional = true})); // the node would take it
        AssertResourceManagerStats(rm, 650, 100); // the node total is not charged, not even for a moment
        UNIT_ASSERT_VALUES_EQUAL(tx->TotalMemoryCookie->MemoryAvailability.load(), 450);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 50);
        UNIT_ASSERT_VALUES_EQUAL(denied->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 0);

        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 50, .Optional = true}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 0);

        // mandatory: past the pool threshold up to the pool limit, a pool denial past it
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100}));
        UNIT_ASSERT(!rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 1}));
        UNIT_ASSERT_VALUES_EQUAL(denied->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughMemory"), 1);
        AssertResourceManagerStats(rm, 500, 100);

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 350 + 50 + 100});
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// Concurrent optional requests are decided one at a time under the resource manager lock: together they never take
// the node total past the spilling threshold, whatever the lock-free cookies said when they were issued
void KqpRm::OptionalMemoryConcurrent() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        // 200 left below the threshold: a single thread is refused already, it asks for 340 and more between its frees
        UNIT_ASSERT(rm->AllocateResources(*tx, 0, NRm::TKqpResourcesRequest{.Memory = 600}));

        NPar::LocalExecutor().RunAdditionalThreads(10);
        std::atomic<ui64> granted = 0;
        std::atomic<ui64> refused = 0;
        std::atomic<bool> overThreshold = false;

        NPar::LocalExecutor().ExecRange([&](int index) {
            const ui64 taskId = index + 1;
            ui64 held = 0;
            for (auto j = 0u; j < 200u; j++) {
                const ui64 memory = (j % 7 + 1) * 10;
                if (rm->AllocateResources(*tx, taskId, NRm::TKqpResourcesRequest{.Memory = memory, .Optional = true})) {
                    granted++;
                    held += memory;
                    if (tx->GetMemoryAvailability() < 0) {
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
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 200);

        rm->FreeResources(*tx, 0, NRm::TKqpResourcesRequest{.Memory = 600});
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

void KqpRm::P09PoolLimitAndAllocated() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm, "pool_a", 50, "db1");
    NRm::TKqpResourcesRequest request{.Memory = 100};

    UNIT_ASSERT(rm->AllocateResources(*tx, 1, request));

    auto sensorGroup = GetPoolSensorGroup("db1", "pool_a");
    UNIT_ASSERT_VALUES_EQUAL(sensorGroup->GetCounter("MemoryLimit", false)->Val(), 500);
    UNIT_ASSERT_VALUES_EQUAL(sensorGroup->GetCounter("MemoryAllocated", false)->Val(), 100);

    UNIT_ASSERT(rm->AllocateResources(*tx, 2, request));
    UNIT_ASSERT_VALUES_EQUAL(sensorGroup->GetCounter("MemoryAllocated", false)->Val(), 200);

    rm->FreeResources(*tx, 1, request);
    UNIT_ASSERT_VALUES_EQUAL(sensorGroup->GetCounter("MemoryAllocated", false)->Val(), 100);

    rm->FreeResources(*tx, 2, request);
    UNIT_ASSERT_VALUES_EQUAL(sensorGroup->GetCounter("MemoryAllocated", false)->Val(), 0);
}

void KqpRm::P11PoolDenied() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm, "pool_c", 10, "db1");
    NRm::TKqpResourcesRequest request{.Memory = 40};

    UNIT_ASSERT(rm->AllocateResources(*tx, 1, request));
    UNIT_ASSERT(rm->AllocateResources(*tx, 2, request));

    auto deniedCtr = GetPoolSensorGroup("db1", "pool_c")->GetCounter("MemoryDeniedRequests", true);
    UNIT_ASSERT_VALUES_EQUAL(deniedCtr->Val(), 0);

    UNIT_ASSERT(!rm->AllocateResources(*tx, 3, request));
    UNIT_ASSERT_VALUES_EQUAL(deniedCtr->Val(), 1);

    UNIT_ASSERT(!rm->AllocateResources(*tx, 4, request));
    UNIT_ASSERT_VALUES_EQUAL(deniedCtr->Val(), 2);
}

void KqpRm::P14PoolSensorsPersistAcrossIdle() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    NRm::TKqpResourcesRequest request{.Memory = 40};

    {
        auto tx = MakeTx(1, rm, "pool_p14", 10, "db1");
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, request));
        rm->FreeResources(*tx, 1, request);
    }

    auto sensorGroup = GetPoolSensorGroup("db1", "pool_p14");
    auto limitCtr = sensorGroup->GetCounter("MemoryLimit", false);
    auto allocCtr = sensorGroup->GetCounter("MemoryAllocated", false);
    auto deniedCtr = sensorGroup->GetCounter("MemoryDeniedRequests", true);

    UNIT_ASSERT_VALUES_EQUAL(limitCtr->Val(), 100);
    UNIT_ASSERT_VALUES_EQUAL(allocCtr->Val(), 0);

    {
        auto tx = MakeTx(2, rm, "pool_p14", 10, "db1");
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, request));
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, request));
        UNIT_ASSERT(!rm->AllocateResources(*tx, 3, request));

        UNIT_ASSERT_VALUES_EQUAL(limitCtr->Val(), 100);
        UNIT_ASSERT_VALUES_EQUAL(allocCtr->Val(), 80);
        UNIT_ASSERT_VALUES_EQUAL(deniedCtr->Val(), 1);
    }
}

void KqpRm::P15PoolSensorsAppearAfterFlagEnabled() {
    SetPoolsCountersFlag(false);
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    NRm::TKqpResourcesRequest request{.Memory = 40};

    {
        auto tx = MakeTx(1, rm, "pool_p15", 10, "db1");
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, request));
        rm->FreeResources(*tx, 1, request);
    }
    UNIT_ASSERT(!FindPoolSensorGroup("db1", "pool_p15"));

    SetPoolsCountersFlag(true);

    {
        auto tx = MakeTx(2, rm, "pool_p15", 10, "db1");
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, request));

        auto sensorGroup = GetPoolSensorGroup("db1", "pool_p15");
        UNIT_ASSERT_VALUES_EQUAL(sensorGroup->GetCounter("MemoryLimit", false)->Val(), 100);
        UNIT_ASSERT_VALUES_EQUAL(sensorGroup->GetCounter("MemoryAllocated", false)->Val(), 40);
    }
}

void KqpRm::P16MonPageListsIdlePool() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    NRm::TKqpResourcesRequest request{.Memory = 40};

    {
        auto tx = MakeTx(1, rm, "pool_idle", 10, "db1");
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, request));
        rm->FreeResources(*tx, 1, request);
    }

    auto liveTx = MakeTx(2, rm, "pool_live", 10, "db1");
    UNIT_ASSERT(rm->AllocateResources(*liveTx, 1, request));

    const TString page = RenderRmMonPage();
    UNIT_ASSERT_STRING_CONTAINS(page, "<td>db1</td><td>pool_idle</td><td>100</td><td>0</td><td>0</td>");
    UNIT_ASSERT_STRING_CONTAINS(page, "<td>db1</td><td>pool_live</td><td>100</td><td>40</td><td>0</td>");
}

// The memory arena (issue #53093), with the fixture config: no band, so a demand past the arena grows it at once on
// the allocation path, and the units priced at 0. A growth merges a delta task into the arena task, so the merged
// donor counts as a finished task. A free only moves the demand: the periodic pass gives the surplus back.
void KqpRm::ArenaFollowsExternalMemory() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    AssertArenaSensors(0, 0, 0);

    {
        auto tx = MakeTx(1, rm);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100}));
        AssertResourceManagerStats(rm, 900, 99);
        AssertResourceBrokerSensors(0, 100, 0, 0, 1);
        AssertArenaSensors(100, 100, 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().ExternalMemory, 100);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 700); // the spilling threshold at 800 sees the prepay

        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 50}));
        AssertResourceManagerStats(rm, 850, 99);
        AssertResourceBrokerSensors(0, 150, 0, 1, 1);
        AssertArenaSensors(150, 150, 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 2);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 2);

        rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 50});
        AssertResourceManagerStats(rm, 850, 99);
        AssertResourceBrokerSensors(0, 150, 0, 1, 1);
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().ExternalMemory, 100);
        TickArenaAdjust();
        AssertResourceManagerStats(rm, 900, 99);
        AssertResourceBrokerSensors(0, 100, 0, 1, 1);
        AssertArenaSensors(100, 100, 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 1);

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100});
        AssertResourceManagerStats(rm, 900, 100);
        TickArenaAdjust();
        AssertResourceManagerStats(rm, 1000, 100);
        AssertResourceBrokerSensors(0, 0, 0, 2, 0);
        AssertArenaSensors(0, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().ExternalMemory, 0);
    }

    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 2);
}

// MinFree = 100, MaxFree = 300: the pass resizes the arena to the demand plus 200 whenever its free part leaves the
// band, and leaves it alone inside it; a demand that overruns the arena by 300 or less waits for the pass
void KqpRm::ArenaHysteresis() {
    StartRms({MakeArenaConfig(0, 100, 300), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        // free -100 < 100, below the burst threshold: grown to 100 + 200 by the pass
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 100}));
        AssertResourceBrokerSensors(0, 0, 0, 0, 0);
        AssertResourceManagerStats(rm, 1000, 100);
        TickArenaAdjust();
        AssertResourceBrokerSensors(0, 300, 0, 0, 1);
        AssertResourceManagerStats(rm, 700, 100);
        AssertArenaSensors(300, 100, 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);

        // free 100: in band
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 100}));
        TickArenaAdjust();
        AssertArenaSensors(300, 200, 0);
        AssertResourceBrokerSensors(0, 300, 0, 0, 1);
        AssertResourceManagerStats(rm, 700, 100);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);

        // free 50 < 100: grown to 250 + 200
        UNIT_ASSERT(rm->AllocateResources(*tx, 3, NRm::TKqpResourcesRequest{.ExternalMemory = 50}));
        AssertResourceBrokerSensors(0, 300, 0, 0, 1);
        TickArenaAdjust();
        AssertResourceBrokerSensors(0, 450, 0, 1, 1);
        AssertResourceManagerStats(rm, 550, 100);
        AssertArenaSensors(450, 250, 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 2);

        // the arena backs nothing any more, so the pass gives the whole of it back
        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 200});
        rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 50});
        AssertResourceBrokerSensors(0, 450, 0, 1, 1);
        AssertResourceManagerStats(rm, 550, 100);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 0);
        TickArenaAdjust();
        AssertResourceBrokerSensors(0, 0, 0, 2, 0);
        AssertResourceManagerStats(rm, 1000, 100);
        AssertArenaSensors(0, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 1);

        TickArenaAdjust();
        AssertResourceBrokerSensors(0, 0, 0, 2, 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 1);
    }

    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 0);
}

// A demand that overruns the arena by more than MaxFree (300 here) grows it on the allocation path, to the demand plus
// the headroom of 200, RM/ArenaBurstGrows; an overrun up to MaxFree is left to the pass
void KqpRm::ArenaBurstGrowsOnAllocation() {
    StartRms({MakeArenaConfig(0, 100, 300), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);

    // 200 over an empty arena: the pass grows it to 400
    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 200}));
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 400, 0, 0, 1);
    AssertResourceManagerStats(rm, 600, 100);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 0);

    // 500 over 400: not past 400 + 300
    UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 300}));
    AssertResourceBrokerSensors(0, 400, 0, 0, 1);
    AssertResourceManagerStats(rm, 600, 100);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 0);

    // 750 over 400: the request itself grows the arena to 950 and charges it
    UNIT_ASSERT(rm->AllocateResources(*tx, 3, NRm::TKqpResourcesRequest{.ExternalMemory = 250}));
    AssertResourceBrokerSensors(0, 950, 0, 1, 1);
    AssertResourceManagerStats(rm, 50, 100);
    AssertArenaSensors(950, 750, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 2);

    rm->FreeResources(*tx, 3, NRm::TKqpResourcesRequest{.ExternalMemory = 250});
    rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 300});
    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 200});
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 2, 0);
    AssertResourceManagerStats(rm, 1000, 100);
    AssertArenaSensors(0, 0, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);
}

// Every granted execution unit takes ExecutionUnitMemory (10 here) from the arena; a refused one takes nothing
void KqpRm::ArenaExecutionUnitMemory() {
    StartRms({MakeArenaConfig(10, 0, 0), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx1 = MakeTx(1, rm);
    auto tx2 = MakeTx(2, rm);

    auto result = rm->AllocateResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1000});
    UNIT_ASSERT(!result);
    UNIT_ASSERT_EQUAL(result.GetStatus(), NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/NotEnoughComputeActors"), 1);
    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
    AssertArenaSensors(0, 0, 0);

    UNIT_ASSERT(rm->AllocateResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 3}));
    AssertResourceBrokerSensors(0, 30, 0, 0, 1);
    AssertResourceManagerStats(rm, 970, 97);
    AssertArenaSensors(30, 30, 0);

    // a mixed request: the per tx task of the memory part plus 20 more in the arena
    UNIT_ASSERT(rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 2, .Memory = 100}));
    AssertResourceBrokerSensors(0, 150, 0, 1, 2);
    AssertResourceManagerStats(rm, 850, 95);
    AssertArenaSensors(50, 50, 0);

    // the memory part goes back at once, the arena part on the pass
    rm->FreeResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 2, .Memory = 100});
    AssertResourceBrokerSensors(0, 50, 0, 1, 2);
    AssertResourceManagerStats(rm, 950, 97);
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 30, 0, 1, 2);
    AssertResourceManagerStats(rm, 970, 97);
    AssertArenaSensors(30, 30, 0);

    tx2.Reset(); // finishes the per tx task
    AssertResourceBrokerSensors(0, 30, 0, 2, 1);

    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 3});
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 3, 0);
    AssertResourceManagerStats(rm, 1000, 100);
    AssertArenaSensors(0, 0, 0);
}

// The execution units in use are a count priced when the arena is adjusted: a config reload re-prices them on the
// next pass
void KqpRm::ArenaConfigReloadRepricesUnits() {
    StartRms({MakeArenaConfig(10, 0, 0), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 2}));
    AssertResourceBrokerSensors(0, 20, 0, 0, 1);
    AssertResourceManagerStats(rm, 980, 98);

    Reconfigure(MakeArenaConfig(25, 0, 0));
    AssertResourceBrokerSensors(0, 20, 0, 0, 1);
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 50, 0, 1, 1);
    AssertResourceManagerStats(rm, 950, 98);
    AssertArenaSensors(50, 50, 0);

    Reconfigure(MakeArenaConfig(0, 0, 0));
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 2, 0);
    AssertResourceManagerStats(rm, 1000, 98);
    AssertArenaSensors(0, 0, 0);

    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 2});
    AssertResourceBrokerSensors(0, 0, 0, 2, 0);
    AssertResourceManagerStats(rm, 1000, 100);
}

// The resource broker refuses the growth (queue limit 50'000, another task in flight): the request is satisfied
// anyway, the deficit is charged to the node total, and the periodic pass asks again once there is room
void KqpRm::ArenaDeficitWhenBrokerRefuses() {
    auto config = MakeKqpResourceManagerConfig();
    config.SetQueryMemoryLimit(100'000'000); // the node total does not refuse first, as it would in production
    const ui64 qml = config.GetQueryMemoryLimit();

    StartRms({config, MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx1 = MakeTx(1, rm);
    auto tx2 = MakeTx(2, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 1'000}));
    AssertResourceBrokerSensors(0, 1000, 0, 0, 1);

    // 1'000 + 49'500 > 50'000: refused, and the deficit equals the delta so there is nothing smaller to retry
    UNIT_ASSERT(rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 49'500}));
    AssertResourceBrokerSensors(0, 1000, 0, 0, 1);
    AssertResourceManagerStats(rm, qml - 1'000 - 49'500, 98);
    AssertArenaSensors(0, 49'500, 49'500);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);

    // the allocation path does not ask again after a refusal, or a node under memory pressure would ask once per
    // request: 49'600 past the arena, and no resource broker call
    UNIT_ASSERT(rm->AllocateResources(*tx2, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 100}));
    rm->FreeResources(*tx2, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 100});
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 1);

    // 400 + 49'500 <= 50'000, so the growth the resource broker refused goes through on the next pass
    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.Memory = 600});
    AssertResourceBrokerSensors(0, 400, 0, 0, 1);
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 49'900, 0, 0, 2);
    AssertResourceManagerStats(rm, qml - 400 - 49'500, 98);
    AssertArenaSensors(49'500, 49'500, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 1);

    rm->FreeResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 49'500});
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 400, 0, 1, 1);
    AssertResourceManagerStats(rm, qml - 400, 99);
    AssertArenaSensors(0, 0, 0);

    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 400});
    AssertResourceBrokerSensors(0, 0, 0, 1, 1);
    AssertResourceManagerStats(rm, qml, 100);

    tx1.Reset();
    AssertResourceBrokerSensors(0, 0, 0, 2, 0);
}

// A refused growth is logged on a change of outcome only: the periodic pass keeps asking in silence, the allocation
// path does not ask at all, and a refusal after a grant is news again. A refusal goes stale once a pass finds no
// growth wanted
void KqpRm::ArenaGrowRefusalLoggedOnChange() {
    auto config = MakeArenaConfig(0, 1'000, 3'000); // headroom 2'000
    config.SetQueryMemoryLimit(100'000'000); // the node total does not refuse first, as it would in production

    // the runtime hands a log event to the logger actor on the spot, past the mailboxes and the observer, so the
    // event filter is what sees it
    struct TGrowthLine {
        TString Text;
        TString Size;
        TString Delta;
        TString Granted;
    };
    TVector<TGrowthLine> lines;
    Runtime->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NLog::PRI_NOTICE);
    TTestActorRuntimeBase::TEventFilter prevFilter;
    prevFilter = Runtime->SetEventFilter([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == NLog::TEvLog::EventType) {
            const auto* log = ev->Get<NLog::TEvLog>();
            if (log->Component == NKikimrServices::KQP_RESOURCE_MANAGER && log->Line.StartsWith("Memory arena growth")) {
                const auto& fields = *log->StructuredMessage;
                lines.push_back({log->Line, fields.GetValue<TString>("size").value_or(""),
                    fields.GetValue<TString>("delta").value_or(""), fields.GetValue<TString>("granted").value_or("")});
            }
        }
        return prevFilter(runtime, ev);
    });

    StartRms({config, MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx1 = MakeTx(1, rm);
    auto tx2 = MakeTx(2, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 1'000}));

    // 1'000 + 47'500 + 2'000 > 50'000 refused, the deficit of 47'500 alone granted: not the full growth, so the
    // gate is set
    UNIT_ASSERT(rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 47'500}));
    AssertArenaSensors(47'500, 47'500, 0);
    UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(lines[0].Text, "Memory arena growth partly granted by the resource broker");
    UNIT_ASSERT_VALUES_EQUAL(lines[0].Size, "0");
    UNIT_ASSERT_VALUES_EQUAL(lines[0].Delta, "49500");
    UNIT_ASSERT_VALUES_EQUAL(lines[0].Granted, "47500");

    // the passes ask for the headroom again and are refused again, in silence; a dispatch of two seconds runs at
    // least one of them, and possibly more
    const i64 grows = RmRate("RM/ArenaGrows");
    TickArenaAdjust();
    TickArenaAdjust();
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), grows);
    UNIT_ASSERT_GE(RmRate("RM/ArenaGrowFailures"), 2);
    UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);

    // the allocation path does not ask while the refusal stands, not even past the burst threshold: 47'500 + 3'100
    // overruns the arena by more than 3'000, and the demand change waits for the next pass
    const i64 failures = RmRate("RM/ArenaGrowFailures");
    UNIT_ASSERT(rm->AllocateResources(*tx2, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 3'100}));
    rm->FreeResources(*tx2, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 3'100});
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), failures);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), grows);

    // the memory tx1 gives back makes room for the headroom: the next pass gets it, which is logged once
    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.Memory = 1'000});
    TickArenaAdjust();
    AssertArenaSensors(49'500, 47'500, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), grows + 1);
    UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(lines[1].Text, "Memory arena growth granted again by the resource broker");
    UNIT_ASSERT_VALUES_EQUAL(lines[1].Size, "47500");
    UNIT_ASSERT_VALUES_EQUAL(lines[1].Delta, "2000");

    // 2'000 more eat the whole free part, so the pass asks for the headroom again: 49'500 + 2'000 > 50'000, refused
    // outright, and news again
    UNIT_ASSERT(rm->AllocateResources(*tx2, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 1'000}));
    UNIT_ASSERT(rm->AllocateResources(*tx2, 3, NRm::TKqpResourcesRequest{.ExternalMemory = 1'000}));
    TickArenaAdjust();
    AssertArenaSensors(49'500, 49'500, 0);
    UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(lines[2].Text, "Memory arena growth refused by the resource broker");
    UNIT_ASSERT_VALUES_EQUAL(lines[2].Size, "49500");
    UNIT_ASSERT_VALUES_EQUAL(lines[2].Delta, "2000");
    UNIT_ASSERT_VALUES_EQUAL(lines[2].Granted, "0");

    rm->FreeResources(*tx2, 3, NRm::TKqpResourcesRequest{.ExternalMemory = 1'000});
    rm->FreeResources(*tx2, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 1'000});
    rm->FreeResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 47'500});
    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1});
    TickArenaAdjust();
    AssertArenaSensors(0, 0, 0);
    AssertResourceManagerStats(rm, config.GetQueryMemoryLimit(), 100);
    UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);

    // the pass that emptied the arena found no growth wanted, so the refusal is stale: a grant with no refusal
    // before it is not news
    UNIT_ASSERT(rm->AllocateResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 1'000}));
    TickArenaAdjust();
    AssertArenaSensors(3'000, 1'000, 0);
    UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);

    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 1'000});
    TickArenaAdjust();
    AssertArenaSensors(0, 0, 0);
    AssertResourceManagerStats(rm, config.GetQueryMemoryLimit(), 100);
    UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);
    Runtime->SetEventFilter(prevFilter);
}

// When the full growth (demand plus the hysteresis headroom) is refused, the arena asks for the deficit alone;
// the headroom stays pending until the periodic pass asks again
void KqpRm::ArenaGrowRetriesWithDeficitOnly() {
    auto config = MakeArenaConfig(0, 1'000, 3'000); // headroom 2'000
    config.SetQueryMemoryLimit(100'000'000);
    const ui64 qml = config.GetQueryMemoryLimit();

    StartRms({config, MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx1 = MakeTx(1, rm);
    auto tx2 = MakeTx(2, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 1'000}));
    AssertResourceBrokerSensors(0, 1000, 0, 0, 1);

    // 1'000 + 49'500 > 50'000 refused, 1'000 + 47'500 <= 50'000 granted
    UNIT_ASSERT(rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 47'500}));
    AssertResourceBrokerSensors(0, 48'500, 0, 0, 2);
    AssertResourceManagerStats(rm, qml - 1'000 - 47'500, 98);
    AssertArenaSensors(47'500, 47'500, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);

    // the per tx memory returned by tx1 makes room for the headroom: 47'500 + 2'000 <= 50'000, taken by the pass
    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.Memory = 1'000});
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 49'500, 0, 1, 2);
    AssertResourceManagerStats(rm, qml - 49'500, 98);
    AssertArenaSensors(49'500, 47'500, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 2);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);

    // the arena covers the freed demand: the surplus waits for the periodic pass, which shrinks it to the headroom
    rm->FreeResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 47'500});
    AssertResourceBrokerSensors(0, 49'500, 0, 1, 2);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 0);

    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 2, 1);
    AssertResourceManagerStats(rm, qml, 99);
    AssertArenaSensors(0, 0, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 1);

    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1});
    AssertResourceBrokerSensors(0, 0, 0, 2, 1);
    AssertResourceManagerStats(rm, qml, 100);

    tx1.Reset();
    tx2.Reset();
    AssertResourceBrokerSensors(0, 0, 0, 3, 0);
    AssertResourceManagerStats(rm, qml, 100);
}

// Demand accepted before the resource broker is attached is charged and in deficit; the arena task is created by
// the first pass after the resource broker arrives
void KqpRm::ArenaBrokerNotReady() {
    auto prevObserverFunc = Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvResourceBroker::EvResourceBrokerResponse) {
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100}));
    AssertResourceManagerStats(rm, 900, 99);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
    AssertArenaSensors(0, 100, 100);
    UNIT_ASSERT_STRING_CONTAINS(RenderRmMonPage(),
        "Memory arena: size 0, used 100 (external 100, execution units 1 x 0), charged 100, deficit 100, broker task 0");

    // the memory part still needs the resource broker
    auto result = rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100});
    UNIT_ASSERT(!result);
    UNIT_ASSERT_EQUAL(result.GetStatus(), NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR);

    Runtime->SetObserverFunc(prevObserverFunc);
    Runtime->Send(new IEventHandle(ResourceBrokers[0], ResourceManagers[0], new TEvResourceBroker::TEvResourceBrokerRequest));
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 100, 0, 0, 1);
    AssertArenaSensors(100, 100, 0);
    AssertResourceManagerStats(rm, 900, 99);

    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100});
    TickArenaAdjust();
    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
}

// The arena part of a request is applied after the memory part succeeded: a refused memory part leaves the
// arena untouched, no rollback is involved
void KqpRm::ArenaMixedRequestMemoryFailureLeavesArenaUntouched() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);

    auto result = rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 10'000, .ExternalMemory = 100});
    UNIT_ASSERT(!result);
    UNIT_ASSERT_EQUAL(result.GetStatus(), NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY);
    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
    AssertArenaSensors(0, 0, 0);
    UNIT_ASSERT_VALUES_EQUAL(tx->TxExternalDataQueryMemory.load(), 0);
    UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().ExternalMemory, 0);

    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 100, .ExternalMemory = 100}));
    AssertResourceManagerStats(rm, 800, 99);
    AssertResourceBrokerSensors(0, 200, 0, 0, 2);
    AssertArenaSensors(100, 100, 0);

    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 100, .ExternalMemory = 100});
    TickArenaAdjust();
    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 1, 1);
    AssertArenaSensors(0, 0, 0);

    tx.Reset();
    AssertResourceBrokerSensors(0, 0, 0, 2, 0);
}

// Concurrent demand changes: a growth on the allocation path that finds another resize in progress does not wait for
// it, and the pass converges to the final demand with no change lost
void KqpRm::ArenaConcurrent() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);

        NPar::LocalExecutor().RunAdditionalThreads(10);
        std::atomic<ui64> failedAllocations = 0;

        NPar::LocalExecutor().ExecRange([&](int index) {
            const ui64 taskId = index + 1;
            for (auto j = 0u; j < 20u; j++) {
                const ui64 external = (j % 20 + 1) * 10;
                if (!rm->AllocateResources(*tx, taskId, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = external})) {
                    failedAllocations++;
                    continue;
                }
                rm->FreeResources(*tx, taskId, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = external});
            }
        }, 0, 10, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);

        UNIT_ASSERT_VALUES_EQUAL(failedAllocations.load(), 0); // the arena never refuses
        TickArenaAdjust();
        AssertResourceManagerStats(rm, 1000, 100);
        AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 0);
        AssertArenaSensors(0, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);
    }

    AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 0);
}

// The arena charge counts toward the spilling threshold like any other allocation: the prepaid memory past the
// threshold makes the resource manager refuse optional requests and the node total refuse memory requests
void KqpRm::ArenaSpillingPressure() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 850}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -50); // 800 - 850
        AssertResourceManagerStats(rm, 150, 99);

        auto cm = CreateChannelQuotaManager(rm, tx, 0, 16);
        UNIT_ASSERT(!cm->AllocateQuota(16, /* isOptional = */ true));
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/OptionalMemoryRefused"), 1);
        UNIT_ASSERT_VALUES_EQUAL(tx->TxFailedAllocationSize.load(), 0); // refused quietly
        cm.reset();

        UNIT_ASSERT(!rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 200}));
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 150}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -200);
        AssertResourceManagerStats(rm, 0, 99);

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 150});
        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 850});
        TickArenaAdjust();
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 800);
        AssertResourceManagerStats(rm, 1000, 100);
    }
}

// EnableMemoryArena = false: the demand is only counted, the resource broker is not involved and the node total
// is not charged; a runtime switch creates or finishes the arena task on the next pass
void KqpRm::ArenaDisabled() {
    auto disabled = MakeKqpResourceManagerConfig();
    disabled.SetEnableMemoryArena(false);

    StartRms({disabled, MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);

    // the gauge follows on the periodic pass
    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100}));
    AssertResourceManagerStats(rm, 1000, 99);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
    UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().ExternalMemory, 100);
    UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 800);
    TickArenaAdjust();
    AssertArenaSensors(0, 100, 0);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);

    Reconfigure(MakeKqpResourceManagerConfig()); // enabled
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 100, 0, 0, 1);
    AssertResourceManagerStats(rm, 900, 99);
    AssertArenaSensors(100, 100, 0);
    UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 700);

    Reconfigure(disabled);
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
    AssertResourceManagerStats(rm, 1000, 99);
    AssertArenaSensors(0, 100, 0);

    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100});
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
    AssertResourceManagerStats(rm, 1000, 100);
    UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().ExternalMemory, 0);
    TickArenaAdjust();
    AssertArenaSensors(0, 0, 0);
}

void KqpRm::ArenaMonPage() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);
    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100}));

    UNIT_ASSERT_STRING_CONTAINS(RenderRmMonPage(),
        "Memory arena: size 100, used 100 (external 100, execution units 1 x 0), charged 100, deficit 0, broker task ");

    // the demand is still shown when the arena is disabled, the supply and the charge are not
    auto disabled = MakeKqpResourceManagerConfig();
    disabled.SetEnableMemoryArena(false);
    Reconfigure(disabled);
    TickArenaAdjust();
    UNIT_ASSERT_STRING_CONTAINS(RenderRmMonPage(),
        "Memory arena: size 0, used 100 (external 100, execution units 1 x 0), charged 0, deficit 0, broker task 0, disabled");
}

// The resource manager outlives its actor: after the actor died the resource broker dropped the arena task and the
// node total is not charged any more; the demand of the surviving txs is still counted, and the resource broker is
// not called any more
void KqpRm::ArenaStoppedAfterRmPoison() {
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm_second = GetKqpResourceManager(ResourceManagers[1].NodeId());
    auto tx = MakeTx(1, rm_second);

    UNIT_ASSERT(rm_second->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100}));
    AssertResourceBrokerSensors(0, 100, 0, 0, 1);
    AssertResourceManagerStats(rm_second, 900, 99);

    const TActorId edge = Runtime->AllocateEdgeActor(1);
    Runtime->Send(new IEventHandle(
        ResourceManagers[1], edge, new TEvents::TEvPoison, IEventHandle::FlagTrackDelivery, 0),
        1, false);

    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvents::TSystem::Poison, 1);
    UNIT_ASSERT(Runtime->DispatchEvents(options));
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    // the resource broker removed the tasks of the dead actor
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
    AssertArenaSensors(0, 100, 0);
    AssertResourceManagerStats(rm_second, 1000, 99);

    // a new demand is still counted, and the resource broker is not asked to grow the arena
    UNIT_ASSERT(rm_second->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 50}));
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
    AssertResourceManagerStats(rm_second, 1000, 99);
    UNIT_ASSERT_VALUES_EQUAL(rm_second->GetLocalResources().ExternalMemory, 150);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);

    rm_second->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 50});
    rm_second->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100});
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
    AssertResourceManagerStats(rm_second, 1000, 100);
    UNIT_ASSERT_VALUES_EQUAL(rm_second->GetLocalResources().ExternalMemory, 0);
}

// An external memory only request takes the Memory == 0 path of AllocateResources, which does not publish: the pass
// publishes the charge it moves, the headroom of the hysteresis with it
void KqpRm::ArenaChargePublished() {
    StartRms({MakeArenaConfig(0, 100, 300), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    // drain the bootstrap time publishes first, so that the only publish left is the one of the arena
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm);

    auto tx = MakeTx(1, rm);
    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100}));
    AssertResourceManagerStats(rm, 1000, 99);
    TickArenaAdjust();
    AssertResourceManagerStats(rm, 700, 99); // 100 plus the headroom of 200
    CheckSnapshot(0, {{700, 99}, {1000, 100}}, rm);

    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100});
    AssertResourceManagerStats(rm, 700, 100);
    TickArenaAdjust();
    AssertResourceManagerStats(rm, 1000, 100);
    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm);
}

// A raised resource broker queue limit lets a refused arena growth through on the next pass: the resource manager
// is subscribed to the queue config, and the node total follows the limit the resource broker pushes
void KqpRm::ArenaDeficitRetriedOnQueueLimitRaise() {
    auto config = MakeKqpResourceManagerConfig();
    config.SetQueryMemoryLimit(100'000'000);

    StartRms({config, MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx1 = MakeTx(1, rm);
    auto tx2 = MakeTx(2, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 1'000}));
    UNIT_ASSERT(rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 49'500}));
    AssertResourceBrokerSensors(0, 1000, 0, 0, 1);
    AssertArenaSensors(0, 49'500, 49'500);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 1);

    // the queue limit goes from 50'000 to 100'000, the node total follows it
    auto brokerConfig = MakeResourceBrokerTestConfig();
    auto* limit = brokerConfig.MutableQueues(1)->MutableLimit();
    limit->ClearResource();
    limit->SetCpu(4);
    limit->SetMemory(100'000);
    auto configure = MakeHolder<TEvResourceBroker::TEvConfigure>();
    configure->Record.CopyFrom(brokerConfig);
    Runtime->Send(new IEventHandle(ResourceBrokers[0], Runtime->AllocateEdgeActor(), configure.Release()));
    TDispatchOptions pushed;
    pushed.FinalEvents.emplace_back(TEvResourceBroker::EvConfigResponse, 1);
    UNIT_ASSERT(Runtime->DispatchEvents(pushed));
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 50'500, 0, 0, 2);
    AssertArenaSensors(49'500, 49'500, 0);
    AssertResourceManagerStats(rm, 100'000 - 1'000 - 49'500, 98);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 1);

    rm->FreeResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 49'500});
    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 1'000});
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 1, 1);
    AssertResourceManagerStats(rm, 100'000, 100);
    tx1.Reset();
    AssertResourceBrokerSensors(0, 0, 0, 2, 0);
}

// A config reload that moves the thresholds resizes the arena on the next pass; a max below the min is raised to it
void KqpRm::ArenaConfigReloadMovesThresholds() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 100}));
    AssertResourceBrokerSensors(0, 100, 0, 0, 1);
    AssertArenaSensors(100, 100, 0);

    // free 0 < 100: grown to 100 + 200
    Reconfigure(MakeArenaConfig(0, 100, 300));
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 300, 0, 1, 1);
    AssertResourceManagerStats(rm, 700, 100);
    AssertArenaSensors(300, 100, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 2);

    // the max is raised to the min: the band is [300, 300], free 200 < 300: grown to 100 + 300
    Reconfigure(MakeArenaConfig(0, 300, 100));
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 400, 0, 2, 1);
    AssertResourceManagerStats(rm, 600, 100);
    AssertArenaSensors(400, 100, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 3);

    // free 300 > 0: shrunk to the demand
    Reconfigure(MakeArenaConfig(0, 0, 0));
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 100, 0, 2, 1);
    AssertResourceManagerStats(rm, 900, 100);
    AssertArenaSensors(100, 100, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 1);

    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 100});
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 3, 0);
    AssertResourceManagerStats(rm, 1000, 100);
}

// A demand past the node total: the arena task stops at the limit, the rest is a charged deficit that pushes the
// node total past it, so nothing else is admitted and the cookies report the pressure
void KqpRm::ArenaDemandPastNodeTotal() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 1'200}));
    AssertResourceBrokerSensors(0, 1000, 0, 0, 1);
    AssertArenaSensors(1000, 1'200, 200);
    AssertResourceManagerStats(rm, 0, 99);
    UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -400); // 800 - 1'200
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0); // the resource broker was not asked for more

    UNIT_ASSERT(!rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 1}));

    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 1'200});
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
    AssertArenaSensors(0, 0, 0);
    AssertResourceManagerStats(rm, 1000, 100);
    UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 800);
}

// The growth cap counts the memory the running queries hold, so the resource broker queue never holds the arena
// plus the tx tasks past the node total; what the cap withholds waits for the periodic pass
void KqpRm::ArenaGrowthCapLeavesRoomForRunningQueries() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx1 = MakeTx(1, rm);
    auto tx2 = MakeTx(2, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx1, 1, NRm::TKqpResourcesRequest{.Memory = 600}));
    AssertResourceBrokerSensors(0, 600, 0, 0, 1);
    AssertResourceManagerStats(rm, 400, 100);

    // the cap is 1000 - 600 held by tx1: only 400 of the 600 demanded are asked of the resource broker
    UNIT_ASSERT(rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 600}));
    AssertArenaSensors(400, 600, 200);
    AssertResourceBrokerSensors(0, 1000, 0, 0, 2); // tx2 has no task of its own: it requested no memory
    AssertResourceManagerStats(rm, 0, 100);
    UNIT_ASSERT_VALUES_EQUAL(tx2->GetMemoryAvailability(), -400); // 800 - 600 - 600
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0); // the resource broker was not asked past the cap

    // the memory tx1 returns raises the cap, and the pass takes the growth that was withheld
    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.Memory = 600});
    TickArenaAdjust();
    AssertArenaSensors(600, 600, 0);
    AssertResourceBrokerSensors(0, 600, 0, 1, 2); // the delta task merged into the arena task counts as finished
    AssertResourceManagerStats(rm, 400, 100);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 2);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);

    rm->FreeResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 600});
    TickArenaAdjust();
    AssertArenaSensors(0, 0, 0);
    AssertResourceBrokerSensors(0, 0, 0, 2, 1);
    AssertResourceManagerStats(rm, 1000, 100);

    tx1.Reset();
    AssertResourceBrokerSensors(0, 0, 0, 3, 0);
}

// The arena demand is the external memory plus the priced execution units, the way the node service requests them:
// one request that takes both, and each part alone moves the arena on the next pass
void KqpRm::ArenaMixesExternalMemoryAndUnits() {
    StartRms({MakeArenaConfig(10, 0, 0), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 2, .ExternalMemory = 100}));
    AssertResourceBrokerSensors(0, 120, 0, 0, 1); // 100 external plus 2 units of 10
    AssertArenaSensors(120, 120, 0);
    AssertResourceManagerStats(rm, 880, 98);
    UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().ExternalMemory, 100);

    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1});
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 110, 0, 0, 1);
    AssertArenaSensors(110, 110, 0);
    AssertResourceManagerStats(rm, 890, 99);

    // the unit left in use is re-priced, the external part is not
    Reconfigure(MakeArenaConfig(20, 0, 0));
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 120, 0, 1, 1);
    AssertArenaSensors(120, 120, 0);
    AssertResourceManagerStats(rm, 880, 99);

    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 100});
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 2, 0);
    AssertArenaSensors(0, 0, 0);
    AssertResourceManagerStats(rm, 1000, 100);
    UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().ExternalMemory, 0);
}

// Churn inside the band never reaches the resource broker: a task that starts or ends only moves the demand, and
// the arena stays above it the whole time
void KqpRm::ArenaAbsorbsChurn() {
    StartRms({MakeArenaConfig(0, 100, 300), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakeTx(1, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 100}));
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 300, 0, 0, 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);

    for (ui32 i = 0; i < 100; ++i) {
        UNIT_ASSERT(rm->AllocateResources(*tx, 2, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 50}));
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().ExternalMemory, 150);
        rm->FreeResources(*tx, 2, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 50});
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().ExternalMemory, 100);
    }

    // one growth at the start and nothing since: the node total carries the arena, which never dropped below the
    // demand
    AssertResourceBrokerSensors(0, 300, 0, 0, 1);
    AssertResourceManagerStats(rm, 700, 100);
    AssertArenaSensors(300, 100, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 0);

    // the last free empties the arena, which the periodic pass then gives back whole
    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 100});
    AssertResourceBrokerSensors(0, 300, 0, 0, 1);
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
    AssertArenaSensors(0, 0, 0);
    AssertResourceManagerStats(rm, 1000, 100);
}

// The free part of the arena counts against the node total like the demand: a Memory request it keeps out is
// refused, also after a pass while the free part stays within the band, and gets in once a pass has shrunk the arena
void KqpRm::ArenaFreePartRefusesMemory() {
    StartRms({MakeArenaConfig(0, 100, 300), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx1 = MakeTx(1, rm);
    auto tx2 = MakeTx(2, rm);

    // 700 plus the headroom of 200: the node total has 100 left
    UNIT_ASSERT(rm->AllocateResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 700}));
    AssertResourceBrokerSensors(0, 900, 0, 0, 1);
    AssertArenaSensors(900, 700, 0);
    AssertResourceManagerStats(rm, 100, 100);

    // the free part of 200 is within the band: the pass keeps it
    UNIT_ASSERT(!rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.Memory = 150}));
    TickArenaAdjust();
    UNIT_ASSERT(!rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.Memory = 150}));
    AssertArenaSensors(900, 700, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 0);

    // the free part of 600 is past the band: refused until the pass shrinks the arena to 300 + 200
    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 400});
    UNIT_ASSERT(!rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.Memory = 150}));
    TickArenaAdjust();
    AssertArenaSensors(500, 300, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 1);
    UNIT_ASSERT(rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.Memory = 150}));
    AssertResourceBrokerSensors(0, 650, 0, 0, 2);
    AssertResourceManagerStats(rm, 350, 100);

    rm->FreeResources(*tx2, 1, NRm::TKqpResourcesRequest{.Memory = 150});
    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 300});
    TickArenaAdjust();
    AssertArenaSensors(0, 0, 0);
    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 1, 1);
    tx2.Reset();
    AssertResourceBrokerSensors(0, 0, 0, 2, 0);
}

// A growth the cap withholds is not asked again on the allocation path, not even past the burst threshold: the
// demand beyond the arena is charged by the next pass
void KqpRm::ArenaWithheldGrowthWaitsForPass() {
    StartRms({MakeArenaConfig(0, 100, 300), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx1 = MakeTx(1, rm);
    auto tx2 = MakeTx(2, rm);

    UNIT_ASSERT(rm->AllocateResources(*tx1, 1, NRm::TKqpResourcesRequest{.Memory = 600}));

    // the band asks for 350 + 200 and the cap allows 1000 - 600: the arena takes 400 and goes on wanting more
    UNIT_ASSERT(rm->AllocateResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 350}));
    AssertResourceBrokerSensors(0, 1000, 0, 0, 2);
    AssertArenaSensors(400, 350, 0);
    AssertResourceManagerStats(rm, 0, 100);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);

    // 750 over 400 is past 400 + 300, and still no growth on the allocation path, and no charge
    UNIT_ASSERT(rm->AllocateResources(*tx2, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 400}));
    AssertArenaSensors(400, 350, 0);
    UNIT_ASSERT_VALUES_EQUAL(tx2->GetMemoryAvailability(), -200); // 800 - 600 - 400
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);

    // the pass charges the 350 the arena does not back; the cap still withholds the growth
    TickArenaAdjust();
    AssertArenaSensors(400, 750, 350);
    UNIT_ASSERT_VALUES_EQUAL(tx2->GetMemoryAvailability(), -550); // 800 - 600 - 750
    AssertResourceBrokerSensors(0, 1000, 0, 0, 2);
    AssertResourceManagerStats(rm, 0, 100);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrows"), 1);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);

    rm->FreeResources(*tx2, 2, NRm::TKqpResourcesRequest{.ExternalMemory = 400});
    rm->FreeResources(*tx2, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 350});
    rm->FreeResources(*tx1, 1, NRm::TKqpResourcesRequest{.Memory = 600});
    TickArenaAdjust();
    AssertArenaSensors(0, 0, 0);
    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 1, 1);
    tx1.Reset();
    AssertResourceBrokerSensors(0, 0, 0, 2, 0);
}

// The proto defaults against a kqp queue sized like a small node. A light task the node service starts charges the
// light limit twice, for the program and for its channels, plus the unit price, 3 MiB in all, and the band adds up
// to 128 MiB on top: the arena fills the queue long before the tasks do, and a Memory request that needs its free part
// is refused. Past the queue the demand is a charged deficit: extra Memory requests are refused and the cookies
// report the pressure
void KqpRm::ArenaDefaultsAgainstASmallQueue() {
    auto config = MakeKqpResourceManagerConfig();
    config.ClearExecutionUnitMemory();
    config.ClearMemoryArenaMinFreeSize();
    config.ClearMemoryArenaMaxFreeSize();
    StartRms({config, MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    // the kqp queue of a node with a hard limit of 1.25 GiB, at the 20 percent the memory controller gives it; the
    // resource manager takes its node total from the queue config the resource broker pushes
    const ui64 queue = 256_MB;
    auto brokerConfig = MakeResourceBrokerTestConfig();
    auto* limit = brokerConfig.MutableQueues(1)->MutableLimit();
    limit->ClearResource();
    limit->SetCpu(4);
    limit->SetMemory(queue);
    brokerConfig.MutableResourceLimit()->ClearResource();
    brokerConfig.MutableResourceLimit()->AddResource(10);
    brokerConfig.MutableResourceLimit()->AddResource(queue);
    auto configure = MakeHolder<TEvResourceBroker::TEvConfigure>();
    configure->Record.CopyFrom(brokerConfig);
    Runtime->Send(new IEventHandle(ResourceBrokers[0], Runtime->AllocateEdgeActor(), configure.Release()));
    TDispatchOptions pushed;
    pushed.FinalEvents.emplace_back(TEvResourceBroker::EvConfigResponse, 1);
    UNIT_ASSERT(Runtime->DispatchEvents(pushed));
    AssertResourceManagerStats(rm, queue, 100);

    auto tx = MakeTx(1, rm);
    // what the node service asks for a light task: the light limit for the program and again for its channels
    const NRm::TKqpResourcesRequest lightTask{.ExecutionUnits = 1, .ExternalMemory = 2 * 1_MB};

    // 60 tasks demand 180 MiB. The 43rd overruns the empty arena by more than 128 MiB and grows it to its 129 MiB
    // plus 80 MiB; the pass then finds a free part under 32 MiB and grows the arena as far as the queue allows, so
    // the free part of 76 MiB is within the band and a Memory request of 30 MiB is refused, before and after a pass
    for (ui64 taskId = 1; taskId <= 60; ++taskId) {
        UNIT_ASSERT(rm->AllocateResources(*tx, taskId, lightTask));
    }
    AssertArenaSensors(209_MB, 129_MB, 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);
    TickArenaAdjust();
    AssertArenaSensors(256_MB, 180_MB, 0);
    AssertResourceManagerStats(rm, 0, 40);
    UNIT_ASSERT(!rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 30_MB}));
    TickArenaAdjust();
    UNIT_ASSERT(!rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 30_MB}));
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);

    // 30 more tasks push the demand past the queue: charged as a deficit by the pass, the resource broker not asked
    // for more, and a Memory request of 1 MiB is refused with the cookie reporting the pressure
    for (ui64 taskId = 61; taskId <= 90; ++taskId) {
        UNIT_ASSERT(rm->AllocateResources(*tx, taskId, lightTask));
    }
    TickArenaAdjust();
    AssertArenaSensors(256_MB, 270_MB, 14_MB);
    AssertResourceManagerStats(rm, 0, 10);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);
    UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaBurstGrows"), 1);
    auto refused = rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 1_MB});
    UNIT_ASSERT(!refused);
    UNIT_ASSERT_EQUAL(refused.GetStatus(), NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY);
    UNIT_ASSERT_LT(tx->GetMemoryAvailability(), 0);

    for (ui64 taskId = 1; taskId <= 90; ++taskId) {
        rm->FreeResources(*tx, taskId, lightTask);
    }
    TickArenaAdjust();
    AssertArenaSensors(0, 0, 0);
    AssertResourceManagerStats(rm, queue, 100);
    tx.Reset();
    AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 0);
}

// The arena task outlives the queries it backs, so the resource broker must not take its lifetime for the
// execution time of a query. That average is what the broker shows for the task type and estimates a task's
// finish time from, and the estimate is what a queue's planned resource usage, and so its turn to be scheduled,
// would be built on if a kqp task ever waited in the queue; the instant submits of the resource manager never do
void KqpRm::ArenaLifetimeIsNotAQueryDuration() {
    StartRms({MakeArenaConfig(0, 100, 300), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        // one growth, and no merge with it: the arena task is the first the queue has seen
        auto tx = MakeTx(1, rm);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 100}));
        TickArenaAdjust();
        AssertResourceBrokerSensors(0, 300, 0, 0, 1);

        // long enough that the resource broker would visibly raise its estimate if it took this for a query:
        // one sample of a minute in a window of twenty would carry the average from 5 seconds to nearly 8
        Runtime->UpdateCurrentTime(Runtime->GetCurrentTime() + TDuration::Minutes(1));
        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 100});
    }
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);

    // the minute the arena lasted left the estimate where it started
    const TInstant probeAt = Runtime->GetCurrentTime();
    auto broker = GetInstantBroker();
    const TActorId client = Runtime->AllocateEdgeActor();
    UNIT_ASSERT(broker->SubmitTaskInstant(TEvResourceBroker::TEvSubmitTask(
        1'000, "probe", {0, 1}, NLocalDb::KqpResourceManagerTaskName, 0, {}), client));
    UNIT_ASSERT_STRING_CONTAINS(RenderBrokerState(),
        "FinishTime: " + (probeAt + KQP_TASK_DEFAULT_DURATION).ToStringLocalUpToSeconds());

    UNIT_ASSERT(broker->FinishTaskInstant(TEvResourceBroker::TEvFinishTask(1'000, /* cancel */ true), client));
}

// A node total that drops below what the arena holds takes effect on the arena too: the next pass gives back the
// surplus it is no longer entitled to, down to the demand it still backs
void KqpRm::ArenaShrinksWhenNodeTotalDrops() {
    StartRms({MakeArenaConfig(0, 100, 300), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    // the resource broker queue config as the resource manager receives it, as in TotalLimitReconfigure
    auto setTotal = [&](ui64 memory) {
        auto response = MakeHolder<TEvResourceBroker::TEvConfigResponse>();
        response->QueueConfig.ConstructInPlace();
        response->QueueConfig->MutableLimit()->SetMemory(memory);
        Runtime->Send(new IEventHandle(ResourceManagers.front(), Runtime->AllocateEdgeActor(), response.Release()));
        Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
    };

    {
        auto tx = MakeTx(1, rm);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 150}));
        TickArenaAdjust();
        AssertResourceBrokerSensors(0, 350, 0, 0, 1); // 150 plus the headroom of 200
        AssertResourceManagerStats(rm, 650, 100);
        AssertArenaSensors(350, 150, 0);

        // still room for all of it
        setTotal(500);
        TickArenaAdjust();
        AssertResourceBrokerSensors(0, 350, 0, 0, 1);
        AssertResourceManagerStats(rm, 150, 100);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 0);

        // 350 no longer fits: the arena gives back what it is not entitled to and keeps backing its 150
        setTotal(250);
        TickArenaAdjust();
        AssertResourceBrokerSensors(0, 250, 0, 0, 1);
        AssertArenaSensors(250, 150, 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 1);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 150});
    }

    // and the whole of it once the demand is gone
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
    AssertArenaSensors(0, 0, 0);
    AssertResourceManagerStats(rm, 250, 100);
}

// A node total that drops is acted on even while the band is asking for a bigger arena: the part the arena does
// not back is given back, although the growth itself stays out of reach
void KqpRm::ArenaReclaimsSurplusWhileGrowthIsWanted() {
    StartRms({MakeArenaConfig(0, 100, 300), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto setTotal = [&](ui64 memory) {
        auto response = MakeHolder<TEvResourceBroker::TEvConfigResponse>();
        response->QueueConfig.ConstructInPlace();
        response->QueueConfig->MutableLimit()->SetMemory(memory);
        Runtime->Send(new IEventHandle(ResourceManagers.front(), Runtime->AllocateEdgeActor(), response.Release()));
        Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
    };

    setTotal(300);
    {
        auto tx = MakeTx(1, rm);
        // the band asks for 250 + 200 and the node total allows 300: the arena takes what it may and goes on
        // wanting more, its free part of 50 staying below the 100 the band wants
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 250}));
        TickArenaAdjust();
        AssertResourceBrokerSensors(0, 300, 0, 0, 1);
        AssertArenaSensors(300, 250, 0);

        // the total drops below the arena: the 20 it does not back are given back all the same
        setTotal(280);
        TickArenaAdjust();
        AssertResourceBrokerSensors(0, 280, 0, 0, 1);
        AssertArenaSensors(280, 250, 0);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaShrinks"), 1);
        UNIT_ASSERT_VALUES_EQUAL(RmRate("RM/ArenaGrowFailures"), 0);

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 250});
    }

    // the pass gives the arena back whole
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
    AssertArenaSensors(0, 0, 0);
    AssertResourceManagerStats(rm, 280, 100);
}

// The resource broker admits a task larger than its own total limit only while nothing at all is running, so an
// arena that outlived its demand would block such a task for good
void KqpRm::ArenaReleasesIdleReservation() {
    StartRms({MakeArenaConfig(0, 100, 300), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto broker = GetInstantBroker();
    const TActorId client = Runtime->AllocateEdgeActor();
    const auto oversized = [&](ui64 taskId) {
        return TEvResourceBroker::TEvSubmitTask(taskId, "oversized",
            {0, TOTAL_MEMORY_LIMIT + 1}, "unknown", 0, {});
    };

    // nothing has run yet, so the task is admitted over the limit
    UNIT_ASSERT(broker->SubmitTaskInstant(oversized(1), client));
    UNIT_ASSERT(broker->FinishTaskInstant(TEvResourceBroker::TEvFinishTask(1), client));

    {
        auto tx = MakeTx(1, rm);
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 100}));
        TickArenaAdjust();
        AssertResourceBrokerSensors(0, 300, 0, 0, 1);
        UNIT_ASSERT(!broker->SubmitTaskInstant(oversized(2), client)); // the arena task is running

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExternalMemory = 100});
    }

    // the arena holds its task until the periodic pass
    UNIT_ASSERT(!broker->SubmitTaskInstant(oversized(3), client));
    TickArenaAdjust();
    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
    AssertArenaSensors(0, 0, 0);

    UNIT_ASSERT(broker->SubmitTaskInstant(oversized(4), client));
    UNIT_ASSERT(broker->FinishTaskInstant(TEvResourceBroker::TEvFinishTask(4), client));
}

// The arena charges the node total only: the pool of a tx sees the memory requests of the tx, not its prepay
void KqpRm::ArenaDoesNotChargePools() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());
    auto tx = MakePoolTx(1, rm, /* memoryPoolPercent = */ 50); // pool limit 500, threshold at 400 used
    UNIT_ASSERT(tx->PoolMemoryCookie);

    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 300}));
    UNIT_ASSERT_VALUES_EQUAL(tx->TotalMemoryCookie->MemoryAvailability.load(), 500); // 1000 - 300 - 200
    UNIT_ASSERT_VALUES_EQUAL(tx->PoolMemoryCookie->MemoryAvailability.load(), 400);
    UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 400);

    UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100}));
    UNIT_ASSERT_VALUES_EQUAL(tx->TotalMemoryCookie->MemoryAvailability.load(), 400);
    UNIT_ASSERT_VALUES_EQUAL(tx->PoolMemoryCookie->MemoryAvailability.load(), 300);
    UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 300);

    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100});
    rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = 300});
    TickArenaAdjust();
    UNIT_ASSERT_VALUES_EQUAL(tx->TotalMemoryCookie->MemoryAvailability.load(), 800);
    UNIT_ASSERT_VALUES_EQUAL(tx->PoolMemoryCookie->MemoryAvailability.load(), 400);
}

} // namespace NKqp
} // namespace NKikimr
