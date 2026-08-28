#include "auto_config_initializer.h"
#include "config_helpers.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/subsystems/stats.h>
#include <ydb/library/actors/core/thread_context.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/event.h>

#include <atomic>

Y_UNIT_TEST_SUITE(AutoConfig) {

using namespace NKikimr;
using namespace NAutoConfigInitializer;

namespace {

class TDelayedSignalActor : public NActors::TActorBootstrapped<TDelayedSignalActor> {
public:
    TDelayedSignalActor(
            TManualEvent* ready,
            TManualEvent* done,
            NActors::TActorId* actorId,
            std::atomic<ui32>* executionOwnerPoolId)
        : Ready(ready)
        , Done(done)
        , ActorId(actorId)
        , ExecutionOwnerPoolId(executionOwnerPoolId)
    {}

    void Bootstrap() {
        Become(&TDelayedSignalActor::StateFunc);
        *ActorId = SelfId();
        Ready->Signal();
    }

    STFUNC(StateFunc) {
        Y_VERIFY_S(
            ev->GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType,
            "unexpected event type# " << ev->GetTypeRewrite());
        ExecutionOwnerPoolId->store(NActors::TlsThreadContext->OwnerPoolId(), std::memory_order_release);
        Done->Signal();
        PassAway();
    }

private:
    TManualEvent* const Ready;
    TManualEvent* const Done;
    NActors::TActorId* const ActorId;
    std::atomic<ui32>* const ExecutionOwnerPoolId;
};

class TStartAdjacentActor : public NActors::TActorBootstrapped<TStartAdjacentActor> {
public:
    TStartAdjacentActor(
            ui32 adjacentPoolId,
            TManualEvent* ready,
            TManualEvent* done,
            NActors::TActorId* actorId,
            std::atomic<ui32>* executionOwnerPoolId)
        : AdjacentPoolId(adjacentPoolId)
        , Ready(ready)
        , Done(done)
        , ActorId(actorId)
        , ExecutionOwnerPoolId(executionOwnerPoolId)
    {}

    void Bootstrap() {
        Register(
            new TDelayedSignalActor(Ready, Done, ActorId, ExecutionOwnerPoolId),
            NActors::TMailboxType::HTSwap,
            AdjacentPoolId);
        PassAway();
    }

private:
    const ui32 AdjacentPoolId;
    TManualEvent* const Ready;
    TManualEvent* const Done;
    NActors::TActorId* const ActorId;
    std::atomic<ui32>* const ExecutionOwnerPoolId;
};

THolder<NActors::TActorSystemSetup> CreateActorSystemSetup(
        const NKikimrConfig::TActorSystemConfig& config)
{
    auto setup = MakeHolder<NActors::TActorSystemSetup>();
    setup->NodeId = 1;
    setup->CpuManager.Shared.United = config.GetUseUnitedPool();
    setup->CpuManager.PingInfoByPool.resize(config.ExecutorSize());
    ui32 poolId = 0;
    for (const auto& poolConfig : config.GetExecutor()) {
        NActorSystemConfigHelpers::AddExecutorPool(
            setup->CpuManager,
            poolConfig,
            config,
            poolId++,
            nullptr);
    }
    setup->Scheduler.Reset(NActors::CreateSchedulerThread(
        NActorSystemConfigHelpers::CreateSchedulerConfig(config.GetScheduler())));
    return setup;
}

const NActors::TBasicExecutorPoolConfig& FindBasicPool(
        const NActors::TCpuManagerConfig& config,
        ui32 poolId)
{
    const NActors::TBasicExecutorPoolConfig* result = nullptr;
    for (const auto& pool : config.Basic) {
        if (pool.PoolId == poolId) {
            result = &pool;
            break;
        }
    }
    UNIT_ASSERT_C(result, "BASIC executor pool " << poolId << " was not configured");
    return *result;
}

ui64 GetSharedParkedTicks(const NActors::TActorSystem& actorSystem, ui32 poolId) {
    NActors::TExecutorPoolStats poolStats;
    TVector<NActors::TExecutorThreadStats> threadStats;
    TVector<NActors::TExecutorThreadStats> sharedThreadStats;
    NActors::GetActorSystemStats(actorSystem).GetPoolStats(
        poolId,
        poolStats,
        threadStats,
        sharedThreadStats);

    ui64 parkedTicks = 0;
    for (const auto& stats : sharedThreadStats) {
        parkedTicks += stats.SafeParkedTicks;
    }
    return parkedTicks;
}

bool WaitForSharedThreadToPark(
        const NActors::TActorSystem& actorSystem,
        ui32 poolId,
        TDuration timeout)
{
    const ui64 initialParkedTicks = GetSharedParkedTicks(actorSystem, poolId);
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        Sleep(TDuration::MilliSeconds(1));
        if (GetSharedParkedTicks(actorSystem, poolId) > initialParkedTicks) {
            return true;
        }
    }
    return false;
}

} // anonymous namespace

#define ASSERT_POOLS(pools, sys, user, batch, io, ic) \
    do { \
        UNIT_ASSERT_VALUES_EQUAL(pools.SystemPoolId, sys); \
        UNIT_ASSERT_VALUES_EQUAL(pools.UserPoolId, user); \
        UNIT_ASSERT_VALUES_EQUAL(pools.BatchPoolId, batch); \
        UNIT_ASSERT_VALUES_EQUAL(pools.IOPoolId, io); \
        UNIT_ASSERT_VALUES_EQUAL(pools.ICPoolId, ic); \
    } while (false) \
// ASSERT_POOLS

Y_UNIT_TEST(GetASPoolsith1CPU) {
    TASPools pools = GetASPools(1);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(1);
    pools = GetASPools(config, true);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    UNIT_ASSERT_VALUES_EQUAL(pools.GetIndeces(),  (std::vector<ui8>{0, 0, 0, 1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{40, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolNames(), (std::vector<TString>{"Common", "IO"}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolCount(), 2);
}

Y_UNIT_TEST(GetASPoolsWith2CPUs) {
    TASPools pools = GetASPools(2);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(2);
    pools = GetASPools(config, true);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    UNIT_ASSERT_VALUES_EQUAL(pools.GetIndeces(),  (std::vector<ui8>{0, 0, 0, 1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{40, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolNames(), (std::vector<TString>{"Common", "IO"}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolCount(), 2);
}

Y_UNIT_TEST(GetASPoolsWith3CPUs) {
    TASPools pools = GetASPools(3);
    ASSERT_POOLS(pools, 0, 0, 1, 2, 3);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(3);
    pools = GetASPools(config, true);
    ASSERT_POOLS(pools, 0, 0, 1, 2, 3);

    UNIT_ASSERT_VALUES_EQUAL(pools.GetIndeces(),  (std::vector<ui8>{0, 0, 1, 2, 3}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{30, 10, 0, 40}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolNames(), (std::vector<TString>{"Common", "Batch", "IO", "IC"}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolCount(), 4);
}

Y_UNIT_TEST(GetASPoolsWith4AndMoreCPUs) {
    for (ui32 threadCount = 4; threadCount < 128; ++threadCount) {
        TASPools pools = GetASPools(threadCount);
        ASSERT_POOLS(pools, 0, 1, 2, 3, 4);

        NKikimrConfig::TActorSystemConfig config;
        config.SetUseAutoConfig(true);
        config.SetCpuCount(threadCount);
        pools = GetASPools(config, true);
        ASSERT_POOLS(pools, 0, 1, 2, 3, 4);

        UNIT_ASSERT_VALUES_EQUAL(pools.GetIndeces(),  (std::vector<ui8>{0, 1, 2, 3, 4}));
        UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{30, 20, 10, 0, 40}));
        UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolNames(), (std::vector<TString>{"System", "User", "Batch", "IO", "IC"}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetRealPoolCount(), 5);
    }
}


Y_UNIT_TEST(GetServicePoolsWith1CPU) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(1);
    TMap<TString, ui32> services = GetServicePools(config, true);
    UNIT_ASSERT_VALUES_EQUAL(services, (TMap<TString, ui32>{{"Interconnect", 0}}));
}

Y_UNIT_TEST(GetServicePoolsWith2CPUs) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(2);
    TMap<TString, ui32> services = GetServicePools(config, true);
    UNIT_ASSERT_VALUES_EQUAL(services, (TMap<TString, ui32>{{"Interconnect", 0}}));
}

Y_UNIT_TEST(GetServicePoolsWith3CPUs) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(3);
    TMap<TString, ui32> services = GetServicePools(config, true);
    UNIT_ASSERT_VALUES_EQUAL(services, (TMap<TString, ui32>{{"Interconnect", 3}}));
}

Y_UNIT_TEST(GetServicePoolsWith4AndMoreCPUs) {
    for (ui32 threadCount = 4; threadCount < 128; ++threadCount) {
        NKikimrConfig::TActorSystemConfig config;
        config.SetUseAutoConfig(true);
        config.SetCpuCount(threadCount);
        TMap<TString, ui32> services = GetServicePools(config, true);
        UNIT_ASSERT_VALUES_EQUAL(services, (TMap<TString, ui32>{{"Interconnect", 4}}));
    }
}

Y_UNIT_TEST(SharedAndUnitedAutoConfigMatrix) {
    for (ui32 cpuCount = 1; cpuCount <= 4; ++cpuCount) {
        for (const bool useSharedThreads : {false, true}) {
            for (const bool useUnitedPool : {false, true}) {
                NKikimrConfig::TActorSystemConfig config;
                config.SetCpuCount(cpuCount);
                config.SetUseSharedThreads(useSharedThreads);
                config.SetUseUnitedPool(useUnitedPool);

                ApplyAutoConfig(&config, false, false);

                bool hasBasicPool = false;
                for (const auto& executor : config.GetExecutor()) {
                    if (executor.GetType() != NKikimrConfig::TActorSystemConfig::TExecutor::BASIC) {
                        continue;
                    }
                    hasBasicPool = true;
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        executor.GetAllThreadsAreShared(), useUnitedPool,
                        "cpu# " << cpuCount << " shared# " << useSharedThreads
                        << " united# " << useUnitedPool << " pool# " << executor.GetName());
                }
                UNIT_ASSERT_C(hasBasicPool, "cpu# " << cpuCount << " produced no BASIC pools");
            }
        }
    }
}

Y_UNIT_TEST(AutoConfiguredAdjacentPoolWakesAfterIdle) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetCpuCount(2);
    config.SetUseSharedThreads(true);
    config.SetUseUnitedPool(true);

    ApplyAutoConfig(&config, false, false);

    const ui32 ownerPoolId = config.GetSysExecutor();
    const ui32 adjacentPoolId = config.GetBatchExecutor();
    auto setup = CreateActorSystemSetup(config);

    UNIT_ASSERT(setup->CpuManager.Shared.United);
    UNIT_ASSERT_VALUES_EQUAL(setup->CpuManager.Basic.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(setup->CpuManager.IO.size(), 1);

    const auto& ownerPool = FindBasicPool(setup->CpuManager, ownerPoolId);
    UNIT_ASSERT_VALUES_EQUAL(ownerPool.PoolName, "User");
    UNIT_ASSERT_VALUES_EQUAL(ownerPool.Threads, 1);
    UNIT_ASSERT_VALUES_EQUAL(ownerPool.DefaultThreadCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(ownerPool.MaxThreadCount, 1);
    UNIT_ASSERT(ownerPool.HasSharedThread);
    UNIT_ASSERT(ownerPool.AllThreadsAreShared);
    UNIT_ASSERT_VALUES_EQUAL(ownerPool.ForcedForeignSlotCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(ownerPool.AdjacentPools, (std::vector<i16>{static_cast<i16>(adjacentPoolId)}));

    const auto& adjacentPool = FindBasicPool(setup->CpuManager, adjacentPoolId);
    UNIT_ASSERT_VALUES_EQUAL(adjacentPool.PoolName, "Batch");
    UNIT_ASSERT_VALUES_EQUAL(adjacentPool.Threads, 0);
    UNIT_ASSERT_VALUES_EQUAL(adjacentPool.DefaultThreadCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(adjacentPool.MaxThreadCount, 0);
    UNIT_ASSERT(!adjacentPool.HasSharedThread);
    UNIT_ASSERT(adjacentPool.AllThreadsAreShared);
    UNIT_ASSERT_VALUES_EQUAL(adjacentPool.ForcedForeignSlotCount, 0);
    UNIT_ASSERT(adjacentPool.AdjacentPools.empty());

    NActors::TActorSystem actorSystem(setup);
    actorSystem.Start();

    TManualEvent ready;
    TManualEvent done;
    NActors::TActorId adjacentActorId;
    std::atomic<ui32> executionOwnerPoolId = Max<ui32>();
    actorSystem.Register(
        new TStartAdjacentActor(
            adjacentPoolId,
            &ready,
            &done,
            &adjacentActorId,
            &executionOwnerPoolId),
        NActors::TMailboxType::HTSwap,
        ownerPoolId);

    const bool actorIsReady = ready.WaitT(TDuration::Seconds(5));
    const bool ownerIsParked = actorIsReady
        && WaitForSharedThreadToPark(actorSystem, ownerPoolId, TDuration::Seconds(5));
    if (ownerIsParked) {
        actorSystem.Schedule(
            TDuration::MilliSeconds(100),
            new NActors::IEventHandle(
                adjacentActorId,
                NActors::TActorId(),
                new NActors::TEvents::TEvWakeup()));
    }
    const bool completed = ownerIsParked && done.WaitT(TDuration::Seconds(5));

    actorSystem.Stop();
    UNIT_ASSERT_C(actorIsReady, "auto-configured Batch actor did not initialize");
    UNIT_ASSERT_C(ownerIsParked, "auto-configured adjacent User thread did not become idle");
    UNIT_ASSERT_C(completed,
        "auto-configured Batch pool did not execute a delayed event after its adjacent User owner became idle");
    UNIT_ASSERT_VALUES_EQUAL_C(
        executionOwnerPoolId.load(std::memory_order_acquire),
        ownerPoolId,
        "Batch activation was not executed by its adjacent User owner");
}

} // Y_UNIT_TEST_SUITE(AutoConfig)
