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

#include <array>
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

class THoldThreadActor : public NActors::TActorBootstrapped<THoldThreadActor> {
public:
    THoldThreadActor(TManualEvent* started, TManualEvent* release)
        : Started(started)
        , Release(release)
    {}

    void Bootstrap() {
        Started->Signal();
        Release->Wait();
        PassAway();
    }

private:
    TManualEvent* const Started;
    TManualEvent* const Release;
};

class TSignalOnBootstrapActor : public NActors::TActorBootstrapped<TSignalOnBootstrapActor> {
public:
    explicit TSignalOnBootstrapActor(TManualEvent* done)
        : Done(done)
    {}

    void Bootstrap() {
        Done->Signal();
        PassAway();
    }

private:
    TManualEvent* const Done;
};

class TRecordOwnerOnBootstrapActor
    : public NActors::TActorBootstrapped<TRecordOwnerOnBootstrapActor>
{
public:
    TRecordOwnerOnBootstrapActor(
            TManualEvent* done,
            std::atomic<ui32>* executionOwnerPoolId)
        : Done(done)
        , ExecutionOwnerPoolId(executionOwnerPoolId)
    {}

    void Bootstrap() {
        ExecutionOwnerPoolId->store(
            NActors::TlsThreadContext->OwnerPoolId(),
            std::memory_order_release);
        Done->Signal();
        PassAway();
    }

private:
    TManualEvent* const Done;
    std::atomic<ui32>* const ExecutionOwnerPoolId;
};

class TSendWakeupActor : public NActors::TActorBootstrapped<TSendWakeupActor> {
public:
    TSendWakeupActor(NActors::TActorId target, TManualEvent* sent)
        : Target(target)
        , Sent(sent)
    {}

    void Bootstrap() {
        Send(Target, new NActors::TEvents::TEvWakeup());
        Sent->Signal();
        PassAway();
    }

private:
    const NActors::TActorId Target;
    TManualEvent* const Sent;
};

THolder<NActors::TActorSystemSetup> CreateActorSystemSetup(
        const NKikimrConfig::TActorSystemConfig& config)
{
    auto setup = MakeHolder<NActors::TActorSystemSetup>();
    setup->NodeId = 1;
    setup->CpuManager.Shared.United = config.GetUseUnitedPool();
    NActorSystemConfigHelpers::AddExecutorPools(setup->CpuManager, config, nullptr);
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

enum class EAdjacentActivationSource {
    DirectSend,
    ActorSend,
    Registration,
};

void TestAutoConfiguredAdjacentPoolActivationAfterIdle(
        EAdjacentActivationSource source)
{
    NKikimrConfig::TActorSystemConfig config;
    config.SetCpuCount(2);
    config.SetUseSharedThreads(true);
    config.SetUseUnitedPool(true);

    ApplyAutoConfig(&config, false, false);

    const ui32 ownerPoolId = config.GetSysExecutor();
    const ui32 adjacentPoolId = config.GetBatchExecutor();
    const ui32 foreignPoolId = config.GetServiceExecutor(0).GetExecutorId();
    auto setup = CreateActorSystemSetup(config);

    const auto& ownerPool = FindBasicPool(setup->CpuManager, ownerPoolId);
    UNIT_ASSERT_VALUES_EQUAL(ownerPool.DefaultThreadCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(
        ownerPool.AdjacentPools,
        (std::vector<i16>{static_cast<i16>(adjacentPoolId)}));

    const auto& adjacentPool = FindBasicPool(setup->CpuManager, adjacentPoolId);
    UNIT_ASSERT_VALUES_EQUAL(adjacentPool.DefaultThreadCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(adjacentPool.ForcedForeignSlotCount, 0);

    const auto& foreignPool = FindBasicPool(setup->CpuManager, foreignPoolId);
    UNIT_ASSERT_VALUES_EQUAL(foreignPool.DefaultThreadCount, 1);

    NActors::TActorSystem actorSystem(setup);
    actorSystem.Start();

    TManualEvent ready;
    TManualEvent sent;
    TManualEvent done;
    NActors::TActorId adjacentActorId;
    std::atomic<ui32> executionOwnerPoolId = Max<ui32>();

    bool actorIsReady = source == EAdjacentActivationSource::Registration;
    if (!actorIsReady) {
        actorSystem.Register(
            new TStartAdjacentActor(
                adjacentPoolId,
                &ready,
                &done,
                &adjacentActorId,
                &executionOwnerPoolId),
            NActors::TMailboxType::HTSwap,
            ownerPoolId);
        actorIsReady = ready.WaitT(TDuration::Seconds(5));
    }

    const bool ownerIsParked = actorIsReady
        && WaitForSharedThreadToPark(actorSystem, ownerPoolId, TDuration::Seconds(5));

    bool activationWasPublished = false;
    if (ownerIsParked) {
        switch (source) {
            case EAdjacentActivationSource::DirectSend:
                activationWasPublished = actorSystem.Send(
                    adjacentActorId,
                    new NActors::TEvents::TEvWakeup());
                break;

            case EAdjacentActivationSource::ActorSend:
                actorSystem.Register(
                    new TSendWakeupActor(adjacentActorId, &sent),
                    NActors::TMailboxType::HTSwap,
                    foreignPoolId);
                activationWasPublished = sent.WaitT(TDuration::Seconds(5));
                break;

            case EAdjacentActivationSource::Registration:
                actorSystem.Register(
                    new TRecordOwnerOnBootstrapActor(&done, &executionOwnerPoolId),
                    NActors::TMailboxType::HTSwap,
                    adjacentPoolId);
                activationWasPublished = true;
                break;
        }
    }

    const bool completed = activationWasPublished
        && done.WaitT(TDuration::Seconds(5));

    actorSystem.Stop();
    UNIT_ASSERT_C(actorIsReady, "auto-configured Batch actor did not initialize");
    UNIT_ASSERT_C(ownerIsParked,
        "auto-configured adjacent User owner did not become idle");
    UNIT_ASSERT_C(activationWasPublished,
        "failed to publish an activation to the auto-configured Batch pool");
    UNIT_ASSERT_C(completed,
        "auto-configured Batch activation did not execute after its owner became idle");
    UNIT_ASSERT_VALUES_EQUAL_C(
        executionOwnerPoolId.load(std::memory_order_acquire),
        ownerPoolId,
        "Batch activation was not executed by its adjacent User owner");
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

Y_UNIT_TEST(GetManualPoolsUseExecutorIndicesDirectly) {
    NKikimrConfig::TActorSystemConfig config;

    auto* placement = config.AddExecutor();
    placement->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    placement->SetThreads(1);
    placement->SetPlacement(0);

    auto* system = config.AddExecutor();
    system->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    system->SetName("System");

    auto* user = config.AddExecutor();
    user->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    user->SetName("User");

    auto* io = config.AddExecutor();
    io->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::IO);
    io->SetName("IO");

    auto* batch = config.AddExecutor();
    batch->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
    batch->SetName("Batch");

    config.SetSysExecutor(1);
    config.SetUserExecutor(2);
    config.SetIoExecutor(3);
    config.SetBatchExecutor(4);

    auto* interconnect = config.AddServiceExecutor();
    interconnect->SetServiceName("Interconnect");
    interconnect->SetExecutorId(3);

    auto* background = config.AddServiceExecutor();
    background->SetServiceName("Background");
    background->SetExecutorId(4);

    const TASPools pools = GetASPools(config, false);
    ASSERT_POOLS(pools, 1, 2, 4, 3, 3);

    TMap<TString, ui32> services = GetServicePools(config, false);
    UNIT_ASSERT_VALUES_EQUAL(services, (TMap<TString, ui32>{{"Background", 4}, {"Interconnect", 3}}));
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

Y_UNIT_TEST(AutoConfiguredAdjacentPoolWaitsForBusyOwnerWithoutForeignSlots) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetCpuCount(2);
    config.SetUseSharedThreads(true);
    config.SetUseUnitedPool(true);

    ApplyAutoConfig(&config, false, false);

    const ui32 ownerPoolId = config.GetSysExecutor();
    const ui32 adjacentPoolId = config.GetBatchExecutor();
    const ui32 foreignPoolId = config.GetServiceExecutor(0).GetExecutorId();
    auto setup = CreateActorSystemSetup(config);

    const auto& ownerPool = FindBasicPool(setup->CpuManager, ownerPoolId);
    UNIT_ASSERT_VALUES_EQUAL(ownerPool.DefaultThreadCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(
        ownerPool.AdjacentPools,
        (std::vector<i16>{static_cast<i16>(adjacentPoolId)}));

    const auto& adjacentPool = FindBasicPool(setup->CpuManager, adjacentPoolId);
    UNIT_ASSERT_VALUES_EQUAL(adjacentPool.DefaultThreadCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(adjacentPool.ForcedForeignSlotCount, 0);

    const auto& foreignPool = FindBasicPool(setup->CpuManager, foreignPoolId);
    UNIT_ASSERT_VALUES_EQUAL(foreignPool.DefaultThreadCount, 1);

    NActors::TActorSystem actorSystem(setup);
    actorSystem.Start();

    TManualEvent adjacentReady;
    TManualEvent adjacentDone;
    NActors::TActorId adjacentActorId;
    std::atomic<ui32> executionOwnerPoolId = Max<ui32>();
    actorSystem.Register(
        new TStartAdjacentActor(
            adjacentPoolId,
            &adjacentReady,
            &adjacentDone,
            &adjacentActorId,
            &executionOwnerPoolId),
        NActors::TMailboxType::HTSwap,
        ownerPoolId);
    const bool adjacentActorIsReady = adjacentReady.WaitT(TDuration::Seconds(5));

    TManualEvent ownerStarted;
    TManualEvent releaseOwner;
    if (adjacentActorIsReady) {
        actorSystem.Register(
            new THoldThreadActor(&ownerStarted, &releaseOwner),
            NActors::TMailboxType::HTSwap,
            adjacentPoolId);
    }
    const bool ownerIsBusy = adjacentActorIsReady
        && ownerStarted.WaitT(TDuration::Seconds(5));

    TManualEvent foreignDone;
    if (ownerIsBusy) {
        actorSystem.Register(
            new TSignalOnBootstrapActor(&foreignDone),
            NActors::TMailboxType::HTSwap,
            foreignPoolId);
    }
    const bool foreignThreadIsAvailable = ownerIsBusy
        && foreignDone.WaitT(TDuration::Seconds(5));

    bool eventWasSent = false;
    bool completedWhileOwnerWasBusy = false;
    if (foreignThreadIsAvailable) {
        eventWasSent = actorSystem.Send(
            adjacentActorId,
            new NActors::TEvents::TEvWakeup());
        completedWhileOwnerWasBusy = eventWasSent
            && adjacentDone.WaitT(TDuration::Seconds(1));
    }

    releaseOwner.Signal();
    const bool completedAfterOwnerWasReleased = eventWasSent
        && adjacentDone.WaitT(TDuration::Seconds(5));

    actorSystem.Stop();
    UNIT_ASSERT_C(adjacentActorIsReady, "auto-configured Batch actor did not initialize");
    UNIT_ASSERT_C(ownerIsBusy, "adjacent User owner did not start blocking work");
    UNIT_ASSERT_C(foreignThreadIsAvailable,
        "non-adjacent shared thread did not execute while the adjacent owner was busy");
    UNIT_ASSERT_C(eventWasSent, "failed to send an event to the auto-configured Batch actor");
    UNIT_ASSERT_C(!completedWhileOwnerWasBusy,
        "Batch activation used a foreign thread despite its zero forced foreign-slot limit");
    UNIT_ASSERT_C(completedAfterOwnerWasReleased,
        "Batch activation did not execute after its adjacent User owner was released");
    UNIT_ASSERT_VALUES_EQUAL_C(
        executionOwnerPoolId.load(std::memory_order_acquire),
        ownerPoolId,
        "Batch activation was not executed by its adjacent User owner");
}

Y_UNIT_TEST(AutoConfiguredSingleCpuOwnerRunsAllAdjacentPoolsAfterIdle) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetCpuCount(1);
    config.SetUseSharedThreads(true);
    config.SetUseUnitedPool(true);

    ApplyAutoConfig(&config, false, false);

    static constexpr size_t AdjacentPoolCount = 3;
    const ui32 ownerPoolId = config.GetServiceExecutor(0).GetExecutorId();
    const std::array<ui32, AdjacentPoolCount> adjacentPoolIds = {
        config.GetUserExecutor(),
        config.GetSysExecutor(),
        config.GetBatchExecutor(),
    };
    auto setup = CreateActorSystemSetup(config);

    const auto& ownerPool = FindBasicPool(setup->CpuManager, ownerPoolId);
    UNIT_ASSERT_VALUES_EQUAL(ownerPool.PoolName, "IC");
    UNIT_ASSERT_VALUES_EQUAL(ownerPool.DefaultThreadCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(
        ownerPool.AdjacentPools,
        (std::vector<i16>{
            static_cast<i16>(adjacentPoolIds[0]),
            static_cast<i16>(adjacentPoolIds[1]),
            static_cast<i16>(adjacentPoolIds[2]),
        }));
    for (ui32 adjacentPoolId : adjacentPoolIds) {
        const auto& adjacentPool = FindBasicPool(setup->CpuManager, adjacentPoolId);
        UNIT_ASSERT_VALUES_EQUAL(adjacentPool.DefaultThreadCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(adjacentPool.ForcedForeignSlotCount, 0);
    }

    NActors::TActorSystem actorSystem(setup);
    actorSystem.Start();

    std::array<TManualEvent, AdjacentPoolCount> done;
    std::array<std::atomic<ui32>, AdjacentPoolCount> executionOwnerPoolIds;
    std::array<bool, AdjacentPoolCount> ownerWasParked = {};
    std::array<bool, AdjacentPoolCount> completed = {};
    for (auto& executionOwnerPoolId : executionOwnerPoolIds) {
        executionOwnerPoolId.store(Max<ui32>(), std::memory_order_relaxed);
    }

    for (size_t i = 0; i < adjacentPoolIds.size(); ++i) {
        ownerWasParked[i] = WaitForSharedThreadToPark(
            actorSystem,
            ownerPoolId,
            TDuration::Seconds(5));
        if (ownerWasParked[i]) {
            actorSystem.Register(
                new TRecordOwnerOnBootstrapActor(&done[i], &executionOwnerPoolIds[i]),
                NActors::TMailboxType::HTSwap,
                adjacentPoolIds[i]);
            completed[i] = done[i].WaitT(TDuration::Seconds(5));
        }
    }

    actorSystem.Stop();
    for (size_t i = 0; i < adjacentPoolIds.size(); ++i) {
        UNIT_ASSERT_C(ownerWasParked[i],
            "single-CPU IC owner did not become idle before activating pool "
                << adjacentPoolIds[i]);
        UNIT_ASSERT_C(completed[i],
            "single-CPU adjacent pool " << adjacentPoolIds[i]
                << " did not execute after its IC owner became idle");
        UNIT_ASSERT_VALUES_EQUAL_C(
            executionOwnerPoolIds[i].load(std::memory_order_acquire),
            ownerPoolId,
            "single-CPU adjacent pool " << adjacentPoolIds[i]
                << " was not executed by its IC owner");
    }
}

Y_UNIT_TEST(AutoConfiguredAdjacentPoolWakesAfterIdleOnDirectSend) {
    TestAutoConfiguredAdjacentPoolActivationAfterIdle(
        EAdjacentActivationSource::DirectSend);
}

Y_UNIT_TEST(AutoConfiguredAdjacentPoolWakesAfterIdleOnActorSend) {
    TestAutoConfiguredAdjacentPoolActivationAfterIdle(
        EAdjacentActivationSource::ActorSend);
}

Y_UNIT_TEST(AutoConfiguredAdjacentPoolWakesAfterIdleOnRegistration) {
    TestAutoConfiguredAdjacentPoolActivationAfterIdle(
        EAdjacentActivationSource::Registration);
}

Y_UNIT_TEST(UnitedPoolFallsBackToForeignThreadWithoutAdjacentOwner) {
    static constexpr ui32 TargetPoolId = 0;
    static constexpr ui32 WorkerPoolId = 1;

    auto setup = MakeHolder<NActors::TActorSystemSetup>();
    setup->NodeId = 1;
    setup->CpuManager.Shared.United = true;
    setup->CpuManager.Basic.emplace_back(NActors::TBasicExecutorPoolConfig{
        .PoolId = TargetPoolId,
        .PoolName = "TargetPool",
        .Threads = 0,
        .SpinThreshold = 0,
        .MinThreadCount = 0,
        .MaxThreadCount = 0,
        .DefaultThreadCount = 0,
        .Priority = 20,
        .AllThreadsAreShared = true,
        .ForcedForeignSlotCount = 1,
    });
    setup->CpuManager.Basic.emplace_back(NActors::TBasicExecutorPoolConfig{
        .PoolId = WorkerPoolId,
        .PoolName = "WorkerPool",
        .Threads = 1,
        .SpinThreshold = 0,
        .MinThreadCount = 1,
        .MaxThreadCount = 1,
        .DefaultThreadCount = 1,
        .Priority = 10,
        .HasSharedThread = true,
        .AllThreadsAreShared = true,
    });
    setup->Scheduler = NActors::CreateSchedulerThread(NActors::TSchedulerConfig());

    NActors::TActorSystem actorSystem(setup);
    actorSystem.Start();

    const bool workerIsParked = WaitForSharedThreadToPark(
        actorSystem,
        WorkerPoolId,
        TDuration::Seconds(5));

    TManualEvent done;
    std::atomic<ui32> executionOwnerPoolId = Max<ui32>();
    if (workerIsParked) {
        actorSystem.Register(
            new TRecordOwnerOnBootstrapActor(&done, &executionOwnerPoolId),
            NActors::TMailboxType::HTSwap,
            TargetPoolId);
    }
    const bool completed = workerIsParked
        && done.WaitT(TDuration::Seconds(5));

    actorSystem.Stop();
    UNIT_ASSERT_C(workerIsParked,
        "unrelated united-pool worker did not become idle");
    UNIT_ASSERT_C(completed,
        "pool without an adjacent owner did not wake an eligible foreign thread");
    UNIT_ASSERT_VALUES_EQUAL_C(
        executionOwnerPoolId.load(std::memory_order_acquire),
        WorkerPoolId,
        "activation without an adjacent owner was not executed by the foreign worker");
}

} // Y_UNIT_TEST_SUITE(AutoConfig)
