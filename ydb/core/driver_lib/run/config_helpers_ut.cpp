#include "config_helpers.h"

#include <ydb/library/actors/util/affinity.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ActorSystemConfigHelpers) {

using namespace NKikimr;
using TExecutorConfig = NKikimrConfig::TActorSystemConfig::TExecutor;

TExecutorConfig* AddExecutor(NKikimrConfig::TActorSystemConfig& config, TExecutorConfig::EType type,
        const TString& name = {}) {
    auto* executor = config.AddExecutor();
    executor->SetType(type);
    if (!name.empty()) {
        executor->SetName(name);
    }
    return executor;
}

void AssertCpuMask(const TCpuMask& actual, const TString& expectedCpuList) {
    const TCpuMask expected(expectedCpuList);
    UNIT_ASSERT_C((actual - expected).IsEmpty(), "actual affinity contains unexpected CPUs");
    UNIT_ASSERT_C((expected - actual).IsEmpty(), "actual affinity is missing expected CPUs");
}

void AssertCpuMasksEqual(const TCpuMask& actual, const TCpuMask& expected) {
    UNIT_ASSERT_C((actual - expected).IsEmpty(), "actual affinity contains unexpected CPUs");
    UNIT_ASSERT_C((expected - actual).IsEmpty(), "actual affinity is missing expected CPUs");
}

const NActors::TBasicExecutorPoolConfig* FindBasicPool(
        const NActors::TCpuManagerConfig& config, ui32 poolId) {
    for (const auto& pool : config.Basic) {
        if (pool.PoolId == poolId) {
            return &pool;
        }
    }
    return nullptr;
}

Y_UNIT_TEST(HarmonizerNeedyCpuWindow) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* defaultExecutor = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    defaultExecutor->SetThreads(1);
    defaultExecutor->SetMaxThreads(2);

    auto* configuredExecutor = AddExecutor(systemConfig, TExecutorConfig::BASIC, "User");
    configuredExecutor->SetThreads(1);
    configuredExecutor->SetMaxThreads(2);
    configuredExecutor->SetHarmonizerNeedyCpuWindowSeconds(30);

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[0].HarmonizerNeedyCpuWindowSeconds, 1);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[1].HarmonizerNeedyCpuWindowSeconds, 30);
}

Y_UNIT_TEST(BasicAndIoExecutorsWithoutPlacementUseCurrentAffinity) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* basic = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    basic->SetThreads(1);

    auto* io = AddExecutor(systemConfig, TExecutorConfig::IO, "IO");
    io->SetThreads(1);

    TAffinity currentAffinity;
    currentAffinity.Current();
    const TCpuMask expectedAffinity = currentAffinity;

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 1);
    AssertCpuMasksEqual(cpuManager.Basic.front().Affinity, expectedAffinity);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO.size(), 1);
    AssertCpuMasksEqual(cpuManager.IO.front().Affinity, expectedAffinity);
}

Y_UNIT_TEST(BlobStorageAndInterconnectSessionExecutorsReferencePoolLists) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    for (const TString& name : {"System", "BS0", "BS1", "ICSession"}) {
        auto* executor = AddExecutor(systemConfig, TExecutorConfig::BASIC, name);
        executor->SetThreads(1);
    }

    systemConfig.AddBlobStorageExecutor(1);
    systemConfig.AddBlobStorageExecutor(2);
    systemConfig.AddInterconnectSessionExecutor(3);
    systemConfig.AddInterconnectSessionExecutor(2);

    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig),
        (TVector<ui32>{1, 2}));
    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetInterconnectSessionExecutorPoolIds(systemConfig),
        (TVector<ui32>{3, 2}));
}

Y_UNIT_TEST(ExecutorPoolListsAreEmptyByDefault) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(1);

    UNIT_ASSERT(NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig).empty());
    UNIT_ASSERT(NActorSystemConfigHelpers::GetInterconnectSessionExecutorPoolIds(systemConfig).empty());
}

Y_UNIT_TEST(BasicExecutorsUsePlacementAffinityWithoutAffectingOtherPools) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(2);

    auto* firstBlobStorage = AddExecutor(systemConfig, TExecutorConfig::BASIC, "BS0");
    firstBlobStorage->SetThreads(3);
    firstBlobStorage->SetPlacement(0);

    auto* secondBlobStorage = AddExecutor(systemConfig, TExecutorConfig::BASIC, "BS1");
    secondBlobStorage->SetThreads(3);
    secondBlobStorage->SetPlacement(1);

    auto* io = AddExecutor(systemConfig, TExecutorConfig::IO, "IO");
    io->SetThreads(1);

    systemConfig.AddBlobStorageExecutor(1);
    systemConfig.AddBlobStorageExecutor(2);

    TCpuTopology cpuTopology;
    cpuTopology.AllCpus = TCpuMask(TString("0-7"));
    cpuTopology.PlacementGroups = {
        {.Id = 10, .Cpus = TCpuMask(TString("0-1"))},
        {.Id = 20, .Cpus = TCpuMask(TString("2-3"))},
    };

    TAffinity currentAffinity;
    currentAffinity.Current();
    const TCpuMask expectedRegularPoolAffinity = currentAffinity;

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr, cpuTopology);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.GetExecutorsCount(), 4);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.PingInfoByPool.size(), 4);

    const auto* systemPool = FindBasicPool(cpuManager, 0);
    UNIT_ASSERT(systemPool);
    AssertCpuMasksEqual(systemPool->Affinity, expectedRegularPoolAffinity);

    const auto* firstBlobStoragePool = FindBasicPool(cpuManager, 1);
    UNIT_ASSERT(firstBlobStoragePool);
    UNIT_ASSERT_VALUES_EQUAL(firstBlobStoragePool->PoolName, "BS0");
    UNIT_ASSERT_VALUES_EQUAL(firstBlobStoragePool->Threads, 3);
    AssertCpuMask(firstBlobStoragePool->Affinity, "0-1");

    const auto* secondBlobStoragePool = FindBasicPool(cpuManager, 2);
    UNIT_ASSERT(secondBlobStoragePool);
    UNIT_ASSERT_VALUES_EQUAL(secondBlobStoragePool->PoolName, "BS1");
    UNIT_ASSERT_VALUES_EQUAL(secondBlobStoragePool->Threads, 3);
    AssertCpuMask(secondBlobStoragePool->Affinity, "2-3");

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO[0].PoolId, 3);
    AssertCpuMasksEqual(cpuManager.IO[0].Affinity, expectedRegularPoolAffinity);
}

Y_UNIT_TEST(MultipleBasicExecutorsCanSharePlacement) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* blobStorage = AddExecutor(systemConfig, TExecutorConfig::BASIC, "BS");
    blobStorage->SetThreads(1);
    blobStorage->SetPlacement(1);

    auto* interconnectSession = AddExecutor(systemConfig, TExecutorConfig::BASIC, "ICSession");
    interconnectSession->SetThreads(1);
    interconnectSession->SetPlacement(1);

    TCpuTopology cpuTopology;
    cpuTopology.AllCpus = TCpuMask(TString("0-3"));
    cpuTopology.PlacementGroups = {
        {.Id = 0, .Cpus = TCpuMask(TString("0-1"))},
        {.Id = 1, .Cpus = TCpuMask(TString("2-3"))},
    };

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr, cpuTopology);

    const auto* blobStoragePool = FindBasicPool(cpuManager, 0);
    const auto* interconnectSessionPool = FindBasicPool(cpuManager, 1);
    UNIT_ASSERT(blobStoragePool);
    UNIT_ASSERT(interconnectSessionPool);
    AssertCpuMask(blobStoragePool->Affinity, "2-3");
    AssertCpuMask(interconnectSessionPool->Affinity, "2-3");
}

} // ActorSystemConfigHelpers
