#include "config_helpers.h"

#include <ydb/library/actors/util/affinity.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

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

} // anonymous namespace

Y_UNIT_TEST_SUITE(ActorSystemConfigHelpers) {

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

Y_UNIT_TEST(OnlyExplicitBlobStorageExecutorsAreSelected) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(1);

    auto* firstBlobStorage = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT);
    firstBlobStorage->SetPlacementGroups(2);

    auto* io = AddExecutor(systemConfig, TExecutorConfig::IO, "IO");
    io->SetThreads(1);

    auto* secondBlobStorage = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT, "OtherBlobStorage");
    secondBlobStorage->SetPlacementGroups(1);

    systemConfig.AddBlobStorageExecutor(1);
    systemConfig.AddBlobStorageExecutor(3);

    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig),
        (TVector<ui32>{1, 2, 4}));
}

Y_UNIT_TEST(PlacementExecutorsAreNotSelectedForBlobStorageByDefault) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* placement = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT);
    placement->SetPlacementGroups(2);

    UNIT_ASSERT(
        NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig).empty());
}

Y_UNIT_TEST(BlobStorageExecutorSelectionPreservesConfiguredOrder) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* firstPlacement = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT);
    firstPlacement->SetPlacementGroups(2);

    auto* basic = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    basic->SetThreads(1);

    auto* secondPlacement = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT);
    secondPlacement->SetPlacementGroups(1);

    systemConfig.AddBlobStorageExecutor(2);
    systemConfig.AddBlobStorageExecutor(0);

    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig),
        (TVector<ui32>{3, 0, 1}));
}

Y_UNIT_TEST(PlacementExecutorsUsePlacementGroupAffinityAndLeaveOtherCpusForRegularPools) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(2);

    auto* blobStorage = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT, "BlobStorage");
    blobStorage->SetPlacementGroups(2);
    blobStorage->SetPlacementGroupThreads(3);

    auto* io = AddExecutor(systemConfig, TExecutorConfig::IO, "IO");
    io->SetThreads(1);

    TCpuTopology cpuTopology;
    cpuTopology.AllCpus = TCpuMask(TString("0-7"));
    cpuTopology.PlacementGroups = {
        {.Id = 0, .Cpus = TCpuMask(TString("0-1"))},
        {.Id = 1, .Cpus = TCpuMask(TString("2-3"))},
    };

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr, cpuTopology);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.GetExecutorsCount(), 4);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.PingInfoByPool.size(), 4);

    const auto* systemPool = FindBasicPool(cpuManager, 0);
    UNIT_ASSERT(systemPool);
    UNIT_ASSERT_VALUES_EQUAL(systemPool->PoolName, "System");
    AssertCpuMask(systemPool->Affinity, "4-7");

    const auto* firstPlacementPool = FindBasicPool(cpuManager, 1);
    UNIT_ASSERT(firstPlacementPool);
    UNIT_ASSERT_VALUES_EQUAL(firstPlacementPool->PoolName, "BlobStorage0");
    UNIT_ASSERT_VALUES_EQUAL(firstPlacementPool->Threads, 3);
    UNIT_ASSERT_VALUES_EQUAL(firstPlacementPool->MinThreadCount, 3);
    UNIT_ASSERT_VALUES_EQUAL(firstPlacementPool->MaxThreadCount, 3);
    AssertCpuMask(firstPlacementPool->Affinity, "0-1");

    const auto* secondPlacementPool = FindBasicPool(cpuManager, 2);
    UNIT_ASSERT(secondPlacementPool);
    UNIT_ASSERT_VALUES_EQUAL(secondPlacementPool->PoolName, "BlobStorage1");
    UNIT_ASSERT_VALUES_EQUAL(secondPlacementPool->Threads, 3);
    AssertCpuMask(secondPlacementPool->Affinity, "2-3");

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO[0].PoolId, 3);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO[0].PoolName, "IO");
    AssertCpuMask(cpuManager.IO[0].Affinity, "4-7");
}

} // Y_UNIT_TEST_SUITE(ActorSystemConfigHelpers)
} // namespace NKikimr
