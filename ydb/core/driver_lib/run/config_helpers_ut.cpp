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

Y_UNIT_TEST(SelectedPlacementBlobStorageExecutorExpandsIntoItsPools) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(1);

    auto* blobStorage = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT);
    blobStorage->SetPlacementGroupCount(2);

    auto* io = AddExecutor(systemConfig, TExecutorConfig::IO, "IO");
    io->SetThreads(1);

    systemConfig.SetBlobStorageExecutor(1);

    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig),
        (TVector<ui32>{1, 2}));
}

Y_UNIT_TEST(BlobStorageExecutorIsNotSelectedByDefault) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* placement = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT);
    placement->SetPlacementGroupCount(2);

    UNIT_ASSERT(
        NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig).empty());
}

Y_UNIT_TEST(BlobStorageExecutorCanReferenceBasicOrIoExecutor) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* placement = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT);
    placement->SetPlacementGroupCount(2);

    auto* basic = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    basic->SetThreads(1);

    auto* io = AddExecutor(systemConfig, TExecutorConfig::IO, "IO");
    io->SetThreads(1);

    systemConfig.SetBlobStorageExecutor(1);
    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig),
        (TVector<ui32>{2}));

    systemConfig.SetBlobStorageExecutor(2);
    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig),
        (TVector<ui32>{3}));
}

Y_UNIT_TEST(InterconnectSessionExecutorIsOptional) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(1);

    UNIT_ASSERT(
        NActorSystemConfigHelpers::GetInterconnectSessionExecutorPoolIds(systemConfig).empty());
}

Y_UNIT_TEST(InterconnectSessionExecutorExpandsPlacementPools) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(1);

    auto* blobStorage = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT, "BS");
    blobStorage->SetPlacementGroupCount(2);

    auto* interconnectSession = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT, "ICSession");
    interconnectSession->SetPlacementGroupCount(3);

    systemConfig.SetBlobStorageExecutor(1);
    systemConfig.SetInterconnectSessionExecutor(2);
    systemConfig.SetUseSharedThreads(false);

    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetInterconnectSessionExecutorPoolIds(systemConfig),
        (TVector<ui32>{3, 4, 5}));
}

Y_UNIT_TEST(BlobStorageAndInterconnectSessionExecutorsUseDistinctPlacementGroups) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(1);

    auto* blobStorage = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT, "BS");
    blobStorage->SetPlacementGroupCount(2);
    blobStorage->SetPlacementGroupThreads(1);

    auto* interconnectSession = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT, "ICSession");
    interconnectSession->SetPlacementGroupCount(2);
    interconnectSession->SetPlacementGroupThreads(1);

    auto* interconnect = AddExecutor(systemConfig, TExecutorConfig::BASIC, "IC");
    interconnect->SetThreads(1);

    systemConfig.SetSysExecutor(0);
    systemConfig.SetBlobStorageExecutor(1);
    systemConfig.SetInterconnectSessionExecutor(2);
    auto* interconnectService = systemConfig.AddServiceExecutor();
    interconnectService->SetServiceName("Interconnect");
    interconnectService->SetExecutorId(3);

    TCpuTopology cpuTopology;
    cpuTopology.AllCpus = TCpuMask(TString("0-11"));
    cpuTopology.PlacementGroups = {
        {.Id = 0, .Cpus = TCpuMask(TString("0-1"))},
        {.Id = 1, .Cpus = TCpuMask(TString("2-3"))},
        {.Id = 2, .Cpus = TCpuMask(TString("4-5"))},
        {.Id = 3, .Cpus = TCpuMask(TString("6-7"))},
    };

    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig),
        (TVector<ui32>{1, 2}));
    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetInterconnectSessionExecutorPoolIds(systemConfig),
        (TVector<ui32>{3, 4}));

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr, cpuTopology);

    const auto* firstInterconnectSessionPool = FindBasicPool(cpuManager, 3);
    UNIT_ASSERT(firstInterconnectSessionPool);
    UNIT_ASSERT_VALUES_EQUAL(firstInterconnectSessionPool->PoolName, "ICSession0");
    AssertCpuMask(firstInterconnectSessionPool->Affinity, "4-5");

    const auto* secondInterconnectSessionPool = FindBasicPool(cpuManager, 4);
    UNIT_ASSERT(secondInterconnectSessionPool);
    UNIT_ASSERT_VALUES_EQUAL(secondInterconnectSessionPool->PoolName, "ICSession1");
    AssertCpuMask(secondInterconnectSessionPool->Affinity, "6-7");
}

Y_UNIT_TEST(ExplicitPlacementGroupsCanBeSharedByPlacementAndRegularExecutors) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(2);
    system->AddPlacementGroups(1);
    system->AddPlacementGroups(2);

    auto* blobStorage = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT, "BS");
    blobStorage->SetPlacementGroupCount(2);
    blobStorage->SetPlacementGroupThreads(3);
    blobStorage->AddPlacementGroups(1);
    blobStorage->AddPlacementGroups(2);

    auto* interconnectSession = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT, "ICSession");
    interconnectSession->SetPlacementGroupCount(2);
    interconnectSession->SetPlacementGroupThreads(4);
    interconnectSession->AddPlacementGroups(1);
    interconnectSession->AddPlacementGroups(2);

    auto* io = AddExecutor(systemConfig, TExecutorConfig::IO, "IO");
    io->SetThreads(1);

    TCpuTopology cpuTopology;
    cpuTopology.AllCpus = TCpuMask(TString("0-7"));
    cpuTopology.PlacementGroups = {
        {.Id = 0, .Cpus = TCpuMask(TString("0-1"))},
        {.Id = 1, .Cpus = TCpuMask(TString("2-3"))},
        {.Id = 2, .Cpus = TCpuMask(TString("4-5"))},
        {.Id = 3, .Cpus = TCpuMask(TString("6-7"))},
    };

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr, cpuTopology);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.GetExecutorsCount(), 6);

    const auto* systemPool = FindBasicPool(cpuManager, 0);
    UNIT_ASSERT(systemPool);
    AssertCpuMask(systemPool->Affinity, "2-5");

    const auto* firstBlobStoragePool = FindBasicPool(cpuManager, 1);
    UNIT_ASSERT(firstBlobStoragePool);
    UNIT_ASSERT_VALUES_EQUAL(firstBlobStoragePool->PoolName, "BS0");
    UNIT_ASSERT_VALUES_EQUAL(firstBlobStoragePool->Threads, 3);
    AssertCpuMask(firstBlobStoragePool->Affinity, "2-3");

    const auto* secondBlobStoragePool = FindBasicPool(cpuManager, 2);
    UNIT_ASSERT(secondBlobStoragePool);
    UNIT_ASSERT_VALUES_EQUAL(secondBlobStoragePool->PoolName, "BS1");
    AssertCpuMask(secondBlobStoragePool->Affinity, "4-5");

    const auto* firstInterconnectSessionPool = FindBasicPool(cpuManager, 3);
    UNIT_ASSERT(firstInterconnectSessionPool);
    UNIT_ASSERT_VALUES_EQUAL(firstInterconnectSessionPool->PoolName, "ICSession0");
    UNIT_ASSERT_VALUES_EQUAL(firstInterconnectSessionPool->Threads, 4);
    AssertCpuMask(firstInterconnectSessionPool->Affinity, "2-3");

    const auto* secondInterconnectSessionPool = FindBasicPool(cpuManager, 4);
    UNIT_ASSERT(secondInterconnectSessionPool);
    UNIT_ASSERT_VALUES_EQUAL(secondInterconnectSessionPool->PoolName, "ICSession1");
    AssertCpuMask(secondInterconnectSessionPool->Affinity, "4-5");

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO[0].PoolId, 5);
    AssertCpuMask(cpuManager.IO[0].Affinity, "0-1,6-7");
}

Y_UNIT_TEST(ExplicitPlacementGroupOrderControlsExpandedPoolAffinity) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* placement = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT, "BS");
    placement->SetPlacementGroupCount(3);
    placement->SetPlacementGroupThreads(1);
    placement->AddPlacementGroups(2);
    placement->AddPlacementGroups(0);
    placement->AddPlacementGroups(1);

    TCpuTopology cpuTopology;
    cpuTopology.AllCpus = TCpuMask(TString("0-5"));
    cpuTopology.PlacementGroups = {
        {.Id = 0, .Cpus = TCpuMask(TString("0-1"))},
        {.Id = 1, .Cpus = TCpuMask(TString("2-3"))},
        {.Id = 2, .Cpus = TCpuMask(TString("4-5"))},
    };

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr, cpuTopology);

    const auto* firstPool = FindBasicPool(cpuManager, 0);
    const auto* secondPool = FindBasicPool(cpuManager, 1);
    const auto* thirdPool = FindBasicPool(cpuManager, 2);
    UNIT_ASSERT(firstPool);
    UNIT_ASSERT(secondPool);
    UNIT_ASSERT(thirdPool);
    AssertCpuMask(firstPool->Affinity, "4-5");
    AssertCpuMask(secondPool->Affinity, "0-1");
    AssertCpuMask(thirdPool->Affinity, "2-3");
}

Y_UNIT_TEST(RegularExecutorsCanUsePlacementGroupsWithoutPlacementExecutors) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* basic = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    basic->SetThreads(2);
    basic->AddPlacementGroups(0);
    basic->AddPlacementGroups(2);

    auto* io = AddExecutor(systemConfig, TExecutorConfig::IO, "IO");
    io->SetThreads(1);
    io->AddPlacementGroups(1);
    io->AddPlacementGroups(2);

    TCpuTopology cpuTopology;
    cpuTopology.AllCpus = TCpuMask(TString("0-5"));
    cpuTopology.PlacementGroups = {
        {.Id = 0, .Cpus = TCpuMask(TString("0-1"))},
        {.Id = 1, .Cpus = TCpuMask(TString("2-3"))},
        {.Id = 2, .Cpus = TCpuMask(TString("4-5"))},
    };

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr, cpuTopology);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 1);
    AssertCpuMask(cpuManager.Basic[0].Affinity, "0-1,4-5");
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO.size(), 1);
    AssertCpuMask(cpuManager.IO[0].Affinity, "2-5");
}

Y_UNIT_TEST(PlacementExecutorsUsePlacementGroupAffinityAndLeaveOtherCpusForRegularPools) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(2);

    auto* blobStorage = AddExecutor(systemConfig, TExecutorConfig::PLACEMENT, "BS");
    blobStorage->SetPlacementGroupCount(2);
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
    UNIT_ASSERT_VALUES_EQUAL(firstPlacementPool->PoolName, "BS0");
    UNIT_ASSERT_VALUES_EQUAL(firstPlacementPool->Threads, 3);
    UNIT_ASSERT_VALUES_EQUAL(firstPlacementPool->MinThreadCount, 3);
    UNIT_ASSERT_VALUES_EQUAL(firstPlacementPool->MaxThreadCount, 3);
    AssertCpuMask(firstPlacementPool->Affinity, "0-1");

    const auto* secondPlacementPool = FindBasicPool(cpuManager, 2);
    UNIT_ASSERT(secondPlacementPool);
    UNIT_ASSERT_VALUES_EQUAL(secondPlacementPool->PoolName, "BS1");
    UNIT_ASSERT_VALUES_EQUAL(secondPlacementPool->Threads, 3);
    AssertCpuMask(secondPlacementPool->Affinity, "2-3");

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO[0].PoolId, 3);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO[0].PoolName, "IO");
    AssertCpuMask(cpuManager.IO[0].Affinity, "4-7");
}

} // Y_UNIT_TEST_SUITE(ActorSystemConfigHelpers)
} // namespace NKikimr
