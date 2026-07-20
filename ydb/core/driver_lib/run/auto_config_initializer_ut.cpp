#include "auto_config_initializer.h"

#include <ydb/core/protos/config.pb.h>
#include <library/cpp/testing/unittest/registar.h>


Y_UNIT_TEST_SUITE(AutoConfig) {

using namespace NKikimr;
using namespace NAutoConfigInitializer;

#define ASSERT_POOLS(pools, sys, user, batch, io, ic) \
    do { \
        UNIT_ASSERT_VALUES_EQUAL(pools.SystemPoolId, sys); \
        UNIT_ASSERT_VALUES_EQUAL(pools.UserPoolId, user); \
        UNIT_ASSERT_VALUES_EQUAL(pools.BatchPoolId, batch); \
        UNIT_ASSERT_VALUES_EQUAL(pools.IOPoolId, io); \
        UNIT_ASSERT_VALUES_EQUAL(pools.ICPoolId, ic); \
    } while (false) \
// ASSERT_POOLS

Y_UNIT_TEST(GetExecutorPoolLayoutWith1CPU) {
    TExecutorPoolLayout pools = GetExecutorPoolLayout(1);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(1);
    pools = GetExecutorPoolLayout(config, true);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    UNIT_ASSERT_VALUES_EQUAL(pools.GetIndices(),  (std::vector<ui8>{0, 0, 0, 1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{40, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetExecutorPoolNames(), (std::vector<TString>{"Common", "IO"}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetExecutorPoolCount(), 2);
}

Y_UNIT_TEST(GetExecutorPoolLayoutWith2CPUs) {
    TExecutorPoolLayout pools = GetExecutorPoolLayout(2);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(2);
    pools = GetExecutorPoolLayout(config, true);
    ASSERT_POOLS(pools, 0, 0, 0, 1, 0);

    UNIT_ASSERT_VALUES_EQUAL(pools.GetIndices(),  (std::vector<ui8>{0, 0, 0, 1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{40, 0}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetExecutorPoolNames(), (std::vector<TString>{"Common", "IO"}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetExecutorPoolCount(), 2);
}

Y_UNIT_TEST(GetExecutorPoolLayoutWith3CPUs) {
    TExecutorPoolLayout pools = GetExecutorPoolLayout(3);
    ASSERT_POOLS(pools, 0, 0, 1, 2, 3);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(3);
    pools = GetExecutorPoolLayout(config, true);
    ASSERT_POOLS(pools, 0, 0, 1, 2, 3);

    UNIT_ASSERT_VALUES_EQUAL(pools.GetIndices(),  (std::vector<ui8>{0, 0, 1, 2, 3}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{30, 10, 0, 40}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetExecutorPoolNames(), (std::vector<TString>{"Common", "Batch", "IO", "IC"}));
    UNIT_ASSERT_VALUES_EQUAL(pools.GetExecutorPoolCount(), 4);
}

Y_UNIT_TEST(GetExecutorPoolLayoutWith4AndMoreCPUs) {
    for (ui32 threadCount = 4; threadCount < 128; ++threadCount) {
        TExecutorPoolLayout pools = GetExecutorPoolLayout(threadCount);
        ASSERT_POOLS(pools, 0, 1, 2, 3, 4);

        NKikimrConfig::TActorSystemConfig config;
        config.SetUseAutoConfig(true);
        config.SetCpuCount(threadCount);
        pools = GetExecutorPoolLayout(config, true);
        ASSERT_POOLS(pools, 0, 1, 2, 3, 4);

        UNIT_ASSERT_VALUES_EQUAL(pools.GetIndices(),  (std::vector<ui8>{0, 1, 2, 3, 4}));
        UNIT_ASSERT_VALUES_EQUAL(pools.GetPriorities(), (std::vector<ui8>{30, 20, 10, 0, 40}));
        UNIT_ASSERT_VALUES_EQUAL(pools.GetExecutorPoolNames(), (std::vector<TString>{"System", "User", "Batch", "IO", "IC"}));
        UNIT_ASSERT_VALUES_EQUAL(pools.GetExecutorPoolCount(), 5);
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

namespace {

const NKikimrConfig::TActorSystemConfig::TExecutor* FindExecutorByName(
    const NKikimrConfig::TActorSystemConfig& config,
    const TString& name)
{
    for (const auto& executor : config.GetExecutor()) {
        if (executor.GetName() == name) {
            return &executor;
        }
    }

    return nullptr;
}

void SetExecutorPool(TCpuTableRow& row, ui8 poolId, EExecutorPoolKind poolKind, i16 threads, i16 maxThreads, ui8 priority) {
    row.ExecutorPools[poolId] = TExecutorPoolConfig{
        .Kind = poolKind,
        .ThreadCount = threads,
        .MaxThreadCount = maxThreads,
        .Priority = priority,
    };
}

void SetLogicalToExecutorPool(TCpuTableRow& row, ELogicalPoolKind logicalPoolKind, ui8 executorPoolId) {
    row.LogicalPoolToExecutorPool[static_cast<size_t>(logicalPoolKind)] = executorPoolId;
}

void SetDefaultFivePoolMapping(TCpuTableRow& row) {
    SetLogicalToExecutorPool(row, ELogicalPoolKind::System, 0);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::User, 1);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::Batch, 2);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::IO, 3);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::IC, 4);
}

void BuildDenseScalableCpuTable(TDefaultCpuTable& cpuTable) {
    for (i16 cpuCount = 1; cpuCount <= MaxPreparedCpuCount; ++cpuCount) {
        auto& row = cpuTable.GetPreparedRow(cpuCount);
        row.ExecutorPoolCount = 5;

        const i16 systemThreads = 1;
        const i16 userThreads = cpuCount >= 2 ? 1 : 0;
        const i16 batchThreads = cpuCount >= 3 ? 1 : 0;
        const i16 icThreads = cpuCount - systemThreads - userThreads - batchThreads;

        SetExecutorPool(row, 0, EExecutorPoolKind::System, systemThreads, systemThreads ? systemThreads + 1 : 0, 30);
        SetExecutorPool(row, 1, EExecutorPoolKind::User, userThreads, userThreads ? userThreads + 1 : 0, 20);
        SetExecutorPool(row, 2, EExecutorPoolKind::Batch, batchThreads, batchThreads ? batchThreads + 1 : 0, 10);
        SetExecutorPool(row, 3, EExecutorPoolKind::IO, 0, 0, 0);
        SetExecutorPool(row, 4, EExecutorPoolKind::IC, icThreads, icThreads ? icThreads + 1 : 0, 40);
        SetDefaultFivePoolMapping(row);
    }
}

} // anonymous

Y_UNIT_TEST(ApplyAutoConfigWithCustomCpuTable) {
    TDefaultCpuTable cpuTable{};
    auto& row = cpuTable.GetPreparedRow(4);
    row.ExecutorPoolCount = 5;
    SetExecutorPool(row, 0, EExecutorPoolKind::System, 1, 2, 30);
    SetExecutorPool(row, 1, EExecutorPoolKind::User, 1, 3, 20);
    SetExecutorPool(row, 2, EExecutorPoolKind::Batch, 1, 2, 10);
    SetExecutorPool(row, 3, EExecutorPoolKind::IO, 0, 0, 0);
    SetExecutorPool(row, 4, EExecutorPoolKind::IC, 1, 2, 40);
    SetDefaultFivePoolMapping(row);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(4);

    ApplyAutoConfig(&config, TAutoConfigOptions{
        .CpuTable = &cpuTable,
    });

    UNIT_ASSERT_VALUES_EQUAL(config.ExecutorSize(), 5);

    const auto* system = FindExecutorByName(config, "System");
    const auto* user = FindExecutorByName(config, "User");
    const auto* batch = FindExecutorByName(config, "Batch");
    const auto* ic = FindExecutorByName(config, "IC");
    UNIT_ASSERT(system);
    UNIT_ASSERT(user);
    UNIT_ASSERT(batch);
    UNIT_ASSERT(ic);

    UNIT_ASSERT_VALUES_EQUAL(system->GetThreads(), 1);
    UNIT_ASSERT_VALUES_EQUAL(system->GetMaxThreads(), 2);
    UNIT_ASSERT_VALUES_EQUAL(user->GetThreads(), 1);
    UNIT_ASSERT_VALUES_EQUAL(user->GetMaxThreads(), 3);
    UNIT_ASSERT_VALUES_EQUAL(batch->GetThreads(), 1);
    UNIT_ASSERT_VALUES_EQUAL(batch->GetMaxThreads(), 2);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetThreads(), 1);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetMaxThreads(), 2);
}

Y_UNIT_TEST(ApplyAutoConfigUsesCustomCpuTableWithoutForcedTinyConfiguration) {
    TDefaultCpuTable cpuTable{};
    auto& row = cpuTable.GetPreparedRow(3);
    row.ExecutorPoolCount = 4;
    SetExecutorPool(row, 0, EExecutorPoolKind::Common, 2, 3, 30);
    SetExecutorPool(row, 1, EExecutorPoolKind::Batch, 0, 0, 10);
    SetExecutorPool(row, 2, EExecutorPoolKind::IO, 0, 0, 0);
    SetExecutorPool(row, 3, EExecutorPoolKind::IC, 1, 1, 40);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::System, 0);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::User, 0);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::Batch, 1);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::IO, 2);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::IC, 3);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(3);
    config.SetUseSharedThreads(true);

    ApplyAutoConfig(&config, TAutoConfigOptions{
        .CpuTable = &cpuTable,
    });

    UNIT_ASSERT_VALUES_EQUAL(config.ExecutorSize(), 4);

    const auto* common = FindExecutorByName(config, "Common");
    const auto* batch = FindExecutorByName(config, "Batch");
    const auto* ic = FindExecutorByName(config, "IC");
    UNIT_ASSERT(common);
    UNIT_ASSERT(batch);
    UNIT_ASSERT(ic);
    UNIT_ASSERT_VALUES_EQUAL(common->GetThreads(), 2);
    UNIT_ASSERT_VALUES_EQUAL(common->GetMaxThreads(), 3);
    UNIT_ASSERT_VALUES_EQUAL(batch->GetThreads(), 0);
    UNIT_ASSERT_VALUES_EQUAL(batch->GetMaxThreads(), 0);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetThreads(), 1);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetMaxThreads(), 1);
}

Y_UNIT_TEST(ApplyAutoConfigUsesCustomCpuTableInTinyConfiguration) {
    TDefaultCpuTable cpuTable{};
    auto& row = cpuTable.GetPreparedRow(3);
    row.ExecutorPoolCount = 4;
    SetExecutorPool(row, 0, EExecutorPoolKind::Common, 2, 3, 30);
    SetExecutorPool(row, 1, EExecutorPoolKind::Batch, 0, 0, 10);
    SetExecutorPool(row, 2, EExecutorPoolKind::IO, 0, 0, 0);
    SetExecutorPool(row, 3, EExecutorPoolKind::IC, 1, 1, 40);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::System, 0);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::User, 0);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::Batch, 1);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::IO, 2);
    SetLogicalToExecutorPool(row, ELogicalPoolKind::IC, 3);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(3);
    config.SetUseSharedThreads(true);

    ApplyAutoConfig(&config, TAutoConfigOptions{
        .CpuTable = &cpuTable,
    });

    UNIT_ASSERT_VALUES_EQUAL(config.ExecutorSize(), 4);
    UNIT_ASSERT_VALUES_EQUAL(config.GetSysExecutor(), 0);
    UNIT_ASSERT_VALUES_EQUAL(config.GetUserExecutor(), 0);
    UNIT_ASSERT_VALUES_EQUAL(config.GetBatchExecutor(), 1);
    UNIT_ASSERT_VALUES_EQUAL(config.GetIoExecutor(), 2);

    const auto* common = FindExecutorByName(config, "Common");
    const auto* batch = FindExecutorByName(config, "Batch");
    const auto* ic = FindExecutorByName(config, "IC");
    UNIT_ASSERT(common);
    UNIT_ASSERT(batch);
    UNIT_ASSERT(ic);
    UNIT_ASSERT_VALUES_EQUAL(common->GetThreads(), 2);
    UNIT_ASSERT_VALUES_EQUAL(common->GetMaxThreads(), 3);
    UNIT_ASSERT_VALUES_EQUAL(batch->GetThreads(), 0);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetThreads(), 1);
}

Y_UNIT_TEST(ApplyAutoConfigUsesBuiltInTinyConfigurationFromCpuTable) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(2);
    config.SetUseSharedThreads(true);

    ApplyAutoConfig(&config, TAutoConfigOptions{});

    UNIT_ASSERT_VALUES_EQUAL(config.ExecutorSize(), 5);
    UNIT_ASSERT_VALUES_EQUAL(config.GetUserExecutor(), 0);
    UNIT_ASSERT_VALUES_EQUAL(config.GetSysExecutor(), 1);
    UNIT_ASSERT_VALUES_EQUAL(config.GetBatchExecutor(), 2);
    UNIT_ASSERT_VALUES_EQUAL(config.GetIoExecutor(), 3);
    UNIT_ASSERT_VALUES_EQUAL(config.GetServiceExecutor(0).GetExecutorId(), 4);

    const auto* system = FindExecutorByName(config, "System");
    const auto* user = FindExecutorByName(config, "User");
    const auto* batch = FindExecutorByName(config, "Batch");
    const auto* ic = FindExecutorByName(config, "IC");
    UNIT_ASSERT(system);
    UNIT_ASSERT(user);
    UNIT_ASSERT(batch);
    UNIT_ASSERT(ic);

    UNIT_ASSERT_VALUES_EQUAL(system->GetThreads(), 0);
    UNIT_ASSERT_VALUES_EQUAL(system->GetMaxThreads(), 0);
    UNIT_ASSERT_VALUES_EQUAL(system->GetHasSharedThread(), false);
    UNIT_ASSERT_VALUES_EQUAL(system->GetForcedForeignSlots(), 1);

    UNIT_ASSERT_VALUES_EQUAL(user->GetThreads(), 1);
    UNIT_ASSERT_VALUES_EQUAL(user->GetMaxThreads(), 1);
    UNIT_ASSERT_VALUES_EQUAL(user->GetHasSharedThread(), true);
    UNIT_ASSERT_VALUES_EQUAL(user->GetForcedForeignSlots(), 1);
    UNIT_ASSERT_VALUES_EQUAL(user->AdjacentPoolsSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(user->GetAdjacentPools(0), 2);

    UNIT_ASSERT_VALUES_EQUAL(batch->GetThreads(), 0);
    UNIT_ASSERT_VALUES_EQUAL(batch->GetForcedForeignSlots(), 0);

    UNIT_ASSERT_VALUES_EQUAL(ic->GetThreads(), 1);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetHasSharedThread(), true);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetForcedForeignSlots(), 1);
    UNIT_ASSERT_VALUES_EQUAL(ic->AdjacentPoolsSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetAdjacentPools(0), 0);
}

Y_UNIT_TEST(ApplyAutoConfigUsesTinyProfileWhenForced) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(8);

    ApplyAutoConfig(&config, TAutoConfigOptions{
        .ForceTinyConfiguration = true,
    });

    UNIT_ASSERT_VALUES_EQUAL(
        static_cast<int>(config.GetActorSystemProfile()),
        static_cast<int>(NKikimrConfig::TActorSystemConfig::LOW_CPU_CONSUMPTION));
}

Y_UNIT_TEST(ApplyAutoConfigUsesBuiltInDefaultScalingAbove30Cpus) {
    struct TCase {
        i16 CpuCount;
        i16 SystemThreads;
        i16 UserThreads;
        i16 BatchThreads;
        i16 ICThreads;
    };

    for (const TCase testCase : {
        TCase{33, 19, 2, 5, 7},
        TCase{61, 37, 3, 9, 12},
    }) {
        NKikimrConfig::TActorSystemConfig config;
        config.SetUseAutoConfig(true);
        config.SetCpuCount(testCase.CpuCount);

        ApplyAutoConfig(&config, TAutoConfigOptions{});

        const auto* system = FindExecutorByName(config, "System");
        const auto* user = FindExecutorByName(config, "User");
        const auto* batch = FindExecutorByName(config, "Batch");
        const auto* ic = FindExecutorByName(config, "IC");
        UNIT_ASSERT(system);
        UNIT_ASSERT(user);
        UNIT_ASSERT(batch);
        UNIT_ASSERT(ic);

        UNIT_ASSERT_VALUES_EQUAL(system->GetThreads(), testCase.SystemThreads);
        UNIT_ASSERT_VALUES_EQUAL(user->GetThreads(), testCase.UserThreads);
        UNIT_ASSERT_VALUES_EQUAL(batch->GetThreads(), testCase.BatchThreads);
        UNIT_ASSERT_VALUES_EQUAL(ic->GetThreads(), testCase.ICThreads);
    }
}

Y_UNIT_TEST(ApplyAutoConfigUsesConsistentLogicalPoolIds) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(4);

    ApplyAutoConfig(&config, TAutoConfigOptions{});

    UNIT_ASSERT_VALUES_EQUAL(config.GetSysExecutor(), 0);
    UNIT_ASSERT_VALUES_EQUAL(config.GetUserExecutor(), 1);
    UNIT_ASSERT_VALUES_EQUAL(config.GetBatchExecutor(), 2);
    UNIT_ASSERT_VALUES_EQUAL(config.GetIoExecutor(), 3);
}

Y_UNIT_TEST(ApplyAutoConfigCustomCpuTableUsesChunkedScaling) {
    TDefaultCpuTable cpuTable{};
    BuildDenseScalableCpuTable(cpuTable);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(35);

    ApplyAutoConfig(&config, TAutoConfigOptions{
        .CpuTable = &cpuTable,
    });

    const auto* system = FindExecutorByName(config, "System");
    const auto* user = FindExecutorByName(config, "User");
    const auto* batch = FindExecutorByName(config, "Batch");
    const auto* ic = FindExecutorByName(config, "IC");
    UNIT_ASSERT(system);
    UNIT_ASSERT(user);
    UNIT_ASSERT(batch);
    UNIT_ASSERT(ic);

    UNIT_ASSERT_VALUES_EQUAL(system->GetThreads(), 2);
    UNIT_ASSERT_VALUES_EQUAL(system->GetMaxThreads(), 4);
    UNIT_ASSERT_VALUES_EQUAL(user->GetThreads(), 2);
    UNIT_ASSERT_VALUES_EQUAL(user->GetMaxThreads(), 4);
    UNIT_ASSERT_VALUES_EQUAL(batch->GetThreads(), 2);
    UNIT_ASSERT_VALUES_EQUAL(batch->GetMaxThreads(), 4);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetThreads(), 29);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetMaxThreads(), 31);
}

Y_UNIT_TEST(GetExecutorPoolLayoutIgnoresExplicitPoolIdsUntilExecutorsMaterialized) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(4);
    config.SetSysExecutor(7);
    config.SetUserExecutor(6);
    config.SetBatchExecutor(5);
    config.SetIoExecutor(4);
    auto* serviceExecutor = config.AddServiceExecutor();
    serviceExecutor->SetServiceName("Interconnect");
    serviceExecutor->SetExecutorId(3);

    const TExecutorPoolLayout pools = GetExecutorPoolLayout(config, true);
    const TMap<TString, ui32> servicePools = GetServicePools(config, true);
    const auto it = servicePools.find("Interconnect");

    UNIT_ASSERT_VALUES_EQUAL(pools.SystemPoolId, 0);
    UNIT_ASSERT_VALUES_EQUAL(pools.UserPoolId, 1);
    UNIT_ASSERT_VALUES_EQUAL(pools.BatchPoolId, 2);
    UNIT_ASSERT_VALUES_EQUAL(pools.IOPoolId, 3);
    UNIT_ASSERT_VALUES_EQUAL(pools.ICPoolId, 4);
    UNIT_ASSERT_UNEQUAL(it, servicePools.end());
    UNIT_ASSERT_VALUES_EQUAL(it->second, 4);
}

Y_UNIT_TEST(ApplyAutoConfigReusesExistingInterconnectServiceExecutor) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetCpuCount(4);

    auto* pqExecutor = config.AddServiceExecutor();
    pqExecutor->SetServiceName("PQ");
    pqExecutor->SetExecutorId(7);

    auto* icExecutor = config.AddServiceExecutor();
    icExecutor->SetServiceName("Interconnect");
    icExecutor->SetExecutorId(42);

    ApplyAutoConfig(&config, TAutoConfigOptions{});

    UNIT_ASSERT_VALUES_EQUAL(config.ServiceExecutorSize(), 2);
    UNIT_ASSERT_VALUES_EQUAL(config.GetServiceExecutor(0).GetServiceName(), "PQ");
    UNIT_ASSERT_VALUES_EQUAL(config.GetServiceExecutor(0).GetExecutorId(), 7);
    UNIT_ASSERT_VALUES_EQUAL(config.GetServiceExecutor(1).GetServiceName(), "Interconnect");
    UNIT_ASSERT_VALUES_EQUAL(config.GetServiceExecutor(1).GetExecutorId(), 4);
}

Y_UNIT_TEST(ApplyAutoConfigDoesNotDuplicateInterconnectServiceExecutorOnRepeatedCalls) {
    NKikimrConfig::TActorSystemConfig config;
    config.SetCpuCount(4);

    ApplyAutoConfig(&config, TAutoConfigOptions{});
    ApplyAutoConfig(&config, TAutoConfigOptions{});

    UNIT_ASSERT_VALUES_EQUAL(config.ServiceExecutorSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(config.GetServiceExecutor(0).GetServiceName(), "Interconnect");
    UNIT_ASSERT_VALUES_EQUAL(config.GetServiceExecutor(0).GetExecutorId(), 4);
}

} // Y_UNIT_TEST_SUITE(AutoConfig)
