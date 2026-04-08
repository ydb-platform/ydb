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

void SetRealPool(TCpuTableRow& row, ui8 poolId, ERealPoolKind poolKind, i16 threads, i16 maxThreads, ui8 priority) {
    row.RealPools[poolId] = TRealPoolConfig{
        .Kind = poolKind,
        .ThreadCount = threads,
        .MaxThreadCount = maxThreads,
        .Priority = priority,
    };
}

void SetLogicalToRealPool(TCpuTableRow& row, EPoolKind logicalPoolKind, ui8 realPoolId) {
    row.LogicalToRealPool[static_cast<size_t>(logicalPoolKind)] = realPoolId;
}

} // anonymous

Y_UNIT_TEST(ApplyAutoConfigWithCustomCpuTable) {
    TDefaultCpuTable cpuTable{};
    auto& row = cpuTable.GetPreparedRow(4);
    row.RealPoolCount = 5;
    SetRealPool(row, 0, ERealPoolKind::System, 1, 2, 30);
    SetRealPool(row, 1, ERealPoolKind::User, 1, 3, 20);
    SetRealPool(row, 2, ERealPoolKind::Batch, 1, 2, 10);
    SetRealPool(row, 3, ERealPoolKind::IO, 0, 0, 0);
    SetRealPool(row, 4, ERealPoolKind::IC, 1, 2, 40);
    SetLogicalToRealPool(row, EPoolKind::System, 0);
    SetLogicalToRealPool(row, EPoolKind::User, 1);
    SetLogicalToRealPool(row, EPoolKind::Batch, 2);
    SetLogicalToRealPool(row, EPoolKind::IO, 3);
    SetLogicalToRealPool(row, EPoolKind::IC, 4);

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
    row.RealPoolCount = 4;
    SetRealPool(row, 0, ERealPoolKind::Common, 2, 3, 30);
    SetRealPool(row, 1, ERealPoolKind::Batch, 0, 0, 10);
    SetRealPool(row, 2, ERealPoolKind::IO, 0, 0, 0);
    SetRealPool(row, 3, ERealPoolKind::IC, 1, 1, 40);
    SetLogicalToRealPool(row, EPoolKind::System, 0);
    SetLogicalToRealPool(row, EPoolKind::User, 0);
    SetLogicalToRealPool(row, EPoolKind::Batch, 1);
    SetLogicalToRealPool(row, EPoolKind::IO, 2);
    SetLogicalToRealPool(row, EPoolKind::IC, 3);

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
    row.RealPoolCount = 4;
    SetRealPool(row, 0, ERealPoolKind::Common, 2, 3, 30);
    SetRealPool(row, 1, ERealPoolKind::Batch, 0, 0, 10);
    SetRealPool(row, 2, ERealPoolKind::IO, 0, 0, 0);
    SetRealPool(row, 3, ERealPoolKind::IC, 1, 1, 40);
    SetLogicalToRealPool(row, EPoolKind::System, 0);
    SetLogicalToRealPool(row, EPoolKind::User, 0);
    SetLogicalToRealPool(row, EPoolKind::Batch, 1);
    SetLogicalToRealPool(row, EPoolKind::IO, 2);
    SetLogicalToRealPool(row, EPoolKind::IC, 3);

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
    auto& row5 = cpuTable.GetPreparedRow(5);
    row5.RealPoolCount = 5;
    SetRealPool(row5, 0, ERealPoolKind::System, 1, 2, 30);
    SetRealPool(row5, 1, ERealPoolKind::User, 2, 3, 20);
    SetRealPool(row5, 2, ERealPoolKind::Batch, 1, 1, 10);
    SetRealPool(row5, 3, ERealPoolKind::IO, 0, 0, 0);
    SetRealPool(row5, 4, ERealPoolKind::IC, 1, 2, 40);
    SetLogicalToRealPool(row5, EPoolKind::System, 0);
    SetLogicalToRealPool(row5, EPoolKind::User, 1);
    SetLogicalToRealPool(row5, EPoolKind::Batch, 2);
    SetLogicalToRealPool(row5, EPoolKind::IO, 3);
    SetLogicalToRealPool(row5, EPoolKind::IC, 4);

    auto& row30 = cpuTable.GetPreparedRow(30);
    row30.RealPoolCount = 5;
    SetRealPool(row30, 0, ERealPoolKind::System, 10, 12, 30);
    SetRealPool(row30, 1, ERealPoolKind::User, 12, 14, 20);
    SetRealPool(row30, 2, ERealPoolKind::Batch, 4, 5, 10);
    SetRealPool(row30, 3, ERealPoolKind::IO, 0, 0, 0);
    SetRealPool(row30, 4, ERealPoolKind::IC, 4, 6, 40);
    SetLogicalToRealPool(row30, EPoolKind::System, 0);
    SetLogicalToRealPool(row30, EPoolKind::User, 1);
    SetLogicalToRealPool(row30, EPoolKind::Batch, 2);
    SetLogicalToRealPool(row30, EPoolKind::IO, 3);
    SetLogicalToRealPool(row30, EPoolKind::IC, 4);

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

    UNIT_ASSERT_VALUES_EQUAL(system->GetThreads(), 11);
    UNIT_ASSERT_VALUES_EQUAL(system->GetMaxThreads(), 14);
    UNIT_ASSERT_VALUES_EQUAL(user->GetThreads(), 14);
    UNIT_ASSERT_VALUES_EQUAL(user->GetMaxThreads(), 17);
    UNIT_ASSERT_VALUES_EQUAL(batch->GetThreads(), 5);
    UNIT_ASSERT_VALUES_EQUAL(batch->GetMaxThreads(), 6);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetThreads(), 5);
    UNIT_ASSERT_VALUES_EQUAL(ic->GetMaxThreads(), 8);
}

} // Y_UNIT_TEST_SUITE(AutoConfig)
