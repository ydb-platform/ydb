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

void SetPool(TCpuTableRow& row, EPoolKind poolKind, i16 threads, i16 maxThreads) {
    row[static_cast<size_t>(poolKind)] = TPoolConfig{
        .ThreadCount = threads,
        .MaxThreadCount = maxThreads,
    };
}

} // anonymous

Y_UNIT_TEST(ApplyAutoConfigWithCustomCpuTable) {
    TCpuTable cpuTable{};
    SetPool(cpuTable[4], EPoolKind::System, 1, 2);
    SetPool(cpuTable[4], EPoolKind::User, 1, 3);
    SetPool(cpuTable[4], EPoolKind::Batch, 1, 2);
    SetPool(cpuTable[4], EPoolKind::IC, 1, 2);

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

Y_UNIT_TEST(ApplyAutoConfigDisablesTinyConfigurationWhenRequested) {
    TCpuTable cpuTable{};
    SetPool(cpuTable[3], EPoolKind::System, 2, 2);
    SetPool(cpuTable[3], EPoolKind::IC, 1, 1);

    NKikimrConfig::TActorSystemConfig config;
    config.SetUseAutoConfig(true);
    config.SetCpuCount(3);
    config.SetUseSharedThreads(true);

    ApplyAutoConfig(&config, TAutoConfigOptions{
        .EnableTinyConfiguration = false,
        .CpuTable = &cpuTable,
    });

    UNIT_ASSERT_VALUES_EQUAL(config.ExecutorSize(), 4);

    const auto* common = FindExecutorByName(config, "Common");
    UNIT_ASSERT(common);
    UNIT_ASSERT_VALUES_EQUAL(common->GetThreads(), 2);
}

Y_UNIT_TEST(ApplyAutoConfigCustomCpuTableUsesChunkedScaling) {
    TCpuTable cpuTable{};
    SetPool(cpuTable[5], EPoolKind::System, 1, 2);
    SetPool(cpuTable[5], EPoolKind::User, 2, 3);
    SetPool(cpuTable[5], EPoolKind::Batch, 1, 1);
    SetPool(cpuTable[5], EPoolKind::IC, 1, 2);

    SetPool(cpuTable[30], EPoolKind::System, 10, 12);
    SetPool(cpuTable[30], EPoolKind::User, 12, 14);
    SetPool(cpuTable[30], EPoolKind::Batch, 4, 5);
    SetPool(cpuTable[30], EPoolKind::IC, 4, 6);

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
