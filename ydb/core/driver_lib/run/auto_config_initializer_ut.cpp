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

} // Y_UNIT_TEST_SUITE(AutoConfig)
