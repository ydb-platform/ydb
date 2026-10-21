#include "validator_nameservice.h"
#include "validator_ut_common.h"

#include <ydb/library/yaml_config/yaml_config_parser.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConsole {

using namespace NTests;

namespace {

void CheckConfig(const NKikimrConfig::TStaticNameserviceConfig &oldConfig,
                 const NKikimrConfig::TStaticNameserviceConfig &newConfig,
                 bool result,
                 ui32 warnings = 0)
{
    NKikimrConfig::TAppConfig oldCfg;
    oldCfg.MutableNameserviceConfig()->CopyFrom(oldConfig);
    NKikimrConfig::TAppConfig newCfg;
    newCfg.MutableNameserviceConfig()->CopyFrom(newConfig);
    NTests::CheckConfig<TNameserviceConfigValidator>(oldCfg, newCfg, result, warnings);
}

void CheckConfig(const NKikimrConfig::TStaticNameserviceConfig &config,
                 bool result,
                 ui32 warnings = 0)
{
    CheckConfig({}, config, result, warnings);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(NameserviceConfigValidatorTests) {
    Y_UNIT_TEST(TestEmptyConfig) {
        CheckConfig({}, false);
    }

    Y_UNIT_TEST(TestAddNewNode) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        AddNode(5, "host5", 105, "rhost5", "addr5", "dc1", newCfg);
        AddNode(6, "host1", 102, "rhost3", "addr4", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, true);
    }

    Y_UNIT_TEST(TestDuplicatingId) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        AddNode(1, "host5", 105, "rhost5", "addr5", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, false);
    }

    Y_UNIT_TEST(TestDuplicatingHostPort) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        AddNode(5, "host1", 101, "rhost5", "addr5", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, false);
    }

    Y_UNIT_TEST(TestDuplicatingResolveHostPort) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        AddNode(5, "host5", 101, "rhost1", "addr5", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, false);
    }

    Y_UNIT_TEST(TestDuplicatingAddrPort) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        AddNode(5, "host5", 101, "rhost5", "addr1", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, false);
    }

    Y_UNIT_TEST(TestLongDC) {
        const auto oldCfg = MakeDefaultNameserviceConfig();
        for (bool walleLocation : {false, true}) {
            auto newCfg = oldCfg;
            auto* node = newCfg.MutableNode(0);
            node->ClearWalleLocation();
            auto* location = walleLocation ? node->MutableWalleLocation() : node->MutableLocation();
            for (const TString dc : {"dc11", "dc111", "ABCDEF"}) {
                location->SetDataCenter(dc);
                NKikimrConfig::TAppConfig appConfig;
                appConfig.MutableNameserviceConfig()->CopyFrom(newCfg);
                NTests::CheckConfig<TNameserviceConfigValidator>(appConfig, true, 0);
                CheckConfig(oldCfg, newCfg, true);
            }
        }
    }

    Y_UNIT_TEST(TestLongDCInHosts) {
        const auto config = NYaml::Parse(R"(
self_management_config:
  enabled: true
  erasure_species: none
  pdisk_type: NVME
host_configs:
  - nvme: [disk1]
hosts:
  - host: host1
    location:
      data_center: ABCDEF
      rack: rack1
      body: 1
)");
        const auto& nameservice = config.GetNameserviceConfig();
        UNIT_ASSERT_VALUES_EQUAL(nameservice.NodeSize(), 1);
        UNIT_ASSERT(!nameservice.GetNode(0).HasWalleLocation());
        UNIT_ASSERT_VALUES_EQUAL(nameservice.GetNode(0).GetLocation().GetDataCenter(), "ABCDEF");
        CheckConfig(nameservice, true);
    }

    Y_UNIT_TEST(TestModifyClusterUUID) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        newCfg.SetClusterUUID("cluster_uuid_2");
        CheckConfig(oldCfg, newCfg, false);
    }

    Y_UNIT_TEST(TestModifyIdForHostPort) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        RemoveNode(1, newCfg);
        AddNode(5, "host1", 101, "rhost5", "addr5", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, false);
    }

    Y_UNIT_TEST(TestModifyIdForResolveHostPort) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        RemoveNode(1, newCfg);
        AddNode(5, "host5", 101, "rhost1", "addr5", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, false);
    }

    Y_UNIT_TEST(TestModifyIdForAddrPort) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        RemoveNode(1, newCfg);
        AddNode(5, "host5", 101, "rhost5", "addr1", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, false);
    }

    Y_UNIT_TEST(TestModifyHost) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        UpdateNode(1, "host5", 101, "rhost1", "addr1", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, true, 1);
    }

    Y_UNIT_TEST(TestModifyResolveHost) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        UpdateNode(1, "host1", 101, "rhost5", "addr1", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, true, 1);
    }

    Y_UNIT_TEST(TestModifyPort) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        UpdateNode(1, "host1", 105, "rhost1", "addr1", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, true, 1);
    }


    Y_UNIT_TEST(TestRemoveTooMany) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        RemoveNode(1, newCfg);
        CheckConfig(oldCfg, newCfg, true);
        RemoveNode(2, newCfg);
        CheckConfig(oldCfg, newCfg, true);
        RemoveNode(3, newCfg);
        CheckConfig(oldCfg, newCfg, true, 1);
        RemoveNode(1, oldCfg);
        CheckConfig(oldCfg, newCfg, true, 1);
        RemoveNode(2, oldCfg);
        CheckConfig(oldCfg, newCfg, true);
    }

    Y_UNIT_TEST(TestEmptyAddresses) {
        NKikimrConfig::TStaticNameserviceConfig oldCfg = MakeDefaultNameserviceConfig();
        NKikimrConfig::TStaticNameserviceConfig newCfg = MakeDefaultNameserviceConfig();
        AddNode(5, "host1", 102, "rhost3", "", "dc1", newCfg);
        AddNode(6, "host6", 19001, "rhost6", "", "dc1", newCfg);
        AddNode(7, "host7", 19001, "rhost7", "", "dc1", newCfg);
        CheckConfig(oldCfg, newCfg, true);
        NKikimrConfig::TStaticNameserviceConfig midCfg = newCfg;
        AddNode(8, "host8", 19001, "rhost8", "", "dc1", newCfg);
        CheckConfig(midCfg, newCfg, true);
    }
}

} // namespace NKikimr::NConsole
