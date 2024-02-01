#include "yaml_config_parser.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(YamlConfigParser) {
    Y_UNIT_TEST(InlineHostDrives) {
      TString str = R"(
static_erasure: none
hosts:
- host: localhost
  port: 19001
  drive:
    - path: SectorMap:1:64
      type: SSD
- host: localhost
  port: 19002
  drive:
    - path: /foo/bar
      type: SSD
- host: localhost
  port: 19003
  drive:
    - type: RAM
    - type: RAM
domains_config:
  domain:
  - name: Root
    storage_pool_types:
    - kind: ssd
      pool_config:
        box_id: 1
        erasure_species: none
        kind: ssd
        pdisk_filter:
        - property:
          - type: SSD
        vdisk_kind: Default
)";
        auto originalCfg = NYaml::Yaml2Json(YAML::Load(str), true);
        auto transformedCfg = originalCfg;
        NYaml::TransformConfig(transformedCfg, true, false);
        UNIT_ASSERT(transformedCfg.Has("host_configs"));
        for (ui8 i : {0, 1}) {
            UNIT_ASSERT(transformedCfg["hosts"][i].Has("host_config_id"));
            UNIT_ASSERT(transformedCfg["host_configs"][i]["drive"].IsArray());
            UNIT_ASSERT_VALUES_EQUAL(
                transformedCfg["host_configs"][i]["host_config_id"],
                transformedCfg["hosts"][i]["host_config_id"]
            );
            UNIT_ASSERT_VALUES_EQUAL(
                transformedCfg["host_configs"][i]["drive"],
                originalCfg["hosts"][i]["drive"]
            );
        }
        UNIT_ASSERT_VALUES_EQUAL(transformedCfg["host_configs"][2]["drive"][0]["type"], "SSD");
        UNIT_ASSERT_VALUES_EQUAL(transformedCfg["host_configs"][2]["drive"][0]["path"], "SectorMap:1:64");
        UNIT_ASSERT_VALUES_EQUAL(transformedCfg["host_configs"][2]["drive"][1]["path"], "SectorMap:2:64");
    }

    Y_UNIT_TEST(SingleNodeCase) {
      TString str = R"(
hosts:
- host: localhost
  drive:
    - type: RAM
)";
        auto originalCfg = NYaml::Yaml2Json(YAML::Load(str), true);
        auto transformedCfg = originalCfg;
        NYaml::TransformConfig(transformedCfg, true, false);

        UNIT_ASSERT_VALUES_EQUAL(transformedCfg["static_erasure"], "none");
        UNIT_ASSERT_VALUES_EQUAL(transformedCfg["hosts"].GetArraySafe().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(transformedCfg["hosts"][0]["node_id"].GetUIntegerSafe(), 1);
        UNIT_ASSERT(transformedCfg["domains_config"].IsMap());
        UNIT_ASSERT(transformedCfg["domains_config"]["domain"].IsArray());
        UNIT_ASSERT(transformedCfg["domains_config"]["state_storage"].IsArray());
        UNIT_ASSERT(transformedCfg["blob_storage_config"].IsMap());
        UNIT_ASSERT(transformedCfg["channel_profile_config"].IsMap());
    }
}