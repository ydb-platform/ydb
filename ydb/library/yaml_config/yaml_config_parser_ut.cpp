#include "yaml_config_parser.h"

#include <ydb/core/erasure/erasure.h>
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

    Y_UNIT_TEST(ClusterCase) {
      TString str = R"(
erasure: mirror-3-dc
default_disk_type: NVME
host_configs:
- drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
  host_config_id: 1
hosts:
- host: ydb-node-zone-a-1.local
  host_config_id: 1
  location:
    body: 1
    data_center: 'zone-a'
    rack: '1'
- host: ydb-node-zone-a-2.local
  host_config_id: 1
  location:
    body: 2
    data_center: 'zone-a'
    rack: '2'
- host: ydb-node-zone-a-3.local
  host_config_id: 1
  location:
    body: 3
    data_center: 'zone-a'
    rack: '3'

- host: ydb-node-zone-b-1.local
  host_config_id: 1
  location:
    body: 4
    data_center: 'zone-b'
    rack: '4'
- host: ydb-node-zone-b-2.local
  host_config_id: 1
  location:
    body: 5
    data_center: 'zone-b'
    rack: '5'
- host: ydb-node-zone-b-3.local
  host_config_id: 1
  location:
    body: 6
    data_center: 'zone-b'
    rack: '6'

- host: ydb-node-zone-c-1.local
  host_config_id: 1
  location:
    body: 7
    data_center: 'zone-c'
    rack: '7'
- host: ydb-node-zone-c-2.local
  host_config_id: 1
  location:
    body: 8
    data_center: 'zone-c'
    rack: '8'
- host: ydb-node-zone-c-3.local
  host_config_id: 1
  location:
    body: 9
    data_center: 'zone-c'
    rack: '9'

domains_config:
  domain:
  - name: Root
    storage_pool_types:
    - pool_config:
        box_id: 1
  state_storage:
  - ring:
      node: [1, 2, 3, 4, 5, 6, 7, 8, 9]
      nto_select: 9
    ssid: 1
blob_storage_config:
  service_set:
    groups:
    - rings:
      - fail_domains:
        - vdisk_locations:
          - node_id: "ydb-node-zone-a-1.local"
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: "ydb-node-zone-a-2.local"
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: "ydb-node-zone-a-3.local"
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
      - fail_domains:
        - vdisk_locations:
          - node_id: "ydb-node-zone-b-1.local"
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: "ydb-node-zone-b-2.local"
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: "ydb-node-zone-b-3.local"
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
      - fail_domains:
        - vdisk_locations:
          - node_id: "ydb-node-zone-c-1.local"
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: "ydb-node-zone-c-2.local"
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
        - vdisk_locations:
          - node_id: "ydb-node-zone-c-3.local"
            path: /dev/disk/by-partlabel/ydb_disk_ssd_01
channel_profile_config:
  profile:
  - channel:
    - pdisk_category: 1
    - pdisk_category: 1
    - pdisk_category: 1
    profile_id: 0
tls:
    cert: "/opt/ydb/certs/node.crt"
    key: "/opt/ydb/certs/node.key"
    ca: "/opt/ydb/certs/ca.crt"
)";
        auto originalCfg = NYaml::Yaml2Json(YAML::Load(str), true);
        auto transformedCfg = originalCfg;
        NYaml::TransformConfig(transformedCfg, true, false);

        // Check distributed storage settings
        const auto& storagePoolTypes = transformedCfg["domains_config"]["domain"][0]["storage_pool_types"][0];
        const auto& serviceSet = transformedCfg["blob_storage_config"]["service_set"];
        const auto& group = serviceSet["groups"][0];
        const auto& channels = transformedCfg["channel_profile_config"]["profile"][0]["channel"];
        const TString& erasure = transformedCfg["erasure"].GetStringSafe();
        const TString& diskType = transformedCfg["default_disk_type"].GetStringSafe();
        TString diskTypeLower(diskType);
        diskTypeLower.to_lower();
        UNIT_ASSERT_VALUES_EQUAL(transformedCfg["static_erasure"], erasure);
        UNIT_ASSERT_VALUES_EQUAL(transformedCfg["host_configs"][0]["drive"][0]["type"], "NVME");
        UNIT_ASSERT_VALUES_EQUAL(storagePoolTypes["kind"], diskTypeLower);
        UNIT_ASSERT_VALUES_EQUAL(storagePoolTypes["pool_config"]["kind"], diskTypeLower);
        UNIT_ASSERT_VALUES_EQUAL(storagePoolTypes["pool_config"]["pdisk_filter"][0]["property"][0]["type"], diskType);
        UNIT_ASSERT_VALUES_EQUAL(storagePoolTypes["pool_config"]["vdisk_kind"], "Default");
        UNIT_ASSERT_VALUES_EQUAL(group["erasure_species"], TErasureType::ErasureMirror3dc);
        for (const auto& pdisk : serviceSet["pdisks"].GetArraySafe()) {
          UNIT_ASSERT_VALUES_EQUAL(pdisk["pdisk_category"].GetUIntegerSafe(), 2 /* NVME */);
        }
        for (const auto& ring : group["rings"].GetArraySafe()) {
          for (const auto& failDomain : ring["fail_domains"].GetArraySafe()) {
            for (const auto& vdiskLocation : failDomain["vdisk_locations"].GetArraySafe()) {
              UNIT_ASSERT_VALUES_EQUAL(vdiskLocation["pdisk_category"], diskType);
            }
          }
        }
        for (const auto& channel : channels.GetArraySafe()) {
          UNIT_ASSERT_VALUES_EQUAL(channel["erasure_species"], erasure);
          UNIT_ASSERT_VALUES_EQUAL(channel["pdisk_category"], 1);
          UNIT_ASSERT_VALUES_EQUAL(channel["storage_pool_kind"], diskTypeLower);
        }

        // Check TLS settings
        const auto& tlsConfig = transformedCfg["tls"];
        const auto& grpcConfig = transformedCfg["grpc_config"];
        const auto& interconnectConfig = transformedCfg["interconnect_config"];
        const std::vector<std::pair<TString,TString>> tlsSettings {
          {"cert", "path_to_certificate_file"},
          {"key", "path_to_private_key_file"},
          {"ca", "path_to_ca_file"}
        };
        for (const auto& tlsSetting : tlsSettings) {
          UNIT_ASSERT_VALUES_EQUAL(tlsConfig[tlsSetting.first], grpcConfig[tlsSetting.first]);
          UNIT_ASSERT_VALUES_EQUAL(tlsConfig[tlsSetting.first], interconnectConfig[tlsSetting.second]);
        }
        UNIT_ASSERT(interconnectConfig["start_tcp"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(interconnectConfig["encryption_mode"].GetStringSafe(), "OPTIONAL");

        // Check empty channel_profile_config
        auto cfgWithoutChannelProfile = originalCfg;
        cfgWithoutChannelProfile.EraseValue("channel_profile_config");
        NYaml::TransformConfig(cfgWithoutChannelProfile, true, false);
        UNIT_ASSERT_VALUES_EQUAL(cfgWithoutChannelProfile["channel_profile_config"]["profile"][0]["profile_id"], 0);
        for (const auto& channel : cfgWithoutChannelProfile["channel_profile_config"]["profile"][0]["channel"].GetArraySafe()) {
          UNIT_ASSERT_VALUES_EQUAL(channel["erasure_species"], erasure);
          UNIT_ASSERT_VALUES_EQUAL(channel["pdisk_category"], 1);
          UNIT_ASSERT_VALUES_EQUAL(channel["storage_pool_kind"], diskTypeLower);
        }
    }
}