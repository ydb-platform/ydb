#include <ydb/library/yaml_config/public/migration/config_migration.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

namespace {

    NFyaml::TMapping Config(NFyaml::TDocument& document) {
        return document.Root().Map().at("config").Map();
    }

    void RemoveMapValue(NFyaml::TMapping& map, TStringBuf key) {
        if (auto value = map.pair_at_opt(TString(key)); value) {
            map.Remove(value.Key());
        }
    }

    NFyaml::TDocument ParseSuccessfulMerge(const NYamlConfig::TMigrationConfigMergeResult& result) {
        UNIT_ASSERT_C(!result.HasConflicts, result.Config);
        return NFyaml::TDocument::Parse(result.Config);
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(ConfigMigration) {
    Y_UNIT_TEST(Merge_StaticTopology) {
        const char* staticConfig = R"(
monitoring_config:
  port: 8765
hosts:
- host: static-node
host_configs:
- host_config_id: 1
blob_storage_config:
  cache_pdisks: false
  service_set:
    pdisks:
    - node_id: 1
      pdisk_id: 1
    vdisks:
    - storage_pool_name: static
    groups:
    - group_id: 1
    availability_domains: [1]
static_erasure: mirror-3-dc
)";
        const char* dynamicConfig = R"(
metadata:
  kind: MainConfig
  version: 7
config:
  log_config:
    default_level: 4
  hosts:
  - host: dynamic-node
  host_configs:
  - host_config_id: 2
  blob_storage_config:
    cache_pdisks: false
    service_set:
      pdisks:
      - node_id: 2
        pdisk_id: 2
      vdisks:
      - storage_pool_name: dynamic
      groups:
      - group_id: 2
      availability_domains: [1]
  static_erasure: none
selector_config: []
custom_root_field: preserved
)";

        auto result = ParseSuccessfulMerge(NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig));
        auto root = result.Root().Map();
        auto config = root.at("config").Map();

        UNIT_ASSERT_VALUES_EQUAL(config.at("monitoring_config").Map().at("port").Scalar(), "8765");
        UNIT_ASSERT_VALUES_EQUAL(config.at("log_config").Map().at("default_level").Scalar(), "4");
        UNIT_ASSERT_VALUES_EQUAL(config.at("hosts").Sequence().at(0).Map().at("host").Scalar(), "static-node");
        UNIT_ASSERT_VALUES_EQUAL(config.at("host_configs").Sequence().at(0).Map().at("host_config_id").Scalar(), "1");
        UNIT_ASSERT_VALUES_EQUAL(config.at("static_erasure").Scalar(), "mirror-3-dc");

        auto blobStorage = config.at("blob_storage_config").Map();
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.at("cache_pdisks").Scalar(), "false");
        auto serviceSet = blobStorage.at("service_set").Map();
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.at("pdisks").Sequence().at(0).Map().at("node_id").Scalar(), "1");
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.at("vdisks").Sequence().at(0).Map().at("storage_pool_name").Scalar(), "static");
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.at("groups").Sequence().at(0).Map().at("group_id").Scalar(), "1");
        UNIT_ASSERT_VALUES_EQUAL(serviceSet.at("availability_domains").Sequence().at(0).Scalar(), "1");
        UNIT_ASSERT_VALUES_EQUAL(root.at("metadata").Map().at("version").Scalar(), "7");
        UNIT_ASSERT_VALUES_EQUAL(root.at("custom_root_field").Scalar(), "preserved");
    }

    Y_UNIT_TEST(Merge_DynamicOnlySections) {
        const char* staticConfig = R"(
hosts:
- host: static-node
)";
        const char* dynamicConfig = R"(
config:
  nameservice_config:
    cluster_uuid: dynamic-cluster
  blob_storage_config:
    infer_pdisk_slot_count_settings:
      ssd:
        unit_size: 1000000000
)";

        const auto result = NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig, "static.yaml", "dynamic.yaml");
        UNIT_ASSERT(result.HasConflicts);
        for (const auto expected : {
                 TStringBuf("<<<<<<< static.yaml\n=======\n  nameservice_config:"),
                 TStringBuf("cluster_uuid: dynamic-cluster"),
                 TStringBuf("<<<<<<< static.yaml\n=======\n  blob_storage_config:"),
                 TStringBuf("infer_pdisk_slot_count_settings:"),
                 TStringBuf(">>>>>>> dynamic.yaml"),
             }) {
            UNIT_ASSERT_C(result.Config.Contains(expected), "Missing '" << expected << "' in:\n" << result.Config);
        }
    }

    Y_UNIT_TEST(Merge_Conflicts) {
        const char* staticConfig = R"(
nameservice_config:
  cluster_uuid: static-cluster
domains_config:
  forbid_implicit_storage_pools: true
blob_storage_config:
  cache_pdisks: false
  service_set:
    groups:
    - group_id: 1
)";
        const char* dynamicConfig = R"(
config:
  nameservice_config:
    cluster_uuid: dynamic-cluster
  domains_config:
    domain:
    - name: Root
  blob_storage_config:
    cache_pdisks: true
    service_set:
      groups:
      - group_id: 2
)";

        const auto result = NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig, "static.yaml", "dynamic.yaml");
        UNIT_ASSERT(result.HasConflicts);
        for (const auto expected : {
                 TStringBuf("<<<<<<< static.yaml\n  nameservice_config:"),
                 TStringBuf("cluster_uuid: static-cluster"),
                 TStringBuf("=======\n  nameservice_config:"),
                 TStringBuf("cluster_uuid: dynamic-cluster"),
                 TStringBuf(">>>>>>> dynamic.yaml"),
                 TStringBuf("<<<<<<< static.yaml\n  domains_config:"),
                 TStringBuf("forbid_implicit_storage_pools: true"),
                 TStringBuf("- name: Root"),
                 TStringBuf("<<<<<<< static.yaml\n  blob_storage_config:"),
                 TStringBuf("cache_pdisks: false"),
                 TStringBuf("cache_pdisks: true"),
             }) {
            UNIT_ASSERT_C(result.Config.Contains(expected), "Missing '" << expected << "' in:\n" << result.Config);
        }
        UNIT_ASSERT(!result.Config.Contains("__ydb_config_migration_conflict_"));
    }

    Y_UNIT_TEST(Merge_SecurityConflict) {
        const char* staticConfig = R"(
domains_config:
  security_config:
    default_users:
    - name: static-user
)";
        const char* dynamicConfig = R"(
config:
  security_config:
    default_users:
    - name: dynamic-user
)";

        const auto result = NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig);

        UNIT_ASSERT(result.HasConflicts);
        UNIT_ASSERT(result.Config.Contains("<<<<<<< static config\n  domains_config:"));
        UNIT_ASSERT(result.Config.Contains("- name: static-user"));
        UNIT_ASSERT(result.Config.Contains("=======\n  security_config:"));
        UNIT_ASSERT(result.Config.Contains("- name: dynamic-user"));
    }

    Y_UNIT_TEST(Merge_StoragePoolsConflict) {
        const char* staticConfig = R"(
domains_config:
  state_storage:
  - ss_id: 1
)";
        const char* dynamicConfig = R"(
config:
  storage_pool_types:
  - kind: ssd
  domain_name: Root
)";

        const auto result = NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig);

        UNIT_ASSERT(result.HasConflicts);
        UNIT_ASSERT(result.Config.Contains("<<<<<<< static config\n  domains_config:"));
        UNIT_ASSERT(result.Config.Contains("state_storage:"));
        UNIT_ASSERT(result.Config.Contains("=======\n  storage_pool_types:"));
        UNIT_ASSERT(result.Config.Contains("domain_name: Root"));
    }

    Y_UNIT_TEST(Merge_ErasureConflict) {
        const auto aliasConflict = NYamlConfig::MergeConfigsForMigration(
            "static_erasure: mirror-3-dc\n",
            "config:\n  erasure: block-4-2\n");

        UNIT_ASSERT(aliasConflict.HasConflicts);
        UNIT_ASSERT(aliasConflict.Config.Contains("<<<<<<< static config\n  static_erasure: mirror-3-dc"));
        UNIT_ASSERT(aliasConflict.Config.Contains("=======\n  erasure: block-4-2"));

        const auto selfManagementConflict = NYamlConfig::MergeConfigsForMigration(
            "static_erasure: mirror-3-dc\n",
            "config:\n  self_management_config:\n    erasure_species: block-4-2\n");

        UNIT_ASSERT(selfManagementConflict.HasConflicts);
        UNIT_ASSERT(selfManagementConflict.Config.Contains("<<<<<<< static config\n  static_erasure: mirror-3-dc"));
        UNIT_ASSERT(selfManagementConflict.Config.Contains("=======\n  self_management_config:"));
        UNIT_ASSERT(selfManagementConflict.Config.Contains("erasure_species: block-4-2"));
    }

    Y_UNIT_TEST(Merge_EquivalentAliases) {
        const char* staticConfig = R"(
domains_config:
  security_config:
    default_users:
    - name: root
static_erasure: block-4-2
)";
        const char* dynamicConfig = R"(
config:
  security_config:
    default_users:
    - name: root
  erasure: block-4-2
)";

        const auto result = NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig);

        UNIT_ASSERT_C(!result.HasConflicts, result.Config);
        UNIT_ASSERT_NO_EXCEPTION(NFyaml::TDocument::Parse(result.Config));
    }

    Y_UNIT_TEST(Merge_PlaceholderCollision) {
        const char* staticConfig = R"(
domains_config:
  domain:
  - name: StaticDomain
)";
        const char* dynamicConfig = R"(
config:
  harmless: __ydb_config_migration_conflict_0__
  domains_config:
    domain:
    - name: DynamicDomain
)";

        const auto result = NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig);

        UNIT_ASSERT(result.HasConflicts);
        UNIT_ASSERT(result.Config.Contains("  harmless: __ydb_config_migration_conflict_0__\n"));
        UNIT_ASSERT(result.Config.Contains("<<<<<<< static config\n  domains_config:"));
        UNIT_ASSERT(result.Config.Contains("- name: StaticDomain"));
        UNIT_ASSERT(result.Config.Contains("- name: DynamicDomain"));
    }

    Y_UNIT_TEST(Merge_Selectors) {
        const char* staticConfig = R"(
hosts:
- host: static-node
domains_config:
  domain:
  - name: StaticDomain
  security_config:
    default_users:
    - name: root
)";
        const char* dynamicConfig = R"(
config:
  hosts:
  - host: dynamic-node
  domains_config:
    domain:
    - name: StaticDomain
    security_config:
      default_users:
      - name: root
allowed_labels:
  host:
    type: string
selector_config:
- description: node override
  selector:
    host: node-1
  config:
    hosts:
    - host: selector-node
    domains_config:
      domain:
      - name: SelectorDomain
      security_config:
        default_users:
        - name: selector-user
)";

        auto result = ParseSuccessfulMerge(NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig));
        auto config = Config(result);
        auto selectorConfig = result.Root().Map().at("selector_config").Sequence().at(0).Map().at("config").Map();

        UNIT_ASSERT(!config.Has("security_config"));
        UNIT_ASSERT_VALUES_EQUAL(
            config.at("domains_config").Map().at("security_config").Map().at("default_users").Sequence().at(0).Map().at("name").Scalar(),
            "root");
        UNIT_ASSERT_VALUES_EQUAL(selectorConfig.at("hosts").Sequence().at(0).Map().at("host").Scalar(), "selector-node");
        auto selectorDomains = selectorConfig.at("domains_config");
        UNIT_ASSERT(!selectorDomains.Tag());
        UNIT_ASSERT_VALUES_EQUAL(selectorDomains.Map().at("domain").Sequence().at(0).Map().at("name").Scalar(), "SelectorDomain");
        UNIT_ASSERT_VALUES_EQUAL(
            selectorDomains.Map().at("security_config").Map().at("default_users").Sequence().at(0).Map().at("name").Scalar(),
            "selector-user");
    }

    Y_UNIT_TEST(ToggleFeatureFlag) {
        const char* input = R"(
metadata:
  kind: MainConfig
  version: 7
config:
  feature_flags:
    switch_to_config_v2: false
    enable_database_admin: true
selector_config: []
)";

        auto enabled = NYamlConfig::SetConfigV2FeatureFlag(input, true);
        auto featureFlags = Config(enabled).at("feature_flags").Map();

        UNIT_ASSERT_VALUES_EQUAL(featureFlags.at("switch_to_config_v2").Scalar(), "true");
        UNIT_ASSERT_VALUES_EQUAL(featureFlags.at("enable_database_admin").Scalar(), "true");
        UNIT_ASSERT_VALUES_EQUAL(enabled.Root().Map().at("metadata").Map().at("version").Scalar(), "7");
        UNIT_ASSERT(enabled.Root().Map().Has("selector_config"));

        TStringStream serialized;
        serialized << enabled;
        auto disabled = NYamlConfig::SetConfigV2FeatureFlag(serialized.Str(), false);
        UNIT_ASSERT_VALUES_EQUAL(Config(disabled).at("feature_flags").Map().at("switch_to_config_v2").Scalar(), "false");
    }

    Y_UNIT_TEST(ToggleSelfManagement) {
        const char* input = R"(
config:
  feature_flags:
    switch_to_config_v2: true
  self_management_config:
    enabled: false
    generation: 10
)";

        UNIT_ASSERT(!NYamlConfig::IsSelfManagementEnabled(input));
        auto enabled = NYamlConfig::SetSelfManagement(input, true);
        auto selfManagement = Config(enabled).at("self_management_config").Map();

        UNIT_ASSERT_VALUES_EQUAL(selfManagement.at("enabled").Scalar(), "true");
        UNIT_ASSERT_VALUES_EQUAL(selfManagement.at("generation").Scalar(), "10");

        TStringStream serialized;
        serialized << enabled;
        UNIT_ASSERT(NYamlConfig::IsSelfManagementEnabled(serialized.Str()));
        auto disabled = NYamlConfig::SetSelfManagement(serialized.Str(), false);
        UNIT_ASSERT_VALUES_EQUAL(Config(disabled).at("self_management_config").Map().at("enabled").Scalar(), "false");

        TStringStream disabledSerialized;
        disabledSerialized << disabled;
        auto v2Disabled = NYamlConfig::SetConfigV2FeatureFlag(disabledSerialized.Str(), false);
        UNIT_ASSERT_VALUES_EQUAL(Config(v2Disabled).at("feature_flags").Map().at("switch_to_config_v2").Scalar(), "false");
    }

    Y_UNIT_TEST(SelfManagementRequiresV2) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::SetSelfManagement("config: {}", true),
            NYamlConfig::TYamlConfigEx,
            "switch_to_config_v2: true");
    }

    Y_UNIT_TEST(SetDiskFailDomainType) {
        auto result = NFyaml::TDocument::Parse("config: {}");
        UNIT_ASSERT(!NYamlConfig::HasDiskFailDomainType(result));
        NYamlConfig::SetDiskFailDomainType(result);
        UNIT_ASSERT_VALUES_EQUAL(Config(result).at("fail_domain_type").Scalar(), "disk");
        UNIT_ASSERT(NYamlConfig::HasDiskFailDomainType(result));
    }

    Y_UNIT_TEST(V2DisableRequiresSelfManagementOff) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::SetConfigV2FeatureFlag(R"(
config:
  feature_flags:
    switch_to_config_v2: true
  self_management_config:
    enabled: true
)",
                                                false),
            NYamlConfig::TYamlConfigEx,
            "Disable self-management");
    }

    Y_UNIT_TEST(ToggleFlagsWithSelectors) {
        const char* input = R"(
config: {}
selector_config:
- description: node settings
  selector: {}
  config:
    feature_flags: !inherit
      enable_database_admin: true
    self_management_config: !inherit
      generation: 10
)";

        auto withFeatureFlag = NYamlConfig::SetConfigV2FeatureFlag(input, true);
        TStringStream serialized;
        serialized << withFeatureFlag;
        auto result = NYamlConfig::SetSelfManagement(serialized.Str(), true);

        auto config = Config(result);
        UNIT_ASSERT_VALUES_EQUAL(config.at("feature_flags").Map().at("switch_to_config_v2").Scalar(), "true");
        UNIT_ASSERT_VALUES_EQUAL(config.at("self_management_config").Map().at("enabled").Scalar(), "true");

        auto selectorConfig = result.Root().Map().at("selector_config").Sequence().at(0).Map().at("config").Map();
        const auto featureFlagsTag = selectorConfig.at("feature_flags").Tag();
        const auto selfManagementTag = selectorConfig.at("self_management_config").Tag();
        UNIT_ASSERT(featureFlagsTag);
        UNIT_ASSERT(selfManagementTag);
        UNIT_ASSERT_VALUES_EQUAL(*featureFlagsTag, "!inherit");
        UNIT_ASSERT_VALUES_EQUAL(*selfManagementTag, "!inherit");

        TStringStream enabledSerialized;
        enabledSerialized << result;
        auto withoutSelfManagement = NYamlConfig::SetSelfManagement(enabledSerialized.Str(), false);
        TStringStream selfManagementDisabled;
        selfManagementDisabled << withoutSelfManagement;
        auto disabled = NYamlConfig::SetConfigV2FeatureFlag(selfManagementDisabled.Str(), false);

        auto disabledConfig = Config(disabled);
        UNIT_ASSERT_VALUES_EQUAL(disabledConfig.at("feature_flags").Map().at("switch_to_config_v2").Scalar(), "false");
        UNIT_ASSERT_VALUES_EQUAL(disabledConfig.at("self_management_config").Map().at("enabled").Scalar(), "false");
    }

    Y_UNIT_TEST(RejectsSelectorOverrides) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::SetConfigV2FeatureFlag(R"(
config: {}
selector_config:
- description: node settings
  selector: {}
  config:
    feature_flags: !inherit
      switch_to_config_v2: false
)",
                                                   true),
            NYamlConfig::TYamlConfigEx,
            "set 'switch_to_config_v2: true'");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::SetSelfManagement(R"(
config:
  feature_flags:
    switch_to_config_v2: true
selector_config:
- description: node settings
  selector: {}
  config:
    self_management_config:
      generation: 10
)",
                                               true),
            NYamlConfig::TYamlConfigEx,
            "set 'enabled: true'");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::SetConfigV2FeatureFlag(R"(
config: {}
selector_config:
- description: node settings
  selector: {}
  config:
    feature_flags:
      switch_to_config_v2: true
)",
                                                   false),
            NYamlConfig::TYamlConfigEx,
            "set 'switch_to_config_v2: false'");
    }

    Y_UNIT_TEST(RejectsMergeMarkers) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::SetConfigV2FeatureFlag(
                "config:\n"
                "<<<<<<< static.yaml\n"
                "  log_config: {}\n"
                "=======\n"
                "  log_config: {}\n"
                ">>>>>>> dynamic.yaml\n",
                true),
            NYamlConfig::TYamlConfigEx,
            "unresolved merge conflict at line 2");
    }

    Y_UNIT_TEST(Cleanup_LegacyFields) {
        const char* input = R"(
metadata:
  kind: MainConfig
  version: 9
config:
  feature_flags:
    switch_to_config_v2: true
  self_management_config:
    enabled: true
  domains_config:
    domain:
    - name: CustomDomain
      storage_pool_types:
      - kind: ssd
    state_storage:
    - ss_id: 1
    explicit_state_storage_config:
      ss_id: 1
      ring:
        node: [1, 2, 3]
    explicit_state_storage_board_config:
      ss_id: 1
      ring:
        node: [2, 3, 4]
    explicit_scheme_board_config:
      ss_id: 1
      ring:
        node: [3, 4, 5]
    security_config:
      default_users:
      - name: root
  blob_storage_config:
    service_set:
      pdisks:
      - node_id: 1
        pdisk_id: 1
      vdisks:
      - storage_pool_name: static
      groups:
      - group_id: 0
      availability_domains: [1]
      repl_broker_config:
        rate_bytes_per_second: 1000000
      enable_proxy_mock: false
    cache_pdisks: false
    bsc_settings:
      default_max_slots: 16
  channel_profile_config:
    profile:
    - profile_id: 0
  static_erasure: mirror-3-dc
  log_config:
    default_level: 4
)";

        auto result = NYamlConfig::CleanupConfigV2Migration(input);
        auto config = Config(result);
        auto expected = NFyaml::TDocument::Parse(input);
        auto expectedConfig = Config(expected);
        auto expectedDomains = expectedConfig.at("domains_config").Map();
        auto expectedServiceSet = expectedConfig.at("blob_storage_config").Map().at("service_set").Map();
        RemoveMapValue(expectedDomains, "state_storage");
        for (const auto key : {TStringBuf("pdisks"), TStringBuf("vdisks"), TStringBuf("groups")}) {
            RemoveMapValue(expectedServiceSet, key);
        }

        UNIT_ASSERT_C(result.Root().DeepEqual(expected.Root()), "Cleanup changed fields other than V1-managed topology");

        auto domains = config.at("domains_config").Map();
        UNIT_ASSERT(!domains.Has("state_storage"));
        UNIT_ASSERT(domains.Has("explicit_state_storage_config"));
        UNIT_ASSERT(domains.Has("explicit_state_storage_board_config"));
        UNIT_ASSERT(domains.Has("explicit_scheme_board_config"));
        UNIT_ASSERT(domains.Has("security_config"));
        UNIT_ASSERT(domains.at("domain").Sequence().at(0).Map().Has("storage_pool_types"));

        auto blobStorage = config.at("blob_storage_config").Map();
        auto serviceSet = blobStorage.at("service_set").Map();
        UNIT_ASSERT(!serviceSet.Has("pdisks"));
        UNIT_ASSERT(!serviceSet.Has("vdisks"));
        UNIT_ASSERT(!serviceSet.Has("groups"));
        UNIT_ASSERT(serviceSet.Has("availability_domains"));
        UNIT_ASSERT(serviceSet.Has("repl_broker_config"));
        UNIT_ASSERT(serviceSet.Has("enable_proxy_mock"));
        UNIT_ASSERT(blobStorage.Has("cache_pdisks"));
        UNIT_ASSERT(blobStorage.Has("bsc_settings"));

        UNIT_ASSERT(config.Has("channel_profile_config"));
        UNIT_ASSERT_VALUES_EQUAL(config.at("static_erasure").Scalar(), "mirror-3-dc");
        UNIT_ASSERT_VALUES_EQUAL(config.at("log_config").Map().at("default_level").Scalar(), "4");
        UNIT_ASSERT_VALUES_EQUAL(result.Root().Map().at("metadata").Map().at("version").Scalar(), "9");
    }

    Y_UNIT_TEST(Cleanup_Selectors) {
        const char* input = R"(
config:
  feature_flags:
    switch_to_config_v2: true
  self_management_config:
    enabled: true
  blob_storage_config:
    service_set:
      pdisks: [{}]
      vdisks: [{}]
      groups: [{}]
      availability_domains: [1]
selector_config:
- selector:
    host: node-1
  config:
    feature_flags: !inherit
      enable_database_admin: true
    self_management_config: !inherit
      generation: 10
    blob_storage_config: !inherit
      cache_pdisks: false
      service_set: !inherit
        pdisks: [{}]
        vdisks: [{}]
        groups: [{}]
        repl_broker_config:
          max_in_flight_read_bytes: 1024
    channel_profile_config:
      profile: []
)";

        auto result = NYamlConfig::CleanupConfigV2Migration(input);
        auto selectorConfig = result.Root().Map().at("selector_config").Sequence().at(0).Map().at("config").Map();
        auto expected = NFyaml::TDocument::Parse(input);
        auto expectedConfig = Config(expected);
        auto expectedServiceSet = expectedConfig.at("blob_storage_config").Map().at("service_set").Map();
        for (const auto key : {TStringBuf("pdisks"), TStringBuf("vdisks"), TStringBuf("groups")}) {
            RemoveMapValue(expectedServiceSet, key);
        }
        auto expectedSelectorConfig = expected.Root().Map().at("selector_config").Sequence().at(0).Map().at("config").Map();
        auto expectedSelectorServiceSet = expectedSelectorConfig.at("blob_storage_config").Map().at("service_set").Map();
        for (const auto key : {TStringBuf("pdisks"), TStringBuf("vdisks"), TStringBuf("groups")}) {
            RemoveMapValue(expectedSelectorServiceSet, key);
        }

        UNIT_ASSERT_C(result.Root().DeepEqual(expected.Root()), "Cleanup changed selector fields other than static-group topology");

        auto serviceSet = Config(result).at("blob_storage_config").Map().at("service_set").Map();
        UNIT_ASSERT(serviceSet.Has("availability_domains"));
        auto selectorBlobStorage = selectorConfig.at("blob_storage_config");
        auto selectorServiceSet = selectorBlobStorage.Map().at("service_set");
        UNIT_ASSERT(selectorBlobStorage.Tag());
        UNIT_ASSERT(selectorServiceSet.Tag());
        UNIT_ASSERT(selectorServiceSet.Map().Has("repl_broker_config"));
        UNIT_ASSERT(selectorConfig.Has("channel_profile_config"));
    }

    Y_UNIT_TEST(Cleanup_RejectsSelectorStateStorage) {
        const char* input = R"(
config:
  feature_flags:
    switch_to_config_v2: true
  self_management_config:
    enabled: true
selector_config:
- selector:
    host: node-1
  config:
    domains_config: !inherit
      state_storage:
      - ss_id: 1
)";

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::CleanupConfigV2Migration(input),
            NYamlConfig::TYamlConfigEx,
            "selector_config[0].config.domains_config.state_storage");
    }

    Y_UNIT_TEST(Cleanup_Idempotent) {
        const char* input = R"(
config:
  feature_flags:
    switch_to_config_v2: true
  self_management_config:
    enabled: true
  domains_config:
    state_storage:
    - ss_id: 1
  blob_storage_config:
    service_set:
      pdisks: [{}]
      vdisks: [{}]
      groups: [{}]
      availability_domains: [1]
    cache_pdisks: false
  channel_profile_config:
    profile: []
)";

        auto first = NYamlConfig::CleanupConfigV2Migration(input);
        TStringStream serialized;
        serialized << first;
        auto second = NYamlConfig::CleanupConfigV2Migration(serialized.Str());

        UNIT_ASSERT(first.Root().DeepEqual(second.Root()));
        UNIT_ASSERT(Config(second).at("blob_storage_config").Map().at("service_set").Map().Has("availability_domains"));
        UNIT_ASSERT(Config(second).Has("channel_profile_config"));
    }

    Y_UNIT_TEST(Cleanup_RequiresV2) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::CleanupConfigV2Migration(R"(
config:
  self_management_config:
    enabled: true
  blob_storage_config: {}
)"),
            NYamlConfig::TYamlConfigEx,
            "switch_to_config_v2: true");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::CleanupConfigV2Migration(R"(
config:
  feature_flags:
    switch_to_config_v2: true
  blob_storage_config: {}
)"),
            NYamlConfig::TYamlConfigEx,
            "self_management_config.enabled: true");
    }

    Y_UNIT_TEST(RejectsInvalidInput) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::MergeConfigsForMigration("config: {}", "config: {}"),
            NYamlConfig::TYamlConfigEx,
            "Static config must use simple V1 format");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::MergeConfigsForMigration("log_config: {}", "metadata: {}"),
            NYamlConfig::TYamlConfigEx,
            "Dynamic config must have a 'config' section");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::CleanupConfigV2Migration("metadata: {}"),
            NYamlConfig::TYamlConfigEx,
            "Config must have a 'config' section");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::MergeConfigsForMigration("log_config: [\n", "config: {}\n", "static.yaml", "dynamic.yaml"),
            NYamlConfig::TYamlConfigEx,
            "Failed to parse 'static.yaml'");
    }
} // Y_UNIT_TEST_SUITE(ConfigMigration)
