#include <ydb/library/yaml_config/public/migration/config_migration.h>

#include "yaml_config_parser.h"

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

using namespace NKikimr;

namespace {

    TString PrepareCleanupInput(TString input) {
        auto doc = NFyaml::TDocument::Parse(std::move(input));
        auto root = doc.Root();
        if (root.Type() == NFyaml::ENodeType::Mapping && root.Map().Has("config") && root.Map().at("config").Type() == NFyaml::ENodeType::Mapping) {
            auto config = root.Map().at("config").Map();
            auto ensureTrue = [&](TStringBuf sectionName, TStringBuf fieldName) {
                const TString sectionKey(sectionName);
                if (!config.Has(sectionKey)) {
                    config.Append(doc.CreateScalar(sectionKey), doc.CreateMapping());
                }
                auto section = config.at(sectionKey);
                if (section.Type() == NFyaml::ENodeType::Mapping && !section.Map().Has(TString(fieldName))) {
                    section.Map().Append(doc.CreateScalar(TString(fieldName)), doc.CreateScalar("true"));
                }
            };
            ensureTrue("feature_flags", "switch_to_config_v2");
            ensureTrue("self_management_config", "enabled");
        }

        TStringStream normalized;
        normalized << doc;
        return normalized.Str();
    }

    NFyaml::TDocument Cleanup(TString input) {
        return NYamlConfig::CleanupConfigV2Migration(PrepareCleanupInput(std::move(input)));
    }

    NFyaml::TDocument CleanupAndAssertSameAppConfig(TString input) {
        const auto preparedInput = PrepareCleanupInput(std::move(input));
        const auto before = NYaml::Parse(preparedInput);
        auto result = NYamlConfig::CleanupConfigV2Migration(preparedInput);

        TStringStream cleanedConfig;
        cleanedConfig << result;
        const auto after = NYaml::Parse(cleanedConfig.Str());

        google::protobuf::util::MessageDifferencer differencer;
        TString diff;
        differencer.ReportDifferencesToString(&diff);
        UNIT_ASSERT_C(differencer.Compare(before, after),
                      "Cleanup changed the resulting TAppConfig:\n" << diff << "\nCleaned config:\n" << cleanedConfig.Str());
        return result;
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(ConfigMigration) {
    Y_UNIT_TEST(MergeConfigsForMigration_AppliesSectionPriorities) {
        const char* staticConfig = R"(
actor_system_config:
  threads: 4
monitoring_config:
  port: 8765
hosts:
- host: static-node
host_configs:
- host_config_id: 1
nameservice_config:
  cluster_uuid: static-cluster
blob_storage_config:
  marker: static
channel_profile_config:
  marker: static
static_erasure: mirror-3-dc
domains_config:
  domain:
  - name: StaticDomain
  state_storage:
  - ss_id: 1
  security_config:
    default_users:
    - name: static-user
  named_compaction_policy:
  - name: static-policy
)";
        const char* dynamicConfig = R"(
metadata:
  kind: MainConfig
  version: 7
config:
  actor_system_config:
    threads: 8
  hosts:
  - host: dynamic-node
  host_configs:
  - host_config_id: 2
  nameservice_config:
    cluster_uuid: dynamic-cluster
  blob_storage_config:
    marker: dynamic
  channel_profile_config:
    marker: dynamic
  static_erasure: none
  domains_config:
    domain:
    - name: DynamicDomain
    state_storage:
    - ss_id: 2
    security_config:
      default_users:
      - name: dynamic-user
    named_compaction_policy:
    - name: dynamic-policy
selector_config: []
custom_root_field: preserved
)";

        auto result = NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig);
        auto root = result.Config.Root().Map();
        auto config = root.at("config").Map();

        UNIT_ASSERT_VALUES_EQUAL(config.at("actor_system_config").Map().at("threads").Scalar(), "8");
        UNIT_ASSERT_VALUES_EQUAL(config.at("monitoring_config").Map().at("port").Scalar(), "8765");
        UNIT_ASSERT_VALUES_EQUAL(config.at("hosts").Sequence().at(0).Map().at("host").Scalar(), "static-node");
        UNIT_ASSERT_VALUES_EQUAL(config.at("host_configs").Sequence().at(0).Map().at("host_config_id").Scalar(), "1");
        UNIT_ASSERT_VALUES_EQUAL(config.at("nameservice_config").Map().at("cluster_uuid").Scalar(), "static-cluster");
        UNIT_ASSERT_VALUES_EQUAL(config.at("blob_storage_config").Map().at("marker").Scalar(), "static");
        UNIT_ASSERT_VALUES_EQUAL(config.at("channel_profile_config").Map().at("marker").Scalar(), "static");
        UNIT_ASSERT_VALUES_EQUAL(config.at("static_erasure").Scalar(), "mirror-3-dc");

        auto domains = config.at("domains_config").Map();
        UNIT_ASSERT_VALUES_EQUAL(domains.at("domain").Sequence().at(0).Map().at("name").Scalar(), "StaticDomain");
        UNIT_ASSERT_VALUES_EQUAL(domains.at("state_storage").Sequence().at(0).Map().at("ss_id").Scalar(), "1");
        UNIT_ASSERT(!domains.Has("security_config"));
        UNIT_ASSERT_VALUES_EQUAL(
            config.at("security_config").Map().at("default_users").Sequence().at(0).Map().at("name").Scalar(),
            "dynamic-user");
        UNIT_ASSERT_VALUES_EQUAL(
            domains.at("named_compaction_policy").Sequence().at(0).Map().at("name").Scalar(),
            "static-policy");

        UNIT_ASSERT_VALUES_EQUAL(root.at("metadata").Map().at("version").Scalar(), "7");
        UNIT_ASSERT_VALUES_EQUAL(root.at("custom_root_field").Scalar(), "preserved");

        auto hasPrecedencePath = [&](TStringBuf expected) {
            for (const auto& path : result.StaticConfigPrecedencePaths) {
                if (path == expected) {
                    return true;
                }
            }
            return false;
        };
        UNIT_ASSERT(hasPrecedencePath("config.hosts"));
        UNIT_ASSERT(hasPrecedencePath("config.domains_config"));
    }

    Y_UNIT_TEST(MergeConfigsForMigration_KeepsDynamicStorageWhenStaticIsMissing) {
        const char* staticConfig = R"(
log_config:
  default_level: 4
)";
        const char* dynamicConfig = R"(
config:
  hosts:
  - host: dynamic-node
  log_config:
    default_level: 6
)";

        auto result = NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig);
        auto config = result.Config.Root().Map().at("config").Map();

        UNIT_ASSERT_VALUES_EQUAL(config.at("hosts").Sequence().at(0).Map().at("host").Scalar(), "dynamic-node");
        UNIT_ASSERT_VALUES_EQUAL(config.at("log_config").Map().at("default_level").Scalar(), "6");
        UNIT_ASSERT(result.StaticConfigPrecedencePaths.empty());
    }

    Y_UNIT_TEST(MergeConfigsForMigration_PrefersTopLevelDynamicSecurityConfig) {
        const char* staticConfig = R"(
domains_config:
  domain:
  - name: StaticDomain
  security_config:
    default_users:
    - name: static-user
)";
        const char* dynamicConfig = R"(
config:
  security_config:
    default_users:
    - name: dynamic-user
  domains_config:
    security_config:
      default_users:
      - name: nested-dynamic-user
)";

        auto result = NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig);
        auto config = result.Config.Root().Map().at("config").Map();

        UNIT_ASSERT_VALUES_EQUAL(
            config.at("security_config").Map().at("default_users").Sequence().at(0).Map().at("name").Scalar(),
            "dynamic-user");
        UNIT_ASSERT(!config.at("domains_config").Map().Has("security_config"));
    }

    Y_UNIT_TEST(MergeConfigsForMigration_PreservesSelectors) {
        const char* staticConfig = R"(
hosts:
- host: static-node
domains_config:
  domain:
  - name: StaticDomain
)";
        const char* dynamicConfig = R"(
config:
  hosts:
  - host: dynamic-node
  domains_config:
    domain:
    - name: DynamicDomain
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

        auto result = NYamlConfig::MergeConfigsForMigration(staticConfig, dynamicConfig);
        auto root = result.Config.Root().Map();
        auto selectorConfig = root.at("selector_config").Sequence().at(0).Map().at("config").Map();

        UNIT_ASSERT_VALUES_EQUAL(
            selectorConfig.at("hosts").Sequence().at(0).Map().at("host").Scalar(),
            "selector-node");
        auto selectorDomains = selectorConfig.at("domains_config");
        UNIT_ASSERT(!selectorDomains.Tag());
        UNIT_ASSERT_VALUES_EQUAL(
            selectorDomains.Map().at("domain").Sequence().at(0).Map().at("name").Scalar(),
            "SelectorDomain");
        UNIT_ASSERT(selectorDomains.Map().Has("security_config"));
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_PromotesValuesAndRemovesLegacyStorage) {
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
        pool_config:
          geometry:
            realm_level_begin: 10
            realm_level_end: 20
            domain_level_begin: 10
            domain_level_end: 256
    security_config:
      default_users:
      - name: root
  blob_storage_config:
    service_set: {}
  static_erasure: mirror-3-dc
  log_config:
    default_level: 4
selector_config:
- description: preserved selector
  selector: {}
  config:
    log_config:
      default_level: 5
)";

        auto result = Cleanup(input);
        auto root = result.Root().Map();
        auto config = root.at("config").Map();

        UNIT_ASSERT(!config.Has("domains_config"));
        UNIT_ASSERT(!config.Has("blob_storage_config"));
        UNIT_ASSERT(!config.Has("static_erasure"));
        UNIT_ASSERT_VALUES_EQUAL(config.at("fail_domain_type").Scalar(), "disk");
        UNIT_ASSERT_VALUES_EQUAL(config.at("domain_name").Scalar(), "CustomDomain");
        UNIT_ASSERT_VALUES_EQUAL(
            config.at("storage_pool_types").Sequence().at(0).Map().at("kind").Scalar(),
            "ssd");
        UNIT_ASSERT_VALUES_EQUAL(
            config.at("security_config").Map().at("default_users").Sequence().at(0).Map().at("name").Scalar(),
            "root");
        UNIT_ASSERT_VALUES_EQUAL(config.at("erasure").Scalar(), "mirror-3-dc");
        UNIT_ASSERT_VALUES_EQUAL(config.at("feature_flags").Map().at("switch_to_config_v2").Scalar(), "true");
        UNIT_ASSERT_VALUES_EQUAL(config.at("self_management_config").Map().at("enabled").Scalar(), "true");
        UNIT_ASSERT_VALUES_EQUAL(config.at("log_config").Map().at("default_level").Scalar(), "4");
        UNIT_ASSERT_VALUES_EQUAL(root.at("metadata").Map().at("version").Scalar(), "9");
        UNIT_ASSERT_VALUES_EQUAL(
            root.at("selector_config").Sequence().at(0).Map().at("config").Map().at("log_config").Map().at("default_level").Scalar(),
            "5");
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_ReplacesCanonicalStorageDefaults) {
        const char* input = R"(
config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: /dev/disk-1
      type: SSD
  hosts:
  - host: node-1
    host_config_id: 1
  - host: node-2
    host_config_id: 1
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: 1
          erasure_species: mirror-3-dc
          geometry:
            realm_level_begin: 10
            realm_level_end: 20
            domain_level_begin: 10
            domain_level_end: 256
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
  blob_storage_config: {}
  static_erasure: mirror-3-dc
  channel_profile_config:
    profile:
    - profile_id: 0
      channel:
      - erasure_species: mirror-3-dc
        pdisk_category: 1
        storage_pool_kind: ssd
      - erasure_species: mirror-3-dc
        pdisk_category: 1
        storage_pool_kind: ssd
      - erasure_species: mirror-3-dc
        pdisk_category: 1
        storage_pool_kind: ssd
)";

        auto result = Cleanup(input);
        auto config = result.Root().Map().at("config").Map();

        UNIT_ASSERT_VALUES_EQUAL(config.at("fail_domain_type").Scalar(), "disk");
        UNIT_ASSERT_VALUES_EQUAL(config.at("default_disk_type").Scalar(), "SSD");
        UNIT_ASSERT_VALUES_EQUAL(config.at("erasure").Scalar(), "mirror-3-dc");
        UNIT_ASSERT(!config.Has("storage_pool_types"));
        UNIT_ASSERT(!config.Has("channel_profile_config"));
        UNIT_ASSERT(!config.Has("domains_config"));
        UNIT_ASSERT(!config.Has("blob_storage_config"));
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_PreservesAppConfigWhenReplacingCanonicalRackDefaults) {
        const char* input = R"(
config:
  hosts:
  - host: node-1
    drive:
    - path: /dev/disk-1
      type: SSD
  - host: node-2
    drive:
    - path: /dev/disk-2
      type: SSD
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - pool_config:
          erasure_species: mirror-3-dc
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
  blob_storage_config: {}
  static_erasure: mirror-3-dc
  channel_profile_config:
    profile:
    - profile_id: 0
      channel:
      - erasure_species: mirror-3-dc
        pdisk_category: 1
      - erasure_species: mirror-3-dc
        pdisk_category: 1
      - erasure_species: mirror-3-dc
        pdisk_category: 1
)";

        auto result = CleanupAndAssertSameAppConfig(input);
        auto config = result.Root().Map().at("config").Map();

        UNIT_ASSERT_VALUES_EQUAL(config.at("fail_domain_type").Scalar(), "rack");
        UNIT_ASSERT_VALUES_EQUAL(config.at("default_disk_type").Scalar(), "SSD");
        UNIT_ASSERT_VALUES_EQUAL(config.at("erasure").Scalar(), "mirror-3-dc");
        UNIT_ASSERT(!config.Has("storage_pool_types"));
        UNIT_ASSERT(!config.Has("channel_profile_config"));
        UNIT_ASSERT(!config.Has("domains_config"));
        UNIT_ASSERT(!config.Has("blob_storage_config"));
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_ReplacesCanonicalSingleNodeDefaults) {
        const char* input = R"(
config:
  hosts:
  - host: localhost
    drive:
    - path: /dev/disk/by-partlabel/ydb_disk_01
      type: ROT
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: rot
        pool_config:
          box_id: 1
          erasure_species: none
          pdisk_filter:
          - property:
            - type: ROT
          vdisk_kind: Default
  blob_storage_config: {}
  static_erasure: none
  channel_profile_config:
    profile:
    - profile_id: 0
      channel:
      - erasure_species: none
        pdisk_category: 1
        storage_pool_kind: rot
      - erasure_species: none
        pdisk_category: 1
        storage_pool_kind: rot
      - erasure_species: none
        pdisk_category: 1
        storage_pool_kind: rot
)";

        auto result = Cleanup(input);
        auto config = result.Root().Map().at("config").Map();

        UNIT_ASSERT_VALUES_EQUAL(config.at("fail_domain_type").Scalar(), "rack");
        UNIT_ASSERT_VALUES_EQUAL(config.at("default_disk_type").Scalar(), "ROT");
        UNIT_ASSERT_VALUES_EQUAL(config.at("erasure").Scalar(), "none");
        UNIT_ASSERT(!config.Has("storage_pool_types"));
        UNIT_ASSERT(!config.Has("channel_profile_config"));
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_PreservesNonCanonicalStorageSettingsWithoutChangingAppConfig) {
        const char* input = R"(
config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: /dev/disk-1
      type: SSD
  hosts:
  - host: node-1
    host_config_id: 1
  - host: node-2
    host_config_id: 1
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: 1
          encryption_mode: 1
          erasure_species: mirror-3-dc
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
  blob_storage_config: {}
  static_erasure: mirror-3-dc
  channel_profile_config:
    profile:
    - profile_id: 7
      channel:
      - erasure_species: mirror-3-dc
        pdisk_category: 1
        storage_pool_kind: ssd
)";

        auto result = CleanupAndAssertSameAppConfig(input);
        auto config = result.Root().Map().at("config").Map();

        UNIT_ASSERT_VALUES_EQUAL(config.at("fail_domain_type").Scalar(), "rack");
        UNIT_ASSERT_VALUES_EQUAL(config.at("default_disk_type").Scalar(), "SSD");
        UNIT_ASSERT_VALUES_EQUAL(
            config.at("storage_pool_types").Sequence().at(0).Map().at("pool_config").Map().at("encryption_mode").Scalar(),
            "1");
        UNIT_ASSERT_VALUES_EQUAL(
            config.at("channel_profile_config").Map().at("profile").Sequence().at(0).Map().at("profile_id").Scalar(),
            "7");
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_KeepsCustomStoragePoolNestedWithOtherDomainSettings) {
        const char* input = R"(
config:
  host_configs:
  - host_config_id: 1
    drive:
    - path: /dev/disk-1
      type: SSD
  hosts:
  - host: node-1
    host_config_id: 1
  - host: node-2
    host_config_id: 1
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          encryption_mode: 1
          erasure_species: mirror-3-dc
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
    named_compaction_policy:
    - name: custom
      policy: {}
  blob_storage_config: {}
  static_erasure: mirror-3-dc
)";

        auto result = Cleanup(input);
        auto config = result.Root().Map().at("config").Map();
        auto domains = config.at("domains_config").Map();
        auto nestedPool = domains.at("domain").Sequence().at(0).Map().at("storage_pool_types").Sequence().at(0).Map();

        UNIT_ASSERT(!config.Has("storage_pool_types"));
        UNIT_ASSERT_VALUES_EQUAL(nestedPool.at("pool_config").Map().at("encryption_mode").Scalar(), "1");
        UNIT_ASSERT(domains.Has("named_compaction_policy"));
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_DoesNotPromoteDefaultDomainName) {
        const char* input = R"(
config:
  domains_config:
    domain:
    - name: Root
  blob_storage_config: {}
)";

        auto result = Cleanup(input);
        auto config = result.Root().Map().at("config").Map();

        UNIT_ASSERT(!config.Has("domain_name"));
        UNIT_ASSERT(!config.Has("domains_config"));
        UNIT_ASSERT(!config.Has("blob_storage_config"));
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_AcceptsMatchingV2Values) {
        const char* input = R"(
config:
  domain_name: CustomDomain
  storage_pool_types:
  - kind: ssd
  security_config:
    default_users:
    - name: root
  erasure: mirror-3-dc
  domains_config:
    domain:
    - name: CustomDomain
      storage_pool_types:
      - kind: ssd
    security_config:
      default_users:
      - name: root
  blob_storage_config: {}
  static_erasure: mirror-3-dc
)";

        auto result = Cleanup(input);
        auto config = result.Root().Map().at("config").Map();

        UNIT_ASSERT_VALUES_EQUAL(config.at("domain_name").Scalar(), "CustomDomain");
        UNIT_ASSERT_VALUES_EQUAL(config.at("storage_pool_types").Sequence().at(0).Map().at("kind").Scalar(), "ssd");
        UNIT_ASSERT_VALUES_EQUAL(
            config.at("security_config").Map().at("default_users").Sequence().at(0).Map().at("name").Scalar(),
            "root");
        UNIT_ASSERT_VALUES_EQUAL(config.at("erasure").Scalar(), "mirror-3-dc");
        UNIT_ASSERT(!config.Has("domains_config"));
        UNIT_ASSERT(!config.Has("blob_storage_config"));
        UNIT_ASSERT(!config.Has("static_erasure"));
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_InfersRackFromImplicitPoolGeometry) {
        const char* input = R"(
config:
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: hdd
        pool_config:
          kind: hdd
      - kind: encrypted-hdd
        pool_config:
          kind: hdd
  blob_storage_config: {}
)";

        auto result = Cleanup(input);
        auto config = result.Root().Map().at("config").Map();

        UNIT_ASSERT_VALUES_EQUAL(config.at("fail_domain_type").Scalar(), "rack");
        UNIT_ASSERT(config.Has("storage_pool_types"));
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_DoesNotInferFailDomainTypeFromMixedGeometry) {
        const char* input = R"(
config:
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: rack-pool
        pool_config:
          geometry:
            realm_level_begin: 10
            realm_level_end: 20
            domain_level_begin: 10
            domain_level_end: 40
      - kind: disk-pool
        pool_config:
          geometry:
            realm_level_begin: 10
            realm_level_end: 20
            domain_level_begin: 10
            domain_level_end: 256
  blob_storage_config: {}
)";

        auto result = Cleanup(input);
        auto config = result.Root().Map().at("config").Map();

        UNIT_ASSERT(!config.Has("fail_domain_type"));
        UNIT_ASSERT(config.Has("storage_pool_types"));
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_RejectsConflictingRepresentations) {
        const char* conflictingSecurity = R"(
config:
  security_config:
    default_users:
    - name: new-user
  domains_config:
    security_config:
      default_users:
      - name: old-user
  blob_storage_config: {}
)";
        const char* conflictingDomainName = R"(
config:
  domain_name: NewDomain
  domains_config:
    domain:
    - name: OldDomain
  blob_storage_config: {}
)";
        const char* conflictingStoragePoolTypes = R"(
config:
  storage_pool_types:
  - kind: new
  domains_config:
    domain:
    - storage_pool_types:
      - kind: old
  blob_storage_config: {}
)";
        const char* conflictingErasure = R"(
config:
  erasure: block-4-2
  static_erasure: mirror-3-dc
  domains_config: {}
  blob_storage_config: {}
)";

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Cleanup(conflictingSecurity),
            NYamlConfig::TYamlConfigEx,
            "config.security_config");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Cleanup(conflictingDomainName),
            NYamlConfig::TYamlConfigEx,
            "config.domain_name");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Cleanup(conflictingStoragePoolTypes),
            NYamlConfig::TYamlConfigEx,
            "config.storage_pool_types");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Cleanup(conflictingErasure),
            NYamlConfig::TYamlConfigEx,
            "config.erasure");
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_RejectsAmbiguousOrIncompleteInput) {
        const char* multipleDomains = R"(
config:
  domains_config:
    domain:
    - name: One
    - name: Two
  blob_storage_config: {}
)";

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Cleanup(multipleDomains),
            NYamlConfig::TYamlConfigEx,
            "more than one");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Cleanup("config: {}"),
            NYamlConfig::TYamlConfigEx,
            "freshly fetched config");
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_RejectsUnsafeContext) {
        const char* selfManagementDisabled = R"(
config:
  feature_flags:
    switch_to_config_v2: true
  self_management_config:
    enabled: false
  domains_config: {}
  blob_storage_config: {}
)";
        const char* configV2Disabled = R"(
config:
  feature_flags:
    switch_to_config_v2: false
  self_management_config:
    enabled: true
  domains_config: {}
  blob_storage_config: {}
)";
        const char* selectorStorageOverride = R"(
config:
  feature_flags:
    switch_to_config_v2: true
  self_management_config:
    enabled: true
  domains_config: {}
  blob_storage_config: {}
selector_config:
- selector: {}
  config:
    domains_config: {}
)";
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::CleanupConfigV2Migration(selfManagementDisabled),
            NYamlConfig::TYamlConfigEx,
            "self_management_config.enabled");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::CleanupConfigV2Migration(configV2Disabled),
            NYamlConfig::TYamlConfigEx,
            "feature_flags.switch_to_config_v2");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::CleanupConfigV2Migration(selectorStorageOverride),
            NYamlConfig::TYamlConfigEx,
            "selector_config.config.domains_config");
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_PromotesSecurityOnlySelectorOverride) {
        const char* input = R"(
config:
  domains_config: {}
  blob_storage_config: {}
selector_config:
- description: enforce user token
  selector:
    host: node-1
  config:
    domains_config: !inherit
      security_config: !inherit
        enforce_user_token_requirement: true
)";

        auto result = Cleanup(input);
        auto selector = result.Root().Map().at("selector_config").Sequence().at(0).Map();
        auto selectorConfig = selector.at("config").Map();

        UNIT_ASSERT(!selectorConfig.Has("domains_config"));
        UNIT_ASSERT(selectorConfig.Has("security_config"));
        auto security = selectorConfig.at("security_config");
        UNIT_ASSERT(security.Tag());
        UNIT_ASSERT_VALUES_EQUAL(*security.Tag(), "!inherit");
        UNIT_ASSERT_VALUES_EQUAL(security.Map().at("enforce_user_token_requirement").Scalar(), "true");
        UNIT_ASSERT_VALUES_EQUAL(selector.at("description").Scalar(), "enforce user token");
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_PreservesNonManagedContainerFields) {
        const char* input = R"(
config:
  domains_config:
    domain:
    - name: Root
    state_storage:
    - ssid: 1
    named_compaction_policy:
    - name: custom
      policy: {}
    explicit_state_storage_config:
      ring: {}
    explicit_state_storage_board_config:
      ring: {}
    explicit_scheme_board_config:
      ring: {}
    security_config:
      default_users:
      - name: root
  blob_storage_config:
    service_set: {}
    define_host_config: []
    define_box: []
    cache_pdisks: false
    cache_vdisks: false
)";

        auto result = Cleanup(input);
        auto config = result.Root().Map().at("config").Map();
        auto domains = config.at("domains_config").Map();
        auto blobStorage = config.at("blob_storage_config").Map();

        UNIT_ASSERT_VALUES_EQUAL(domains.at("domain").Sequence().at(0).Map().at("name").Scalar(), "Root");
        UNIT_ASSERT(!domains.Has("state_storage"));
        UNIT_ASSERT(domains.Has("named_compaction_policy"));
        UNIT_ASSERT(domains.Has("explicit_state_storage_config"));
        UNIT_ASSERT(domains.Has("explicit_state_storage_board_config"));
        UNIT_ASSERT(domains.Has("explicit_scheme_board_config"));
        UNIT_ASSERT(!domains.Has("security_config"));
        UNIT_ASSERT_VALUES_EQUAL(config.at("security_config").Map().at("default_users").Sequence().at(0).Map().at("name").Scalar(), "root");

        UNIT_ASSERT(!blobStorage.Has("service_set"));
        UNIT_ASSERT(!blobStorage.Has("define_host_config"));
        UNIT_ASSERT(!blobStorage.Has("define_box"));
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.at("cache_pdisks").Scalar(), "false");
        UNIT_ASSERT_VALUES_EQUAL(blobStorage.at("cache_vdisks").Scalar(), "false");
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_DoesNotChangeImplicitDriveType) {
        const char* input = R"(
config:
  hosts:
  - host: node-1
    drive:
    - path: /dev/disk-1
  - host: node-2
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: rot
        pool_config:
          erasure_species: mirror-3-dc
          kind: rot
          pdisk_filter:
          - property:
            - type: ROT
          vdisk_kind: Default
  blob_storage_config: {}
  static_erasure: mirror-3-dc
)";

        auto result = Cleanup(input);
        auto config = result.Root().Map().at("config").Map();

        UNIT_ASSERT(!config.Has("default_disk_type"));
        UNIT_ASSERT(config.Has("storage_pool_types"));
    }

    Y_UNIT_TEST(CleanupConfigV2Migration_DoesNotOverrideDifferentDriveType) {
        const char* input = R"(
config:
  hosts:
  - host: node-1
    drive:
    - path: /dev/disk-1
      type: ROT
  - host: node-2
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          erasure_species: mirror-3-dc
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
  blob_storage_config: {}
  static_erasure: mirror-3-dc
)";

        auto result = Cleanup(input);
        auto config = result.Root().Map().at("config").Map();

        UNIT_ASSERT(!config.Has("default_disk_type"));
        UNIT_ASSERT(config.Has("storage_pool_types"));
    }

    Y_UNIT_TEST(MergeConfigsForMigration_ValidatesInputFormats) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::MergeConfigsForMigration("config: {}", "config: {}"),
            NYamlConfig::TYamlConfigEx,
            "Static config must use simple V1 format");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            NYamlConfig::MergeConfigsForMigration("log_config: {}", "metadata: {}"),
            NYamlConfig::TYamlConfigEx,
            "Dynamic config must have a 'config' section");
    }
} // Y_UNIT_TEST_SUITE(ConfigMigration)
