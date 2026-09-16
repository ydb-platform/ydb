#include "yaml_config_parser.h"

#include <ydb/core/path_aliasing/path_normalizer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

namespace NKikimr::NYaml {

    Y_UNIT_TEST_SUITE(PathAliasingYamlConfig) {
        Y_UNIT_TEST(ParsesOrderedRulesAndPreservesReplacementEscapes) {
            const auto config = Parse(R"(
path_rewrite_config:
  rules:
    - pattern: '^/kfront(/|$)'
      replacement: '/failover/kfront\1'
    - pattern: '^/legacy/([^/]+)/tables(/.*)?$'
      replacement: '/archive/\1\2'
)", false);
            UNIT_ASSERT(config.HasPathRewriteConfig());
            const auto& aliases = config.GetPathRewriteConfig();
            UNIT_ASSERT_VALUES_EQUAL(aliases.RulesSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(0).GetPattern(), R"(^/kfront(/|$))");
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(0).GetReplacement(), R"(/failover/kfront\1)");
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(1).GetPattern(), R"(^/legacy/([^/]+)/tables(/.*)?$)");
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(1).GetReplacement(), R"(/archive/\1\2)");

            const NPathAliasing::TPathNormalizer normalizer(aliases);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), "/failover/kfront/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/legacy/user/tables/table"), "/archive/user/table");
        }

        Y_UNIT_TEST(OmittedAndEmptyConfigurationDisableAliasing) {
            for (const TString& yaml : {TString("grpc_config: {port: 2135}"), TString("path_rewrite_config: {}"), TString("path_rewrite_config: {rules: []}")}) {
                const auto config = Parse(yaml, false);
                const NPathAliasing::TPathNormalizer normalizer(config.GetPathRewriteConfig());
                UNIT_ASSERT(normalizer.Empty());
                UNIT_ASSERT(normalizer.GetFingerprint().empty());
                UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), "/kfront/table");
            }
        }

        Y_UNIT_TEST(TransformationPreservesRulesAlongsideStoragePoolTypes) {
            const auto config = Parse(R"(
path_rewrite_config:
  rules:
    - pattern: '^/kfront(/|$)'
      replacement: '/failover/kfront\1'
storage_pool_types:
  - kind: ssd
    pool_config:
      box_id: 1
      erasure_species: mirror-3-dc
      kind: ssd
      vdisk_kind: Default
host_configs:
  - nvme: [disk1]
hosts:
  - host: fqdn1
)", true);
            UNIT_ASSERT(config.HasPathRewriteConfig());
            UNIT_ASSERT_VALUES_EQUAL(config.GetPathRewriteConfig().RulesSize(), 1);
            const NPathAliasing::TPathNormalizer normalizer(config.GetPathRewriteConfig());
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), "/failover/kfront/table");
            UNIT_ASSERT_VALUES_EQUAL(config.GetDomainsConfig().GetDomain(0).StoragePoolTypesSize(), 1);
        }

        Y_UNIT_TEST(PreservesPresenceOfExplicitEmptyReplacement) {
            const auto config = Parse(R"(
path_rewrite_config:
  rules:
    - pattern: '^/prefix'
      replacement: ''
)", false);
            const auto& rule = config.GetPathRewriteConfig().GetRules(0);
            UNIT_ASSERT(rule.HasReplacement());
            UNIT_ASSERT(rule.GetReplacement().empty());
            const NPathAliasing::TPathNormalizer normalizer(config.GetPathRewriteConfig());
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/prefix/table"), "/table");
        }

        Y_UNIT_TEST(ParsedInvalidRuleFailsWhenConfigurationIsCompiled) {
            const auto config = Parse(R"(
path_rewrite_config:
  rules:
    - pattern: '^/kfront'
      replacement: '/failover/kfront'
    - pattern: '^/other'
      replacement: '/target\1'
)", false);
            UNIT_ASSERT_EXCEPTION_CONTAINS(NPathAliasing::TPathNormalizer{config.GetPathRewriteConfig()}, yexception, "rule");
        }
    } // Y_UNIT_TEST_SUITE(PathAliasingYamlConfig)

} // namespace NKikimr::NYaml
