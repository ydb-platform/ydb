#include "yaml_config_parser.h"

#include <ydb/core/path_aliasing/path_normalizer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

namespace NKikimr::NYaml {

    Y_UNIT_TEST_SUITE(PathAliasingYamlConfig) {
        Y_UNIT_TEST(ParsesOrderedPrefixesAndNormalizesMatchedPaths) {
            const auto config = Parse(R"(
resource_path_prefix_mapping:
  rules:
    - src: '/kfront/'
      dst: '/failover/kfront/'
    - src: '/kfront/tables'
      dst: '/archive'
)", false);
            UNIT_ASSERT(config.HasResourcePathPrefixMapping());
            const auto& aliases = config.GetResourcePathPrefixMapping();
            UNIT_ASSERT_VALUES_EQUAL(aliases.RulesSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(0).GetSrc(), "/kfront/");
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(0).GetDst(), "/failover/kfront/");
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(1).GetSrc(), "/kfront/tables");
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(1).GetDst(), "/archive");

            const NPathAliasing::TPathNormalizer normalizer(aliases);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), "/failover/kfront/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront"), "/failover/kfront");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/"), "/failover/kfront");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/tables/table/"), "/failover/kfront/tables/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfrontend/table/"), "/kfrontend/table/");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfrontend//table///"), "/kfrontend//table///");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront//table///"), "/failover/kfront/table");
        }

        Y_UNIT_TEST(OmittedAndEmptyConfigurationDisableAliasing) {
            for (const TString& yaml : {TString("grpc_config: {port: 2135}"), TString("resource_path_prefix_mapping: {}"), TString("resource_path_prefix_mapping: {rules: []}")}) {
                const auto config = Parse(yaml, false);
                const NPathAliasing::TPathNormalizer normalizer(config.GetResourcePathPrefixMapping());
                UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), "/kfront/table");
                UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront//table///"), "/kfront//table///");
            }
        }

        Y_UNIT_TEST(PrefixesTreatMetacharactersLiterally) {
            const auto config = Parse(R"(
resource_path_prefix_mapping:
  rules:
    - src: '/literal.[a-z]+'
      dst: '/archive/\1'
)", false);
            const NPathAliasing::TPathNormalizer normalizer(config.GetResourcePathPrefixMapping());
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/literal.[a-z]+/table"), R"(/archive/\1/table)");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/literal.x/table"), "/literal.x/table");
        }

        Y_UNIT_TEST(RootDestinationReplacesThePrefix) {
            const auto config = Parse(R"(
resource_path_prefix_mapping:
  rules:
    - src: '/prefix'
      dst: '/'
)", false);
            const NPathAliasing::TPathNormalizer normalizer(config.GetResourcePathPrefixMapping());
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/prefix"), "/");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/prefix/"), "/");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/prefix/table"), "/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/prefix//table///"), "/table");
        }

        Y_UNIT_TEST(RejectsMissingEmptyAndRelativePrefixes) {
            for (const TString& yaml : {
                     TString("resource_path_prefix_mapping: {rules: [{dst: '/target'}]}"),
                     TString("resource_path_prefix_mapping: {rules: [{src: '/source'}]}"),
                     TString("resource_path_prefix_mapping: {rules: [{src: '', dst: '/target'}]}"),
                     TString("resource_path_prefix_mapping: {rules: [{src: '/source', dst: ''}]}"),
                     TString("resource_path_prefix_mapping: {rules: [{src: 'source', dst: '/target'}]}"),
                     TString("resource_path_prefix_mapping: {rules: [{src: '/source', dst: 'target'}]}"),
                 }) {
                const auto config = Parse(yaml, false);
                UNIT_ASSERT_EXCEPTION(NPathAliasing::TPathNormalizer(config.GetResourcePathPrefixMapping()), yexception);
            }
        }

    } // Y_UNIT_TEST_SUITE(PathAliasingYamlConfig)

} // namespace NKikimr::NYaml
