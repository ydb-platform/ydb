#include "yaml_config_parser.h"

#include <ydb/core/path_aliasing/path_normalizer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

namespace NKikimr::NYaml {

    Y_UNIT_TEST_SUITE(PathAliasingYamlConfig) {
        Y_UNIT_TEST(ParsesOrderedPrefixesAndIgnoresOneTrailingSlash) {
            const auto config = Parse(R"(
path_rewrite_config:
  rules:
    - src: '/kfront/'
      dst: '/failover/kfront/'
    - src: '/kfront/tables'
      dst: '/archive'
)", false);
            UNIT_ASSERT(config.HasPathRewriteConfig());
            const auto& aliases = config.GetPathRewriteConfig();
            UNIT_ASSERT_VALUES_EQUAL(aliases.RulesSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(0).GetSrc(), "/kfront/");
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(0).GetDst(), "/failover/kfront/");
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(1).GetSrc(), "/kfront/tables");
            UNIT_ASSERT_VALUES_EQUAL(aliases.GetRules(1).GetDst(), "/archive");

            const NPathAliasing::TPathNormalizer normalizer(aliases);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), "/failover/kfront/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/"), "/failover/kfront");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/tables/table/"), "/failover/kfront/tables/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfrontend/table/"), "/kfrontend/table/");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront//table///"), "/failover/kfront//table//");
        }

        Y_UNIT_TEST(OmittedAndEmptyConfigurationDisableAliasing) {
            for (const TString& yaml : {TString("grpc_config: {port: 2135}"), TString("path_rewrite_config: {}"), TString("path_rewrite_config: {rules: []}")}) {
                const auto config = Parse(yaml, false);
                const NPathAliasing::TPathNormalizer normalizer(config.GetPathRewriteConfig());
                UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), "/kfront/table");
            }
        }

        Y_UNIT_TEST(PrefixesTreatMetacharactersLiterally) {
            const auto config = Parse(R"(
path_rewrite_config:
  rules:
    - src: '/literal.[a-z]+'
      dst: '/archive/\1'
)", false);
            const NPathAliasing::TPathNormalizer normalizer(config.GetPathRewriteConfig());
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/literal.[a-z]+/table"), R"(/archive/\1/table)");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/literal.x/table"), "/literal.x/table");
        }

        Y_UNIT_TEST(RootDestinationReplacesThePrefix) {
            const auto config = Parse(R"(
path_rewrite_config:
  rules:
    - src: '/prefix'
      dst: '/'
)", false);
            const NPathAliasing::TPathNormalizer normalizer(config.GetPathRewriteConfig());
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/prefix"), "/");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/prefix/table"), "/table");
        }

        Y_UNIT_TEST(RejectsMissingEmptyAndRelativePrefixes) {
            for (const TString& yaml : {
                     TString("path_rewrite_config: {rules: [{dst: '/target'}]}"),
                     TString("path_rewrite_config: {rules: [{src: '/source'}]}"),
                     TString("path_rewrite_config: {rules: [{src: '', dst: '/target'}]}"),
                     TString("path_rewrite_config: {rules: [{src: '/source', dst: ''}]}"),
                     TString("path_rewrite_config: {rules: [{src: 'source', dst: '/target'}]}"),
                     TString("path_rewrite_config: {rules: [{src: '/source', dst: 'target'}]}"),
                 }) {
                const auto config = Parse(yaml, false);
                UNIT_ASSERT_EXCEPTION(NPathAliasing::TPathNormalizer(config.GetPathRewriteConfig()), yexception);
            }
        }

    } // Y_UNIT_TEST_SUITE(PathAliasingYamlConfig)

} // namespace NKikimr::NYaml
