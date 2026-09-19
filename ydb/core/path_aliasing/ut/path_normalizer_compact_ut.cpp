#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NPathAliasing {
    namespace {

        void AddRule(NKikimrConfig::TPathRewriteConfig& config, const TString& pattern, const TString& replacement) {
            auto* rule = config.AddRules();
            rule->SetPattern(pattern);
            rule->SetReplacement(replacement);
        }

    } // namespace

    Y_UNIT_TEST_SUITE(PathNormalizerCompact) {
        Y_UNIT_TEST(DisabledEmptyAndUnmatchedInputsArePreservedByteForByte) {
            const TPathNormalizer disabled;
            const TPathNormalizer empty{NKikimrConfig::TPathRewriteConfig{}};

            NKikimrConfig::TPathRewriteConfig unmatchedConfig;
            AddRule(unmatchedConfig, R"(^/alias(/|$))", R"(/Root\1)");
            const TPathNormalizer unmatched(unmatchedConfig);

            for (const TString& path : {
                     TString{},
                     TString("/"),
                     TString("relative/path"),
                     TString("Root/Table"),
                     TString("./relative/../path"),
                     TString("relative//path"),
                     TString("/Root//Table"),
                     TString("/Root/./Table"),
                     TString("/Root/../Table"),
                     TString("/Other"),
                     TString("/Root/Table"),
                 }) {
                UNIT_ASSERT_VALUES_EQUAL(disabled.NormalizePath(path), path);
                UNIT_ASSERT_VALUES_EQUAL(empty.NormalizePath(path), path);
                UNIT_ASSERT_VALUES_EQUAL(unmatched.NormalizePath(path), path);
            }

            NKikimrConfig::TPathRewriteConfig emptyMatchConfig;
            AddRule(emptyMatchConfig, "", "/unexpected");
            UNIT_ASSERT_VALUES_EQUAL(TPathNormalizer(emptyMatchConfig).NormalizePath(""), "");
        }

        Y_UNIT_TEST(MatchesRawInputWithoutPathReconstruction) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, R"(^/raw//\.\./alias$)", "/target//./resource");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/raw//../alias"), "/target//./resource");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/raw/../alias"), "/raw/../alias");
        }

        Y_UNIT_TEST(PrefixReplacementPreservesTheUnmatchedSuffix) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, R"(^/alias(/|$))", R"(/Root\1)");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/alias"), "/Root");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/alias/Table/Nested"), "/Root/Table/Nested");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/aliases/Table"), "/aliases/Table");
        }

        Y_UNIT_TEST(CapturesAreExpandedFromTheSuppliedString) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, R"(^/tenant/([^/]+)/tables(/.*)?$)", R"(/Root/\1\2)");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/tenant/alice/tables"), "/Root/alice");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/tenant/alice/tables/one"), "/Root/alice/one");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/other/alice/tables/one"), "/other/alice/tables/one");
        }

        Y_UNIT_TEST(FirstMatchWinsIncludingIdentityAndResultsAreNotChained) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, R"(^/identity(/|$))", R"(/identity\1)");
            AddRule(config, "^/identity", "/wrong");
            AddRule(config, "^/first", "/second");
            AddRule(config, "^/second", "/third");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/identity/Table"), "/identity/Table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/first/Table"), "/second/Table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/second/Table"), "/third/Table");
        }

        Y_UNIT_TEST(RulesRemainOrderedWithoutAnArtificialCountLimit) {
            NKikimrConfig::TPathRewriteConfig config;
            for (size_t index = 0; index < 128; ++index) {
                AddRule(config, TStringBuilder() << "^/alias" << index << '$', TStringBuilder() << "/target" << index);
            }
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/alias0"), "/target0");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/alias127"), "/target127");
        }

        Y_UNIT_TEST(InvalidRulesAreRejectedAtConstruction) {
            NKikimrConfig::TPathRewriteConfig invalidPattern;
            AddRule(invalidPattern, "(", "/target");
            UNIT_ASSERT_EXCEPTION(TPathNormalizer{invalidPattern}, yexception);

            NKikimrConfig::TPathRewriteConfig invalidReplacement;
            AddRule(invalidReplacement, "^/alias", R"(/target\1)");
            UNIT_ASSERT_EXCEPTION(TPathNormalizer{invalidReplacement}, yexception);

            NKikimrConfig::TPathRewriteConfig missingPattern;
            missingPattern.AddRules()->SetReplacement("/target");
            UNIT_ASSERT_EXCEPTION(TPathNormalizer{missingPattern}, yexception);

            NKikimrConfig::TPathRewriteConfig missingReplacement;
            missingReplacement.AddRules()->SetPattern("^/alias");
            UNIT_ASSERT_EXCEPTION(TPathNormalizer{missingReplacement}, yexception);
        }
    } // Y_UNIT_TEST_SUITE(PathNormalizerCompact)

} // namespace NKikimr::NPathAliasing
