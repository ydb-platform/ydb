#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NPathAliasing {
    namespace {

        void AddRule(NKikimrConfig::TPathRewriteConfig& config, const TString& src, const TString& dst) {
            auto* rule = config.AddRules();
            rule->SetSrc(src);
            rule->SetDst(dst);
        }

    } // namespace

    Y_UNIT_TEST_SUITE(PathNormalizerCompact) {
        Y_UNIT_TEST(DisabledEmptyAndUnmatchedInputsArePreservedByteForByte) {
            const TPathNormalizer disabled;
            const TPathNormalizer empty{NKikimrConfig::TPathRewriteConfig{}};

            NKikimrConfig::TPathRewriteConfig unmatchedConfig;
            AddRule(unmatchedConfig, "/ru", "/backup/ru");
            const TPathNormalizer unmatched(unmatchedConfig);

            for (const TString& path : {
                     TString{},
                     TString("/"),
                     TString("relative/path"),
                     TString("ru/mydb"),
                     TString("Root/Table"),
                     TString("./relative/../path"),
                     TString("relative//path"),
                     TString("/Root//Table"),
                     TString("/Root/./Table"),
                     TString("/Root/../Table"),
                     TString("/Other///"),
                     TString("/Root/Table"),
                 }) {
                UNIT_ASSERT_VALUES_EQUAL(disabled.NormalizePath(path), path);
                UNIT_ASSERT_VALUES_EQUAL(empty.NormalizePath(path), path);
                UNIT_ASSERT_VALUES_EQUAL(unmatched.NormalizePath(path), path);
            }
        }

        Y_UNIT_TEST(TrailingSourceSlashesAreIgnoredAndMatchedPathsAreNormalized) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/ru/", "/backup//ru/");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru"), "/backup/ru");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/"), "/backup/ru");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru//"), "/backup/ru");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/mydb"), "/backup/ru/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/mydb/"), "/backup/ru/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru//mydb///"), "/backup/ru/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("//ru/mydb"), "//ru/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/russian///"), "/russian///");
        }

        Y_UNIT_TEST(PrefixReplacementRequiresAWholePathComponent) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/ru", "/backup/ru");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru"), "/backup/ru");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/"), "/backup/ru");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru//"), "/backup/ru");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/mydb"), "/backup/ru/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/mydb/Table"), "/backup/ru/mydb/Table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/russian/mydb"), "/russian/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/RU/mydb"), "/RU/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/other/ru/mydb"), "/other/ru/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("other/ru/mydb"), "other/ru/mydb");
        }

        Y_UNIT_TEST(RepeatedTrailingSourceSlashesPreserveFirstMatchPrecedence) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/ru///", "/first//");
            AddRule(config, "/ru", "/second");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru"), "/first");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/"), "/first");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru//db/"), "/first/db");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/russian//"), "/russian//");
        }

        Y_UNIT_TEST(PrefixReplacementRepairsJoins) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/with-slash/", "/backup");
            AddRule(config, "/without-slash", "/backup/");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/with-slash/table"), "/backup/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/without-slash/table"), "/backup/table");
        }

        Y_UNIT_TEST(RegexMetacharactersAreLiteralInBothPrefixes) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/tenant.[a]+(b)$", R"(/backup.\1$)");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/tenant.[a]+(b)$"), R"(/backup.\1$)");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/tenant.[a]+(b)$/Table"), R"(/backup.\1$/Table)");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/tenant.aab"), "/tenant.aab");
        }

        Y_UNIT_TEST(FirstMatchWinsIncludingIdentityAndResultsAreNotChained) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/identity", "/identity");
            AddRule(config, "/identity", "/wrong");
            AddRule(config, "/first", "/second");
            AddRule(config, "/first/nested", "/more-specific");
            AddRule(config, "/second", "/third");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/identity/Table"), "/identity/Table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/identity//Table/"), "/identity/Table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/first/Table"), "/second/Table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/first/nested/Table"), "/second/nested/Table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/second/Table"), "/third/Table");
        }

        Y_UNIT_TEST(RulesRemainOrderedWithoutAnArtificialCountLimit) {
            NKikimrConfig::TPathRewriteConfig config;
            for (size_t index = 0; index < 128; ++index) {
                AddRule(config, TStringBuilder() << "/alias" << index, TStringBuilder() << "/target" << index << "/end");
            }
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/alias0"), "/target0/end");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/alias127"), "/target127/end");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/alias127/Table"), "/target127/end/Table");
        }

        Y_UNIT_TEST(RootSourceMatchesOnlyAbsolutePaths) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "///", "/backup/");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/"), "/backup");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/mydb"), "/backup/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/mydb/Table"), "/backup/mydb/Table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("mydb/Table"), "mydb/Table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath(""), "");
        }

        Y_UNIT_TEST(RootDestinationJoinsWithoutRepeatedSlashes) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/ru", "/");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru"), "/");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/"), "/");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/mydb"), "/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/mydb/Table"), "/mydb/Table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru//mydb"), "/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/russian"), "/russian");
        }

        Y_UNIT_TEST(RootIdentityStopsBeforeFollowingRules) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/", "/");
            AddRule(config, "/ru", "/wrong");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/"), "/");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/mydb"), "/ru/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("//ru//mydb///"), "/ru/mydb");
        }

        Y_UNIT_TEST(DotComponentsRemainAfterMatchedSlashNormalization) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/ru", "/backup/ru");
            AddRule(config, "/literal//./alias", "/target//../literal");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/./mydb"), "/backup/ru/./mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/ru/../mydb"), "/backup/ru/../mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/literal//./alias/mydb"), "/target/../literal/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/literal/./alias/mydb"), "/literal/./alias/mydb");
        }

        Y_UNIT_TEST(NormalizerOwnsPrefixesAfterConfigurationIsDestroyed) {
            TPathNormalizer normalizer;
            {
                NKikimrConfig::TPathRewriteConfig config;
                AddRule(config, "/ru", "/backup/ru");
                normalizer = TPathNormalizer(config);
                config.ClearRules();
            }
            const TPathNormalizer copy = normalizer;
            normalizer = TPathNormalizer();

            UNIT_ASSERT_VALUES_EQUAL(copy.NormalizePath("/ru/mydb"), "/backup/ru/mydb");
        }

        Y_UNIT_TEST(MissingEmptyAndRelativePrefixesAreRejectedAtConstruction) {
            NKikimrConfig::TPathRewriteConfig missingSource;
            missingSource.AddRules()->SetDst("/target");
            UNIT_ASSERT_EXCEPTION(TPathNormalizer{missingSource}, yexception);

            NKikimrConfig::TPathRewriteConfig missingDestination;
            missingDestination.AddRules()->SetSrc("/ru");
            UNIT_ASSERT_EXCEPTION(TPathNormalizer{missingDestination}, yexception);

            for (const TString& invalid : {TString{}, TString("relative"), TString("./relative"), TString("../relative"), TString(" /ru")}) {
                NKikimrConfig::TPathRewriteConfig invalidSource;
                AddRule(invalidSource, invalid, "/target");
                UNIT_ASSERT_EXCEPTION(TPathNormalizer{invalidSource}, yexception);

                NKikimrConfig::TPathRewriteConfig invalidDestination;
                AddRule(invalidDestination, "/ru", invalid);
                UNIT_ASSERT_EXCEPTION(TPathNormalizer{invalidDestination}, yexception);
            }
        }
    } // Y_UNIT_TEST_SUITE(PathNormalizerCompact)

} // namespace NKikimr::NPathAliasing
