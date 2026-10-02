#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <utility>

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
                     TString("//Other///"),
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
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("//ru/mydb"), "/backup/ru/mydb");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("///ru/mydb"), "/backup/ru/mydb");
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

        Y_UNIT_TEST(OverlappingSourcesPreserveFirstMatchPrecedence) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/first", "/target");
            AddRule(config, "/first/nested", "/more-specific");
            const TPathNormalizer normalizer(config);

            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/first/Table"), "/target/Table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/first/nested/Table"), "/target/nested/Table");
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

        Y_UNIT_TEST(OverlappingSourceAndDestinationAreRejected) {
            for (const auto& [src, dst] : {
                     std::pair{TString("/alias"), TString("/alias")},
                     std::pair{TString("/alias"), TString("/alias/nested")},
                     std::pair{TString("/alias/nested"), TString("/alias")},
                     std::pair{TString("///"), TString("/backup/")},
                     std::pair{TString("/alias"), TString("/")},
                     std::pair{TString("/"), TString("/")},
                 }) {
                NKikimrConfig::TPathRewriteConfig config;
                AddRule(config, src, dst);
                UNIT_ASSERT_EXCEPTION(TPathNormalizer{config}, yexception);
            }
        }

        Y_UNIT_TEST(ChainsThroughDestinationOrSuffixAreRejectedInEitherOrder) {
            for (const TString& src : {TString("/local"), TString("/local/nested"), TString("//local//nested/")}) {
                for (const TString& dst : {TString("/local"), TString("/local/nested"), TString("//local//nested/")}) {
                    for (const bool reverse : {false, true}) {
                        NKikimrConfig::TPathRewriteConfig config;
                        if (reverse) {
                            AddRule(config, src, "/other");
                            AddRule(config, "/alias", dst);
                        } else {
                            AddRule(config, "/alias", dst);
                            AddRule(config, src, "/other");
                        }
                        UNIT_ASSERT_EXCEPTION(TPathNormalizer{config}, yexception);
                    }
                }
            }
        }

        Y_UNIT_TEST(CyclesAreRejected) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/first", "/second");
            AddRule(config, "/second", "/third");
            AddRule(config, "/third", "/first");
            UNIT_ASSERT_EXCEPTION(TPathNormalizer{config}, yexception);
        }

        Y_UNIT_TEST(AcceptedRulesAreIdempotent) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "/alias", "/local/");
            AddRule(config, "/alias/nested", "/target");
            AddRule(config, "/locality", "/other");
            const TPathNormalizer normalizer(config);

            for (const TString& path : {
                     TString("/alias"), TString("/alias/table"), TString("//alias//nested/table/"),
                     TString("/locality/table"), TString("/local/table"), TString("/other//table/"),
                     TString("/alias/./table"), TString("/alias/../table"), TString("relative/path"), TString("/"),
                 }) {
                const auto normalized = normalizer.NormalizePath(path);
                UNIT_ASSERT_VALUES_EQUAL_C(normalizer.NormalizePath(normalized), normalized, path);
            }
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/locality/table"), "/other/table");
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
