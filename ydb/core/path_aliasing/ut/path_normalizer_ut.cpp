#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <array>
#include <future>

namespace NKikimr::NPathAliasing {
    namespace {

        void AddRule(NKikimrConfig::TPathRewriteConfig& config, const TString& pattern, const TString& replacement) {
            auto* rule = config.AddRules();
            rule->SetPattern(pattern);
            rule->SetReplacement(replacement);
        }

        NKikimrConfig::TPathRewriteConfig PrefixConfig() {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, R"(^/kfront(/|$))", R"(/failover/kfront\1)");
            return config;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(PathNormalizer) {
        Y_UNIT_TEST(DefaultAndEmptyConfigurationPreserveInput) {
            const TPathNormalizer defaultNormalizer;
            const TPathNormalizer emptyNormalizer{NKikimrConfig::TPathRewriteConfig{}};
            UNIT_ASSERT(defaultNormalizer.Empty());
            UNIT_ASSERT(emptyNormalizer.Empty());
            UNIT_ASSERT(defaultNormalizer.GetFingerprint().empty());
            UNIT_ASSERT(emptyNormalizer.GetFingerprint().empty());
            for (const TString& path : {TString{}, TString("/"), TString("/kfront/table"), TString("//kfront/./table/")}) {
                UNIT_ASSERT_VALUES_EQUAL(defaultNormalizer.NormalizePath(path), path);
                UNIT_ASSERT_VALUES_EQUAL(emptyNormalizer.NormalizePath(path), path);
            }
        }

        Y_UNIT_TEST(PrefixAndBoundaryPreserveUnmatchedSuffix) {
            const TPathNormalizer normalizer(PrefixConfig());
            UNIT_ASSERT(!normalizer.Empty());
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront"), "/failover/kfront");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/"), "/failover/kfront/");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/anything/nested"), "/failover/kfront/anything/nested");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfrontend/table"), "/kfrontend/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/other/kfront/table"), "/other/kfront/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/Kfront/table"), "/Kfront/table");
        }

        Y_UNIT_TEST(NoMatchDoesNotLexicallyNormalizePath) {
            const TPathNormalizer normalizer(PrefixConfig());
            const TString path = "/other//./table/";
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath(path), path);
        }

        Y_UNIT_TEST(SeveralCapturesAndOptionalCapture) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, R"(^/legacy/([^/]+)/tables(/.*)?$)", R"(/archive/\1\2)");
            const TPathNormalizer normalizer(config);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/legacy/user/tables"), "/archive/user");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/legacy/user/tables/nested/table"), "/archive/user/nested/table");
        }

        Y_UNIT_TEST(WholeMatchCapture) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "^/kfront", R"(/failover\0)");
            const TPathNormalizer normalizer(config);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), "/failover/kfront/table");
        }

        Y_UNIT_TEST(HighestReplacementCaptureReference) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "^/(a)(b)(c)(d)(e)(f)(g)(h)(i)$", R"(/\9\1)");
            const TPathNormalizer normalizer(config);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/abcdefghi"), "/ia");
        }

        Y_UNIT_TEST(InputViewIsLengthBounded) {
            const TPathNormalizer normalizer(PrefixConfig());
            const TString storage = "/kfront/table/ignored";
            const TStringBuf path(storage.data(), TStringBuf("/kfront/table").size());
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath(path), "/failover/kfront/table");
        }

        Y_UNIT_TEST(UnanchoredRuleReplacesOnlyFirstOccurrence) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "kfront", "target");
            const TPathNormalizer normalizer(config);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/root/kfront/kfront/table"), "/root/target/kfront/table");
        }

        Y_UNIT_TEST(FirstMatchingRuleWins) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, R"(^/kfront/special(/|$))", R"(/dedicated\1)");
            AddRule(config, R"(^/kfront(/|$))", R"(/failover/kfront\1)");
            const TPathNormalizer normalizer(config);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/special/table"), "/dedicated/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/ordinary"), "/failover/kfront/ordinary");
        }

        Y_UNIT_TEST(IdentityMatchPreventsLaterRules) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "^/kfront", "/kfront");
            AddRule(config, "^/kfront", "/failover/kfront");
            const TPathNormalizer normalizer(config);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), "/kfront/table");
        }

        Y_UNIT_TEST(RewriteIsNotAppliedRecursively) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "^/first/", "/second/");
            AddRule(config, "^/second/", "/third/");
            const TPathNormalizer normalizer(config);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/first/table"), "/second/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/second/table"), "/third/table");
        }

        Y_UNIT_TEST(CyclicRulesStillApplyOnce) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "^/first/", "/second/");
            AddRule(config, "^/second/", "/first/");
            const TPathNormalizer normalizer(config);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/first/table"), "/second/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/second/table"), "/first/table");
        }

        Y_UNIT_TEST(ExplicitEmptyPatternAndReplacementAreAllowed) {
            NKikimrConfig::TPathRewriteConfig insertConfig;
            AddRule(insertConfig, "", "/failover");
            const TPathNormalizer insertNormalizer(insertConfig);
            UNIT_ASSERT_VALUES_EQUAL(insertNormalizer.NormalizePath("/kfront/table"), "/failover/kfront/table");
            UNIT_ASSERT_VALUES_EQUAL(insertNormalizer.NormalizePath(TStringBuf{}), "/failover");

            NKikimrConfig::TPathRewriteConfig removeConfig;
            AddRule(removeConfig, "^/kfront$", "");
            const TPathNormalizer removeNormalizer(removeConfig);
            UNIT_ASSERT_VALUES_EQUAL(removeNormalizer.NormalizePath("/kfront"), "");
        }

        Y_UNIT_TEST(PathValidationRemainsWithCaller) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "^/kfront", "relative");
            const TPathNormalizer normalizer(config);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), "relative/table");
        }

        Y_UNIT_TEST(EscapedBackslashInReplacement) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "^/kfront", R"(/failover\\store)");
            const TPathNormalizer normalizer(config);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), R"(/failover\store/table)");
        }

        Y_UNIT_TEST(RejectsInvalidRegularExpression) {
            auto config = PrefixConfig();
            AddRule(config, "(", "/target");
            UNIT_ASSERT_EXCEPTION_CONTAINS(TPathNormalizer{config}, yexception, "rule");
        }

        Y_UNIT_TEST(RejectsUnavailableCaptureReference) {
            auto config = PrefixConfig();
            AddRule(config, "^/other", R"(/target\1)");
            UNIT_ASSERT_EXCEPTION_CONTAINS(TPathNormalizer{config}, yexception, "rule");
        }

        Y_UNIT_TEST(RejectsMalformedReplacementEscape) {
            auto config = PrefixConfig();
            AddRule(config, "^/other", R"(/target\q)");
            UNIT_ASSERT_EXCEPTION_CONTAINS(TPathNormalizer{config}, yexception, "rule");
        }

        Y_UNIT_TEST(RejectsMissingPattern) {
            auto config = PrefixConfig();
            config.AddRules()->SetReplacement("/target");
            UNIT_ASSERT_EXCEPTION_CONTAINS(TPathNormalizer{config}, yexception, "rule");
        }

        Y_UNIT_TEST(RejectsMissingReplacement) {
            auto config = PrefixConfig();
            config.AddRules()->SetPattern("^/other");
            UNIT_ASSERT_EXCEPTION_CONTAINS(TPathNormalizer{config}, yexception, "rule");
        }

        Y_UNIT_TEST(OwnsImmutableConfiguration) {
            auto config = PrefixConfig();
            const TPathNormalizer normalizer(config);
            const TString fingerprint = normalizer.GetFingerprint();
            config.MutableRules(0)->SetReplacement("/changed");
            config.ClearRules();
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/kfront/table"), "/failover/kfront/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.GetFingerprint(), fingerprint);
        }

        Y_UNIT_TEST(FingerprintIsStableAndSensitiveToConfiguration) {
            auto config = PrefixConfig();
            AddRule(config, "^/second", "/third");
            const TString fingerprint = TPathNormalizer(config).GetFingerprint();
            UNIT_ASSERT(!fingerprint.empty());
            UNIT_ASSERT_VALUES_EQUAL(TPathNormalizer(config).GetFingerprint(), fingerprint);

            auto changed = config;
            changed.MutableRules(0)->SetPattern("^/other");
            changed.MutableRules(0)->SetReplacement("/failover");
            UNIT_ASSERT(TPathNormalizer(changed).GetFingerprint() != fingerprint);

            changed = config;
            changed.MutableRules(0)->SetReplacement(R"(/other\1)");
            UNIT_ASSERT(TPathNormalizer(changed).GetFingerprint() != fingerprint);

            changed = config;
            changed.MutableRules()->SwapElements(0, 1);
            UNIT_ASSERT(TPathNormalizer(changed).GetFingerprint() != fingerprint);
        }

        Y_UNIT_TEST(FingerprintSeparatesPatternAndReplacement) {
            NKikimrConfig::TPathRewriteConfig first;
            AddRule(first, "a", "bc");
            NKikimrConfig::TPathRewriteConfig second;
            AddRule(second, "ab", "c");
            UNIT_ASSERT(TPathNormalizer(first).GetFingerprint() != TPathNormalizer(second).GetFingerprint());
        }

        Y_UNIT_TEST(FingerprintIncludesBytesAfterEmbeddedNull) {
            auto first = PrefixConfig();
            first.MutableRules(0)->SetReplacement(TString("/target\0suffix", 14));
            auto second = first;
            second.MutableRules(0)->SetReplacement("/target");
            UNIT_ASSERT(TPathNormalizer(first).GetFingerprint() != TPathNormalizer(second).GetFingerprint());
        }

        Y_UNIT_TEST(TenThousandRulesSupportFirstLastAndNoMatch) {
            NKikimrConfig::TPathRewriteConfig config;
            for (size_t index = 0; index < 10000; ++index) {
                AddRule(config, TStringBuilder() << "^/alias" << index << "/", TStringBuilder() << "/target" << index << "/");
            }
            const TPathNormalizer normalizer(config);
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/alias0/table"), "/target0/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/alias9999/table"), "/target9999/table");
            UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/unmatched/table"), "/unmatched/table");
        }

        Y_UNIT_TEST(LongUnmatchedSuffixIsPreserved) {
            const TPathNormalizer normalizer(PrefixConfig());
            const TString suffix(64 * 1024, 'x');
            const TString actual = normalizer.NormalizePath(TString("/kfront/") + suffix);
            const TString expected = TString("/failover/kfront/") + suffix;
            UNIT_ASSERT_C(actual == expected, "Rewriting must preserve the complete 64 KiB suffix");
        }

        Y_UNIT_TEST(ConcurrentCallsUseSameImmutableRules) {
            const TPathNormalizer normalizer(PrefixConfig());
            const TString fingerprint = normalizer.GetFingerprint();
            std::array<std::future<bool>, 4> workers;
            for (auto& worker : workers) {
                worker = std::async(std::launch::async, [&normalizer, &fingerprint] {
                    for (size_t iteration = 0; iteration < 1000; ++iteration) {
                        if (normalizer.NormalizePath("/kfront/table") != "/failover/kfront/table" || normalizer.NormalizePath("/unmatched/table") != "/unmatched/table" || normalizer.GetFingerprint() != fingerprint) {
                            return false;
                        }
                    }
                    return true;
                });
            }
            for (auto& worker : workers) {
                UNIT_ASSERT(worker.get());
            }
        }
    } // Y_UNIT_TEST_SUITE(PathNormalizer)

} // namespace NKikimr::NPathAliasing
