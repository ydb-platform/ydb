#include <ydb/core/path_aliasing/context/path_context.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPathAliasing {
namespace {

void AddRule(NKikimrConfig::TPathRewriteConfig& config, const TString& pattern, const TString& replacement) {
    auto* rule = config.AddRules();
    rule->SetPattern(pattern);
    rule->SetReplacement(replacement);
}

TPathNormalizer PrefixRules() {
    NKikimrConfig::TPathRewriteConfig config;
    AddRule(config, "^/kfront(/|$)", R"(/failover/kfront\1)");
    return TPathNormalizer(config);
}

void AssertResult(const TPathContext& context, const TString& input, const TString& expected, EPathRewriteOutcome outcome) {
    const auto result = context.NormalizePath(input);
    UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
    UNIT_ASSERT_VALUES_EQUAL(result->Path, expected);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(outcome));
}

} // namespace

Y_UNIT_TEST_SUITE(PathAliasingContext) {
    Y_UNIT_TEST(DisabledRulesPreserveDatabasePresenceAndBytes) {
        const TPathContext absent(TPathNormalizer{}, Nothing());
        UNIT_ASSERT(absent.Empty());
        UNIT_ASSERT(!absent.GetLogicalDatabase());
        UNIT_ASSERT(!absent.GetDatabase());
        UNIT_ASSERT(absent.GetError().empty());
        UNIT_ASSERT(absent.GetFingerprint().empty());

        for (const TString& database : {TString(), TString("//Root/./tenant/"), TString("Root/tenant")}) {
            const TPathContext context(TPathNormalizer{}, database);
            UNIT_ASSERT(context.GetLogicalDatabase());
            UNIT_ASSERT(context.GetDatabase());
            UNIT_ASSERT_VALUES_EQUAL(*context.GetLogicalDatabase(), database);
            UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabase(), database);
            UNIT_ASSERT(context.GetError().empty());
            AssertResult(context, database, database, EPathRewriteOutcome::NoMatch);
        }
    }

    Y_UNIT_TEST(ActiveUnmatchedRulesDoNotChangeLegacyInput) {
        const TString rawDatabase("//Root/./tenant/");
        const TPathContext context(PrefixRules(), rawDatabase);
        UNIT_ASSERT(!context.Empty());
        UNIT_ASSERT_VALUES_EQUAL(*context.GetLogicalDatabase(), rawDatabase);
        UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabase(), rawDatabase);
        UNIT_ASSERT(context.GetError().empty());
        for (const TString& input : {TString(), TString("Table"), TString("/Root//./Table/"), TString("/Root/../Table")}) {
            AssertResult(context, input, input, EPathRewriteOutcome::NoMatch);
        }
    }

    Y_UNIT_TEST(EffectiveDatabaseUsesCanonicalCandidateButKeepsLogicalBytes) {
        const TPathContext context(PrefixRules(), TString("//kfront//tenant/"));
        UNIT_ASSERT_VALUES_EQUAL(*context.GetLogicalDatabase(), "//kfront//tenant/");
        UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabase(), "/failover/kfront/tenant");
        UNIT_ASSERT(context.GetError().empty());
        UNIT_ASSERT_VALUES_EQUAL(context.GetFingerprint(), PrefixRules().GetFingerprint());
    }

    Y_UNIT_TEST(AbsentAndEmptyDatabaseAreNeverPassedToRules) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "", "/injected");
        const TPathContext absent(TPathNormalizer{config}, Nothing());
        UNIT_ASSERT(!absent.GetLogicalDatabase());
        UNIT_ASSERT(!absent.GetDatabase());
        UNIT_ASSERT(absent.GetError().empty());
        const TPathContext empty(TPathNormalizer{config}, TString());
        UNIT_ASSERT(empty.GetLogicalDatabase());
        UNIT_ASSERT(empty.GetDatabase());
        UNIT_ASSERT(empty.GetDatabase()->empty());
        UNIT_ASSERT(empty.GetError().empty());
    }

    Y_UNIT_TEST(AllSlashesDatabaseIsRootRatherThanAbsent) {
        // A slash-only DB is present and refers to the root namespace.
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "^/$", "/Root");
        const TPathContext context(TPathNormalizer{config}, TString("///"));
        UNIT_ASSERT_VALUES_EQUAL(*context.GetLogicalDatabase(), "///");
        UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabase(), "/Root");
        UNIT_ASSERT(context.GetError().empty());
    }

    Y_UNIT_TEST(ResourceInputIsCompleteAndNeverJoinedOrCanonicalized) {
        const TPathContext context(PrefixRules(), TString("/kfront/tenant"));
        AssertResult(context, "Table", "Table", EPathRewriteOutcome::NoMatch);
        AssertResult(context, "kfront/Table", "kfront/Table", EPathRewriteOutcome::NoMatch);
        AssertResult(context, "//kfront/Table", "//kfront/Table", EPathRewriteOutcome::NoMatch);
        AssertResult(context, "/kfront/Table", "/failover/kfront/Table", EPathRewriteOutcome::Rewritten);
        AssertResult(context, "/Root/Table", "/Root/Table", EPathRewriteOutcome::NoMatch);
    }

    Y_UNIT_TEST(EmptyResourceOperandIsNeverInventedByEmptyPattern) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "", "/injected");
        const TPathContext context(TPathNormalizer{config}, TString("/Root"));
        AssertResult(context, TString(), TString(), EPathRewriteOutcome::NoMatch);
        AssertResult(context, "/Table", "/injected/Table", EPathRewriteOutcome::Rewritten);
    }

    Y_UNIT_TEST(IdentityWinsWithoutCanonicalizingOrValidatingLegacyInput) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "^/kfront", "/kfront");
        AddRule(config, "^/kfront", "/failover/kfront");
        const TString rawDatabase("/kfront//tenant/");
        const TPathContext context(TPathNormalizer{config}, rawDatabase);
        UNIT_ASSERT_VALUES_EQUAL(*context.GetLogicalDatabase(), rawDatabase);
        UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabase(), rawDatabase);
        UNIT_ASSERT(context.GetError().empty());
        AssertResult(context, "/kfront//./Table/", "/kfront//./Table/", EPathRewriteOutcome::Identity);
    }

    Y_UNIT_TEST(ChangedTargetsHaveCanonicalSlashes) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "^/kfront", "//failover//kfront/");
        const TPathContext context(TPathNormalizer{config}, TString("/kfront/tenant/"));
        UNIT_ASSERT_VALUES_EQUAL(*context.GetLogicalDatabase(), "/kfront/tenant/");
        UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabase(), "/failover/kfront/tenant");
        AssertResult(context, "/kfront//Table/", "/failover/kfront/Table", EPathRewriteOutcome::Rewritten);
    }

    Y_UNIT_TEST(ChangedTargetThatCanonicalizesToInputIsStillRewritten) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "^/kfront", "//kfront");
        const TPathContext context(TPathNormalizer{config}, Nothing());
        AssertResult(context, "/kfront/Table", "/kfront/Table", EPathRewriteOutcome::Rewritten);
    }

    Y_UNIT_TEST(FirstMatchingRuleIsAppliedExactlyOnce) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "^/kfront", "/failover/kfront");
        AddRule(config, "^/failover/kfront", "/decoy/kfront");
        const TPathContext context(TPathNormalizer{config}, TString("/kfront"));
        UNIT_ASSERT_VALUES_EQUAL(*context.GetDatabase(), "/failover/kfront");
        AssertResult(context, "/kfront/Table", "/failover/kfront/Table", EPathRewriteOutcome::Rewritten);
        AssertResult(context, "/kfront/Table", "/failover/kfront/Table", EPathRewriteOutcome::Rewritten);
    }

    Y_UNIT_TEST(ChangedInvalidResourceTargetsAreOrdinaryErrors) {
        TString withNul("/Root");
        withNul.push_back('\0');
        withNul.append("/Table");
        for (const TString& replacement : {
                TString(), TString("relative"), TString("/Root/../Table"),
                TString("/Root/./Table"), TString("/Root/.."), TString("/Root/."), withNul}) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "^/kfront/Table$", replacement);
            const TPathContext context(TPathNormalizer{config}, TString("/kfront"));
            UNIT_ASSERT(context.GetError().empty());
            const auto result = context.NormalizePath("/kfront/Table");
            UNIT_ASSERT_C(result.IsFail(), "An invalid rewritten resource path must fail");
            UNIT_ASSERT(!result.GetErrorMessage().empty());
        }
    }

    Y_UNIT_TEST(InvalidDatabaseTargetIsReportedWithoutChangingLogicalInput) {
        for (const TString& replacement : {TString(), TString("relative"), TString("/Root/../tenant")}) {
            NKikimrConfig::TPathRewriteConfig config;
            AddRule(config, "^/kfront$", replacement);
            const TPathContext context(TPathNormalizer{config}, TString("/kfront"));
            UNIT_ASSERT_VALUES_EQUAL(*context.GetLogicalDatabase(), "/kfront");
            UNIT_ASSERT(!context.GetError().empty());
        }
    }

    Y_UNIT_TEST(RootAndSystemPathsAreValidTargets) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "^/system$", "/Root/.sys/partition_stats");
        AddRule(config, "^/root$", "/");
        AddRule(config, "^/ellipsis$", "/Root/.../Table");
        const TPathContext context(TPathNormalizer{config}, Nothing());
        AssertResult(context, "/system", "/Root/.sys/partition_stats", EPathRewriteOutcome::Rewritten);
        AssertResult(context, "/root", "/", EPathRewriteOutcome::Rewritten);
        AssertResult(context, "/ellipsis", "/Root/.../Table", EPathRewriteOutcome::Rewritten);
    }
}

} // namespace NKikimr::NPathAliasing
