#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPathAliasing {
namespace {

void AddRule(NKikimrConfig::TPathRewriteConfig& config, const TString& pattern, const TString& replacement) {
    auto* rule = config.AddRules();
    rule->SetPattern(pattern);
    rule->SetReplacement(replacement);
}

} // namespace

Y_UNIT_TEST_SUITE(PathNormalizerMatchResult) {
    Y_UNIT_TEST(DisabledAndUnmatchedLeaveOutputUntouched) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "^/alias(/|$)", R"(/Root\1)");
        const TPathNormalizer active(config);
        const TPathNormalizer disabled;
        TString output(65536, 'x');
        const auto* originalData = output.data();
        UNIT_ASSERT(!disabled.TryRewritePath("/Root/Table", output));
        UNIT_ASSERT(output.data() == originalData);
        UNIT_ASSERT_VALUES_EQUAL(output.size(), 65536);
        UNIT_ASSERT(!active.TryRewritePath("/Root/Table", output));
        UNIT_ASSERT(output.data() == originalData);
        UNIT_ASSERT_VALUES_EQUAL(output.size(), 65536);
        UNIT_ASSERT_VALUES_EQUAL(output.front(), 'x');
        UNIT_ASSERT_VALUES_EQUAL(output.back(), 'x');
    }

    Y_UNIT_TEST(IdentityIsAWinningMatchAndStopsLaterRules) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "^/alias(/|$)", R"(/alias\1)");
        AddRule(config, "^/alias(/|$)", R"(/Root\1)");
        const TPathNormalizer normalizer(config);
        TString output("sentinel");
        UNIT_ASSERT(normalizer.TryRewritePath("/alias/Table", output));
        UNIT_ASSERT_VALUES_EQUAL(output, "/alias/Table");
        UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/alias/Table"), output);
    }

    Y_UNIT_TEST(ChangedMatchProducesExactlyOneRewrite) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "^/alias/([^/]+)(/.*)?$", R"(/Root/\1\2)");
        AddRule(config, "^/Root(/|$)", R"(/Decoy\1)");
        const TPathNormalizer normalizer(config);
        TString output("sentinel");
        UNIT_ASSERT(normalizer.TryRewritePath("/alias/tenant/Table", output));
        UNIT_ASSERT_VALUES_EQUAL(output, "/Root/tenant/Table");
        UNIT_ASSERT_VALUES_EQUAL(normalizer.NormalizePath("/alias/tenant/Table"), output);
    }

    Y_UNIT_TEST(EmptyOutputIsAMatchNotAMiss) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "^/alias$", "");
        const TPathNormalizer normalizer(config);
        TString output("sentinel");
        UNIT_ASSERT(normalizer.TryRewritePath("/alias", output));
        UNIT_ASSERT(output.empty());
        output = "untouched";
        UNIT_ASSERT(!normalizer.TryRewritePath("/alias/Table", output));
        UNIT_ASSERT_VALUES_EQUAL(output, "untouched");
    }

    Y_UNIT_TEST(InputMayReferToTheOutputBuffer) {
        NKikimrConfig::TPathRewriteConfig config;
        AddRule(config, "^/alias(/|$)", R"(/Root\1)");
        const TPathNormalizer normalizer(config);
        TString path("/alias/Table");
        UNIT_ASSERT(normalizer.TryRewritePath(TStringBuf(path), path));
        UNIT_ASSERT_VALUES_EQUAL(path, "/Root/Table");
        UNIT_ASSERT(!normalizer.TryRewritePath(TStringBuf(path), path));
        UNIT_ASSERT_VALUES_EQUAL(path, "/Root/Table");
    }

    Y_UNIT_TEST(RewrittenPathValidationAcceptsLocalAbsolutePaths) {
        for (const TStringBuf path : {
                TStringBuf("/"), TStringBuf("/Root"), TStringBuf("/Root/Table"),
                TStringBuf("//Root//Table/"), TStringBuf("/Root/.sys/partition_stats"),
                TStringBuf("/Root/with.dots-and_underscores"), TStringBuf("/Root/.../Table")}) {
            UNIT_ASSERT_C(IsValidRewrittenPath(path), path);
        }
    }

    Y_UNIT_TEST(RewrittenPathValidationRejectsMissingRootAndDotSegments) {
        for (const TStringBuf path : {
                TStringBuf{}, TStringBuf("Root/Table"), TStringBuf("Table"),
                TStringBuf("./Table"), TStringBuf("../Table"),
                TStringBuf("/./Table"), TStringBuf("/../Table"),
                TStringBuf("/Root/."), TStringBuf("/Root/.."),
                TStringBuf("/Root//.//Table"), TStringBuf("/Root//..//Table/")}) {
            UNIT_ASSERT_C(!IsValidRewrittenPath(path), path);
        }
    }

    Y_UNIT_TEST(RewrittenPathValidationChecksEmbeddedNulAndRespectsViewBounds) {
        TString path("/Root");
        path.push_back('\0');
        path.append("/Table");
        UNIT_ASSERT(!IsValidRewrittenPath(path));
        UNIT_ASSERT(IsValidRewrittenPath(TStringBuf(path.data(), 5)));
        const TString bounded("/Root/../Outside");
        UNIT_ASSERT(IsValidRewrittenPath(TStringBuf(bounded.data(), 5)));
        UNIT_ASSERT(!IsValidRewrittenPath(TStringBuf(bounded.data(), bounded.size())));
    }
}

} // namespace NKikimr::NPathAliasing
