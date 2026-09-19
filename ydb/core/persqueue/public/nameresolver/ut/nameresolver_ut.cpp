#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <expected>

using namespace NKikimr;
using namespace NKikimr::NPQ::NNameResolver;

namespace {

constexpr TStringBuf LbRoot = "/Root/LbCommunal";
constexpr TStringBuf Database = "/Root/Db";

class TNameResolverFixture : public NUnitTest::TBaseFixture {
public:
    void SetUp(NUnitTest::TTestContext&) override {
        auto& pqConfig = ActorSystemStub.AppData.PQConfig;
        pqConfig.SetTopicsAreFirstClassCitizen(true);
        pqConfig.SetRoot("/Root/PQ");
        pqConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(TString{LbRoot});
    }

    void SetFcc(bool value) {
        ActorSystemStub.AppData.PQConfig.SetTopicsAreFirstClassCitizen(value);
    }

    void SetLbRoot(const TString& root) {
        ActorSystemStub.AppData.PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(root);
    }

    static TString Ok(std::expected<TResolvedName, TString> result) {
        if (!result.has_value()) {
            UNIT_FAIL(result.error());
        }
        return result->Path;
    }

    static TResolvedName OkFull(std::expected<TResolvedName, TString> result) {
        if (!result.has_value()) {
            UNIT_FAIL(result.error());
        }
        return *result;
    }

    static void ExpectError(std::expected<TResolvedName, TString> result, TStringBuf reason) {
        if (result.has_value()) {
            UNIT_FAIL(TStringBuilder() << "got path: " << result->Path);
        }
        UNIT_ASSERT_VALUES_EQUAL(result.error(), reason);
    }

    TActorSystemStub ActorSystemStub;
};

} // namespace

Y_UNIT_TEST_SUITE(TNameResolverTest) {

Y_UNIT_TEST_F(FederationRootDbConvertsLegacyRt3, TNameResolverFixture) {
    SetFcc(false);
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "rt3.dc1--account--topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "rt3.dc2--account--topic", "dc1")),
        "/Root/LbCommunal/account/topic-mirrored-from-dc2");
}

Y_UNIT_TEST_F(FederationRootDbAccountTopic, TNameResolverFixture) {
    SetFcc(false);
    {
        const auto resolved = OkFull(ResolveName("", "account/topic", "dc1"));
        UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/LbCommunal/account/topic");
        UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/LbCommunal/account");
    }
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "account/topic", "dc1", "dc2")),
        "/Root/LbCommunal/account/topic-mirrored-from-dc2");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "account/dir/topic", "dc1", "dc2")),
        "/Root/LbCommunal/account/dir/topic-mirrored-from-dc2");
}

Y_UNIT_TEST_F(FederationAbsolutePathUnderLbRootNotDoubled, TNameResolverFixture) {
    // Alter/describe with absolute path under LbRoot and database=/Root (CommitOffsetBadOffsets).
    SetFcc(false);
    const auto resolved = OkFull(ResolveName("/Root", "/Root/LbCommunal/account/topic2", "dc1"));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/LbCommunal/account/topic2");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/LbCommunal/account");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "//Root//LbCommunal//account//topic2", "dc1")),
        "/Root/LbCommunal/account/topic2");
}

Y_UNIT_TEST_F(FederationNavigateDatabaseWithoutLbRoot, TNameResolverFixture) {
    // Describe account1/topic under /Root with empty LbRoot → tenant NavigateDatabase.
    SetFcc(false);
    SetLbRoot("");
    const auto resolved = OkFull(ResolveName("/Root", "account1/topic", "dc1"));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/account1/topic");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/account1");
}

Y_UNIT_TEST_F(FccAbsolutePathNotStrippedByPqRoot, TNameResolverFixture) {
    // PQ Root == /Root must not rewrite /Root/topic → /topic in FCC.
    ActorSystemStub.AppData.PQConfig.SetRoot("/Root");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "/Root/topic1")),
        "/Root/topic1");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "/Root/table1/feed")),
        "/Root/table1/feed");
}

Y_UNIT_TEST_F(FccKeepsPqPathWithLegacyLookingLeaf, TNameResolverFixture) {
    // Path with '/' is modern even if the leaf looks like rt3/-- (TopicService UpdateOffsets).
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "/Root/PQ/rt3.dc1--topic1")),
        "/Root/PQ/rt3.dc1--topic1");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "PQ/rt3.dc1--topic1")),
        "/Root/PQ/rt3.dc1--topic1");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "//Root//PQ//rt3.dc1--topic1")),
        "/Root/PQ/rt3.dc1--topic1");
}

Y_UNIT_TEST_F(FederationUserDbAbsolutePathWithPqRootEqDomain, TNameResolverFixture) {
    // Federation + PQ Root=/Root + tenant DB: keep /Root/test_db/topic1 (do not double DB).
    SetFcc(false);
    SetLbRoot("");
    ActorSystemStub.AppData.PQConfig.SetRoot("/Root");
    const auto resolved = OkFull(ResolveName("/Root/test_db", "/Root/test_db/topic1", "dc1"));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/test_db/topic1");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/test_db");
}

Y_UNIT_TEST_F(FccKeepsLiteralDashDashName, TNameResolverFixture) {
    // Go SDK TestSchemeList uses t.Name(), which contains '--'. That is a legal FCC leaf.
    const auto resolved = OkFull(ResolveName(TString{Database}, "TestSchemeList--test-topic-1"));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/Db/TestSchemeList--test-topic-1");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/Db");
}

Y_UNIT_TEST_F(FccLegacyLookingNameKeepsRequestDatabase, TNameResolverFixture) {
    // FCC: SchemeCache DatabaseName stays the request DB; the name is not remapped to LbRoot.
    const auto resolved = OkFull(ResolveName(TString{Database}, "rt3.dc1--account--topic", "dc1"));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/Db/rt3.dc1--account--topic");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/Db");
}

Y_UNIT_TEST_F(FederationRootDbViaDatabasePrefix, TNameResolverFixture) {
    SetFcc(false);
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "account/topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "/Root/account/topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "/Root/rt3.dc1--account--topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "Root/account--topic", "dc1")),
        "/Root/LbCommunal/account/topic");
}

Y_UNIT_TEST_F(FederationUserDatabaseRelativePath, TNameResolverFixture) {
    SetFcc(false);
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root/LbCommunal/account", "dir/topic", "dc1")),
        "/Root/LbCommunal/account/dir/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root/LbCommunal/account", "dir/topic", "dc1", "dc2")),
        "/Root/LbCommunal/account/dir/topic-mirrored-from-dc2");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root/LbCommunal/account", "topic", "dc1")),
        "/Root/LbCommunal/account/topic");
}

Y_UNIT_TEST_F(FederationUserDatabaseExplicitLegacy, TNameResolverFixture) {
    SetFcc(false);
    // Explicit legacy inside a user account DB still resolves via LbRoot.
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root/LbCommunal/account", "rt3.dc1--other--topic", "dc1")),
        "/Root/LbCommunal/other/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root/LbCommunal/account", "other--topic", "dc1")),
        "/Root/LbCommunal/other/topic");
}

Y_UNIT_TEST_F(FederationMirroredFromEmptyClusterRejected, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("/Root/LbCommunal/account", "dir/topic-mirrored-from-", "dc1"),
        "Malformed mirrored topic path - expected to end with valid cluster name.");
}

Y_UNIT_TEST_F(ModernPathNormalizedWithDatabase, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account/topic", "dc1", "")),
        "/Root/Db/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "/account/topic", "dc1", "")),
        "/Root/Db/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "/Root/Db/account/topic", "dc1", "")),
        "/Root/Db/account/topic");
}

Y_UNIT_TEST_F(FccKeepsLiteralRt3, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc1--account--topic", "dc1", "dc1")),
        "/Root/Db/rt3.dc1--account--topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc1--account--topic", "dc1", "")),
        "/Root/Db/rt3.dc1--account--topic");
}

Y_UNIT_TEST_F(FccKeepsLiteralRemoteRt3, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc2--account--topic", "dc1", "dc2")),
        "/Root/Db/rt3.dc2--account--topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc2--account--topic", "dc1", "")),
        "/Root/Db/rt3.dc2--account--topic");
}

Y_UNIT_TEST_F(FccKeepsLiteralRt3WithAt, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc1--account@dir--topic", "dc1", "")),
        "/Root/Db/rt3.dc1--account@dir--topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc2--account@dir--topic", "dc1", "")),
        "/Root/Db/rt3.dc2--account@dir--topic");
}

Y_UNIT_TEST_F(FccKeepsLiteralShortDashDash, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account--topic", "dc1", "")),
        "/Root/Db/account--topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account@dir--topic", "dc1", "")),
        "/Root/Db/account@dir--topic");
}

Y_UNIT_TEST_F(FccIgnoresDcForLegacyLookingName, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account--topic", "dc1", "dc2")),
        "/Root/Db/account--topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account@dir--topic", "dc1", "dc2")),
        "/Root/Db/account@dir--topic");
}

Y_UNIT_TEST_F(FccBareTopicJoinsDatabase, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "topic", "dc1", "")),
        "/Root/Db/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "topic", "dc1", "dc2")),
        "/Root/Db/topic");
}

Y_UNIT_TEST_F(FccBareInUserDatabaseUnderLbRoot, TNameResolverFixture) {
    // CREATE TOPIC in a dedicated account DB under LbRoot must not remap to LbRoot/topic.
    SetLbRoot("/Root");
    const auto resolved = OkFull(ResolveName("/Root/account1", "topic"));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/account1/topic");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/account1");
}

Y_UNIT_TEST_F(FccKeepsLiteralAtName, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account@dir", "dc1", "")),
        "/Root/Db/account@dir");
}

Y_UNIT_TEST_F(FccIgnoresRt3DcMismatch, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc1--account--topic", "dc1", "dc2")),
        "/Root/Db/rt3.dc1--account--topic");
}

Y_UNIT_TEST_F(FccKeepsLiteralNamesWithoutDc, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account--topic", "", "")),
        "/Root/Db/account--topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "topic", "", "")),
        "/Root/Db/topic");
}

Y_UNIT_TEST_F(FccKeepsMalformedRt3AsLiteral, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.bad", "dc1", "")),
        "/Root/Db/rt3.bad");
}

Y_UNIT_TEST_F(FccKeepsRt3EmptyDcAsLiteral, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.--account--topic", "dc1", "")),
        "/Root/Db/rt3.--account--topic");
}

Y_UNIT_TEST_F(FccKeepsRt3EmptyShortAsLiteral, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc1--", "dc1", "")),
        "/Root/Db/rt3.dc1--");
}

Y_UNIT_TEST_F(FccKeepsTrailingDashDashAsLiteral, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account--", "dc1", "")),
        "/Root/Db/account--");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "--topic", "dc1", "")),
        "/Root/Db/--topic");
}

Y_UNIT_TEST_F(FccEmptyNameJoinsDatabase, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "", "dc1", "")),
        "/Root/Db");
}

Y_UNIT_TEST_F(FccNormalizesDoubleSlashInLegacyLookingName, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account--a//b", "dc1", "")),
        "/Root/Db/account--a/b");
}

Y_UNIT_TEST_F(FccStripsLeadingSlashOnLiteralRt3, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "/rt3.dc1--account--topic", "dc1", "")),
        "/Root/Db/rt3.dc1--account--topic");
}

Y_UNIT_TEST_F(FccDoesNotMirrorLegacyLookingName, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc2--account--topic", "", "dc2")),
        "/Root/Db/rt3.dc2--account--topic");
}

Y_UNIT_TEST_F(FccIgnoresEmptyLbRootForLiteralName, TNameResolverFixture) {
    SetLbRoot("");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc1--account--topic", "dc1", "")),
        "/Root/Db/rt3.dc1--account--topic");
}

Y_UNIT_TEST_F(FccEmptyDatabaseKeepsRelativeLiteralName, TNameResolverFixture) {
    SetLbRoot("");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "rt3.dc1--account--topic", "dc1", "")),
        "/rt3.dc1--account--topic");
}

Y_UNIT_TEST_F(FccDefaultDcKeepsLiteralRt3, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc2--account--topic", "dc1")),
        "/Root/Db/rt3.dc2--account--topic");
}

Y_UNIT_TEST_F(FccDefaultDcKeepsLiteralShortName, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account--topic", "dc1")),
        "/Root/Db/account--topic");
}

Y_UNIT_TEST_F(IsPathPrefixExactMatch, TNameResolverFixture) {
    SetFcc(false);
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root/PQ", "account/topic", "dc1")),
        "/Root/LbCommunal/account/topic");
}

Y_UNIT_TEST_F(TopicPathStartsWithPqRoot, TNameResolverFixture) {
    SetFcc(false);
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "/Root/PQ/rt3.dc1--account--topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "/Root/PQ/account/topic", "dc1")),
        "/Root/LbCommunal/account/topic");
}

Y_UNIT_TEST_F(AbsolutePathWithDoubleSlashCanonized, TNameResolverFixture) {
    SetFcc(false);
    // JoinPath({"/Root/PQ/", name}) produces '//'; accept and resolve like SplitPath.
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "/Root/PQ//rt3.dc1--account--topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "/Root/PQ//rt3.dc1--topic-x", "dc1")),
        "/Root/LbCommunal/topic-x");
}

Y_UNIT_TEST_F(AbsolutePqLegacyKeptWhenNoLbRoot, TNameResolverFixture) {
    SetFcc(false);
    SetLbRoot("");
    // Classic discovery PrimaryPath: PQ root + full legacy leaf (describes_ut topics).
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "/Root/PQ/rt3.dc1--topic-x")),
        "/Root/PQ/rt3.dc1--topic-x");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "/Root/PQ//rt3.dc1--topic-x")),
        "/Root/PQ/rt3.dc1--topic-x");
}

Y_UNIT_TEST_F(FederationModernPathBadName, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("/Root/LbCommunal/account", "dir//topic", "dc1"),
        "Bad topic name for federation: dir//topic");
}

Y_UNIT_TEST_F(FederationExplicitLegacyWithEmptyPqRoot, TNameResolverFixture) {
    SetFcc(false);
    ActorSystemStub.AppData.PQConfig.SetRoot("");
    // /Root prefixes LbRoot → root-like DB even when PQ Root is empty.
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "rt3.dc1--account--topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "account--topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "/Root/rt3.dc1--account--topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "/Root/account/topic", "", "")),
        "/Root/LbCommunal/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "account/topic", "", "")),
        "/Root/LbCommunal/account/topic");
    // Bare names stay under the request database.
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root", "topic1", "", "")),
        "/Root/topic1");
}

Y_UNIT_TEST_F(TryFederationAccountTarget, TNameResolverFixture) {
    SetFcc(false);
    {
        const auto target = TryFederationAccountTarget(
            "/Root/Federation/account/topic", "/Root/Federation");
        UNIT_ASSERT(target.has_value());
        UNIT_ASSERT_VALUES_EQUAL(target->Path, "/Root/Federation/account/topic");
        UNIT_ASSERT_VALUES_EQUAL(target->AccountDatabase, "/Root/Federation/account");
    }
    {
        const auto target = TryFederationAccountTarget(
            "/Root/Federation/account/table/feed", "/Root/Federation");
        UNIT_ASSERT(target.has_value());
        UNIT_ASSERT_VALUES_EQUAL(target->AccountDatabase, "/Root/Federation/account");
    }
    {
        // Trailing slash on federation root and path without leading slash.
        const auto target = TryFederationAccountTarget(
            "Root/Federation/account/topic", "/Root/Federation/");
        UNIT_ASSERT(target.has_value());
        UNIT_ASSERT_VALUES_EQUAL(target->Path, "/Root/Federation/account/topic");
        UNIT_ASSERT_VALUES_EQUAL(target->AccountDatabase, "/Root/Federation/account");
    }
    UNIT_ASSERT(!TryFederationAccountTarget("/Root/Federation/topic1", "/Root/Federation"));
    UNIT_ASSERT(!TryFederationAccountTarget("/Root/account/topic", "/Root/Federation"));
    UNIT_ASSERT(!TryFederationAccountTarget("/Root/Federation/account/topic", ""));
    UNIT_ASSERT(!TryFederationAccountTarget("/Root/Federation", "/Root/Federation"));
}

Y_UNIT_TEST_F(FederationModernPathWithoutDc, TNameResolverFixture) {
    SetFcc(false);
    // Empty dc/localDc: treat as local — no -mirrored-from- suffix.
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root/LbCommunal/account", "dir/topic", "", "")),
        "/Root/LbCommunal/account/dir/topic");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "account/topic", "", "")),
        "/Root/LbCommunal/account/topic");
}

Y_UNIT_TEST_F(FederationPqRootWithTrailingSlash, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("", "/Root/PQ/", "dc1"),
        "Invalid topic path or trailing '/'.");
}

Y_UNIT_TEST_F(FederationOnlySlash, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("", "/", "dc1"),
        "Invalid topic path (only account provided?).");
}

Y_UNIT_TEST_F(FederationEmptyTopicAfterExactPqRoot, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("", "/Root/PQ", "dc1"),
        "Bad topic name (only account provided?).");
}

Y_UNIT_TEST_F(SkipPathPrefixRestoresWhenNoSlashAfterPrefix, TNameResolverFixture) {
    SetFcc(false);
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root/PQ", "Root/PQfoo", "dc1")),
        "/Root/LbCommunal/Root/PQfoo");
}

Y_UNIT_TEST_F(FederationTrailingSlash, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("", "account/", "dc1"),
        "Invalid topic path or trailing '/'.");
}

Y_UNIT_TEST_F(FederationEmptyName, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("", "", "dc1"),
        "Bad topic name for federation: ");
}

Y_UNIT_TEST_F(FederationDoubleSlash, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("", "account--a//b", "dc1"),
        "Bad topic name for federation: account--a//b");
}

Y_UNIT_TEST_F(FederationLegacyParseFailure, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("", "rt3.bad", "dc1"),
        "Malformed legacy style topic name: contains 'rt3.', but no '--'.");
}

Y_UNIT_TEST_F(FederationAccountTopicWithoutLbRoot, TNameResolverFixture) {
    SetFcc(false);
    SetLbRoot("");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "account/topic", "dc1")),
        "account/topic");
}

Y_UNIT_TEST_F(FederationAlreadyMirroredWithoutDc, TNameResolverFixture) {
    SetFcc(false);
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root/db1", "dir/topic-mirrored-from-dc2", "", "")),
        "/Root/db1/dir/topic-mirrored-from-dc2");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root/db1", "dir/topic-mirrored-from-dc2", "dc1", "")),
        "/Root/db1/dir/topic-mirrored-from-dc2");
}

Y_UNIT_TEST_F(FederationAlreadyMirroredDoesNotDoubleSuffix, TNameResolverFixture) {
    SetFcc(false);
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("/Root/db1", "dir/topic-mirrored-from-dc2", "dc1", "dc2")),
        "/Root/db1/dir/topic-mirrored-from-dc2");
}

Y_UNIT_TEST_F(FederationBadMirroredFromRejected, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("/Root/db1", "dir/mirrored-from-dc2", "dc1", ""),
        "Federation topics cannot contain 'mirrored-from' in name unless this is a mirrored topic.");
    ExpectError(
        ResolveName("/Root/db1", "dir/topic-mirrored-from-", "dc1", ""),
        "Malformed mirrored topic path - expected to end with valid cluster name.");
}

Y_UNIT_TEST_F(FederationMirroredFromLocalDcRejected, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("/Root/db1", "dir/topic-mirrored-from-dc1", "dc1", ""),
        "Local topic cannot contain '-mirrored-from' part.");
}

Y_UNIT_TEST_F(FccModernWithEmptyDatabase, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "account/topic")),
        "/account/topic");
}

Y_UNIT_TEST_F(FccModernUncleanWithEmptyDatabase, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "account//topic")),
        "/account/topic");
}

Y_UNIT_TEST_F(FccModernUncleanRelativeNormalized, TNameResolverFixture) {
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account//topic")),
        "/Root/Db/account/topic");
}

Y_UNIT_TEST_F(FccEmptyNameInUserDatabaseUnderLbRoot, TNameResolverFixture) {
    // Empty legacy bare name under account DB → database path itself.
    SetLbRoot("/Root");
    const auto resolved = OkFull(ResolveName("/Root/account1", ""));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/account1");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/account1");
}

Y_UNIT_TEST_F(FccEmptyNameUncleanDatabaseUnderLbRoot, TNameResolverFixture) {
    // Unclean request database + empty relative → '/' + databaseNorm.
    SetLbRoot("/Root");
    const auto resolved = OkFull(ResolveName("/Root/account1/", ""));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/account1");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/account1");
}

Y_UNIT_TEST_F(FccNormalizesDoubleSlashWithoutLegacyConvert, TNameResolverFixture) {
    SetLbRoot("");
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "account--a//b", "dc1", "")),
        "/Root/Db/account--a/b");
}

Y_UNIT_TEST_F(AbsoluteDatabaseUncleanRequestDatabase, TNameResolverFixture) {
    // Trailing slash on request database still yields a clean NavigateDatabase.
    const auto resolved = OkFull(ResolveName("/Root/Db/", "account/topic"));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/Db/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/Db");
}

Y_UNIT_TEST_F(FederationLbRootExactIsRootLike, TNameResolverFixture) {
    SetFcc(false);
    // database == LbRoot → root-like (not a user account DB).
    const auto resolved = OkFull(ResolveName("/Root/LbCommunal", "account/topic", "dc1"));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/LbCommunal/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/LbCommunal/account");
}

Y_UNIT_TEST_F(FederationRootAlreadyMirrored, TNameResolverFixture) {
    SetFcc(false);
    const auto resolved = OkFull(
        ResolveName("/Root", "account/topic-mirrored-from-dc2", "dc1"));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/LbCommunal/account/topic-mirrored-from-dc2");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/LbCommunal/account");
}

Y_UNIT_TEST_F(FederationUserDatabaseLegacyParseFailure, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("/Root/LbCommunal/account", "rt3.bad", "dc1"),
        "Malformed legacy style topic name: contains 'rt3.', but no '--'.");
}

Y_UNIT_TEST_F(FederationBareWithEmptyDatabase, TNameResolverFixture) {
    SetFcc(false);
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName("", "topic", "dc1")),
        "/Root/LbCommunal/topic");
}

Y_UNIT_TEST_F(FederationRootMirroredMalformedRejected, TNameResolverFixture) {
    SetFcc(false);
    ExpectError(
        ResolveName("/Root", "account/mirrored-from-dc2", "dc1"),
        "Federation topics cannot contain 'mirrored-from' in name unless this is a mirrored topic.");
}

Y_UNIT_TEST_F(NavigateDatabaseEmptyWhenNoAccountTopicShape, TNameResolverFixture) {
    SetFcc(false);
    // Path under LbRoot without account/topic shape → keep request database.
    const auto resolved = OkFull(ResolveName(TString{Database}, "rt3.dc1--", "dc1"));
    UNIT_ASSERT_VALUES_EQUAL(resolved.Path, "/Root/LbCommunal");
    UNIT_ASSERT_VALUES_EQUAL(resolved.NavigateDatabase, "/Root/Db");
}

Y_UNIT_TEST_F(TryFederationAccountTargetCanonizesUncleanPath, TNameResolverFixture) {
    const auto target = TryFederationAccountTarget(
        "/Root/Federation/account//topic", "/Root/Federation");
    UNIT_ASSERT(target.has_value());
    UNIT_ASSERT_VALUES_EQUAL(target->Path, "/Root/Federation/account/topic");
    UNIT_ASSERT_VALUES_EQUAL(target->AccountDatabase, "/Root/Federation/account");
}

Y_UNIT_TEST_F(CorrectNameFalseUsesConvertOldTopicName, TNameResolverFixture) {
    SetFcc(false);
    // CorrectName rejects empty producer between '--'; shortLegacy still converts.
    UNIT_ASSERT_VALUES_EQUAL(
        Ok(ResolveName(TString{Database}, "rt3.dc1----topic", "dc1", "")),
        "/Root/LbCommunal/topic");
}

Y_UNIT_TEST_F(NamesFromConfigFederationLocal, TNameResolverFixture) {
    SetFcc(false);
    NKikimrPQ::TPQTabletConfig cfg;
    cfg.SetTopicName("rt3.dc1--account--topic");
    cfg.SetTopicPath("/Root/PQ/rt3.dc1--account--topic");
    cfg.SetFederationAccount("account");
    cfg.SetLocalDC(true);
    cfg.SetDC("dc1");
    cfg.SetYdbDatabasePath("");

    auto names = NamesFromConfig(cfg);
    UNIT_ASSERT_C(names.Valid, names.Reason);
    UNIT_ASSERT(!names.FirstClassCitizen);
    UNIT_ASSERT_VALUES_EQUAL(names.ClientsideName, "rt3.dc1--account--topic");
    UNIT_ASSERT_VALUES_EQUAL(names.ShortClientsideName, "account--topic");
    UNIT_ASSERT_VALUES_EQUAL(names.FederationPath, "account/topic");
    UNIT_ASSERT_VALUES_EQUAL(names.Cluster, "dc1");
    UNIT_ASSERT_VALUES_EQUAL(names.Account, "account");
    UNIT_ASSERT_VALUES_EQUAL(names.LegacyProducer, "account");
    UNIT_ASSERT_VALUES_EQUAL(names.TopicForSrcIdHash, "account--topic");
    UNIT_ASSERT_VALUES_EQUAL(names.InternalName, "rt3.dc1--account--topic");
    UNIT_ASSERT_VALUES_EQUAL(names.ModernName, "topic");
    UNIT_ASSERT_VALUES_EQUAL(names.Path, "/Root/PQ/rt3.dc1--account--topic");
}

Y_UNIT_TEST_F(NamesFromConfigFederationRemoteDc, TNameResolverFixture) {
    SetFcc(false);
    NKikimrPQ::TPQTabletConfig cfg;
    cfg.SetTopicName("rt3.dc2--account@path--topic");
    cfg.SetTopicPath("/lb/account-database/path/topic-mirrored-from-dc2");
    cfg.SetFederationAccount("account");
    cfg.SetLocalDC(false);
    cfg.SetDC("dc2");
    cfg.SetYdbDatabasePath("/lb/account-database");

    auto names = NamesFromConfig(cfg);
    UNIT_ASSERT_C(names.Valid, names.Reason);
    UNIT_ASSERT_VALUES_EQUAL(names.ClientsideName, "rt3.dc2--account@path--topic");
    UNIT_ASSERT_VALUES_EQUAL(names.FederationPath, "account/path/topic");
    UNIT_ASSERT_VALUES_EQUAL(names.Cluster, "dc2");
    UNIT_ASSERT_VALUES_EQUAL(names.ModernName, "path/topic-mirrored-from-dc2");
    UNIT_ASSERT_VALUES_EQUAL(names.Path, "/lb/account-database/path/topic-mirrored-from-dc2");
}

Y_UNIT_TEST_F(NamesFromConfigFcc, TNameResolverFixture) {
    SetFcc(true);
    NKikimrPQ::TPQTabletConfig cfg;
    cfg.SetTopicName("my-stream");
    cfg.SetTopicPath("/lb/database/my-stream");
    cfg.SetYdbDatabasePath("/lb/database");

    auto names = NamesFromConfig(cfg);
    UNIT_ASSERT_C(names.Valid, names.Reason);
    UNIT_ASSERT(names.FirstClassCitizen);
    UNIT_ASSERT_VALUES_EQUAL(names.ClientsideName, "my-stream");
    UNIT_ASSERT_VALUES_EQUAL(names.FederationPath, "my-stream");
    UNIT_ASSERT_VALUES_EQUAL(names.FederationPathWithDC, "my-stream");
    UNIT_ASSERT_VALUES_EQUAL(names.TopicForSrcIdHash, "lb/database/my-stream");
    UNIT_ASSERT_VALUES_EQUAL(names.InternalName, "/lb/database/my-stream");
    UNIT_ASSERT_VALUES_EQUAL(names.ModernName, "my-stream");
    UNIT_ASSERT_VALUES_EQUAL(names.Path, "/lb/database/my-stream");
}

Y_UNIT_TEST_F(NamesFromConfigExplicitTopicPath, TNameResolverFixture) {
    SetFcc(true);
    NKikimrPQ::TPQTabletConfig cfg;
    cfg.SetYdbDatabasePath("/lb/database");

    auto names = NamesFromConfig(cfg, TString("/lb/database/my-stream"));
    UNIT_ASSERT_C(names.Valid, names.Reason);
    UNIT_ASSERT_VALUES_EQUAL(names.ClientsideName, "my-stream");
    UNIT_ASSERT_VALUES_EQUAL(names.Path, "/lb/database/my-stream");
}

Y_UNIT_TEST_F(NamesFromConfigExplicitFirstClassCitizen, TNameResolverFixture) {
    SetFcc(false);
    NKikimrPQ::TPQTabletConfig cfg;
    cfg.SetTopicName("my-stream");
    cfg.SetTopicPath("/lb/database/my-stream");
    cfg.SetYdbDatabasePath("/lb/database");

    auto names = NamesFromConfig(cfg, true);
    UNIT_ASSERT_C(names.Valid, names.Reason);
    UNIT_ASSERT_VALUES_EQUAL(names.ClientsideName, "my-stream");
    UNIT_ASSERT_VALUES_EQUAL(names.Path, "/lb/database/my-stream");
    UNIT_ASSERT_VALUES_EQUAL(names.InternalName, "/lb/database/my-stream");
}

Y_UNIT_TEST_F(NamesFromConfigFccTopicWhenFederationEnabled, TNameResolverFixture) {
    SetFcc(false);
    NKikimrPQ::TPQTabletConfig cfg;
    cfg.SetTopicName("topic-0-test");
    cfg.SetTopicPath("/Root/topic-0-test");
    cfg.SetYdbDatabasePath("/Root");

    auto names = NamesFromConfig(cfg);
    UNIT_ASSERT_C(names.Valid, names.Reason);
    UNIT_ASSERT(names.FirstClassCitizen);
    UNIT_ASSERT_VALUES_EQUAL(names.ClientsideName, "topic-0-test");
    UNIT_ASSERT_VALUES_EQUAL(names.Path, "/Root/topic-0-test");
    UNIT_ASSERT_VALUES_EQUAL(names.InternalName, "/Root/topic-0-test");
}

Y_UNIT_TEST_F(NamesFromConfigLegacyStyleDoubleName, TNameResolverFixture) {
    SetFcc(false);
    NKikimrPQ::TPQTabletConfig cfg;
    cfg.SetTopicName("rt3.dc1--account@account--account");
    cfg.SetTopicPath("/Root/PQ/rt3.dc1--account@account--account");
    cfg.SetFederationAccount("account");
    cfg.SetLocalDC(true);
    cfg.SetDC("dc1");
    cfg.SetYdbDatabasePath("");

    auto names = NamesFromConfig(cfg);
    UNIT_ASSERT_C(names.Valid, names.Reason);
    UNIT_ASSERT_VALUES_EQUAL(names.FederationPath, "account/account/account");
    UNIT_ASSERT_VALUES_EQUAL(names.ClientsideName, "rt3.dc1--account@account--account");
    UNIT_ASSERT_VALUES_EQUAL(names.TopicForSrcIdHash, "account@account--account");
    UNIT_ASSERT_VALUES_EQUAL(names.ModernName, "account/account");
    UNIT_ASSERT_VALUES_EQUAL(names.InternalName, "rt3.dc1--account@account--account");
}

Y_UNIT_TEST_F(NamesFromConfigModernPathLocalAndRemote, TNameResolverFixture) {
    SetFcc(false);
    {
        NKikimrPQ::TPQTabletConfig cfg;
        cfg.SetTopicName("rt3.dc1--account@path--topic");
        cfg.SetTopicPath("/lb/account-database/path/topic");
        cfg.SetFederationAccount("account");
        cfg.SetDC("dc1");
        cfg.SetLocalDC(true);
        cfg.SetYdbDatabasePath("/lb/account-database");

        auto names = NamesFromConfig(cfg);
        UNIT_ASSERT_C(names.Valid, names.Reason);
        UNIT_ASSERT_VALUES_EQUAL(names.Path, "/lb/account-database/path/topic");
        UNIT_ASSERT_VALUES_EQUAL(names.SecondaryPath, "/Root/PQ/rt3.dc1--account@path--topic");
        UNIT_ASSERT_VALUES_EQUAL(names.ModernName, "path/topic");
        UNIT_ASSERT_VALUES_EQUAL(names.ClientsideName, "rt3.dc1--account@path--topic");
        UNIT_ASSERT_VALUES_EQUAL(names.FederationPath, "account/path/topic");
        UNIT_ASSERT_VALUES_EQUAL(names.InternalName, "rt3.dc1--account@path--topic");
        UNIT_ASSERT_VALUES_EQUAL(names.TopicForSrcIdHash, "account@path--topic");
    }
    {
        NKikimrPQ::TPQTabletConfig cfg;
        cfg.SetTopicName("rt3.dc2--account@path--topic");
        cfg.SetLocalDC(false);
        cfg.SetTopicPath("/lb/account-database/path/topic-mirrored-from-dc2");
        cfg.SetFederationAccount("account");
        cfg.SetDC("dc2");
        cfg.SetYdbDatabasePath("/lb/account-database");

        auto names = NamesFromConfig(cfg);
        UNIT_ASSERT_C(names.Valid, names.Reason);
        UNIT_ASSERT_VALUES_EQUAL(names.Path, "/lb/account-database/path/topic-mirrored-from-dc2");
        UNIT_ASSERT_VALUES_EQUAL(names.SecondaryPath, "/Root/PQ/rt3.dc2--account@path--topic");
        UNIT_ASSERT_VALUES_EQUAL(names.ModernName, "path/topic-mirrored-from-dc2");
        UNIT_ASSERT_VALUES_EQUAL(names.ClientsideName, "rt3.dc2--account@path--topic");
        UNIT_ASSERT_VALUES_EQUAL(names.FederationPath, "account/path/topic");
        UNIT_ASSERT_VALUES_EQUAL(names.FederationPathWithDC, "account/path/topic-mirrored-from-dc2");
        UNIT_ASSERT_VALUES_EQUAL(names.TopicForSrcIdHash, "account@path--topic");
    }
}

Y_UNIT_TEST_F(NamesFromConfigNoTopicName, TNameResolverFixture) {
    SetFcc(false);
    NKikimrPQ::TPQTabletConfig cfg;
    cfg.SetTopic("topic");
    cfg.SetTopicPath("/Root/PQ/rt3.dc1--account@path--topic");
    cfg.SetFederationAccount("account");
    cfg.SetDC("dc1");
    cfg.SetLocalDC(true);

    auto names = NamesFromConfig(cfg);
    UNIT_ASSERT_C(names.Valid, names.Reason);
    UNIT_ASSERT_VALUES_EQUAL(names.Path, "/Root/PQ/rt3.dc1--account@path--topic");
    UNIT_ASSERT_VALUES_EQUAL(names.ModernName, "path/topic");
    UNIT_ASSERT_VALUES_EQUAL(names.ClientsideName, "rt3.dc1--account@path--topic");
    UNIT_ASSERT_VALUES_EQUAL(names.FederationPath, "account/path/topic");
}

Y_UNIT_TEST(NamesFromConfigExplicitPrefixNoAppData) {
    NKikimrPQ::TPQTabletConfig cfg;
    cfg.SetTopicName("rt3.dc1--account@path--topic");
    cfg.SetTopicPath("/lb/account-database/path/topic");
    cfg.SetFederationAccount("account");
    cfg.SetDC("dc1");
    cfg.SetLocalDC(true);
    cfg.SetYdbDatabasePath("/lb/account-database");

    auto names = NamesFromConfig(cfg, TString(), false, "Root/PQ", "");
    UNIT_ASSERT_C(names.Valid, names.Reason);
    UNIT_ASSERT_VALUES_EQUAL(names.Path, "/lb/account-database/path/topic");
    UNIT_ASSERT_VALUES_EQUAL(names.SecondaryPath, "/Root/PQ/rt3.dc1--account@path--topic");
    UNIT_ASSERT_VALUES_EQUAL(names.ClientsideName, "rt3.dc1--account@path--topic");
    UNIT_ASSERT_VALUES_EQUAL(names.FederationPath, "account/path/topic");
}

Y_UNIT_TEST_F(NamesFromConfigEmptyPathIsInvalid, TNameResolverFixture) {
    NKikimrPQ::TPQTabletConfig cfg;
    auto names = NamesFromConfig(cfg, TString());
    UNIT_ASSERT(!names.Valid);
}

Y_UNIT_TEST_F(NamesFromConfigDoesNotAbortOnMalformedInput, TNameResolverFixture) {
    auto expectInvalid = [](const NKikimrPQ::TPQTabletConfig& cfg, const TString& path, bool fcc) {
        const auto names = NamesFromConfig(cfg, path, fcc);
        UNIT_ASSERT_C(!names.Valid,
            TStringBuilder() << "fcc=" << fcc << " path=" << path << " reason=" << names.Reason);
        UNIT_ASSERT_C(!names.Reason.empty(), TStringBuilder() << "fcc=" << fcc << " path=" << path);
    };

    const TVector<TString> paths = {
        TString(),
        "nopath",
        "/",
        "//",
        "account/",
        "/Root/PQ/",
        "/Root/PQ/rt3.dc1topic",
        "/Root/PQ/account/topic/",
        "/Root/PQ/account/topic",
        "/lb/db/topic-mirrored-from",
        "rt3.dc1--",
        "/Root/PQ/rt3.--topic",
    };

    for (const bool fcc : {false, true}) {
        SetFcc(fcc);
        NKikimrPQ::TPQTabletConfig empty;
        UNIT_ASSERT_C(!NamesFromConfig(empty).Valid, fcc);
        for (const auto& path : paths) {
            const auto names = NamesFromConfig(empty, path, fcc);
            if (!names.Valid) {
                UNIT_ASSERT_C(!names.Reason.empty(), path);
            }
        }
    }

    NKikimrPQ::TPQTabletConfig empty;
    expectInvalid(empty, TString(), true);
    expectInvalid(empty, TString(), false);
    expectInvalid(empty, TString("nopath"), true);
    expectInvalid(empty, TString("nopath"), false);

    NKikimrPQ::TPQTabletConfig rt3NoDash;
    rt3NoDash.SetTopicPath("/Root/PQ/rt3.dc1topic");
    expectInvalid(rt3NoDash, TString(), false);

    NKikimrPQ::TPQTabletConfig trailing;
    trailing.SetTopicPath("/Root/PQ/account/topic/");
    expectInvalid(trailing, TString(), false);

    NKikimrPQ::TPQTabletConfig noDc;
    noDc.SetTopicPath("/Root/PQ/account/topic");
    expectInvalid(noDc, TString(), false);

    NKikimrPQ::TPQTabletConfig badMirror;
    badMirror.SetTopicPath("/lb/db/topic-mirrored-from");
    badMirror.SetYdbDatabasePath("/lb/db");
    badMirror.SetFederationAccount("account");
    expectInvalid(badMirror, TString(), false);
}

Y_UNIT_TEST_F(NamesFromConfigDoesNotFccMaskMalformedFederation, TNameResolverFixture) {
    SetFcc(false);

    NKikimrPQ::TPQTabletConfig underPq;
    underPq.SetTopicPath("/Root/PQ/account/topic");
    auto names = NamesFromConfig(underPq);
    UNIT_ASSERT(!names.Valid);
    UNIT_ASSERT(!names.FirstClassCitizen);
    UNIT_ASSERT(!names.Reason.empty());

    NKikimrPQ::TPQTabletConfig rt3NoDash;
    rt3NoDash.SetTopicPath("/Root/PQ/rt3.dc1topic");
    names = NamesFromConfig(rt3NoDash, TString("/Root/PQ/rt3.dc1topic"));
    UNIT_ASSERT(!names.Valid);
    UNIT_ASSERT(!names.FirstClassCitizen);

    NKikimrPQ::TPQTabletConfig userDb;
    userDb.SetTopicPath("/lb/db/topic-mirrored-from");
    userDb.SetYdbDatabasePath("/lb/db");
    userDb.SetFederationAccount("account");
    names = NamesFromConfig(userDb);
    UNIT_ASSERT(!names.Valid);
    UNIT_ASSERT(!names.FirstClassCitizen);
}

} // Y_UNIT_TEST_SUITE(TNameResolverTest)
