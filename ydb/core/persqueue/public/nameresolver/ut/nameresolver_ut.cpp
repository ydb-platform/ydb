#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/mailbox.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <expected>

using namespace NKikimr;
using namespace NKikimr::NPQ::NNameResolver;
using namespace NActors;

namespace {

constexpr TStringBuf LbRoot = "/Root/LbCommunal";
constexpr TStringBuf Database = "/Root/Db";

// Minimal AppData + TLS activation context (avoids ydb/core/testlib ↔ gtest conflict).
class TNameResolverTest : public ::testing::Test {
protected:
    TNameResolverTest()
        : AppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr)
    {
        THolder<TActorSystemSetup> setup(new TActorSystemSetup);
        System.Reset(new TActorSystem(setup, &AppData));
        Mailbox.Reset(new TMailbox());
        ExecutorThread.Reset(new TExecutorThread(0, System.Get(), nullptr, "thread"));
        Ctx.Reset(new TActorContext(*Mailbox, *ExecutorThread, GetCycleCountFast(), SelfID));
        PrevCtx = TlsActivationContext;
        TlsActivationContext = Ctx.Get();
    }

    ~TNameResolverTest() override {
        TlsActivationContext = PrevCtx;
        PrevCtx = nullptr;
    }

    void SetUp() override {
        auto& pqConfig = AppData.PQConfig;
        pqConfig.SetTopicsAreFirstClassCitizen(true);
        pqConfig.SetRoot("/Root/PQ");
        pqConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(TString{LbRoot});
    }

    void SetFcc(bool value) {
        AppData.PQConfig.SetTopicsAreFirstClassCitizen(value);
    }

    void SetLbRoot(const TString& root) {
        AppData.PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(root);
    }

    void SetPqRoot(const TString& root) {
        AppData.PQConfig.SetRoot(root);
    }

    static TString Ok(std::expected<TString, TString> result) {
        EXPECT_TRUE(result.has_value()) << (result ? "" : result.error());
        return result.value_or(TString{});
    }

    static void ExpectError(std::expected<TString, TString> result, TStringBuf reason) {
        ASSERT_FALSE(result.has_value()) << "got path: " << result.value_or(TString{});
        EXPECT_EQ(result.error(), reason);
    }

    TAppData AppData;
    THolder<TActorSystem> System;
    THolder<TMailbox> Mailbox;
    THolder<TExecutorThread> ExecutorThread;
    TActorId SelfID;
    THolder<TActorContext> Ctx;
    TActivationContext* PrevCtx = nullptr;
};

} // namespace

TEST_F(TNameResolverTest, FederationRootDbConvertsLegacyRt3) {
    SetFcc(false);
    EXPECT_EQ(
        Ok(ResolveName("", "rt3.dc1--account--topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    EXPECT_EQ(
        Ok(ResolveName("", "rt3.dc2--account--topic", "dc1")),
        "/Root/LbCommunal/account/topic-mirrored-from-dc2");
}

TEST_F(TNameResolverTest, FederationRootDbAccountTopic) {
    SetFcc(false);
    EXPECT_EQ(
        Ok(ResolveName("", "account/topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    EXPECT_EQ(
        Ok(ResolveName("", "account/topic", "dc1", "dc2")),
        "/Root/LbCommunal/account/topic-mirrored-from-dc2");
    EXPECT_EQ(
        Ok(ResolveName("", "account/dir/topic", "dc1", "dc2")),
        "/Root/LbCommunal/account/dir/topic-mirrored-from-dc2");
}

TEST_F(TNameResolverTest, FederationRootDbViaDatabasePrefix) {
    SetFcc(false);
    EXPECT_EQ(
        Ok(ResolveName("/Root", "account/topic", "dc1")),
        "/Root/LbCommunal/account/topic");
}

TEST_F(TNameResolverTest, FederationUserDatabaseRelativePath) {
    SetFcc(false);
    EXPECT_EQ(
        Ok(ResolveName("/Root/LbCommunal/account", "dir/topic", "dc1")),
        "/Root/LbCommunal/account/dir/topic");
    EXPECT_EQ(
        Ok(ResolveName("/Root/LbCommunal/account", "dir/topic", "dc1", "dc2")),
        "/Root/LbCommunal/account/dir/topic-mirrored-from-dc2");
    EXPECT_EQ(
        Ok(ResolveName("/Root/LbCommunal/account", "topic", "dc1")),
        "/Root/LbCommunal/account/topic");
}

TEST_F(TNameResolverTest, ModernPathNormalizedWithDatabase) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "account/topic", "dc1", "")),
        "/Root/Db/account/topic");
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "/account/topic", "dc1", "")),
        "/Root/Db/account/topic");
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "/Root/Db/account/topic", "dc1", "")),
        "/Root/Db/account/topic");
}

TEST_F(TNameResolverTest, LocalRt3) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "rt3.dc1--account--topic", "dc1", "dc1")),
        "/Root/LbCommunal/account/topic");
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "rt3.dc1--account--topic", "dc1", "")),
        "/Root/LbCommunal/account/topic");
}

TEST_F(TNameResolverTest, RemoteRt3Mirrored) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "rt3.dc2--account--topic", "dc1", "dc2")),
        "/Root/LbCommunal/account/topic-mirrored-from-dc2");
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "rt3.dc2--account--topic", "dc1", "")),
        "/Root/LbCommunal/account/topic-mirrored-from-dc2");
}

TEST_F(TNameResolverTest, Rt3WithDirectories) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "rt3.dc1--account@dir--topic", "dc1", "")),
        "/Root/LbCommunal/account/dir/topic");
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "rt3.dc2--account@dir--topic", "dc1", "")),
        "/Root/LbCommunal/account/dir/topic-mirrored-from-dc2");
}

TEST_F(TNameResolverTest, ShortLegacyLocal) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "account--topic", "dc1", "")),
        "/Root/LbCommunal/account/topic");
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "account@dir--topic", "dc1", "")),
        "/Root/LbCommunal/account/dir/topic");
}

TEST_F(TNameResolverTest, ShortLegacyForeignDcMirrored) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "account--topic", "dc1", "dc2")),
        "/Root/LbCommunal/account/topic-mirrored-from-dc2");
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "account@dir--topic", "dc1", "dc2")),
        "/Root/LbCommunal/account/dir/topic-mirrored-from-dc2");
}

TEST_F(TNameResolverTest, BareTopic) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "topic", "dc1", "")),
        "/Root/LbCommunal/topic");
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "topic", "dc1", "dc2")),
        "/Root/LbCommunal/topic-mirrored-from-dc2");
}

TEST_F(TNameResolverTest, AtWithoutDashDashIsLegacy) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "account@dir", "dc1", "")),
        "/Root/LbCommunal/account@dir");
}

TEST_F(TNameResolverTest, DcMismatchReturnsError) {
    ExpectError(
        ResolveName(TString{Database}, "rt3.dc1--account--topic", "dc1", "dc2"),
        "DC specified both in topic name and separate option and they mismatch. ");
}

TEST_F(TNameResolverTest, ShortWithoutDcReturnsError) {
    ExpectError(
        ResolveName(TString{Database}, "account--topic", "", ""),
        "Cannot determine DC: should specify either in topic name, Dc option or LocalDc option. ");
}

TEST_F(TNameResolverTest, MalformedRt3NoDashDash) {
    ExpectError(
        ResolveName(TString{Database}, "rt3.bad", "dc1", ""),
        "Malformed legacy style topic name: contains 'rt3.', but no '--'. ");
}

TEST_F(TNameResolverTest, MalformedRt3EmptyDc) {
    ExpectError(
        ResolveName(TString{Database}, "rt3.--account--topic", "dc1", ""),
        "Internal error: Could not determine DC for topic: rt3.--account--topic. ");
}

TEST_F(TNameResolverTest, MalformedRt3EmptyShort) {
    // CorrectName rejects, but BuildFromLegacyName accepts and converts short part.
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "rt3.dc1--", "dc1", "")),
        "/Root/LbCommunal");
}

TEST_F(TNameResolverTest, ShortDashDashConvertedLikeConverter) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "account--", "dc1", "")),
        "/Root/LbCommunal/account");
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "--topic", "dc1", "")),
        "/Root/LbCommunal/topic");
}

TEST_F(TNameResolverTest, EmptyNameFccIsLegacyBare) {
    // FCC: empty is legacy-style (no '/'); ConvertOldTopicName("") -> "".
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "", "dc1", "")),
        "/Root/LbCommunal");
}

TEST_F(TNameResolverTest, DoubleSlashInLegacyFccConverted) {
    // BuildFromLegacyName has no BasicNameChecks; CanonizePath collapses "//".
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "account--a//b", "dc1", "")),
        "/Root/LbCommunal/account/a/b");
}

TEST_F(TNameResolverTest, LeadingSlashStripped) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "/rt3.dc1--account--topic", "dc1", "")),
        "/Root/LbCommunal/account/topic");
}

TEST_F(TNameResolverTest, MirrorSkippedWhenLocalDcEmpty) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "rt3.dc2--account--topic", "", "dc2")),
        "/Root/LbCommunal/account/topic");
}

TEST_F(TNameResolverTest, FallbackToDatabaseWhenLbRootEmpty) {
    SetLbRoot("");
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "rt3.dc1--account--topic", "dc1", "")),
        "/Root/Db/account/topic");
}

TEST_F(TNameResolverTest, NoRootReturnsModernPathOnly) {
    SetLbRoot("");
    EXPECT_EQ(
        Ok(ResolveName("", "rt3.dc1--account--topic", "dc1", "")),
        "account/topic");
}

TEST_F(TNameResolverTest, DefaultDcParsesFromRt3Name) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "rt3.dc2--account--topic", "dc1")),
        "/Root/LbCommunal/account/topic-mirrored-from-dc2");
}

TEST_F(TNameResolverTest, DefaultDcShortUsesLocalDc) {
    EXPECT_EQ(
        Ok(ResolveName(TString{Database}, "account--topic", "dc1")),
        "/Root/LbCommunal/account/topic");
}

TEST_F(TNameResolverTest, IsPathPrefixExactMatch) {
    SetFcc(false);
    EXPECT_EQ(
        Ok(ResolveName("/Root/PQ", "account/topic", "dc1")),
        "/Root/LbCommunal/account/topic");
}

TEST_F(TNameResolverTest, TopicPathStartsWithPqRoot) {
    SetFcc(false);
    EXPECT_EQ(
        Ok(ResolveName("", "/Root/PQ/rt3.dc1--account--topic", "dc1")),
        "/Root/LbCommunal/account/topic");
    EXPECT_EQ(
        Ok(ResolveName("", "/Root/PQ/account/topic", "dc1")),
        "/Root/LbCommunal/account/topic");
}

TEST_F(TNameResolverTest, FederationModernPathBadName) {
    SetFcc(false);
    // ForFederation BasicNameChecks runs before ParseModernPath.
    ExpectError(
        ResolveName("/Root/LbCommunal/account", "dir//topic", "dc1"),
        "Bad topic name for federation: dir//topic");
}

TEST_F(TNameResolverTest, FederationModernPathWithoutDc) {
    SetFcc(false);
    ExpectError(
        ResolveName("/Root/LbCommunal/account", "dir/topic", "", ""),
        "Cannot determine DC: should specify either with Dc option or LocalDc option. ");
    ExpectError(
        ResolveName("", "account/topic", "", ""),
        "Cannot determine DC: should specify either with Dc option or LocalDc option. ");
}

TEST_F(TNameResolverTest, FederationPqRootWithTrailingSlash) {
    SetFcc(false);
    // "/Root/PQ/" → topicName "Root/PQ/" → EndsWith('/') before classify
    // (same order as TDiscoveryConverter::BuildForFederation).
    ExpectError(
        ResolveName("", "/Root/PQ/", "dc1"),
        "Invalid topic path or trailing '/'. ");
}

TEST_F(TNameResolverTest, FederationOnlySlash) {
    SetFcc(false);
    // "/" → after StripLeadingSlash topic is empty
    ExpectError(
        ResolveName("", "/", "dc1"),
        "Invalid topic path (only account provided?). ");
}

TEST_F(TNameResolverTest, FederationEmptyTopicAfterExactPqRoot) {
    SetFcc(false);
    // topic == PQ root (no trailing slash) → SkipPathPrefix clears topic
    ExpectError(
        ResolveName("", "/Root/PQ", "dc1"),
        "Bad topic name (only account provided?). ");
}

TEST_F(TNameResolverTest, SkipPathPrefixRestoresWhenNoSlashAfterPrefix) {
    SetFcc(false);
    // database is PQ root → SkipPathPrefix(topic, pqPrefix); topic shares byte prefix
    // without '/' → restore original topic, then legacy-parse it.
    EXPECT_EQ(
        Ok(ResolveName("/Root/PQ", "Root/PQfoo", "dc1")),
        "/Root/LbCommunal/Root/PQfoo");
}

TEST_F(TNameResolverTest, FederationTrailingSlash) {
    SetFcc(false);
    ExpectError(
        ResolveName("", "account/", "dc1"),
        "Invalid topic path or trailing '/'. ");
}

TEST_F(TNameResolverTest, FederationEmptyName) {
    SetFcc(false);
    ExpectError(
        ResolveName("", "", "dc1"),
        "Bad topic name for federation: ");
}

TEST_F(TNameResolverTest, FederationDoubleSlash) {
    SetFcc(false);
    ExpectError(
        ResolveName("", "account--a//b", "dc1"),
        "Bad topic name for federation: account--a//b");
}

TEST_F(TNameResolverTest, FederationLegacyParseFailure) {
    SetFcc(false);
    ExpectError(
        ResolveName("", "rt3.bad", "dc1"),
        "Malformed legacy style topic name: contains 'rt3.', but no '--'. ");
}

TEST_F(TNameResolverTest, FederationAccountTopicWithoutLbRoot) {
    SetFcc(false);
    SetLbRoot("");
    EXPECT_EQ(
        Ok(ResolveName("", "account/topic", "dc1")),
        "account/topic");
}
