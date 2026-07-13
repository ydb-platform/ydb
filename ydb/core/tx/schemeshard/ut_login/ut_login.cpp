#include <util/string/join.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/library/login/login.h>
#include <ydb/library/login/password_checker/password_checker.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/testlib/service_mocks/ldap_mock/simple_server.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/ticket_parser.h>
#include <ydb/core/security/login_page.h>
#include <ydb/core/security/ldap_auth_provider/ldap_auth_provider.h>
#include <ydb/core/security/ldap_auth_provider/test_utils/test_settings.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr::NCertTestUtils;
using namespace NSchemeShardUT_Private;

using namespace NKikimr::Tests;

namespace NSchemeShardUT_Private {

TCertStorage CertStorage;

struct TAccountLockoutInitializer {
    size_t AttemptThreshold = 4;
    TString AttemptResetDuration = "1h";
};

void SetAccountLockoutParameters(TTestActorRuntime &runtime, ui64 schemeShard, const TAccountLockoutInitializer& initializer) {
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();

    ::NKikimrProto::TAccountLockout accountLockout;
    accountLockout.SetAttemptThreshold(initializer.AttemptThreshold);
    accountLockout.SetAttemptResetDuration(initializer.AttemptResetDuration);
    *request->Record.MutableConfig()->MutableAuthConfig()->MutableAccountLockout() = accountLockout;
    SetConfig(runtime, schemeShard, std::move(request));
}

struct TCheckSecurityStateExpectedValues {
    size_t PublicKeysSize;
    size_t SidsSize;
};

void CheckSecurityState(const NKikimrScheme::TEvDescribeSchemeResult& describeResult, const TCheckSecurityStateExpectedValues& expectedValues) {
    UNIT_ASSERT(describeResult.HasPathDescription());
    UNIT_ASSERT(describeResult.GetPathDescription().HasDomainDescription());
    UNIT_ASSERT(describeResult.GetPathDescription().GetDomainDescription().HasSecurityState());
    UNIT_ASSERT_VALUES_EQUAL(describeResult.GetPathDescription().GetDomainDescription().GetSecurityState().PublicKeysSize(), expectedValues.PublicKeysSize);
    UNIT_ASSERT_VALUES_EQUAL(describeResult.GetPathDescription().GetDomainDescription().GetSecurityState().SidsSize(), expectedValues.SidsSize);
}

void CheckToken(const TString& token, const NKikimrScheme::TEvDescribeSchemeResult& describeResult, const TString& expectedUsername, const TString& expectedError = "") {
    NLogin::TLoginProvider login;
    login.UpdateSecurityState(describeResult.GetPathDescription().GetDomainDescription().GetSecurityState());
    auto validateResult = login.ValidateToken({.Token = token});
    UNIT_ASSERT_VALUES_EQUAL(validateResult.Error, expectedError);
    UNIT_ASSERT_VALUES_EQUAL(validateResult.User, expectedUsername);
}

}  // namespace NSchemeShardUT_Private

Y_UNIT_TEST_SUITE(TSchemeShardLoginTest) {

    Y_UNIT_TEST(UserLogin1) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            Cerr << describe.DebugString() << Endl;
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        const auto user1Hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 1});
        }

        // public keys are filled after the first login
        auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
        }

        // check token
        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckToken(resultLogin.token(), describe, "user1");
        }
    }

    Y_UNIT_TEST(UserLogin2) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            Cerr << describe.DebugString() << Endl;
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        {
            TString hashes = R"(
                {
                    "version": 1,
                    "argon2id": "HTkpQjtVJgBoA0CZu+i3zg==$ZO37rNB37kP9hzmKRGfwc4aYrboDt4OBDsF1TBn5oLw=",
                    "scram-sha-256": "4096:s0QSrrFVkMTh3k2TTk860A==$LmCubRpIYV1zHMLucTtu7XjhB+PgWwH8ABCYGyVF1mo=:eUrie0C98tEFgygSOtom/fwPmgnMxeq53l7YTFfYncc="
                }
            )";

            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", Base64Encode(hashes));
        }

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 1});
        }

        {
            auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::Argon,
                "ZO37rNB37kP9hzmKRGfwc4aYrboDt4OBDsF1TBn5oLw=");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        }

        {
            auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                "eUrie0C98tEFgygSOtom/fwPmgnMxeq53l7YTFfYncc=");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        }

        {
            // Test SCRAM-SHA256 authentication
            // Using pre-computed values from scram_ut.cpp for password "password1"
            TString authMessage = "n=user,r=clientnonce,r=clientservernonce,s=s0QSrrFVkMTh3k2TTk860A==,i=4096,c=biws,r=clientservernonce";
            auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Scram, NLoginProto::EHashType::ScramSha256,
                "AJgthTHWf0jz/bMHwrWDOHk9SQPpPpvGx937mEzFnCQ=", authMessage);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.serversignature(), "RBEDP7XfP9zTpxx+++HZSiw7kB7MDtfZ5mlBcMSxRQY=");
        }
    }

    Y_UNIT_TEST_FLAG(RemoveUser, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;
        const auto user1Hashes = MakeTestPasswordHashes("password1");
        const auto user2Hashes = MakeTestPasswordHashes("password2");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", user2Hashes.HashedPassword);
        auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "user1");

        // check user has been removed:
        {
            auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                user1Hashes.ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetError(), "Cannot find user 'user1'");
        }
    }

    Y_UNIT_TEST_FLAG(RemoveUser_NonExisting, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;

        for (bool missingOk : {false, true}) {
            auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
            auto transaction = modifyTx->Record.AddTransaction();
            transaction->SetWorkingDir("/MyRoot");
            transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin);

            auto removeUser = transaction->MutableAlterLogin()->MutableRemoveUser();
            removeUser->SetUser("user1");
            removeUser->SetMissingOk(missingOk);

            AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
            TestModificationResults(runtime, txId, TVector<TExpectedResult>{{
                missingOk ? NKikimrScheme::StatusSuccess : NKikimrScheme::StatusPreconditionFailed,
                missingOk ? "" : "User not found"
            }});
        }
    }

    Y_UNIT_TEST_FLAG(RemoveUser_Groups, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;
        const auto user1Hashes = MakeTestPasswordHashes("password1");
        const auto user2Hashes = MakeTestPasswordHashes("password2");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", user2Hashes.HashedPassword);
        auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir1/DirSub1");

        CreateAlterLoginCreateGroup(runtime, ++txId, "/MyRoot", "group");
        AlterLoginAddGroupMembership(runtime, ++txId, "/MyRoot", "user1", "group");
        AlterLoginAddGroupMembership(runtime, ++txId, "/MyRoot", "user2", "group");
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::HasGroup("group", {"user1", "user2"})});

        NACLib::TDiffACL diffACL;
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "group");
        AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", diffACL.SerializeAsString(), "");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),{
            NLs::HasNoRight("+U:user1"), NLs::HasNoEffectiveRight("+U:user1"),
            NLs::HasRight("+U:group"), NLs::HasEffectiveRight("+U:group")});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1/DirSub1"),{
            NLs::HasNoRight("+U:user1"), NLs::HasNoEffectiveRight("+U:user1"),
            NLs::HasNoRight("+U:group"), NLs::HasEffectiveRight("+U:group")});

        CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "user1");

        // check user has been removed:
        {
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::HasGroup("group", {"user2"})});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),{
                NLs::HasNoRight("+U:user1"), NLs::HasNoEffectiveRight("+U:user1"),
                NLs::HasRight("+U:group"), NLs::HasEffectiveRight("+U:group")});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1/DirSub1"),{
                NLs::HasNoRight("+U:user1"), NLs::HasNoEffectiveRight("+U:user1"),
                NLs::HasNoRight("+U:group"), NLs::HasEffectiveRight("+U:group")});
            auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                user1Hashes.ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetError(), "Cannot find user 'user1'");
        }
    }

    Y_UNIT_TEST_FLAG(RemoveUser_Owner, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;
        const auto user1Hashes = MakeTestPasswordHashes("password1");
        const auto user2Hashes = MakeTestPasswordHashes("password2");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", user2Hashes.HashedPassword);
        auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir1/DirSub1");

        AsyncModifyACL(runtime, ++txId, "/MyRoot/Dir1", "DirSub1", NACLib::TDiffACL{}.SerializeAsString(), "user1");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1/DirSub1"),
            {NLs::HasOwner("user1")});

        // Cerr << DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Dir1").DebugString() << Endl;
        // Cerr << DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Dir1").DebugString() << Endl;

        if (StrictAclCheck) {
            CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "user1",
                TVector<TExpectedResult>{{NKikimrScheme::StatusPreconditionFailed, "User user1 owns /MyRoot/Dir1/DirSub1 and can't be removed"}});

            // check user still exists and has their rights:
            {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1/DirSub1"),
                    {NLs::HasOwner("user1")});
                auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                    user1Hashes.ScramServerKey);
                UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetError(), "");
            }
        }

        AsyncModifyACL(runtime, ++txId, "/MyRoot/Dir1", "DirSub1", NACLib::TDiffACL().SerializeAsString(), "user2");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "user1");

        // check user has been removed:
        {
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1/DirSub1"),
                {NLs::HasOwner("user2")});
            auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                user1Hashes.ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetError(), "Cannot find user 'user1'");
        }
    }

    Y_UNIT_TEST_FLAG(RemoveUser_Acl, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;
        const auto user1Hashes = MakeTestPasswordHashes("password1");
        const auto user2Hashes = MakeTestPasswordHashes("password2");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", user2Hashes.HashedPassword);
        auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir1/DirSub1");

        {
            NACLib::TDiffACL diffACL;
            diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1");
            AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", diffACL.SerializeAsString(), "");
            TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
                {NLs::HasRight("+U:user1"), NLs::HasEffectiveRight("+U:user1")});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1/DirSub1"),
                {NLs::HasNoRight("+U:user1"), NLs::HasEffectiveRight("+U:user1")});
        }

        // Cerr << DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Dir1").DebugString() << Endl;
        // Cerr << DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Dir1").DebugString() << Endl;

        if (StrictAclCheck) {
            CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "user1",
                TVector<TExpectedResult>{{NKikimrScheme::StatusPreconditionFailed, "User user1 has an ACL record on /MyRoot/Dir1 and can't be removed"}});

            // check user still exists and has their rights:
            {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
                    {NLs::HasRight("+U:user1"), NLs::HasEffectiveRight("+U:user1")});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1/DirSub1"),
                    {NLs::HasNoRight("+U:user1"), NLs::HasEffectiveRight("+U:user1")});
                auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                    user1Hashes.ScramServerKey);
                UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetError(), "");
            }
        }

        {
            NACLib::TDiffACL diffACL;
            diffACL.RemoveAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1");
            AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", diffACL.SerializeAsString(), "");
            TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
                {NLs::HasNoRight("+U:user1"), NLs::HasNoEffectiveRight("+U:user1")});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1/DirSub1"),
                {NLs::HasNoRight("+U:user1"), NLs::HasNoEffectiveRight("+U:user1")});
        }

        CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "user1");

        // check user has been removed:
        {
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
                {NLs::HasNoRight("+U:user1"), NLs::HasNoEffectiveRight("+U:user1")});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1/DirSub1"),
                {NLs::HasNoRight("+U:user1"), NLs::HasNoEffectiveRight("+U:user1")});
            auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                user1Hashes.ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetError(), "Cannot find user 'user1'");
        }
    }

    Y_UNIT_TEST_FLAG(RemoveGroup, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;

        const auto user1Hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);
        CreateAlterLoginCreateGroup(runtime, ++txId, "/MyRoot", "group1");
        AlterLoginAddGroupMembership(runtime, ++txId, "/MyRoot", "user1", "group1");
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::HasGroup("group1", {"user1"})});

        CreateAlterLoginRemoveGroup(runtime, ++txId, "/MyRoot", "group1");
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::HasNoGroup("group1")});
    }

    Y_UNIT_TEST_FLAG(RemoveGroup_NonExisting, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;

        for (bool missingOk : {false, true}) {
            auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
            auto transaction = modifyTx->Record.AddTransaction();
            transaction->SetWorkingDir("/MyRoot");
            transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterLogin);

            auto removeUser = transaction->MutableAlterLogin()->MutableRemoveGroup();
            removeUser->SetGroup("group1");
            removeUser->SetMissingOk(missingOk);

            AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release());
            TestModificationResults(runtime, txId, TVector<TExpectedResult>{{
                missingOk ? NKikimrScheme::StatusSuccess : NKikimrScheme::StatusPreconditionFailed,
                missingOk ? "" : "Group not found"
            }});
        }
    }

    Y_UNIT_TEST_FLAG(RemoveGroup_Owner, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;

        CreateAlterLoginCreateGroup(runtime, ++txId, "/MyRoot", "group1");
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::HasGroup("group1", {})});

        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", NACLib::TDiffACL{}.SerializeAsString(), "group1");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
                {NLs::HasOwner("group1")});

        if (StrictAclCheck) {
            CreateAlterLoginRemoveGroup(runtime, ++txId, "/MyRoot", "group1",
                TVector<TExpectedResult>{{NKikimrScheme::StatusPreconditionFailed, "Group group1 owns /MyRoot/Dir1 and can't be removed"}});
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                {NLs::HasGroup("group1", {})});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
                    {NLs::HasOwner("group1")});
        }

        AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", NACLib::TDiffACL{}.SerializeAsString(), "root@builtin");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
                {NLs::HasOwner("root@builtin")});

        CreateAlterLoginRemoveGroup(runtime, ++txId, "/MyRoot", "group1");
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::HasNoGroup("group1")});
    }

    Y_UNIT_TEST_FLAG(RemoveGroup_Acl, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;

        CreateAlterLoginCreateGroup(runtime, ++txId, "/MyRoot", "group1");
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::HasGroup("group1", {})});

        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        {
            NACLib::TDiffACL diffACL;
            diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "group1");
            AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", diffACL.SerializeAsString(), "");
        }
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
                {NLs::HasRight("+U:group1")});

        if (StrictAclCheck) {
            CreateAlterLoginRemoveGroup(runtime, ++txId, "/MyRoot", "group1",
                TVector<TExpectedResult>{{NKikimrScheme::StatusPreconditionFailed, "Group group1 has an ACL record on /MyRoot/Dir1 and can't be removed"}});
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                {NLs::HasGroup("group1", {})});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
                    {NLs::HasRight("+U:group1")});
        }

        {
            NACLib::TDiffACL diffACL;
            diffACL.RemoveAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "group1");
            AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", diffACL.SerializeAsString(), "");
        }
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
                {NLs::HasNoRight("+U:group1")});

        CreateAlterLoginRemoveGroup(runtime, ++txId, "/MyRoot", "group1");
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::HasNoGroup("group1")});
    }

    Y_UNIT_TEST(TestExternalLogin) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto resultLogin = LoginExternal(runtime, "user1@ldap");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 0});
        CheckToken(resultLogin.token(), describe, "user1@ldap");
    }

    Y_UNIT_TEST(TestExternalLoginWithIncorrectLdapDomain) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto resultLogin = Login(runtime, "ldapuser@ldap.domain", NLoginProto::ESaslAuthMech::Plain,
            NLoginProto::EHashType::ScramSha256, MakeTestPasswordHashes("password1").ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Cannot find user 'ldapuser@ldap.domain'");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.token(), "");
        auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 0});
    }

    Y_UNIT_TEST_FLAG(AddAccess_NonExisting, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        NACLib::TDiffACL diffACL;
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1");

        if (StrictAclCheck) {
            AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", diffACL.SerializeAsString(), "");
            TestModificationResults(runtime, txId, {{NKikimrScheme::StatusPreconditionFailed, "SID user1 not found in database `/MyRoot`"}});

            AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", NACLib::TDiffACL{}.SerializeAsString(), "user1");
            TestModificationResults(runtime, txId, {{NKikimrScheme::StatusPreconditionFailed, "Owner SID user1 not found in database `/MyRoot`"}});
        }

        const auto user1Hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
            {NLs::HasNoRight("+U:user1"), NLs::HasNoEffectiveRight("+U:user1"), NLs::HasOwner("root@builtin")});

        AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", diffACL.SerializeAsString(), "");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);

        AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", NACLib::TDiffACL{}.SerializeAsString(), "user1");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1"),
            {NLs::HasRight("+U:user1"), NLs::HasEffectiveRight("+U:user1"), NLs::HasOwner("user1")});
    }

    Y_UNIT_TEST_FLAG(AddAccess_NonYdb, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir1");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        {
            NACLib::TDiffACL diffACL;
            diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1@staff");
            AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", diffACL.SerializeAsString(), "");
            TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        }

        {
            AsyncModifyACL(runtime, ++txId, "/MyRoot", "Dir1", NACLib::TDiffACL{}.SerializeAsString(), "user1@staff");
            TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);
        }
    }

    Y_UNIT_TEST(DisableBuiltinAuthMechanism) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().AuthConfig.SetEnableLoginAuthentication(false);
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            Cerr << describe.DebugString() << Endl;
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        const auto user1Hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword, {{NKikimrScheme::StatusPreconditionFailed}});
        auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Login authentication is disabled");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.token(), "");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 0});
        }
    }

    Y_UNIT_TEST(FailedLoginUserUnderNameOfGroup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            Cerr << describe.DebugString() << Endl;
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        CreateAlterLoginCreateGroup(runtime, ++txId, "/MyRoot", "group1");
        auto resultLogin = Login(runtime, "group1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            MakeTestPasswordHashes("password1").ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "group1 is a group");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.token(), "");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
        }
    }

    Y_UNIT_TEST(FailedLoginWithInvalidUser) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            Cerr << describe.DebugString() << Endl;
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            MakeTestPasswordHashes("password1").ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Cannot find user 'user1'");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.token(), "");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 0});
        }
    }

    Y_UNIT_TEST(BanUnbanUser) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            Cerr << describe.DebugString() << Endl;
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        const auto user1Hashes = MakeTestPasswordHashes("123");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);
        auto resultLogin1 = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin1.error(), "");
        ChangeIsEnabledUser(runtime, ++txId, "/MyRoot", "user1", false);
        auto resultLogin2 = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin2.error(), "User user1 login denied: account is blocked");
        ChangeIsEnabledUser(runtime, ++txId, "/MyRoot", "user1", true);
        auto resultLogin3 = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin3.error(), "");
    }

    Y_UNIT_TEST(BanUserWithWaiting) {
        TTestBasicRuntime runtime;

        runtime.AddAppDataInit([] (ui32, NKikimr::TAppData& appData) {
            appData.AuthConfig.MutableAccountLockout()->SetAttemptResetDuration("3s");
        });

        TTestEnv env(runtime);
        auto accountLockoutConfig = runtime.GetAppData().AuthConfig.GetAccountLockout();
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            Cerr << describe.DebugString() << Endl;
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        const auto user1Hashes = MakeTestPasswordHashes("123");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);

        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold(); attempt++) {
            auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                MakeTestPasswordHashes("wrongpassword").ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }

        ChangeIsEnabledUser(runtime, ++txId, "/MyRoot", "user1", false);

        // User is blocked for 3 seconds
        Sleep(TDuration::Seconds(4));

        auto resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "User user1 login denied: account is blocked");
    }

    Y_UNIT_TEST(AccountLockoutAndAutomaticallyUnlock) {
        TTestBasicRuntime runtime;
        runtime.AddAppDataInit([] (ui32 nodeIdx, NKikimr::TAppData& appData) {
            Y_UNUSED(nodeIdx);
            auto accountLockout = appData.AuthConfig.MutableAccountLockout();
            accountLockout->SetAttemptThreshold(4);
            accountLockout->SetAttemptResetDuration("3s");
        });
        TTestEnv env(runtime);
        auto accountLockoutConfig = runtime.GetAppData().AuthConfig.GetAccountLockout();
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        const auto user1Hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);
        NKikimrScheme::TEvLoginResult resultLogin;
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold(); attempt++) {
            resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << attempt).ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold()).ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 login denied: too many failed password attempts");

        // Also do not accept correct password
        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 login denied: too many failed password attempts");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
        }

        // User is blocked for 3 seconds
        Sleep(TDuration::Seconds(4));

        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            MakeTestPasswordHashes("wrongpassword6").ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
            CheckToken(resultLogin.token(), describe, "user1");
        }
    }

    Y_UNIT_TEST(ResetFailedAttemptCount) {
        TTestBasicRuntime runtime;
        runtime.AddAppDataInit([] (ui32 nodeIdx, NKikimr::TAppData& appData) {
            Y_UNUSED(nodeIdx);
            auto accountLockout = appData.AuthConfig.MutableAccountLockout();
            accountLockout->SetAttemptThreshold(4);
            accountLockout->SetAttemptResetDuration("3s");
        });
        TTestEnv env(runtime);
        auto accountLockoutConfig = runtime.GetAppData().AuthConfig.GetAccountLockout();
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        const auto user1Hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);
        NKikimrScheme::TEvLoginResult resultLogin;
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold() - 1; attempt++) {
            resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << attempt).ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
        }

        // FailedAttemptCount will reset in 3 seconds
        Sleep(TDuration::Seconds(4));

        // FailedAttemptCount should be reset
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold() - 1; attempt++) {
            resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold() + attempt).ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
            CheckToken(resultLogin.token(), describe, "user1");
        }
    }

    Y_UNIT_TEST(ChangeAccountLockoutParameters) {
        TTestBasicRuntime runtime;
        runtime.AddAppDataInit([] (ui32 nodeIdx, NKikimr::TAppData& appData) {
            Y_UNUSED(nodeIdx);
            auto accountLockout = appData.AuthConfig.MutableAccountLockout();
            accountLockout->SetAttemptThreshold(4);
            accountLockout->SetAttemptResetDuration("3s");
        });
        TTestEnv env(runtime);
        auto accountLockoutConfig = runtime.GetAppData().AuthConfig.GetAccountLockout();
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        const auto user1Hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);
        NKikimrScheme::TEvLoginResult resultLogin;
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold(); attempt++) {
            resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << attempt).ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold()).ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 login denied: too many failed password attempts");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
        }

        // user is blocked for 3 seconds
        Sleep(TDuration::Seconds(4));

        // Unlock user after 3 seconds
        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
            CheckToken(resultLogin.token(), describe, "user1");
        }

        size_t newAttemptThreshold = 6;
        SetAccountLockoutParameters(runtime, TTestTxConfig::SchemeShard, {.AttemptThreshold = newAttemptThreshold, .AttemptResetDuration = "7s"});

        const auto user2Hashes = MakeTestPasswordHashes("password2");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", user2Hashes.HashedPassword);
        // Now user2 have 6 attempts to login
        for (size_t attempt = 0; attempt < newAttemptThreshold; attempt++) {
            resultLogin = Login(runtime, "user2", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                MakeTestPasswordHashes(TStringBuilder() << "wrongpassword2" << attempt).ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user2", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            MakeTestPasswordHashes(TStringBuilder() << "wrongpassword2" << newAttemptThreshold).ScramServerKey);
        // User is not permitted to log in after 6 attempts
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user2 login denied: too many failed password attempts");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 2});
        }

        // user2 is blocked for 7 seconds
        // After 4 seconds user2 must be locked out
        Sleep(TDuration::Seconds(4));
        resultLogin = Login(runtime, "user2", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            MakeTestPasswordHashes("wrongpassword28").ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user2 login denied: too many failed password attempts");

        // After 7 seconds user2 must be unlocked
        Sleep(TDuration::Seconds(8));

        // Unlock user after 7 sec
        resultLogin = Login(runtime, "user2", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user2Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 2});
            CheckToken(resultLogin.token(), describe, "user2");
        }
    }

    Y_UNIT_TEST(UserStayLockedOutIfEnterValidPassword) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto accountLockoutConfig = runtime.GetAppData().AuthConfig.GetAccountLockout();
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        const auto user1Hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);
        NKikimrScheme::TEvLoginResult resultLogin;
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold(); attempt++) {
            resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << attempt).ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold()).ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 login denied: too many failed password attempts");

        // Also do not accept correct password
        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 login denied: too many failed password attempts");
    }

    Y_UNIT_TEST(CheckThatLockedOutParametersIsRestoredFromLocalDb) {
        TTestBasicRuntime runtime;
        runtime.AddAppDataInit([] (ui32 nodeIdx, NKikimr::TAppData& appData) {
            Y_UNUSED(nodeIdx);
            auto accountLockout = appData.AuthConfig.MutableAccountLockout();
            accountLockout->SetAttemptThreshold(4);
            accountLockout->SetAttemptResetDuration("3s");
        });
        TTestEnv env(runtime);
        auto accountLockoutConfig = runtime.GetAppData().AuthConfig.GetAccountLockout();
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        const auto user1Hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);
        // Make 2 failed login attempts
        NKikimrScheme::TEvLoginResult resultLogin;
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold() / 2; attempt++) {
            resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << attempt).ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // After reboot schemeshard user has only 2 attempts to successful login before lock out
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold() / 2; attempt++) {
            resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << attempt).ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold()).ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 login denied: too many failed password attempts");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 2, .SidsSize = 1});
        }

        Sleep(TDuration::Seconds(2));
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // After reboot schemeshard user1 must be locked out
        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold()).ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 login denied: too many failed password attempts");

        // User1 must be unlocked in 1 second after reboot schemeshard
        Sleep(TDuration::Seconds(2));

        resultLogin = Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 3, .SidsSize = 1});
            CheckToken(resultLogin.token(), describe, "user1");
        }
    }

    Y_UNIT_TEST(ResetFailedAttemptCountAfterModifyUser) {
        TTestBasicRuntime runtime;
        runtime.AddAppDataInit([] (ui32 nodeIdx, NKikimr::TAppData& appData) {
            Y_UNUSED(nodeIdx);
            auto accountLockout = appData.AuthConfig.MutableAccountLockout();
            accountLockout->SetAttemptThreshold(4);
            accountLockout->SetAttemptResetDuration("3s");
        });

        TTestEnv env(runtime);
        auto accountLockoutConfig = runtime.GetAppData().AuthConfig.GetAccountLockout();
        ui64 txId = 100;
        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        TString userName = "user1";
        TString userPassword = "password1";
        const auto userHashes = MakeTestPasswordHashes(userPassword);

        auto blockUser = [&]() {
            for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold(); attempt++) {
                auto resultLogin = Login(runtime, userName, NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                    MakeTestPasswordHashes(TStringBuilder() << "wrongpassword" << attempt).ScramServerKey);
                UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
            }
        };

        auto loginUser = [&](TString error) {
            auto resultLogin = Login(runtime, userName, NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
                userHashes.ScramServerKey);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), error);
        };

        auto reboot = [&]() {
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        };

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", userName, userHashes.HashedPassword);

        blockUser();
        loginUser(TStringBuilder() << "User " << userName << " login denied: too many failed password attempts");
        reboot();
        loginUser(TStringBuilder() << "User " << userName << " login denied: too many failed password attempts");
        ChangeIsEnabledUser(runtime, ++txId, "/MyRoot", userName, true);
        loginUser("");

        blockUser();
        ChangeIsEnabledUser(runtime, ++txId, "/MyRoot", userName, true);
        reboot();
        loginUser("");
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardLoginFinalize) {

    void TestSuccess(const TVector<TString>& admins, const TString& testUser, bool isAdmin) {
        TTestBasicRuntime runtime;
        if (!admins.empty()) {
            runtime.AddAppDataInit([&admins](ui32, NKikimr::TAppData& appData){
                for (const auto& admin : admins) {
                    appData.AdministrationAllowedSIDs.emplace_back(admin);
                }
            });
        }
        TTestEnv env(runtime);
        ui64 txId = 100;

        const auto userHashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", testUser, userHashes.HashedPassword);

        const auto check = NLogin::TLoginProvider::TPasswordCheckResult{.Status =
            NLogin::TLoginProvider::TPasswordCheckResult::EStatus::SUCCESS};
        const auto request = NLogin::TLoginProvider::TLoginUserRequest({.User = testUser});
        // public keys are filled after the first login
        UNIT_ASSERT_VALUES_EQUAL(Login(runtime, testUser, NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            MakeTestPasswordHashes("wrong-password1").ScramServerKey).error(), "Invalid password");
        const auto resultLogin = LoginFinalize(runtime, request, check, "", false);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        CheckToken(resultLogin.token(), describe, testUser);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetIsAdmin(), isAdmin);
    }

    Y_UNIT_TEST(NoPublicKeys) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const auto user1Hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);

        NLogin::TLoginProvider::TPasswordCheckResult check;
        check.FillInvalidPassword();
        const auto request = NLogin::TLoginProvider::TLoginUserRequest({.User = "user1"});
        const auto resultLogin = LoginFinalize(runtime, request, check, "", false);
        // public keys are filled after the first login
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "No key to generate token");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.token(), "");
    }

    Y_UNIT_TEST(InvalidPassword) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const auto user1Hashes = MakeTestPasswordHashes("password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", user1Hashes.HashedPassword);

        NLogin::TLoginProvider::TPasswordCheckResult check;
        check.FillInvalidPassword();
        const auto request = NLogin::TLoginProvider::TLoginUserRequest({.User = "user1"});
        // public keys are filled after the first login
        UNIT_ASSERT_VALUES_EQUAL(Login(runtime, "user1", NLoginProto::ESaslAuthMech::Plain, NLoginProto::EHashType::ScramSha256,
            user1Hashes.ScramServerKey).error(), "");
        const auto resultLogin = LoginFinalize(runtime, request, check, "", false);
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.token(), "");
    }

    Y_UNIT_TEST(Success) {
        TestSuccess({}, "user1", true);
        TestSuccess({"user-admin"}, "user1", false);
        TestSuccess({"user1"}, "user1", true);
    }
}
