#include <util/string/join.h>

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/library/login/login.h>
#include <ydb/library/login/password_checker/password_checker.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/testlib/service_mocks/ldap_mock/ldap_simple_server.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/auditlog_helpers.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/ticket_parser.h>
#include <ydb/core/security/login_page.h>
#include <ydb/core/security/ldap_auth_provider/ldap_auth_provider.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace NSchemeShardUT_Private {

void SetPasswordCheckerParameters(TTestActorRuntime &runtime, ui64 schemeShard, const NLogin::TPasswordComplexity::TInitializer& initializer) {
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();

    ::NKikimrProto::TPasswordComplexity passwordComplexity;
    passwordComplexity.SetMinLength(initializer.MinLength);
    passwordComplexity.SetMinLowerCaseCount(initializer.MinLowerCaseCount);
    passwordComplexity.SetMinUpperCaseCount(initializer.MinUpperCaseCount);
    passwordComplexity.SetMinNumbersCount(initializer.MinNumbersCount);
    passwordComplexity.SetMinSpecialCharsCount(initializer.MinSpecialCharsCount);
    passwordComplexity.SetSpecialChars(initializer.SpecialChars);
    passwordComplexity.SetCanContainUsername(initializer.CanContainUsername);
    *request->Record.MutableConfig()->MutableAuthConfig()->MutablePasswordComplexity() = passwordComplexity;
    SetConfig(runtime, schemeShard, std::move(request));
}

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

    Y_UNIT_TEST(UserLogin) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            Cerr << describe.DebugString() << Endl;
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 1});
        }

        // public keys are filled after the first login
        auto resultLogin = Login(runtime, "user1", "password1");
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

    Y_UNIT_TEST_FLAG(RemoveUser, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", "password2");
        auto resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        CreateAlterLoginRemoveUser(runtime, ++txId, "/MyRoot", "user1");

        // check user has been removed:
        {
            auto resultLogin = Login(runtime, "user1", "password1");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetError(), "Cannot find user: user1");
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
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", "password2");
        auto resultLogin = Login(runtime, "user1", "password1");
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
            auto resultLogin = Login(runtime, "user1", "password1");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetError(), "Cannot find user: user1");
        }
    }

    Y_UNIT_TEST_FLAG(RemoveUser_Owner, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", "password2");
        auto resultLogin = Login(runtime, "user1", "password1");
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
                auto resultLogin = Login(runtime, "user1", "password1");
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
            auto resultLogin = Login(runtime, "user1", "password1");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetError(), "Cannot find user: user1");
        }
    }

    Y_UNIT_TEST_FLAG(RemoveUser_Acl, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", "password2");
        auto resultLogin = Login(runtime, "user1", "password1");
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
                auto resultLogin = Login(runtime, "user1", "password1");
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
            auto resultLogin = Login(runtime, "user1", "password1");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.GetError(), "Cannot find user: user1");
        }
    }

    Y_UNIT_TEST_FLAG(RemoveGroup, StrictAclCheck) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableStrictAclCheck(StrictAclCheck));
        ui64 txId = 100;

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
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
        auto resultLogin = Login(runtime, "user1@ldap", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
        auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
        CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 0});
        CheckToken(resultLogin.token(), describe, "user1@ldap");
    }

    Y_UNIT_TEST(TestExternalLoginWithIncorrectLdapDomain) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto resultLogin = Login(runtime, "ldapuser@ldap.domain", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Cannot find user: ldapuser@ldap.domain");
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

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");

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

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1", {{NKikimrScheme::StatusPreconditionFailed}});
        auto resultLogin = Login(runtime, "user1", "password1");
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
        auto resultLogin = Login(runtime, "group1", "password1");
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

        auto resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Cannot find user: user1");
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

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "123");
        auto resultLogin1 = Login(runtime, "user1", "123");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin1.error(), "");
        ChangeIsEnabledUser(runtime, ++txId, "/MyRoot", "user1", false);
        auto resultLogin2 = Login(runtime, "user1", "123");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin2.error(), "User user1 is not permitted to log in");
        ChangeIsEnabledUser(runtime, ++txId, "/MyRoot", "user1", true);
        auto resultLogin3 = Login(runtime, "user1", "123");
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

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "123");

        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold(); attempt++) {
            auto resultLogin = Login(runtime, "user1", "wrongpassword");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }

        ChangeIsEnabledUser(runtime, ++txId, "/MyRoot", "user1", false);

        // User is blocked for 3 seconds
        Sleep(TDuration::Seconds(4));

        auto resultLogin = Login(runtime, "user1", "123");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "User user1 is not permitted to log in");
    }

    Y_UNIT_TEST(ChangeAcceptablePasswordParameters) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            Cerr << describe.DebugString() << Endl;
            CheckSecurityState(describe, {.PublicKeysSize = 0, .SidsSize = 0});
        }

        // Password parameters:
        //  min length 0
        //  optional: lower case, upper case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        // required: cannot contain username

        {
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "Pass_word1");
            auto resultLogin = Login(runtime, "user1", "Pass_word1");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
            CheckToken(resultLogin.token(), describe, "user1");
        }

        // Accept password without lower case symbols
        {
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", "PASSWORDU2");
            auto resultLogin = Login(runtime, "user2", "PASSWORDU2");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 2});
            CheckToken(resultLogin.token(), describe, "user2");
        }

        // Password parameters:
        //  min length 0
        //  optional: upper case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: lower case = 3, cannot contain username
        {
            SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinLowerCaseCount = 3});
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user3", "PASSWORDU3", {{NKikimrScheme::StatusPreconditionFailed, "Incorrect password format: should contain at least 3 lower case character"}});
            auto resultLogin = Login(runtime, "user3", "PASSWORDU3");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Cannot find user: user3");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 2});
        }

        // Add lower case symbols to password
        {
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user3", "PASswORDu3");
            auto resultLogin = Login(runtime, "user3", "PASswORDu3");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 3});
            CheckToken(resultLogin.token(), describe, "user3");
        }

        // Accept password without upper case symbols
        {
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user4", "passwordu4");
            auto resultLogin = Login(runtime, "user4", "passwordu4");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 4});
            CheckToken(resultLogin.token(), describe, "user4");
        }

        // Password parameters:
        //  min length 0
        //  optional: lower case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: upper case = 3, cannot contain username
        {
            SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinLowerCaseCount = 0, .MinUpperCaseCount = 3});
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user5", "passwordu5", {{NKikimrScheme::StatusPreconditionFailed, "Incorrect password format: should contain at least 3 upper case character"}});
            auto resultLogin = Login(runtime, "user5", "passwordu5");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Cannot find user: user5");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 4});
        }

        // Add 3 upper case symbols to password
        {
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user5", "PASswORDu5");
            auto resultLogin = Login(runtime, "user5", "PASswORDu5");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 5});
            CheckToken(resultLogin.token(), describe, "user5");
        }

        // Accept short password
        {
            SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinUpperCaseCount = 0});
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user6", "passwu6");
            auto resultLogin = Login(runtime, "user6", "passwu6");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 6});
            CheckToken(resultLogin.token(), describe, "user6");
        }

        // Password parameters:
        //  min length 8
        //  optional: lower case, upper case, numbers, special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: cannot contain username
        // Too short password
        {
            SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinLength = 8});
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user7", "passwu7", {{NKikimrScheme::StatusPreconditionFailed, "Password is too short"}});
            auto resultLogin = Login(runtime, "user7", "passwu7");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Cannot find user: user7");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 6});
        }

        // Password has correct length
        {
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user7", "passwordu7");
            auto resultLogin = Login(runtime, "user7", "passwordu7");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 7});
            CheckToken(resultLogin.token(), describe, "user7");
        }

        // Accept password without numbers
        {
            SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinLength = 0});
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user8", "passWorDueitgh");
            auto resultLogin = Login(runtime, "user8", "passWorDueitgh");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 8});
            CheckToken(resultLogin.token(), describe, "user8");
        }

        // Password parameters:
        //  min length 0
        //  optional: lower case, upper case,special symbols from list !@#$%^&*()_+{}|<>?=
        //  required: numbers = 3, cannot contain username
        {
            SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinNumbersCount = 3});
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user9", "passwordunine", {{NKikimrScheme::StatusPreconditionFailed, "Incorrect password format: should contain at least 3 number"}});
            auto resultLogin = Login(runtime, "user9", "passwordunine");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Cannot find user: user9");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 8});
        }

        // Password with numbers
        {
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user9", "pas1swo5rdu9");
            auto resultLogin = Login(runtime, "user9", "pas1swo5rdu9");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 9});
            CheckToken(resultLogin.token(), describe, "user9");
        }

        // Accept password without special symbols
        {
            SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinNumbersCount = 0});
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user10", "passWorDu10");
            auto resultLogin = Login(runtime, "user10", "passWorDu10");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 10});
            CheckToken(resultLogin.token(), describe, "user10");
        }

        // Password parameters:
        //  min length 0
        //  optional: lower case, upper case, numbers
        //  required: special symbols from list !@#$%^&*()_+{}|<>?= , cannot contain username
        {
            SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.MinSpecialCharsCount = 3});
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user11", "passwordu11", {{NKikimrScheme::StatusPreconditionFailed, "Incorrect password format: should contain at least 3 special character"}});
            auto resultLogin = Login(runtime, "user11", "passwordu11");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Cannot find user: user11");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 10});
        }

        // Password with special symbols
        {
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user11", "passwordu11*&%#");
            auto resultLogin = Login(runtime, "user11", "passwordu11*&%#");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 11});
            CheckToken(resultLogin.token(), describe, "user11");
        }

        // Password parameters:
        //  min length 0
        //  optional: lower case, upper case, numbers
        //  required: special symbols from list *# , cannot contain username
        {
            SetPasswordCheckerParameters(runtime, TTestTxConfig::SchemeShard, {.SpecialChars = "*#"}); // Only 2 special symbols are valid
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user12", "passwordu12*&%#", {{NKikimrScheme::StatusPreconditionFailed, "Password contains unacceptable characters"}});
            auto resultLogin = Login(runtime, "user12", "passwordu12*&%#");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Cannot find user: user12");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 11});
        }

        {
            CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user12", "passwordu12*#");
            auto resultLogin = Login(runtime, "user12", "passwordu12*#");
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 12});
            CheckToken(resultLogin.token(), describe, "user12");
        }
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

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        NKikimrScheme::TEvLoginResult resultLogin;
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold(); attempt++) {
            resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << attempt);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold());
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 is not permitted to log in");

        // Also do not accept correct password
        resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 is not permitted to log in");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
        }

        // User is blocked for 3 seconds
        Sleep(TDuration::Seconds(4));

        resultLogin = Login(runtime, "user1", "wrongpassword6");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        resultLogin = Login(runtime, "user1", "password1");
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

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        NKikimrScheme::TEvLoginResult resultLogin;
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold() - 1; attempt++) {
            resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << attempt);
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
            resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold() + attempt);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user1", "password1");
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

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        NKikimrScheme::TEvLoginResult resultLogin;
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold(); attempt++) {
            resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << attempt);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold());
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 is not permitted to log in");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
        }

        // user is blocked for 3 seconds
        Sleep(TDuration::Seconds(4));

        // Unlock user after 3 seconds
        resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 1});
            CheckToken(resultLogin.token(), describe, "user1");
        }

        size_t newAttemptThreshold = 6;
        SetAccountLockoutParameters(runtime, TTestTxConfig::SchemeShard, {.AttemptThreshold = newAttemptThreshold, .AttemptResetDuration = "7s"});

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user2", "password2");
        // Now user2 have 6 attempts to login
        for (size_t attempt = 0; attempt < newAttemptThreshold; attempt++) {
            resultLogin = Login(runtime, "user2", TStringBuilder() << "wrongpassword2" << attempt);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user2", TStringBuilder() << "wrongpassword2" << newAttemptThreshold);
        // User is not permitted to log in after 6 attempts
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user2 is not permitted to log in");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 1, .SidsSize = 2});
        }

        // user2 is blocked for 7 seconds
        // After 4 seconds user2 must be locked out
        Sleep(TDuration::Seconds(4));
        resultLogin = Login(runtime, "user2", "wrongpassword28");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user2 is not permitted to log in");

        // After 7 seconds user2 must be unlocked
        Sleep(TDuration::Seconds(8));

        // Unlock user after 7 sec
        resultLogin = Login(runtime, "user2", "password2");
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

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        NKikimrScheme::TEvLoginResult resultLogin;
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold(); attempt++) {
            resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << attempt);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold());
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 is not permitted to log in");

        // Also do not accept correct password
        resultLogin = Login(runtime, "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 is not permitted to log in");
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

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        // Make 2 failed login attempts
        NKikimrScheme::TEvLoginResult resultLogin;
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold() / 2; attempt++) {
            resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << attempt);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // After reboot schemeshard user has only 2 attempts to successful login before lock out
        for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold() / 2; attempt++) {
            resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << attempt);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
        }
        resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold());
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 is not permitted to log in");

        {
            auto describe = DescribePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            CheckSecurityState(describe, {.PublicKeysSize = 2, .SidsSize = 1});
        }

        Sleep(TDuration::Seconds(2));
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // After reboot schemeshard user1 must be locked out
        resultLogin = Login(runtime, "user1", TStringBuilder() << "wrongpassword" << accountLockoutConfig.GetAttemptThreshold());
        UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), TStringBuilder() << "User user1 is not permitted to log in");

        // User1 must be unlocked in 1 second after reboot schemeshard
        Sleep(TDuration::Seconds(2));

        resultLogin = Login(runtime, "user1", "password1");
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

        auto blockUser = [&]() {
            for (size_t attempt = 0; attempt < accountLockoutConfig.GetAttemptThreshold(); attempt++) {
                auto resultLogin = Login(runtime, userName, TStringBuilder() << "wrongpassword" << attempt);
                UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), "Invalid password");
            }
        };

        auto loginUser = [&](TString error) {
            auto resultLogin = Login(runtime, userName, userPassword);
            UNIT_ASSERT_VALUES_EQUAL(resultLogin.error(), error);
        };

        auto reboot = [&]() {
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        };

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", userName, userPassword);

        blockUser();
        loginUser(TStringBuilder() << "User " << userName << " is not permitted to log in");
        reboot();
        loginUser(TStringBuilder() << "User " << userName << " is not permitted to log in");
        ChangeIsEnabledUser(runtime, ++txId, "/MyRoot", userName, true);
        loginUser("");

        blockUser();
        ChangeIsEnabledUser(runtime, ++txId, "/MyRoot", userName, true);
        reboot();
        loginUser("");
    }
}

namespace NSchemeShardUT_Private {

void EatWholeString(NHttp::THttpIncomingRequestPtr request, const TString& data) {
    request->EnsureEnoughSpaceAvailable(data.size());
    auto size = std::min(request->Avail(), data.size());
    memcpy(request->Pos(), data.data(), size);
    request->Advance(size);
}

NHttp::THttpIncomingRequestPtr MakeLoginRequest(const TString& user, const TString& password) {
    TString payload = [](const auto& user, const auto& password) {
        NJson::TJsonValue value;
        value["user"] = user;
        value["password"] = password;
        return NJson::WriteJson(value, false);
    }(user, password);
    TStringBuilder text;
    text << "POST /login HTTP/1.1\r\n"
        << "Host: test.ydb\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << payload.size() << "\r\n"
        << "\r\n"
        << payload;
    NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
    EatWholeString(request, text);
    // WebLoginService will crash without address
    request->Address = std::make_shared<TSockAddrInet>("127.0.0.1", 0);
    // Cerr << "TEST: http login request: " << text << Endl;
    return request;
}

NHttp::THttpIncomingRequestPtr MakeLogoutRequest(const TString& cookieName, const TString& cookieValue) {
    TStringBuilder text;
    text << "POST /logout HTTP/1.1\r\n"
        << "Host: test.ydb\r\n"
        << "Content-Type: text/plain\r\n"
        << "Cookie: " << cookieName << "=" << cookieValue << "\r\n"
        << "\r\n";
    NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
    EatWholeString(request, text);
    // WebLoginService will crash without address
    request->Address = std::make_shared<TSockAddrInet>("127.0.0.1", 0);
    // Cerr << "TEST: http logout request: " << text << Endl;
    return request;
}

}

Y_UNIT_TEST_SUITE(TWebLoginService) {
    void AuditLogLoginTest(TTestBasicRuntime& runtime, bool isUserAdmin) {
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        TTestEnv env(runtime);

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        ui64 txId = 100;

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);   // +user creation

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "200");
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(!cookies["ydb_session_id"].empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=SUCCESS");
        UNIT_ASSERT(!last.contains("reason"));
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token=");
        UNIT_ASSERT(last.find("sanitized_token={none}") == std::string::npos);

        if (isUserAdmin) {
            UNIT_ASSERT_STRING_CONTAINS(last, "login_user_level=admin");
        } else {
            UNIT_ASSERT(!last.contains("login_user_level=admin"));
        }
    }

    Y_UNIT_TEST(AuditLogEmptySIDsLoginSuccess) {
        TTestBasicRuntime runtime;
        AuditLogLoginTest(runtime, true);
    }

    Y_UNIT_TEST(AuditLogAdminLoginSuccess) {
        TTestBasicRuntime runtime;
        runtime.AddAppDataInit([](ui32, NKikimr::TAppData& appData){
            appData.AdministrationAllowedSIDs.emplace_back("user1");
        });
        AuditLogLoginTest(runtime, true);
    }

    Y_UNIT_TEST(AuditLogLoginSuccess) {
        TTestBasicRuntime runtime;
        runtime.AddAppDataInit([](ui32, NKikimr::TAppData& appData){
            appData.AdministrationAllowedSIDs.emplace_back("user2");
        });
        AuditLogLoginTest(runtime, false);
    }

    Y_UNIT_TEST(AuditLogLoginBadPassword) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        TTestEnv env(runtime);

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        ui64 txId = 100;

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);   // +user creation

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        TString ydbSessionId;
        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1", "bad_password")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "403");
            NJson::TJsonValue body(responseEv->Response->Body);
            UNIT_ASSERT_STRINGS_EQUAL(body.GetStringRobust(), "{\"error\":\"Invalid password\"}");
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Invalid password");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    Y_UNIT_TEST(AuditLogLdapLoginSuccess) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        // Enable and configure ldap auth
        runtime.SetLogPriority(NKikimrServices::LDAP_AUTH_PROVIDER, NActors::NLog::PRI_DEBUG);
        TTestEnv env(runtime);

        // configure ldap auth
        auto ldapPort = runtime.GetPortManager().GetPort();  // randomized port
        {
            auto& appData = runtime.GetAppData();
            appData.AuthConfig.SetUseBlackBox(false);
            appData.AuthConfig.SetUseLoginProvider(true);
            auto& ldapSettings = *appData.AuthConfig.MutableLdapAuthentication();
            ldapSettings.MutableUseTls()->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::NEVER);
            ldapSettings.SetPort(ldapPort);
            ldapSettings.AddHosts("localhost");
            ldapSettings.SetBaseDn("dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindPassword("robouserPassword");
            ldapSettings.SetSearchFilter("uid=$username");
        }

        // start ldap auth provider service
        {
            IActor* ldapAuthProvider = NKikimr::CreateLdapAuthProvider(runtime.GetAppData().AuthConfig.GetLdapAuthentication());
            TActorId ldapAuthProviderId = runtime.Register(ldapAuthProvider);
            runtime.RegisterService(MakeLdapAuthProviderID(), ldapAuthProviderId);
        }

        // start mock ldap server with predefined responses
        auto ldapResponses = [](const TString& login, const TString& password) -> LdapMock::TLdapMockResponses {
            return LdapMock::TLdapMockResponses{
                .BindResponses = {
                    {
                        {{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}},
                        {.Status = LdapMock::EStatus::SUCCESS}
                    },
                    {
                        {{.Login = "uid=" + login + ",dc=search,dc=yandex,dc=net", .Password = password}},
                        {.Status = LdapMock::EStatus::SUCCESS}
                    },
                },
                .SearchResponses = {
                    {
                        {{
                            .BaseDn = "dc=search,dc=yandex,dc=net",
                            .Scope = 2,
                            .DerefAliases = 0,
                            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
                            .Attributes = {"1.1"},
                        }},
                        {
                            .ResponseEntries = {{.Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net"}},
                            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
                        }
                    },
                },
            };
        };
        LdapMock::TLdapSimpleServer ldapServer(ldapPort, ldapResponses("user1", "password1"));

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1@ldap", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "200", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(!cookies["ydb_session_id"].empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=SUCCESS");
        UNIT_ASSERT(!last.contains("detailed_status"));
        UNIT_ASSERT(!last.contains("reason"));
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token=");
        UNIT_ASSERT(last.find("sanitized_token={none}") == std::string::npos);
    }

    Y_UNIT_TEST(AuditLogLdapLoginBadPassword) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        // Enable and configure ldap auth
        runtime.SetLogPriority(NKikimrServices::LDAP_AUTH_PROVIDER, NActors::NLog::PRI_DEBUG);
        TTestEnv env(runtime);

        // configure ldap auth
        auto ldapPort = runtime.GetPortManager().GetPort();  // randomized port
        {
            auto& appData = runtime.GetAppData();
            appData.AuthConfig.SetUseBlackBox(false);
            appData.AuthConfig.SetUseLoginProvider(true);
            auto& ldapSettings = *appData.AuthConfig.MutableLdapAuthentication();
            ldapSettings.MutableUseTls()->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::NEVER);
            ldapSettings.SetPort(ldapPort);
            ldapSettings.AddHosts("localhost");
            ldapSettings.SetBaseDn("dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindPassword("robouserPassword");
            ldapSettings.SetSearchFilter("uid=$username");
        }

        // start ldap auth provider service
        {
            IActor* ldapAuthProvider = NKikimr::CreateLdapAuthProvider(runtime.GetAppData().AuthConfig.GetLdapAuthentication());
            TActorId ldapAuthProviderId = runtime.Register(ldapAuthProvider);
            runtime.RegisterService(MakeLdapAuthProviderID(), ldapAuthProviderId);
        }

        // start mock ldap server with predefined responses
        auto ldapResponses = [](const TString& login, const TString& password) -> LdapMock::TLdapMockResponses {
            return LdapMock::TLdapMockResponses{
                .BindResponses = {
                    {
                        {{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}},
                        {.Status = LdapMock::EStatus::SUCCESS}
                    },
                    {
                        {{.Login = "uid=" + login + ",dc=search,dc=yandex,dc=net", .Password = password}},
                        {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}
                    },
                },
                .SearchResponses = {
                    {
                        {{
                            .BaseDn = "dc=search,dc=yandex,dc=net",
                            .Scope = 2,
                            .DerefAliases = 0,
                            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
                            .Attributes = {"1.1"},
                        }},
                        {
                            .ResponseEntries = {{.Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net"}},
                            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
                        }
                    },
                },
            };
        };
        LdapMock::TLdapSimpleServer ldapServer(ldapPort, ldapResponses("user1", "bad_password"));

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1@ldap", "bad_password")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "403", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(cookies["ydb_session_id"].empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "detailed_status=UNAUTHORIZED");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Could not login via LDAP: LDAP login failed for user uid=user1,dc=search,dc=yandex,dc=net on server ldap://localhost:");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    Y_UNIT_TEST(AuditLogLdapLoginBadUser) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        // Enable and configure ldap auth
        runtime.SetLogPriority(NKikimrServices::LDAP_AUTH_PROVIDER, NActors::NLog::PRI_DEBUG);
        TTestEnv env(runtime);

        // configure ldap auth
        auto ldapPort = runtime.GetPortManager().GetPort();  // randomized port
        {
            auto& appData = runtime.GetAppData();
            appData.AuthConfig.SetUseBlackBox(false);
            appData.AuthConfig.SetUseLoginProvider(true);
            auto& ldapSettings = *appData.AuthConfig.MutableLdapAuthentication();
            ldapSettings.MutableUseTls()->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::NEVER);
            ldapSettings.SetPort(ldapPort);
            ldapSettings.AddHosts("localhost");
            ldapSettings.SetBaseDn("dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindPassword("robouserPassword");
            ldapSettings.SetSearchFilter("uid=$username");
        }

        // start ldap auth provider service
        {
            IActor* ldapAuthProvider = NKikimr::CreateLdapAuthProvider(runtime.GetAppData().AuthConfig.GetLdapAuthentication());
            TActorId ldapAuthProviderId = runtime.Register(ldapAuthProvider);
            runtime.RegisterService(MakeLdapAuthProviderID(), ldapAuthProviderId);
        }

        // start mock ldap server with predefined responses
        auto ldapResponses = [](const TString& login, const TString& password) -> LdapMock::TLdapMockResponses {
            return LdapMock::TLdapMockResponses{
                .BindResponses = {
                    {
                        {{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}},
                        {.Status = LdapMock::EStatus::SUCCESS}
                    },
                    {
                        {{.Login = "uid=" + login + ",dc=search,dc=yandex,dc=net", .Password = password}},
                        {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}
                    },
                },
                .SearchResponses = {
                    {
                        {{
                            .BaseDn = "dc=search,dc=yandex,dc=net",
                            .Scope = 2,
                            .DerefAliases = 0,
                            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
                            .Attributes = {"1.1"},
                        }},
                        {
                            .ResponseEntries = {},
                            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
                        }
                    },
                },
            };
        };
        LdapMock::TLdapSimpleServer ldapServer(ldapPort, ldapResponses("bad_user", "password1"));

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("bad_user@ldap", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "403", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(cookies["ydb_session_id"].empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "detailed_status=UNAUTHORIZED");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Could not login via LDAP: LDAP user bad_user does not exist. LDAP search for filter uid=bad_user on server ldap://localhost:");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=bad_user@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    // LDAP responses to bad BindDn or bad BindPassword are the same, so this test covers the both cases.
    Y_UNIT_TEST(AuditLogLdapLoginBadBind) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        // Enable and configure ldap auth
        runtime.SetLogPriority(NKikimrServices::LDAP_AUTH_PROVIDER, NActors::NLog::PRI_DEBUG);
        TTestEnv env(runtime);

        // configure ldap auth
        auto ldapPort = runtime.GetPortManager().GetPort();  // randomized port
        {
            auto& appData = runtime.GetAppData();
            appData.AuthConfig.SetUseBlackBox(false);
            appData.AuthConfig.SetUseLoginProvider(true);
            auto& ldapSettings = *appData.AuthConfig.MutableLdapAuthentication();
            ldapSettings.MutableUseTls()->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::NEVER);
            ldapSettings.SetPort(ldapPort);
            ldapSettings.AddHosts("localhost");
            ldapSettings.SetBaseDn("dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
            ldapSettings.SetBindPassword("robouserPassword");
            ldapSettings.SetSearchFilter("uid=$username");
        }

        // start ldap auth provider service
        {
            IActor* ldapAuthProvider = NKikimr::CreateLdapAuthProvider(runtime.GetAppData().AuthConfig.GetLdapAuthentication());
            TActorId ldapAuthProviderId = runtime.Register(ldapAuthProvider);
            runtime.RegisterService(MakeLdapAuthProviderID(), ldapAuthProviderId);
        }

        // start mock ldap server with predefined responses
        auto ldapResponses = [](const TString& login, const TString& password) -> LdapMock::TLdapMockResponses {
            return LdapMock::TLdapMockResponses{
                .BindResponses = {
                    {
                        {{.Login = "cn=robouser,dc=search,dc=yandex,dc=net", .Password = "robouserPassword"}},
                        {.Status = LdapMock::EStatus::INVALID_CREDENTIALS}
                    },
                    {
                        {{.Login = "uid=" + login + ",dc=search,dc=yandex,dc=net", .Password = password}},
                        {.Status = LdapMock::EStatus::SUCCESS}
                    },
                },
                .SearchResponses = {
                    {
                        {{
                            .BaseDn = "dc=search,dc=yandex,dc=net",
                            .Scope = 2,
                            .DerefAliases = 0,
                            .Filter = {.Type = LdapMock::EFilterType::LDAP_FILTER_EQUALITY, .Attribute = "uid", .Value = login},
                            .Attributes = {"1.1"},
                        }},
                        {
                            .ResponseEntries = {{.Dn = "uid=" + login + ",dc=search,dc=yandex,dc=net"}},
                            .ResponseDone = {.Status = LdapMock::EStatus::SUCCESS}
                        }
                    },
                },
            };
        };
        LdapMock::TLdapSimpleServer ldapServer(ldapPort, ldapResponses("user1", "password1"));

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);   // alter root subdomain

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1@ldap", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL_C(responseEv->Response->Status, "403", responseEv->Response->Body);
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            UNIT_ASSERT(cookies["ydb_session_id"].empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);  // +user login

        auto last = FindAuditLine(lines, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "component=grpc-login");
        UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=localhost");
        UNIT_ASSERT_STRING_CONTAINS(last, "database=/MyRoot");
        UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGIN");
        UNIT_ASSERT_STRING_CONTAINS(last, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(last, "detailed_status=UNAUTHORIZED");
        UNIT_ASSERT_STRING_CONTAINS(last, "reason=Could not login via LDAP: Could not perform initial LDAP bind for dn cn=robouser,dc=search,dc=yandex,dc=net on server ldap://localhost:");
        UNIT_ASSERT_STRING_CONTAINS(last, "login_user=user1@ldap");
        UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token={none}");
    }

    Y_UNIT_TEST(AuditLogLogout) {
        TTestBasicRuntime runtime;
        std::vector<std::string> lines;
        runtime.AuditLogBackends = std::move(CreateTestAuditLogBackends(lines));
        TTestEnv env(runtime);

        // Add ticket parser to the mix
        {
            NKikimrProto::TAuthConfig authConfig;
            authConfig.SetUseBlackBox(false);
            authConfig.SetUseLoginProvider(true);

            IActor* ticketParser = NKikimr::CreateTicketParser({.AuthConfig = authConfig});
            TActorId ticketParserId = runtime.Register(ticketParser);
            runtime.RegisterService(NKikimr::MakeTicketParserID(), ticketParserId);
        }

        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1);  // alter root subdomain

        ui64 txId = 100;

        CreateAlterLoginCreateUser(runtime, ++txId, "/MyRoot", "user1", "password1");
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2);  // +user creation

        // test body
        const auto target = runtime.Register(CreateWebLoginService());
        const auto edge = runtime.AllocateEdgeActor();

        TString ydbSessionId;
        {
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLoginRequest("user1", "password1")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "200");
            NHttp::THeaders headers(responseEv->Response->Headers);
            NHttp::TCookies cookies(headers["Set-Cookie"]);
            ydbSessionId = cookies["ydb_session_id"];
        }
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);  // +user login

        // New security keys are created in the subdomain as a consequence of a login.
        // In real system they are transferred to the ticket parser by the grpc-proxy
        // on receiving subdomain update notification.
        // Here there are no grpc-proxy, so we should transfer keys to the ticket parser manually.
        {
            const auto describe = DescribePath(runtime, "/MyRoot");
            const auto& securityState = describe.GetPathDescription().GetDomainDescription().GetSecurityState();
            TActorId edge = runtime.AllocateEdgeActor();
            runtime.Send(new IEventHandle(MakeTicketParserID(), edge, new TEvTicketParser::TEvUpdateLoginSecurityState(securityState)), 0);
        }

        // Then we are ready to test some authentication on /logout
        {  // no cookie
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLogoutRequest("not-an-ydb_session_id", ydbSessionId)
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "401");

            // no audit record for actions without auth
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);
        }
        {  // bad cookie
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLogoutRequest("ydb_session_id", "jklhagsfjhg")
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "403");

            // no audit record for actions without auth
            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3);
        }
        {  // good cookie
            runtime.Send(new IEventHandle(target, edge, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(
                MakeLogoutRequest("ydb_session_id", ydbSessionId)
            )));

            TAutoPtr<IEventHandle> handle;
            auto responseEv = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_STRINGS_EQUAL(responseEv->Response->Status, "200");
            {
                NHttp::THeaders headers(responseEv->Response->Headers);
                UNIT_ASSERT_STRINGS_EQUAL(headers["Set-Cookie"], "ydb_session_id=; Max-Age=0");
            }

            UNIT_ASSERT_VALUES_EQUAL(lines.size(), 4);  // +user web logout

            auto last = FindAuditLine(lines, "operation=LOGOUT");
            UNIT_ASSERT_STRING_CONTAINS(last, "component=web-login");
            UNIT_ASSERT_STRING_CONTAINS(last, "remote_address=127.0.0.1");
            UNIT_ASSERT_STRING_CONTAINS(last, "subject=user1");
            UNIT_ASSERT_STRING_CONTAINS(last, "operation=LOGOUT");
            UNIT_ASSERT_STRING_CONTAINS(last, "status=SUCCESS");
            UNIT_ASSERT_STRING_CONTAINS(last, "sanitized_token=");
            UNIT_ASSERT(last.find("sanitized_token={none}") == std::string::npos);
        }
    }
}
