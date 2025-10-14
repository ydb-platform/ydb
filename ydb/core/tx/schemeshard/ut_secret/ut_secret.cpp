#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

namespace {
    using namespace NSchemeShardUT_Private;
    using NKikimrScheme::EStatus;

    void ExpectEqualSecretDescription(
        const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
        const TString& name,
        const TMaybe<TString>& value,
        const ui64 version
    ) {
        UNIT_ASSERT(describeResult.HasPathDescription());
        UNIT_ASSERT(describeResult.GetPathDescription().HasSecretDescription());
        const auto& secretDescription = describeResult.GetPathDescription().GetSecretDescription();
        UNIT_ASSERT_VALUES_EQUAL(secretDescription.GetName(), name);
        if (value) {
            UNIT_ASSERT_VALUES_EQUAL(secretDescription.GetValue(), *value);
        } else {
            UNIT_ASSERT(!secretDescription.HasValue());
        }

        UNIT_ASSERT_VALUES_EQUAL(secretDescription.GetVersion(), version);
    }

    NKikimrScheme::TEvDescribeSchemeResult DescribePathWithSecretValue(
        TTestBasicRuntime& runtime,
        const TString& path
    ) {
        NKikimrSchemeOp::TDescribeOptions opts;
        opts.SetReturnSecretValue(true);
        return DescribePath(runtime, path, opts);
    }

    void AssertHasAccess(
        const int directoryId,
        const ui32 inheritance,
        const bool expectedHasAccess,
        TTestBasicRuntime& runtime,
        ui64& txId,
        TTestEnv& env
    ) {
        /** This test
          * - creates a new directory "/MyRoot/dir" + ToString(directoryId)
          * - provide to the user some grants to this directory
          * - creates a secret in the new directory with InheritPermissions=True
          * - check grants for the secret
          */
        const TString user = "some-user";
        const auto userToken = NACLib::TUserToken(NACLib::TUserToken::TUserTokenInitFields{.UserSID = user});
        const TString& workingDir = "/MyRoot";

        // create container dir
        NACLib::TDiffACL diffACL;
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, user, inheritance);
        AsyncModifyACL(runtime, ++txId, workingDir, "dir" + ToString(directoryId), diffACL.SerializeAsString(), /* newOwner */ "");
        env.TestWaitNotification(runtime, txId);

        // create secret
        const TString workingDirPath = workingDir + "/dir" + ToString(directoryId);
        const TString secretName = "secret-name";
        TestCreateSecret(runtime, ++txId, workingDirPath,
            Sprintf(R"(
                Name: "%s"
                Value: "test-value"
                InheritPermissions: false
            )", secretName.data())
        );
        env.TestWaitNotification(runtime, txId);
        const TString secretPath = workingDirPath + "/" + secretName;
        TestLs(runtime, secretPath, false, NLs::PathExist);

        // assert access
        const auto describeResult = DescribePath(runtime, secretPath).GetPathDescription().GetSelf();
        const TSecurityObject secObj(describeResult.GetOwner(), describeResult.GetEffectiveACL(), /* isContainer */ false);
        UNIT_ASSERT_VALUES_EQUAL(expectedHasAccess, secObj.CheckAccess(NACLib::DescribeSchema, userToken));
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardSecretTest) {
    Y_UNIT_TEST(CreateSecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            ExpectEqualSecretDescription(describeResult, "test-secret", "test-value", 0);
        }

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        {
            const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            ExpectEqualSecretDescription(describeResult, "test-secret", "test-value", 0);
        }
    }

    Y_UNIT_TEST(DefaultDescribeSecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/dir/test-secret");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            ExpectEqualSecretDescription(describeResult, "test-secret", /* value */ Nothing(), 0);
        }

        // check that empty value is not the same as not set value
        TestAlterSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: ""
            )"
        );
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/dir/test-secret");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            ExpectEqualSecretDescription(describeResult, "test-secret", /* value */ Nothing(), 1);
        }
    }

    Y_UNIT_TEST(CreateSecretAndIntermediateDirs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSecret(runtime, ++txId, "/MyRoot",
            R"(
                Name: "dir1/dir2/test-secret"
                Value: "test-value"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir1/dir2/test-secret");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            ExpectEqualSecretDescription(describeResult, "test-secret", "test-value", 0);
        }
    }

    Y_UNIT_TEST(CreateSecretInSubdomain) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "SubDomain"
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateSecret(runtime, ++txId, "/MyRoot/SubDomain",
            R"(
                Name: "test-secret"
                Value: "test-value"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/SubDomain/test-secret");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            ExpectEqualSecretDescription(describeResult, "test-secret", "test-value", 0);
        }
    }

    Y_UNIT_TEST(CreateSecretOverExistingSecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value-init"
            )"
        );
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathExist);

        // operation should fail
        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value-new"
            )",
            {EStatus::StatusSchemeError, EStatus::StatusAlreadyExists}
        );
        env.TestWaitNotification(runtime, txId);

        // the value should remain the same
        const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
        ExpectEqualSecretDescription(describeResult, "test-secret", "test-value-init", 0);
    }

    Y_UNIT_TEST(CreateSecretOverExistingObject) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        // operation should fail
        TestCreateSecret(runtime, ++txId, "/MyRoot",
            R"(
                Name: "dir"
                Value: ""
            )",
            {EStatus::StatusNameConflict}
        );
        env.TestWaitNotification(runtime, txId);

        // the object type should remain the same
        const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsDirectory});
    }

    Y_UNIT_TEST(CreateSecretInheritPermissions) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // setup acl
        NACLib::TDiffACL diffACL;
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user1");
        diffACL.AddAccess(NACLib::EAccessType::Deny, NACLib::DescribeSchema, "user2");
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::AlterSchema, "user1");
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::AlterSchema, "user2");
        AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL.SerializeAsString(), /* newOwner */ "");
        env.TestWaitNotification(runtime, txId);

        // create just a secret
        TestCreateSecret(runtime, ++txId, "/MyRoot",
            R"(
                Name: "secret"
                Value: "value"
                InheritPermissions: true
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // create a secret with an intermediate directory
        TestCreateSecret(runtime, ++txId, "/MyRoot",
            R"(
                Name: "dir/secret"
                Value: "value"
                InheritPermissions: true
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // check that both secrets and created directory inherit permissions from the last existing directory: '/Root' in this case
        const auto user1Token = NACLib::TUserToken(NACLib::TUserToken::TUserTokenInitFields{.UserSID = "user1"});
        const auto user2Token = NACLib::TUserToken(NACLib::TUserToken::TUserTokenInitFields{.UserSID = "user2"});
        for (const auto& path : TVector<TString>{"/MyRoot/secret", "/MyRoot/dir/secret", "/MyRoot/dir"}) {
            auto describeSecret = DescribePath(runtime, path).GetPathDescription().GetSelf();
            { // check effective acl
                const TSecurityObject secObj(describeSecret.GetOwner(), describeSecret.GetEffectiveACL(),
                    /* isContainer */ false);
                UNIT_ASSERT_C(secObj.CheckAccess(NACLib::DescribeSchema, user1Token),
                    "user1 should have grant (inherited from root)");
                UNIT_ASSERT_C(secObj.CheckAccess(NACLib::AlterSchema, user1Token),
                    "user1 should have grant (inherited from root)");

                UNIT_ASSERT_C(!secObj.CheckAccess(NACLib::DescribeSchema, user2Token),
                    "user2 should have no grant (inherited deny from root)");
                UNIT_ASSERT_C(secObj.CheckAccess(NACLib::AlterSchema, user2Token),
                    "user2 should have grant (inherited from root)");
            }

            { // check acl – all aces should be inherited, so be absent on the object itself
                const TSecurityObject secObj(describeSecret.GetOwner(), describeSecret.GetACL(),
                    /* isContainer */ false);
                for (const auto& grant : {NACLib::DescribeSchema, NACLib::AlterSchema}) {
                    for (const auto& userToken : {user1Token, user2Token}) {
                        UNIT_ASSERT_C(!secObj.CheckAccess(grant, userToken),
                            "No aces on the created objects expected");
                    }
                }
            }

            // check that aces are not duplicated
            const NACLib::TACL secretAcl(describeSecret.GetEffectiveACL());
            const auto rootDescribePath = DescribePath(runtime, "/MyRoot");
            const auto describeRoot = rootDescribePath.GetPathDescription().GetSelf();
            const NACLib::TACL rootAcl(describeRoot.GetEffectiveACL());
            // Cannot compare rules themselves because they are actually different: i.e. there's an Inherited=true flag at the secret aces
            UNIT_ASSERT_EQUAL_C(
                secretAcl.GetACE().size(), rootAcl.GetACE().size(),
                "Secret ACL must be inherited, hence the number of rules must should be the same")
            ;
        }
    }

    Y_UNIT_TEST(CreateSecretNoInheritPermissions) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        // setup acl
        {
            NACLib::TDiffACL diffACL;
            diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user1");
            diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::AlterSchema, "user1");
            diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user2");
            AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL.SerializeAsString(), /* newOwner */ "");
            env.TestWaitNotification(runtime, txId);
        }
        {
            NACLib::TDiffACL diffACL;
            diffACL.AddAccess(NACLib::EAccessType::Deny, NACLib::DescribeSchema, "user2");
            AsyncModifyACL(runtime, ++txId, "/MyRoot", "dir", diffACL.SerializeAsString(), /* newOwner */ "");
            env.TestWaitNotification(runtime, txId);
        }

        // create just a secret
        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "secret"
                Value: "value"
                InheritPermissions: false
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // create a secret with intermediate directory
        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "subdir/secret"
                Value: "value"
                InheritPermissions: false
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // check secret grants
        const auto user1Token = NACLib::TUserToken(NACLib::TUserToken::TUserTokenInitFields{.UserSID = "user1"});
        const auto user2Token = NACLib::TUserToken(NACLib::TUserToken::TUserTokenInitFields{.UserSID = "user2"});
        for (const auto& path : TVector<TString>{"/MyRoot/dir/secret", "/MyRoot/dir/subdir/secret"}) {
            const auto describeSecret = DescribePath(runtime, path).GetPathDescription().GetSelf();

            // compare EffectiveACL and ACL
            UNIT_ASSERT_EQUAL_C(describeSecret.GetEffectiveACL(), describeSecret.GetACL(),
                "ACL should be the same, since aces are set on secrets themselves");

            // Check access
            const TSecurityObject secObj(describeSecret.GetOwner(), describeSecret.GetACL(), /* isContainer */ false);
            UNIT_ASSERT_C(secObj.CheckAccess(NACLib::DescribeSchema, user1Token),
                "user1 should have grant (inherited from root)");
            UNIT_ASSERT_C(!secObj.CheckAccess(NACLib::AlterSchema, user1Token),
                "user1 should have no grant (only DescribeSchema grant is inherited)");

            UNIT_ASSERT_C(!secObj.CheckAccess(NACLib::DescribeSchema, user2Token),
                "user2 should have no grant (deny is inherited from dir)");
            UNIT_ASSERT_C(!secObj.CheckAccess(NACLib::AlterSchema, user2Token),
                "user2 should have no grant (only DescribeSchema grant is inherited)");
        }

        // check created directory grants – they should be interited from the root
        const auto describeSecret = DescribePath(runtime, "/MyRoot/dir/subdir").GetPathDescription().GetSelf();
        { // check effective acl
            const TSecurityObject secObjWithEffectiveAcl(describeSecret.GetOwner(), describeSecret.GetEffectiveACL(),
                /* isContainer */ false);
            UNIT_ASSERT_C(secObjWithEffectiveAcl.CheckAccess(NACLib::DescribeSchema, user1Token),
                "user1 should have grant (inherited from root)");
            UNIT_ASSERT_C(secObjWithEffectiveAcl.CheckAccess(NACLib::AlterSchema, user1Token),
                "user1 should have grant (inherited from root)");

            UNIT_ASSERT_C(!secObjWithEffectiveAcl.CheckAccess(NACLib::DescribeSchema, user2Token),
                "user2 should have grant (inherited from root)");
            UNIT_ASSERT_C(!secObjWithEffectiveAcl.CheckAccess(NACLib::AlterSchema, user2Token),
                "user2 should have no grant (was not provided at all)");
        }

        { // check acl – all aces should be inherited, so be absent on the object itself
            const TSecurityObject secObjWithAcl(describeSecret.GetOwner(), describeSecret.GetACL(),
                /* isContainer */ false);
            for (const auto& grant : {NACLib::DescribeSchema, NACLib::AlterSchema}) {
                for (const auto& userToken : {user1Token, user2Token}) {
                    UNIT_ASSERT_C(!secObjWithAcl.CheckAccess(grant, userToken),
                        "No aces on the created directory expected");
                }
            }
        }
    }

    Y_UNIT_TEST(InheritPermissionsWithDifferentInheritanceTypes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        for (int i = 1; i <= 6; ++i) {
            AsyncMkDir(runtime, ++txId, "/MyRoot", "dir" + ToString(i));
            env.TestWaitNotification(runtime, txId);
        }

        // If a user has the DescribeSchema grant on a directory with the default inheritance type,
        // then they will have the DescribeSchema grant on the nested secret
        AssertHasAccess(1, NACLib::EInheritanceType::DefaultInheritanceType, /* expectedHasAccess */ true, runtime, txId, env);

        // If a user has the DescribeSchema grant on a directory with inheritance type equals to InheritNone,
        // then they will NOT have the DescribeSchema grant on the nested secret
        AssertHasAccess(2, NACLib::EInheritanceType::InheritNone, /* expectedHasAccess */ false, runtime, txId, env);

        // If a user has the DescribeSchema grant on a directory with inheritance type equals to InheritObject,
        // then they will have the DescribeSchema grant on the nested secret (since secrets are objects)
        AssertHasAccess(3, NACLib::EInheritanceType::InheritObject, /* expectedHasAccess */ true, runtime, txId, env);

        // If a user has the DescribeSchema grant on a directory with inheritance type equals to InheritContainer,
        // then they will NOT have the DescribeSchema grant on the nested secret (since secrets are objects, but not containers)
        AssertHasAccess(4, NACLib::EInheritanceType::InheritContainer, /* expectedHasAccess */ false, runtime, txId, env);

        // If a user has the DescribeSchema grant on a directory with inheritance type equals to InheritOnly,
        // then they will NOT have the DescribeSchema grant on the nested secret ...
        AssertHasAccess(5, NACLib::EInheritanceType::InheritOnly, /* expectedHasAccess */ false, runtime, txId, env);

        // ... but with the InheritObject type as well, they will have the DescribeSchema grant
        AssertHasAccess(6, NACLib::EInheritanceType::InheritOnly | NACLib::EInheritanceType::InheritObject,
            /* expectedHasAccess */ true, runtime, txId, env);
    }

    Y_UNIT_TEST(AsyncCreateDifferentSecrets) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        AsyncCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret-1"
                Value: "test-value-1"
            )"
        );
        AsyncCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret-2"
                Value: "test-value-2"
            )"
        );

        TestModificationResult(runtime, txId - 1);
        TestModificationResult(runtime, txId);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        for (int i = 1; i <= 2; ++i){
            const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret-" + ToString(i));
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            ExpectEqualSecretDescription(
                describeResult,
                "test-secret-" + ToString(i),
                "test-value-" + ToString(i),
                0
            );
        }
    }

    Y_UNIT_TEST(AsyncCreateSameSecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        for (int i = 0; i < 2; ++i) {
            AsyncCreateSecret(runtime, ++txId, "/MyRoot/dir",
                R"(
                    Name: "test-secret"
                    Value: "test-value"
                )"
            );
        }
        const TVector<TExpectedResult> expectedResults = {EStatus::StatusAccepted,
                                                          EStatus::StatusMultipleModifications,
                                                          EStatus::StatusAlreadyExists};
        TestModificationResults(runtime, txId - 1, expectedResults);
        TestModificationResults(runtime, txId, expectedResults);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
        ExpectEqualSecretDescription(describeResult, "test-secret", "test-value", 0);
    }

    Y_UNIT_TEST(ReadOnlyMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);
        SetSchemeshardReadOnlyMode(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-name"
                Value: "test-value"
            )",
            {{EStatus::StatusReadOnly}}
        );
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/dir/test-name", false, NLs::PathNotExist);

        SetSchemeshardReadOnlyMode(runtime, false);
        sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-name"
                Value: "test-value"
            )"
        );
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/dir/test-name", false, NLs::PathExist);
    }

    Y_UNIT_TEST(EmptySecretName) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: ""
                Value: "test-value"
            )",
            {{EStatus::StatusSchemeError, "error: path part shouldn't be empty"}}
        );
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CreateNotInDatabase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSecret(runtime, ++txId, "/MyRoot",
            R"(
                Name: "test-name"
                Value: "test-value"
            )"
        );
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/test-name", false, NLs::PathExist);
    }

    Y_UNIT_TEST(DropSecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathExist);

        TestDropSecret(runtime, ++txId, "/MyRoot/dir", "test-secret");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathNotExist);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(DropUnexistingSecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestLs(runtime, "/MyRoot/test-secret", false, NLs::PathNotExist);

        TestDropSecret(
            runtime,
            ++txId,
            "/MyRoot",
            "test-secret",
            {EStatus::StatusPathDoesNotExist}
        );
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(DropNotASecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        TestDropSecret(runtime, ++txId, "/MyRoot", "dir", {EStatus::StatusNameConflict});
        env.TestWaitNotification(runtime, txId);

        // the object type should remain the same
        const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsDirectory});
    }

    Y_UNIT_TEST(AsyncDropSameSecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathExist);

        for (int i = 0; i < 2; ++i) {
            AsyncDropSecret(runtime, ++txId, "/MyRoot/dir", "test-secret");
            AsyncDropSecret(runtime, ++txId, "/MyRoot/dir", "test-secret");
        }
        const TVector<TExpectedResult> expectedResults = {EStatus::StatusAccepted,
                                                          EStatus::StatusMultipleModifications,
                                                          EStatus::StatusPathDoesNotExist};
        TestModificationResults(runtime, txId - 1, expectedResults);
        TestModificationResults(runtime, txId, expectedResults);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(AlterExistingSecretMultipleTImes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value-0"
            )"
        );
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathExist);

        TestAlterSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value-1"
            )"
        );
        env.TestWaitNotification(runtime, txId);
        auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
        ExpectEqualSecretDescription(describeResult, "test-secret", "test-value-1", 1);

        TestAlterSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value-2"
            )"
        );
        env.TestWaitNotification(runtime, txId);
        describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
        ExpectEqualSecretDescription(describeResult, "test-secret", "test-value-2", 2);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
        ExpectEqualSecretDescription(describeResult, "test-secret", "test-value-2", 2);
    }

    Y_UNIT_TEST(AlterUnexistingSecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathNotExist);

        TestAlterSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                 Name: "test-secret"
                Value: "test-value"
            )",
             {EStatus::StatusPathDoesNotExist}
        );
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(AlterNotASecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        TestAlterSecret(runtime, ++txId, "/MyRoot",
            R"(
                Name: "dir"
                Value: ""
            )",
             {EStatus::StatusNameConflict}
        );
        env.TestWaitNotification(runtime, txId);

        // the object type should remain the same
        const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsDirectory});
    }

    Y_UNIT_TEST(AsyncAlterSameSecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value-init"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        for (int i = 0; i < 2; ++i) {
            AsyncAlterSecret(runtime, ++txId, "/MyRoot/dir",
                R"(
                    Name: "test-secret"
                    Value: "test-value-new"
                )"
            );
        }
        const TVector<TExpectedResult> expectedResults = {EStatus::StatusAccepted,
                                                          EStatus::StatusMultipleModifications};
        TestModificationResults(runtime, txId - 1, expectedResults);
        TestModificationResults(runtime, txId, expectedResults);
        env.TestWaitNotification(runtime, {txId - 1, txId});

        const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
        ExpectEqualSecretDescription(describeResult, "test-secret", "test-value-new", 1);
    }
}
