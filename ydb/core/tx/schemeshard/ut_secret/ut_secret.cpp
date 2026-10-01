#include <ydb/core/tx/schemeshard/schemeshard_iam_delegation.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <util/string/join.h>
#include <util/string/split.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

namespace {
    using namespace NSchemeShardUT_Private;
    using NKikimrScheme::EStatus;

    // IAM delegation secrets

    TString DelegationSecret(const TString& name, const TString& sa, const TString& cloud, const TString& referrer, const TString& alter = "NONE") {
        return TStringBuilder() << "Name: \"" << name << "\"\n"
            << "IamDelegation { ServiceAccountId: \"" << sa << "\" CloudId: \"" << cloud << "\" ReferrerId: \"" << referrer << "\" }\n"
            << "IamDelegationAlter: IAM_DELEGATION_ALTER_" << alter << "\n";
    }

    TString StageSecret(const TString& name, const TString& sa, const TString& cloud, const TString& referrer) {
        return DelegationSecret(name, sa, cloud, referrer, "STAGE");
    }

    TString PromoteSecret(const TString& name, const TString& referrer) {
        return DelegationSecret(name, "", "", referrer, "PROMOTE");
    }

    TString CancelSecret(const TString& name, const TString& referrer) {
        return DelegationSecret(name, "", "", referrer, "CANCEL");
    }

    TString ConfirmSecret(const TString& name, const TString& referrer) {
        return DelegationSecret(name, "", "", referrer, "CONFIRM");
    }

    // When the outbox hands a revocation out: at once when the statement reported that the setup of the
    // delegation is over (CONFIRM, PROMOTE, CANCEL), after the lease otherwise
    enum class EDue {
        Now,
        AfterLease,
    };

    TString DataSource(const TString& name, const TString& auth, const TString& type = "ObjectStorage") {
        const THashMap<TString, TString> locations = {
            {"ObjectStorage", "Location: \"https://s3.cloud.net/my_bucket\""},
            {"PostgreSQL", "Location: \"localhost:5432\" Properties { Properties { key: \"database_name\" value: \"postgres\" } }"},
            {"Ydb", "Location: \"localhost:2135\" Properties { Properties { key: \"database_name\" value: \"/Root\" } }"},
        };
        return TStringBuilder() << "Name: \"" << name << "\" SourceType: \"" << type << "\" " << locations.at(type) << " Auth { " << auth << " }";
    }

    using TRevocation = NKikimrScheme::TEvClaimIamDelegationRevocationsResult::TRevocation;

    // A schemeshard with the feature on; the test stands in for the node that revokes the delegations of the outbox
    struct TDelegationTest {
        TTestBasicRuntime Runtime;
        TTestEnv Env;
        ui64 TxId = 100;

        explicit TDelegationTest(const TTestEnvOptions& opts = {})
            : Env(Runtime, opts)
        {
            Runtime.GetAppData().FeatureFlags.SetEnableIamDelegationSecrets(true);
        }

        void Wait() {
            Env.TestWaitNotification(Runtime, TxId);
        }

        void Create(const TString& dir, const TString& scheme, const TVector<TExpectedResult>& expected = {EStatus::StatusAccepted}) {
            TestCreateSecret(Runtime, ++TxId, dir, scheme, expected);
            Wait();
        }

        void Alter(const TString& dir, const TString& scheme, const TVector<TExpectedResult>& expected = {EStatus::StatusAccepted}) {
            TestAlterSecret(Runtime, ++TxId, dir, scheme, expected);
            Wait();
        }

        void Replace(const TString& dir, const TString& scheme, const TVector<TExpectedResult>& expected = {EStatus::StatusAccepted}) {
            TestCreateSecretOrReplace(Runtime, ++TxId, dir, scheme, expected);
            Wait();
        }

        // A delegation secret whose setup the statement has reported, as CREATE SECRET leaves it
        void CreateConfirmed(const TString& dir, const TString& name, const TString& sa, const TString& cloud, const TString& referrer) {
            Create(dir, DelegationSecret(name, sa, cloud, referrer));
            Alter(dir, ConfirmSecret(name, referrer));
        }

        void Drop(const TString& dir, const TString& name) {
            TestDropSecret(Runtime, ++TxId, dir, name);
            Wait();
            TestLs(Runtime, dir + "/" + name, false, NLs::PathNotExist);
        }

        NKikimrSchemeOp::TSecretDescription Describe(const TString& path, bool withValue = false) {
            NKikimrSchemeOp::TDescribeOptions opts;
            opts.SetReturnSecretValue(withValue);
            return DescribePath(Runtime, path, opts).GetPathDescription().GetSecretDescription();
        }

        void Reboot() {
            RebootTablet(Runtime, TTestTxConfig::SchemeShard, Runtime.AllocateEdgeActor());
        }

        // Jumps the clock: the timers due by then fire once. Stepping through the 50 ms polls of the storage
        // emulation would cost seconds of real time per virtual minute.
        void Sleep(TDuration duration) {
            Runtime.AdvanceCurrentTime(duration);
            Env.SimulateSleep(Runtime, TDuration::MilliSeconds(1));
        }

        // The revocations the outbox hands out now: due (the setup of the delegation can no longer be in flight)
        // and not claimed by anybody else
        TVector<TRevocation> Claim(TDuration lease = TDuration::Minutes(5)) {
            const TActorId sender = Runtime.AllocateEdgeActor();
            ForwardToTablet(Runtime, TTestTxConfig::SchemeShard, sender, new TEvSchemeShard::TEvClaimIamDelegationRevocations(lease));
            const auto result = Runtime.GrabEdgeEvent<TEvSchemeShard::TEvClaimIamDelegationRevocationsResult>(sender);
            UNIT_ASSERT(result);
            LastClaimId = result->Get()->Record.GetClaimId();
            TVector<TRevocation> revocations(result->Get()->Record.GetRevocations().begin(), result->Get()->Record.GetRevocations().end());
            SortBy(revocations, [](const TRevocation& r) { return r.GetReferrerId(); });
            return revocations;
        }

        TString ClaimedReferrers(TDuration lease = TDuration::Minutes(5)) {
            TVector<TString> referrers;
            for (const auto& revocation : Claim(lease)) {
                referrers.push_back(revocation.GetReferrerId());
            }
            return JoinSeq(",", referrers);
        }

        // Acknowledges revocations with the given claim (the last one by default)
        void Revoked(const TVector<TString>& referrers, TMaybe<ui64> claimId = Nothing()) {
            const TActorId sender = Runtime.AllocateEdgeActor();
            ForwardToTablet(Runtime, TTestTxConfig::SchemeShard, sender, new TEvSchemeShard::TEvIamDelegationsRevoked(claimId.GetOrElse(LastClaimId), referrers));
            UNIT_ASSERT(Runtime.GrabEdgeEvent<TEvSchemeShard::TEvIamDelegationsRevokedResult>(sender));
        }

        ui64 LastClaimId = 0;

        // The revocations the outbox hands out, claimed and acknowledged: gone for good, not just hidden by the claim
        void ExpectRevocations(const TString& referrers, EDue due) {
            if (due == EDue::AfterLease) {
                UNIT_ASSERT_VALUES_EQUAL(ClaimedReferrers(), "");
                Sleep(StagedIamDelegationLease + TDuration::Seconds(1));
            }
            UNIT_ASSERT_VALUES_EQUAL(ClaimedReferrers(TDuration::Seconds(10)), referrers);
            Revoked(StringSplitter(referrers).Split(',').SkipEmpty().ToList<TString>());
            Sleep(TDuration::Seconds(11));
            UNIT_ASSERT_VALUES_EQUAL(ClaimedReferrers(), "");
        }
    };

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

    Y_UNIT_TEST_FLAG(CreateSecretDefaultInheritPermissions, AlwaysSetSystemOwner) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().AlwaysSetSystemOwner = AlwaysSetSystemOwner;

        NACLib::TDiffACL diffACL;
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user1");
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::AlterSchema, "user1");
        AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL.SerializeAsString(), /* newOwner */ "");
        env.TestWaitNotification(runtime, txId);

        // create a secret without specifying InheritPermissions
        TestCreateSecret(runtime, ++txId, "/MyRoot",
            R"(
                Name: "secret"
                Value: "value"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        const auto user1Token = NACLib::TUserToken(NACLib::TUserToken::TUserTokenInitFields{.UserSID = "user1"});
        const auto describeSecret = DescribePath(runtime, "/MyRoot/secret").GetPathDescription().GetSelf();

        if (AlwaysSetSystemOwner) {
            const TSecurityObject secObjEffective(describeSecret.GetOwner(), describeSecret.GetEffectiveACL(),
                /* isContainer */ false);
            UNIT_ASSERT_C(secObjEffective.CheckAccess(NACLib::DescribeSchema, user1Token),
                "user1 should have grant (inherited from root)");
            UNIT_ASSERT_C(secObjEffective.CheckAccess(NACLib::AlterSchema, user1Token),
                "user1 should have grant (inherited from root)");

            const TSecurityObject secObjOwn(describeSecret.GetOwner(), describeSecret.GetACL(),
                /* isContainer */ false);
            UNIT_ASSERT_C(!secObjOwn.CheckAccess(NACLib::AlterSchema, user1Token),
                "No aces on the created secret expected when inheritance is on");
        } else {
            UNIT_ASSERT_EQUAL_C(describeSecret.GetEffectiveACL(), describeSecret.GetACL(),
                "ACL should be the same, since aces are set on the secret itself");

            const TSecurityObject secObj(describeSecret.GetOwner(), describeSecret.GetACL(), /* isContainer */ false);
            UNIT_ASSERT_C(secObj.CheckAccess(NACLib::DescribeSchema, user1Token),
                "user1 should have the DescribeSchema grant (it is always inherited)");
            UNIT_ASSERT_C(!secObj.CheckAccess(NACLib::AlterSchema, user1Token),
                "user1 should have no AlterSchema grant (only DescribeSchema grant is inherited)");
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

    Y_UNIT_TEST(RejectValueParamName) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "dir");
        env.TestWaitNotification(runtime, txId);

        // Create with ValueParamName should be rejected
        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                ValueParamName: "$val"
            )",
            {{EStatus::StatusInvalidParameter, "ValueParamName was passed"}}
        );
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathNotExist);

        // Create normally so we can test alter
        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "original"
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // Alter with ValueParamName should be rejected
        TestAlterSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                ValueParamName: "$val"
            )",
            {{EStatus::StatusInvalidParameter, "ValueParamName was passed"}}
        );
        env.TestWaitNotification(runtime, txId);

        // Value should remain unchanged
        const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
        ExpectEqualSecretDescription(describeResult, "test-secret", "original", 0);
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

    Y_UNIT_TEST(CreateOrReplaceSecretInheritPermissions) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // setup acl on root
        NACLib::TDiffACL diffACL;
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user1");
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::AlterSchema, "user1");
        AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL.SerializeAsString(), /* newOwner */ "");
        env.TestWaitNotification(runtime, txId);

        // create a secret with InheritPermissions: false (ACL is interrupted)
        TestCreateSecret(runtime, ++txId, "/MyRoot",
            R"(
                Name: "secret"
                Value: "value1"
                InheritPermissions: false
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // ACL should be non-empty (inheritance interrupted)
        {
            const auto describeSecret = DescribePath(runtime, "/MyRoot/secret").GetPathDescription().GetSelf();
            UNIT_ASSERT_C(!describeSecret.GetACL().empty(),
                "ACL should be non-empty when InheritPermissions = false");
        }

        // CREATE OR REPLACE with InheritPermissions: true should restore inheritance (clear ACL)
        TestCreateSecretOrReplace(runtime, ++txId, "/MyRoot",
            R"(
                Name: "secret"
                Value: "value2"
                InheritPermissions: true
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // ACL should now be empty (inheritance restored)
        {
            const auto describeSecret = DescribePath(runtime, "/MyRoot/secret").GetPathDescription().GetSelf();
            UNIT_ASSERT_C(describeSecret.GetACL().empty(),
                "ACL should be empty when InheritPermissions = true after CREATE OR REPLACE");
        }

        // CREATE OR REPLACE with InheritPermissions: false should interrupt inheritance again
        TestCreateSecretOrReplace(runtime, ++txId, "/MyRoot",
            R"(
                Name: "secret"
                Value: "value3"
                InheritPermissions: false
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // ACL should be non-empty again
        {
            const auto describeSecret = DescribePath(runtime, "/MyRoot/secret").GetPathDescription().GetSelf();
            UNIT_ASSERT_C(!describeSecret.GetACL().empty(),
                "ACL should be non-empty when InheritPermissions = false after CREATE OR REPLACE");
        }
    }

    Y_UNIT_TEST(CreateOrReplaceSecretPicksUpParentAclChanges) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // setup initial acl on root: user1 gets DescribeSchema + AlterSchema
        NACLib::TDiffACL diffACL;
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user1");
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::AlterSchema, "user1");
        AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL.SerializeAsString(), /* newOwner */ "");
        env.TestWaitNotification(runtime, txId);

        // create a secret with InheritPermissions: false (ACL is interrupted from parent)
        TestCreateSecret(runtime, ++txId, "/MyRoot",
            R"(
                Name: "secret"
                Value: "value1"
                InheritPermissions: false
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // The interrupted ACL should contain user1's DescribeSchema grant
        {
            const auto describeSecret = DescribePath(runtime, "/MyRoot/secret").GetPathDescription().GetSelf();
            UNIT_ASSERT_C(!describeSecret.GetACL().empty(),
                "ACL should be non-empty when InheritPermissions = false");
            NACLib::TACL acl(describeSecret.GetACL());
            bool foundUser1 = false;
            for (const auto& ace : acl.GetACE()) {
                if (ace.GetSID() == "user1" && (ace.GetAccessRight() & NACLib::DescribeSchema)) {
                    foundUser1 = true;
                }
            }
            UNIT_ASSERT_C(foundUser1, "ACL should contain user1's DescribeSchema grant");
        }

        // Now add a new grant for user2 on the parent directory
        NACLib::TDiffACL diffACL2;
        diffACL2.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user2");
        diffACL2.AddAccess(NACLib::EAccessType::Allow, NACLib::AlterSchema, "user2");
        AsyncModifyACL(runtime, ++txId, "", "MyRoot", diffACL2.SerializeAsString(), /* newOwner */ "");
        env.TestWaitNotification(runtime, txId);

        // CREATE OR REPLACE with InheritPermissions: false should pick up the new parent grant
        TestCreateSecretOrReplace(runtime, ++txId, "/MyRoot",
            R"(
                Name: "secret"
                Value: "value2"
                InheritPermissions: false
            )"
        );
        env.TestWaitNotification(runtime, txId);

        // The interrupted ACL should now contain both user1 and user2 DescribeSchema grants
        {
            const auto describeSecret = DescribePath(runtime, "/MyRoot/secret").GetPathDescription().GetSelf();
            UNIT_ASSERT_C(!describeSecret.GetACL().empty(),
                "ACL should be non-empty when InheritPermissions = false");
            NACLib::TACL acl(describeSecret.GetACL());
            bool foundUser1 = false;
            bool foundUser2 = false;
            for (const auto& ace : acl.GetACE()) {
                if (ace.GetSID() == "user1" && (ace.GetAccessRight() & NACLib::DescribeSchema)) {
                    foundUser1 = true;
                }
                if (ace.GetSID() == "user2" && (ace.GetAccessRight() & NACLib::DescribeSchema)) {
                    foundUser2 = true;
                }
            }
            UNIT_ASSERT_C(foundUser1, "ACL should contain user1's DescribeSchema grant");
            UNIT_ASSERT_C(foundUser2, "ACL should contain user2's DescribeSchema grant (picked up from parent)");
        }
    }

    Y_UNIT_TEST(CreateIamDelegationSecret) {
        TDelegationTest t;
        t.Create("/MyRoot", DelegationSecret("sa-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1"));
        const auto check = [&]() {
            const auto secret = t.Describe("/MyRoot/sa-secret", /* withValue */ true);
            UNIT_ASSERT_VALUES_EQUAL(secret.GetName(), "sa-secret");
            UNIT_ASSERT(secret.GetValue().empty()); // the delegation is described, the value never
            UNIT_ASSERT_VALUES_EQUAL(secret.GetIamDelegation().GetServiceAccountId(), "aje-sa-1");
            UNIT_ASSERT_VALUES_EQUAL(secret.GetIamDelegation().GetCloudId(), "b1g-cloud-1");
            UNIT_ASSERT_VALUES_EQUAL(secret.GetIamDelegation().GetReferrerId(), "referrer-1");
        };
        check();
        UNIT_ASSERT_VALUES_EQUAL(t.Describe("/MyRoot/sa-secret").GetVersion(), 0u);
        // the statement reports that IAM set the delegation up
        t.Alter("/MyRoot", ConfirmSecret("sa-secret", "referrer-9"), {{EStatus::StatusPreconditionFailed, "is not the delegation of the secret"}});
        t.Alter("/MyRoot", ConfirmSecret("sa-secret", "referrer-1"));
        UNIT_ASSERT_VALUES_EQUAL(t.Describe("/MyRoot/sa-secret").GetVersion(), 1u);
        t.Reboot();
        check();
        t.Create("/MyRoot", R"(Name: "plain-secret" Value: "v")");
        UNIT_ASSERT(!t.Describe("/MyRoot/plain-secret").HasIamDelegation());

        // the delegation must be complete and the only source
        const TVector<TExpectedResult> invalid = {{EStatus::StatusInvalidParameter}};
        t.Create("/MyRoot", DelegationSecret("bad", "", "b1g-cloud-1", "referrer-2"), invalid);
        t.Create("/MyRoot", DelegationSecret("bad", "aje-sa-2", "", "referrer-2"), invalid);
        t.Create("/MyRoot", DelegationSecret("bad", "aje-sa-2", "b1g-cloud-1", ""), invalid);
        t.Create("/MyRoot", "Value: \"v\"\n" + DelegationSecret("bad", "aje-sa-2", "b1g-cloud-1", "referrer-2"), invalid);
        // an alter mode has no meaning for a new secret
        t.Create("/MyRoot", StageSecret("bad", "aje-sa-2", "b1g-cloud-1", "referrer-2"), invalid);
        t.Create("/MyRoot", R"(Name: "bad" Value: "v" IamDelegationAlter: IAM_DELEGATION_ALTER_CONFIRM)", invalid);
        TestLs(t.Runtime, "/MyRoot/bad", false, NLs::PathNotExist);
        UNIT_ASSERT_VALUES_EQUAL(t.ClaimedReferrers(), ""); // nothing to revoke
    }

    Y_UNIT_TEST(IamDelegationSecretFeatureFlag) {
        TDelegationTest t;
        t.CreateConfirmed("/MyRoot", "sa-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1");
        t.Runtime.GetAppData().FeatureFlags.SetEnableIamDelegationSecrets(false);

        // switched off: no new delegation secrets and no changes to the existing ones, which can still be dropped
        const TVector<TExpectedResult> disabled = {{EStatus::StatusPreconditionFailed}};
        t.Create("/MyRoot", DelegationSecret("sa-secret-2", "aje-sa-2", "b1g-cloud-1", "referrer-2"), disabled);
        t.Alter("/MyRoot", StageSecret("sa-secret", "aje-sa-2", "b1g-cloud-1", "referrer-2"), disabled);
        UNIT_ASSERT_VALUES_EQUAL(t.Describe("/MyRoot/sa-secret").GetVersion(), 1u);
        t.Drop("/MyRoot", "sa-secret");
        t.ExpectRevocations("referrer-1", EDue::Now);
    }

    Y_UNIT_TEST(AlterIamDelegationSecret) {
        TDelegationTest t;
        const auto describe = [&]() { return t.Describe("/MyRoot/sa-secret"); };
        t.CreateConfirmed("/MyRoot", "sa-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1");

        // a replacement is staged next to the delegation the readers keep using, then promoted over it
        t.Alter("/MyRoot", StageSecret("sa-secret", "aje-sa-2", "b1g-cloud-1", "referrer-2"));
        {
            const auto secret = describe();
            UNIT_ASSERT_VALUES_EQUAL(secret.GetVersion(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(secret.GetIamDelegation().GetReferrerId(), "referrer-1");
            UNIT_ASSERT_VALUES_EQUAL(secret.GetPendingIamDelegation().GetServiceAccountId(), "aje-sa-2");
            UNIT_ASSERT_VALUES_EQUAL(secret.GetPendingIamDelegation().GetReferrerId(), "referrer-2");
            UNIT_ASSERT(!secret.HasPendingIamDelegationStagedAt());
            UNIT_ASSERT(!secret.HasIamDelegationSetUp());
        }
        t.Alter("/MyRoot", PromoteSecret("sa-secret", "referrer-2"));
        {
            const auto secret = describe();
            UNIT_ASSERT_VALUES_EQUAL(secret.GetVersion(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(secret.GetIamDelegation().GetServiceAccountId(), "aje-sa-2");
            UNIT_ASSERT_VALUES_EQUAL(secret.GetIamDelegation().GetReferrerId(), "referrer-2");
            UNIT_ASSERT(!secret.HasPendingIamDelegation());
        }
        t.ExpectRevocations("referrer-1", EDue::Now); // its setup was reported

        // the ALTER that staged a replacement may still be setting it up: for the lease, restarts included,
        // nothing can be staged over it; afterwards it is replaced and revoked
        t.Alter("/MyRoot", StageSecret("sa-secret", "aje-sa-3", "b1g-cloud-1", "referrer-3"));
        t.Sleep(StagedIamDelegationLease / 2);
        t.Reboot();
        t.Alter("/MyRoot", StageSecret("sa-secret", "aje-sa-4", "b1g-cloud-1", "referrer-4"),
            {{EStatus::StatusMultipleModifications, "is being set up for the secret by another ALTER"}});
        t.Sleep(StagedIamDelegationLease / 2 + TDuration::Seconds(1));
        t.Alter("/MyRoot", StageSecret("sa-secret", "aje-sa-4", "b1g-cloud-1", "referrer-4"));
        UNIT_ASSERT_VALUES_EQUAL(describe().GetPendingIamDelegation().GetReferrerId(), "referrer-4");
        t.ExpectRevocations("referrer-3", EDue::Now); // its lease has passed

        // cancelled: the statement reports that the setup is over, so the revocation is due at once, and the
        // next replacement can be staged at once
        t.Alter("/MyRoot", CancelSecret("sa-secret", "referrer-4"));
        UNIT_ASSERT_VALUES_EQUAL(describe().GetVersion(), 6u);
        UNIT_ASSERT(!describe().HasPendingIamDelegation());
        t.ExpectRevocations("referrer-4", EDue::Now);
        t.Alter("/MyRoot", StageSecret("sa-secret", "aje-sa-5", "b1g-cloud-1", "referrer-5"));

        // dropped: everything the secret names is revoked, the staged replacement once its setup can no longer be in flight
        t.Drop("/MyRoot", "sa-secret");
        t.ExpectRevocations("referrer-2", EDue::Now);
        t.ExpectRevocations("referrer-5", EDue::AfterLease);
    }

    Y_UNIT_TEST(AlterIamDelegationSecretValidation) {
        TDelegationTest t;
        t.CreateConfirmed("/MyRoot", "sa-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1");
        t.Create("/MyRoot", R"(Name: "plain-secret" Value: "v")");
        const TVector<TExpectedResult> invalid = {{EStatus::StatusInvalidParameter}};

        // the delegation is never changed in place
        t.Alter("/MyRoot", DelegationSecret("sa-secret", "aje-sa-2", "b1g-cloud-1", "referrer-2"), {{EStatus::StatusInvalidParameter, "must equal the current delegation"}});
        t.Alter("/MyRoot", R"(Name: "sa-secret")", invalid);
        t.Alter("/MyRoot", DelegationSecret("sa-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1"));
        // the source never changes
        t.Alter("/MyRoot", R"(Name: "sa-secret" Value: "v")", invalid);
        t.Replace("/MyRoot", R"(Name: "sa-secret" Value: "v")", invalid);
        t.Alter("/MyRoot", DelegationSecret("plain-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1"), invalid);
        t.Replace("/MyRoot", DelegationSecret("plain-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1"), invalid);
        for (const TString alter : {"STAGE", "PROMOTE", "CANCEL", "CONFIRM"}) {
            t.Alter("/MyRoot", TStringBuilder() << "Name: \"plain-secret\" Value: \"v2\" IamDelegationAlter: IAM_DELEGATION_ALTER_" << alter,
                {{EStatus::StatusInvalidParameter, "allowed only for IAM delegation secrets"}});
        }
        // a staged replacement must be complete and new to the secret
        t.Alter("/MyRoot", StageSecret("sa-secret", "", "b1g-cloud-1", "referrer-2"), invalid);
        t.Alter("/MyRoot", StageSecret("sa-secret", "aje-sa-2", "", "referrer-2"), invalid);
        t.Alter("/MyRoot", StageSecret("sa-secret", "aje-sa-2", "b1g-cloud-1", ""), invalid);
        t.Alter("/MyRoot", StageSecret("sa-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1"), {{EStatus::StatusInvalidParameter, "is already named by secret"}});
        t.Alter("/MyRoot", StageSecret("sa-secret", "aje-sa-2", "b1g-cloud-1", "referrer-2"));
        t.Alter("/MyRoot", StageSecret("sa-secret", "aje-sa-2", "b1g-cloud-1", "referrer-2"), {{EStatus::StatusInvalidParameter, "is already named by secret"}});
        // only the staged replacement can be promoted or cancelled, only the current delegation confirmed
        t.Alter("/MyRoot", PromoteSecret("sa-secret", "referrer-9"), {{EStatus::StatusPreconditionFailed, "is not staged for the secret"}});
        t.Alter("/MyRoot", CancelSecret("sa-secret", "referrer-1"), {{EStatus::StatusPreconditionFailed, "is not staged for the secret"}});
        t.Alter("/MyRoot", ConfirmSecret("sa-secret", "referrer-2"), {{EStatus::StatusPreconditionFailed, "is not the delegation of the secret"}});
        // CREATE OR REPLACE over a delegation secret is the same ALTER
        t.Replace("/MyRoot", DelegationSecret("sa-secret", "aje-sa-3", "b1g-cloud-1", "referrer-3"), {{EStatus::StatusInvalidParameter, "must equal the current delegation"}});
        t.Replace("/MyRoot", StageSecret("sa-secret", "aje-sa-3", "b1g-cloud-1", "referrer-3"), {{EStatus::StatusMultipleModifications, "is being set up for the secret by another ALTER"}});
        t.Replace("/MyRoot", PromoteSecret("sa-secret", "referrer-2"));
        {
            const auto secret = t.Describe("/MyRoot/sa-secret");
            UNIT_ASSERT_VALUES_EQUAL(secret.GetVersion(), 4u);
            UNIT_ASSERT_VALUES_EQUAL(secret.GetIamDelegation().GetReferrerId(), "referrer-2");
            UNIT_ASSERT(!secret.HasPendingIamDelegation());
        }
        t.ExpectRevocations("referrer-1", EDue::Now);
        UNIT_ASSERT_VALUES_EQUAL(t.Describe("/MyRoot/plain-secret", true).GetValue(), "v");
    }

    // The outbox: written with the change, handed out at once when the statement reported the setup and after
    // the lease otherwise, hidden while a node holds it, forgotten when the node reports the revocation, kept
    // across restarts.
    Y_UNIT_TEST(IamDelegationRevocationOutbox) {
        TDelegationTest t;
        for (const TString i : {"1", "2", "3"}) {
            t.Create("/MyRoot", DelegationSecret("sa-secret-" + i, "aje-sa-" + i, "b1g-cloud-" + i, "referrer-" + i));
        }
        t.Alter("/MyRoot", ConfirmSecret("sa-secret-1", "referrer-1"));
        t.Alter("/MyRoot", ConfirmSecret("sa-secret-3", "referrer-3"));
        t.Drop("/MyRoot", "sa-secret-1");
        t.Drop("/MyRoot", "sa-secret-2");
        {
            const auto revocations = t.Claim(TDuration::Minutes(5));
            UNIT_ASSERT_VALUES_EQUAL(revocations.size(), 1u); // the setup of the second one may still be in flight
            UNIT_ASSERT_VALUES_EQUAL(revocations[0].GetReferrerId(), "referrer-1");
            UNIT_ASSERT_VALUES_EQUAL(revocations[0].GetServiceAccountId(), "aje-sa-1");
            UNIT_ASSERT_VALUES_EQUAL(revocations[0].GetCloudId(), "b1g-cloud-1");
        }
        // claimed: hidden from other claimers for the lease, then handed out again
        UNIT_ASSERT_VALUES_EQUAL(t.ClaimedReferrers(), "");
        t.Sleep(TDuration::Minutes(5) + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(t.ClaimedReferrers(), "referrer-1");
        // a restart keeps the records and forgets the claims
        t.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(t.ClaimedReferrers(), "referrer-1");
        // acknowledged: forgotten, restarts included; an unknown referrer is ignored
        t.Revoked({"referrer-1", "referrer-9"});
        UNIT_ASSERT_VALUES_EQUAL(t.ClaimedReferrers(), "");
        t.Sleep(StagedIamDelegationLease);
        UNIT_ASSERT_VALUES_EQUAL(t.ClaimedReferrers(), "referrer-2"); // its lease has passed
        t.Revoked({"referrer-2"});
        t.Reboot();
        UNIT_ASSERT_VALUES_EQUAL(t.ClaimedReferrers(), "");
        t.Drop("/MyRoot", "sa-secret-3");
        t.ExpectRevocations("referrer-3", EDue::Now);
    }

    Y_UNIT_TEST(IamDelegationSecretInExternalDataSources) {
        TDelegationTest t(TTestEnvOptions().RunFakeConfigDispatcher(true));
        TestMkDir(t.Runtime, ++t.TxId, "/MyRoot", "dir");
        t.Wait();
        t.Create("/MyRoot/dir", DelegationSecret("sa-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1"));
        t.Create("/MyRoot/dir", R"(Name: "plain-secret" Value: "v")");
        const TString sa = "/MyRoot/dir/sa-secret";
        const TString plain = "/MyRoot/dir/plain-secret";
        const TVector<TExpectedResult> refused = {{EStatus::StatusSchemeError, "is an IAM delegation secret"}};
        const TVector<TExpectedResult> accepted = {{EStatus::StatusAccepted}};
        const auto serviceAccount = [](const TString& secret) { return Sprintf(R"(ServiceAccount { Id: "aje-sa-1" SecretName: "%s" })", secret.c_str()); };
        const auto mdbBasic = [](const TString& saSecret, const TString& password) {
            return Sprintf(R"(MdbBasic { ServiceAccountId: "aje-sa-1" ServiceAccountSecretName: "%s" Login: "user" PasswordSecretName: "%s" })", saSecret.c_str(), password.c_str());
        };
        const auto aws = [](const TString& keyId, const TString& key) {
            return Sprintf(R"(Aws { AwsAccessKeyIdSecretName: "%s" AwsSecretAccessKeySecretName: "%s" AwsRegion: "ru-central1" })", keyId.c_str(), key.c_str());
        };
        const auto token = [](const TString& secret) { return Sprintf(R"(Token { TokenSecretName: "%s" })", secret.c_str()); };
        ui32 n = 0;
        const auto create = [&](const TString& auth, const TString& type, const TVector<TExpectedResult>& expected) {
            const TString name = TStringBuilder() << "Source" << ++n;
            TestCreateExternalDataSource(t.Runtime, ++t.TxId, "/MyRoot", DataSource(name, auth, type), expected);
            t.Wait();
            return name;
        };

        // its value is an IAM token: refused wherever the secret would stand for a key signature, a password or an AWS key
        create(serviceAccount(sa), "ObjectStorage", refused);
        create(Sprintf(R"(Basic { Login: "user" PasswordSecretName: "%s" })", sa.c_str()), "PostgreSQL", refused);
        create(mdbBasic(sa, plain), "PostgreSQL", refused);
        create(mdbBasic(plain, sa), "PostgreSQL", refused);
        create(aws(sa, plain), "ObjectStorage", refused);
        create(aws(plain, sa), "ObjectStorage", refused);
        // accepted where a token is expected, and anywhere for a value secret
        const TString tokenSource = create(token(sa), "Ydb", accepted);
        create(serviceAccount(plain), "ObjectStorage", accepted);
        // a secret this schemeshard cannot resolve to a delegation secret is left to whoever reads it
        for (const TString& name : {"sa-secret", "/MyRoot/dir/no-such-secret", "/MyRoot/dir"}) {
            create(serviceAccount(name), "ObjectStorage", accepted);
        }
        // nor can a data source be altered to misuse it
        TestCreateExternalDataSourceOrReplace(t.Runtime, ++t.TxId, "/MyRoot", DataSource(tokenSource, serviceAccount(sa), "Ydb"), refused);
        TestCreateExternalDataSourceOrReplace(t.Runtime, ++t.TxId, "/MyRoot", DataSource(tokenSource, token(plain), "Ydb"), accepted);
        t.Wait();
        const auto auth = DescribePath(t.Runtime, "/MyRoot/" + tokenSource).GetPathDescription().GetExternalDataSourceDescription().GetAuth();
        UNIT_ASSERT_VALUES_EQUAL(auth.GetToken().GetTokenSecretName(), plain);
    }

    Y_UNIT_TEST(ForceDropRevokesTheDelegations) {
        TDelegationTest t;
        TestMkDir(t.Runtime, ++t.TxId, "/MyRoot", "dir");
        t.Wait();
        TestCreateSubDomain(t.Runtime, ++t.TxId, "/MyRoot", R"(Name: "SubDomain")");
        t.Wait();
        t.CreateConfirmed("/MyRoot/dir", "sa-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1");
        t.Alter("/MyRoot/dir", StageSecret("sa-secret", "aje-sa-2", "b1g-cloud-1", "referrer-2"));
        t.CreateConfirmed("/MyRoot/SubDomain", "sa-secret", "aje-sa-3", "b1g-cloud-1", "referrer-3");

        // the secrets go with their directory and subdomain; the revocations are written with the drops
        TestForceDropUnsafe(t.Runtime, ++t.TxId, DescribePath(t.Runtime, "/MyRoot/dir").GetPathDescription().GetSelf().GetPathId());
        t.Wait();
        TestForceDropSubDomain(t.Runtime, ++t.TxId, "/MyRoot", "SubDomain");
        t.Wait();
        TestLs(t.Runtime, "/MyRoot/dir/sa-secret", false, NLs::PathNotExist);
        TestLs(t.Runtime, "/MyRoot/SubDomain/sa-secret", false, NLs::PathNotExist);
        t.Reboot();
        t.ExpectRevocations("referrer-1,referrer-3", EDue::Now);
        t.ExpectRevocations("referrer-2", EDue::AfterLease);
    }

    // A referrer names one delegation: a secret cannot take one that a secret (its current or staged delegation)
    // or the outbox still holds. Only the node holding the claim may report a revocation.
    Y_UNIT_TEST(IamDelegationReferrerIsUnique) {
        TDelegationTest t;
        const TVector<TExpectedResult> named = {{EStatus::StatusInvalidParameter, "is already named by secret"}};
        t.CreateConfirmed("/MyRoot", "s1", "aje-sa-1", "b1g-cloud-1", "referrer-1");
        t.CreateConfirmed("/MyRoot", "s2", "aje-sa-2", "b1g-cloud-1", "referrer-2");
        t.Create("/MyRoot", DelegationSecret("s3", "aje-sa-3", "b1g-cloud-1", "referrer-1"), named);
        t.Alter("/MyRoot", StageSecret("s2", "aje-sa-3", "b1g-cloud-1", "referrer-1"), named);
        t.Alter("/MyRoot", StageSecret("s1", "aje-sa-3", "b1g-cloud-1", "referrer-3"));
        t.Create("/MyRoot", DelegationSecret("s3", "aje-sa-3", "b1g-cloud-1", "referrer-3"), named); // staged counts
        TestLs(t.Runtime, "/MyRoot/s3", false, NLs::PathNotExist);

        // in the outbox the referrer is still taken; acknowledged, it is free again
        t.Drop("/MyRoot", "s1");
        t.Create("/MyRoot", DelegationSecret("s3", "aje-sa-3", "b1g-cloud-1", "referrer-1"), {{EStatus::StatusInvalidParameter, "is already named by the outbox"}});
        t.ExpectRevocations("referrer-1", EDue::Now);
        t.Create("/MyRoot", DelegationSecret("s3", "aje-sa-3", "b1g-cloud-1", "referrer-1"));

        // a report without a claim, after the claim has run out, or with another claim does not delete the record
        t.Drop("/MyRoot", "s2");
        t.Revoked({"referrer-2"});
        UNIT_ASSERT_VALUES_EQUAL(t.ClaimedReferrers(TDuration::Seconds(10)), "referrer-2");
        const ui64 runOut = t.LastClaimId;
        t.Sleep(TDuration::Seconds(11));
        t.Revoked({"referrer-2"}, runOut);
        UNIT_ASSERT_VALUES_EQUAL(t.ClaimedReferrers(), "referrer-2");
        t.Revoked({"referrer-2"}, runOut);
        t.Revoked({"referrer-2"}); // the live claim
        t.Sleep(TDuration::Minutes(6));
        UNIT_ASSERT_VALUES_EQUAL(t.ClaimedReferrers(), "");
    }

    // A force drop removes a secret under a CREATE or an ALTER that has not reached its plan step yet: what the
    // next version of the secret names goes to the outbox with the rest, whichever of the two operations wins.
    Y_UNIT_TEST(ForceDropUnderOperationRevokesWhatIsNamed) {
        TDelegationTest t;
        TestMkDir(t.Runtime, ++t.TxId, "/MyRoot", "dir");
        t.Wait();
        TestMkDir(t.Runtime, ++t.TxId, "/MyRoot", "dir2");
        t.Wait();
        t.CreateConfirmed("/MyRoot/dir2", "s2", "aje-sa-2", "b1g-cloud-1", "referrer-2");
        const ui64 dir = DescribePath(t.Runtime, "/MyRoot/dir").GetPathDescription().GetSelf().GetPathId();
        const ui64 dir2 = DescribePath(t.Runtime, "/MyRoot/dir2").GetPathDescription().GetSelf().GetPathId();

        const auto blocksTx = [](ui64 txId) {
            return [txId](const auto& ev) {
                for (const auto& tx : ev->Get()->Record.GetTransactions()) {
                    if (tx.GetTxId() == txId) {
                        return true;
                    }
                }
                return false;
            };
        };

        const ui64 create = ++t.TxId;
        TBlockEvents<TEvTxProcessing::TEvPlanStep> createPlan(t.Runtime, blocksTx(create));
        AsyncCreateSecret(t.Runtime, create, "/MyRoot/dir", DelegationSecret("s1", "aje-sa-1", "b1g-cloud-1", "referrer-1"));
        t.Runtime.WaitFor("blocked CREATE plan", [&] { return !createPlan.empty(); });
        const ui64 drop = ++t.TxId;
        TestForceDropUnsafe(t.Runtime, drop, dir);
        createPlan.Unblock().Stop();
        t.Env.TestWaitNotification(t.Runtime, {create, drop});
        TestLs(t.Runtime, "/MyRoot/dir", false, NLs::PathNotExist);

        const ui64 stage = ++t.TxId;
        TBlockEvents<TEvTxProcessing::TEvPlanStep> stagePlan(t.Runtime, blocksTx(stage));
        AsyncAlterSecret(t.Runtime, stage, "/MyRoot/dir2", StageSecret("s2", "aje-sa-3", "b1g-cloud-1", "referrer-3"));
        t.Runtime.WaitFor("blocked STAGE plan", [&] { return !stagePlan.empty(); });
        const ui64 drop2 = ++t.TxId;
        TestForceDropUnsafe(t.Runtime, drop2, dir2);
        stagePlan.Unblock().Stop();
        t.Env.TestWaitNotification(t.Runtime, {stage, drop2});
        TestLs(t.Runtime, "/MyRoot/dir2", false, NLs::PathNotExist);

        t.ExpectRevocations("referrer-2", EDue::Now);
        t.ExpectRevocations("referrer-1,referrer-3", EDue::AfterLease);
    }

    Y_UNIT_TEST(OldAcknowledgementCannotDeleteAReusedReferrerAfterReboot) {
        TDelegationTest t;
        t.CreateConfirmed("/MyRoot", "first", "old-sa", "cloud", "referrer");
        t.Drop("/MyRoot", "first");
        t.Claim();
        const ui64 oldClaim = t.LastClaimId;
        t.Revoked({"referrer"});
        t.CreateConfirmed("/MyRoot", "second", "new-sa", "cloud", "referrer");
        t.Drop("/MyRoot", "second");
        t.Reboot();
        t.Claim();
        UNIT_ASSERT_UNEQUAL(t.LastClaimId, oldClaim);
        t.Revoked({"referrer"}, oldClaim);
        t.Reboot();
        const auto remaining = t.Claim();
        UNIT_ASSERT_VALUES_EQUAL(remaining.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(remaining.front().GetServiceAccountId(), "new-sa");
    }

    Y_UNIT_TEST(NamedIamDelegationsDueTimes) {
        NKikimrSchemeOp::TSecretDescription secret;
        secret.SetValue("v");
        UNIT_ASSERT(NamedIamDelegations(secret).empty());

        // the delegations a secret names, with the time their revocation may be handed out
        secret.MutableIamDelegation()->SetReferrerId("referrer-1");
        secret.SetIamDelegationNamedAt(TInstant::Hours(1).MicroSeconds());
        secret.MutablePendingIamDelegation()->SetReferrerId("referrer-2");
        secret.SetPendingIamDelegationStagedAt(TInstant::Hours(2).MicroSeconds());
        auto named = NamedIamDelegations(secret);
        UNIT_ASSERT_VALUES_EQUAL(named.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(named[0].ReferrerId, "referrer-1");
        UNIT_ASSERT_VALUES_EQUAL(named[0].NotBefore, TInstant::Hours(1) + StagedIamDelegationLease);
        UNIT_ASSERT_VALUES_EQUAL(named[1].ReferrerId, "referrer-2");
        UNIT_ASSERT_VALUES_EQUAL(named[1].NotBefore, TInstant::Hours(2) + StagedIamDelegationLease);
        secret.SetIamDelegationSetUp(true);
        named = NamedIamDelegations(secret);
        UNIT_ASSERT_VALUES_EQUAL(named[0].NotBefore, TInstant::Zero());
        secret.SetValue("v");
        UNIT_ASSERT(!secret.HasIamDelegation());
        UNIT_ASSERT_VALUES_EQUAL(NamedIamDelegations(secret).size(), 1u);
    }
}
