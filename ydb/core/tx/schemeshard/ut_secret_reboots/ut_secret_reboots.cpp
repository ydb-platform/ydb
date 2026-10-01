#include <util/string/join.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

namespace {
    using namespace NSchemeShardUT_Private;

    // IAM delegation secrets

    TString DelegationSecret(const TString& name, const TString& sa, const TString& cloud, const TString& referrer, const TString& alter = "NONE") {
        return TStringBuilder() << "Name: \"" << name << "\"\n"
            << "IamDelegation { ServiceAccountId: \"" << sa << "\" CloudId: \"" << cloud << "\" ReferrerId: \"" << referrer << "\" }\n"
            << "IamDelegationAlter: IAM_DELEGATION_ALTER_" << alter << "\n";
    }

    // Whatever reboots happened, the outbox of a fresh schemeshard hands out exactly these revocations at once
    void ExpectRevocationsAfterRestart(TTestActorRuntime& runtime, const TString& referrers) {
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        const TActorId sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, sender, new TEvSchemeShard::TEvClaimIamDelegationRevocations(TDuration::Minutes(5)));
        const auto result = runtime.GrabEdgeEvent<TEvSchemeShard::TEvClaimIamDelegationRevocationsResult>(sender);
        UNIT_ASSERT(result);
        TVector<TString> claimed;
        for (const auto& revocation : result->Get()->Record.GetRevocations()) {
            claimed.push_back(revocation.GetReferrerId());
        }
        Sort(claimed);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", claimed), referrers);
    }

    void ExpectEqualSecretDescription(
        const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
        const TString& name,
        const TString& value,
        const ui64 version
    ) {
        UNIT_ASSERT(describeResult.HasPathDescription());
        UNIT_ASSERT(describeResult.GetPathDescription().HasSecretDescription());
        const auto& secretDescription = describeResult.GetPathDescription().GetSecretDescription();
        UNIT_ASSERT_VALUES_EQUAL(secretDescription.GetName(), name);
        UNIT_ASSERT_VALUES_EQUAL(secretDescription.GetValue(), value);
        UNIT_ASSERT_VALUES_EQUAL(secretDescription.GetVersion(), version);
    }

    NKikimrScheme::TEvDescribeSchemeResult DescribePathWithSecretValue(
        TTestActorRuntime& runtime,
        const TString& path
    ) {
        NKikimrSchemeOp::TDescribeOptions opts;
        opts.SetReturnSecretValue(true);
        return DescribePath(runtime, path, opts);
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardSecretTestReboots) {
    Y_UNIT_TEST(CreateSecret) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "dir");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateSecret(runtime, ++t.TxId, "/MyRoot/dir",
                R"(
                    Name: "test-secret"
                    Value: "test-value"
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
                TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            }
        });
    }

    Y_UNIT_TEST(AlterSecret) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "dir");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateSecret(runtime, ++t.TxId, "/MyRoot/dir",
                    R"(
                        Name: "test-secret"
                        Value: "test-value-0"
                    )"
                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestAlterSecret(runtime, ++t.TxId, "/MyRoot/dir",
                R"(
                    Name: "test-secret"
                    Value: "test-value-1"
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
                TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
                ExpectEqualSecretDescription(describeResult, "test-secret", "test-value-1", 1);
            }
        });
    }

    Y_UNIT_TEST(DropSecret) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "dir");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestCreateSecret(runtime, ++t.TxId, "/MyRoot/dir",
                    R"(
                        Name: "test-secret"
                        Value: "test-value"
                    )"
                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathExist);
            }

            TestDropSecret(runtime, ++t.TxId, "/MyRoot/dir", "test-secret");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathNotExist);
            }
        });
    }

    Y_UNIT_TEST(IamDelegationSecret) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                runtime.GetAppData().FeatureFlags.SetEnableIamDelegationSecrets(true);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "dir");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateSecret(runtime, ++t.TxId, "/MyRoot/dir", DelegationSecret("sa-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1"));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestAlterSecret(runtime, ++t.TxId, "/MyRoot/dir", DelegationSecret("sa-secret", "", "", "referrer-1", "CONFIRM"));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestAlterSecret(runtime, ++t.TxId, "/MyRoot/dir", DelegationSecret("sa-secret", "aje-sa-2", "b1g-cloud-1", "referrer-2", "STAGE"));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestAlterSecret(runtime, ++t.TxId, "/MyRoot/dir", DelegationSecret("sa-secret", "", "", "referrer-2", "PROMOTE"));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestAlterSecret(runtime, ++t.TxId, "/MyRoot/dir", DelegationSecret("sa-secret", "aje-sa-3", "b1g-cloud-1", "referrer-3", "STAGE"));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestAlterSecret(runtime, ++t.TxId, "/MyRoot/dir", DelegationSecret("sa-secret", "", "", "referrer-3", "CANCEL"));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/sa-secret");
                TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
                const auto& secret = describeResult.GetPathDescription().GetSecretDescription();
                UNIT_ASSERT_VALUES_EQUAL(secret.GetVersion(), 5u);
                UNIT_ASSERT(secret.GetValue().empty());
                UNIT_ASSERT_VALUES_EQUAL(secret.GetIamDelegation().GetServiceAccountId(), "aje-sa-2");
                UNIT_ASSERT_VALUES_EQUAL(secret.GetIamDelegation().GetCloudId(), "b1g-cloud-1");
                UNIT_ASSERT_VALUES_EQUAL(secret.GetIamDelegation().GetReferrerId(), "referrer-2");
                UNIT_ASSERT(!secret.HasPendingIamDelegation());
                ExpectRevocationsAfterRestart(runtime, "referrer-1,referrer-3");
            }
        });
    }

    Y_UNIT_TEST(DropIamDelegationSecret) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                runtime.GetAppData().FeatureFlags.SetEnableIamDelegationSecrets(true);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "dir");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestCreateSubDomain(runtime, ++t.TxId, "/MyRoot", R"(Name: "SubDomain")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestCreateSecret(runtime, ++t.TxId, "/MyRoot/dir", DelegationSecret("sa-secret", "aje-sa-1", "b1g-cloud-1", "referrer-1"));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestAlterSecret(runtime, ++t.TxId, "/MyRoot/dir", DelegationSecret("sa-secret", "", "", "referrer-1", "CONFIRM"));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestCreateSecret(runtime, ++t.TxId, "/MyRoot/SubDomain", DelegationSecret("sa-secret", "aje-sa-2", "b1g-cloud-1", "referrer-2"));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestAlterSecret(runtime, ++t.TxId, "/MyRoot/SubDomain", DelegationSecret("sa-secret", "", "", "referrer-2", "CONFIRM"));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropSecret(runtime, ++t.TxId, "/MyRoot/dir", "sa-secret");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestForceDropSubDomain(runtime, ++t.TxId, "/MyRoot", "SubDomain");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/dir/sa-secret", false, NLs::PathNotExist);
                TestLs(runtime, "/MyRoot/SubDomain", false, NLs::PathNotExist);
                ExpectRevocationsAfterRestart(runtime, "referrer-1,referrer-2");
            }
        });
    }
}
