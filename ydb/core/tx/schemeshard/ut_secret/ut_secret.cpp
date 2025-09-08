#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

namespace {
    using namespace NSchemeShardUT_Private;
    using NKikimrScheme::EStatus;

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
            const auto describeResult = DescribePath(runtime, "/MyRoot/dir/test-secret");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            ExpectEqualSecretDescription(describeResult, "test-secret", "test-value", 0);
        }

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/dir/test-secret");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            ExpectEqualSecretDescription(describeResult, "test-secret", "test-value", 0);
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
            const auto describeResult = DescribePath(runtime, "/MyRoot/dir1/dir2/test-secret");
            TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            ExpectEqualSecretDescription(describeResult, "test-secret", "test-value", 0);
        }
    }

    Y_UNIT_TEST(CreateExistingSecret) {
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

        TestCreateSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value-new"
            )",
            {EStatus::StatusSchemeError, EStatus::StatusAlreadyExists}
        );
        env.TestWaitNotification(runtime, txId);
        const auto describeResult = DescribePath(runtime, "/MyRoot/dir/test-secret");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
        ExpectEqualSecretDescription(describeResult, "test-secret", "test-value-init", 0);
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
            const auto describeResult = DescribePath(runtime, "/MyRoot/dir/test-secret-" + ToString(i));
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

        const auto describeResult = DescribePath(runtime, "/MyRoot/dir/test-secret");
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
        auto describeResult = DescribePath(runtime, "/MyRoot/dir/test-secret");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
        ExpectEqualSecretDescription(describeResult, "test-secret", "test-value-1", 1);

        TestAlterSecret(runtime, ++txId, "/MyRoot/dir",
            R"(
                Name: "test-secret"
                Value: "test-value-2"
            )"
        );
        env.TestWaitNotification(runtime, txId);
        describeResult = DescribePath(runtime, "/MyRoot/dir/test-secret");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
        ExpectEqualSecretDescription(describeResult, "test-secret", "test-value-2", 2);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        describeResult = DescribePath(runtime, "/MyRoot/dir/test-secret");
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

        const auto describeResult = DescribePath(runtime, "/MyRoot/dir/test-secret");
        TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
        ExpectEqualSecretDescription(describeResult, "test-secret", "test-value-new", 1);
    }
}
