#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

namespace {

using namespace NSchemeShardUT_Private;
using NKikimrScheme::EStatus;

void ExpectEqualQueryTexts(const NKikimrScheme::TEvDescribeSchemeResult& describeResult, const TString& queryText) {
    UNIT_ASSERT(describeResult.HasPathDescription());
    UNIT_ASSERT(describeResult.GetPathDescription().HasViewDescription());
    UNIT_ASSERT(describeResult.GetPathDescription().GetViewDescription().HasQueryText());
    UNIT_ASSERT_VALUES_EQUAL(describeResult.GetPathDescription().GetViewDescription().GetQueryText(), queryText);
}

}

Y_UNIT_TEST_SUITE(TSchemeShardViewTest) {
    Y_UNIT_TEST(CreateView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateView(runtime, txId++, "/MyRoot", R"(
                Name: "MyView"
                QueryText: "Some query"
            )"
        );
        env.TestWaitNotification(runtime, txId);
        const auto describeResult = DescribePath(runtime, "/MyRoot/MyView");
        TestDescribeResult(describeResult,
                           {NLs::Finished,
                            NLs::IsView}
        );
        ExpectEqualQueryTexts(describeResult, "Some query");
    }

    Y_UNIT_TEST(DropView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateView(runtime, txId++, "/MyRoot", R"(
                Name: "MyView"
                QueryText: "Some query"
            )"
        );
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/MyView", false, NLs::PathExist);

        TestDropView(runtime, ++txId, "/MyRoot", "MyView");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/MyView", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(AsyncCreateDifferentViews) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "SomeDir");
        AsyncCreateView(runtime, ++txId, "/MyRoot/SomeDir", R"(
                Name: "FirstView"
                QueryText: "First query"
            )"
        );
        AsyncCreateView(runtime, ++txId, "/MyRoot/SomeDir", R"(
                Name: "SecondView"
                QueryText: "Second query"
            )"
        );

        TestModificationResult(runtime, txId - 2);
        TestModificationResult(runtime, txId - 1);
        TestModificationResult(runtime, txId);

        env.TestWaitNotification(runtime, {txId - 2, txId - 1, txId});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/SomeDir"), {NLs::Finished});
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/SomeDir/FirstView");
            TestDescribeResult(describeResult,
                               {NLs::Finished,
                                NLs::IsView}
            );
            ExpectEqualQueryTexts(describeResult, "First query");
        }
        {
            const auto describeResult = DescribePath(runtime, "/MyRoot/SomeDir/SecondView");
            TestDescribeResult(describeResult,
                               {NLs::Finished,
                                NLs::IsView}
            );
            ExpectEqualQueryTexts(describeResult, "Second query");
        }
    }

    Y_UNIT_TEST(AsyncCreateSameView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TString viewConfig = R"(
                Name: "MyView"
                QueryText: "Some query"
            )";

        AsyncCreateView(runtime, ++txId, "/MyRoot", viewConfig);
        AsyncCreateView(runtime, ++txId, "/MyRoot", viewConfig);
        AsyncCreateView(runtime, ++txId, "/MyRoot", viewConfig);

        const TVector<TExpectedResult> expectedResults = {EStatus::StatusAccepted,
                                                          EStatus::StatusMultipleModifications,
                                                          EStatus::StatusAlreadyExists};
        TestModificationResults(runtime, txId - 2, expectedResults);
        TestModificationResults(runtime, txId - 1, expectedResults);
        TestModificationResults(runtime, txId, expectedResults);

        env.TestWaitNotification(runtime, {txId - 2, txId - 1, txId});
        const auto describeResult = DescribePath(runtime, "/MyRoot/MyView");
        TestDescribeResult(describeResult,
                           {NLs::Finished,
                            NLs::IsView}
        );
        ExpectEqualQueryTexts(describeResult, "Some query");
    }

    Y_UNIT_TEST(AsyncDropSameView) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateView(runtime, ++txId, "/MyRoot", R"(
                Name: "MyView"
                QueryText: "Some query"
            )"
        );
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/MyView", false, NLs::PathExist);

        AsyncDropView(runtime, ++txId, "/MyRoot", "MyView");
        AsyncDropView(runtime, ++txId, "/MyRoot", "MyView");
        AsyncDropView(runtime, ++txId, "/MyRoot", "MyView");

        const TVector<TExpectedResult> expectedResults = {EStatus::StatusAccepted,
                                                          EStatus::StatusMultipleModifications,
                                                          EStatus::StatusPathDoesNotExist};
        TestModificationResults(runtime, txId - 2, expectedResults);
        TestModificationResults(runtime, txId - 1, expectedResults);
        TestModificationResults(runtime, txId, expectedResults);

        env.TestWaitNotification(runtime, {txId - 1, txId});
        TestLs(runtime, "/MyRoot/MyView", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(ReadOnlyMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateView(runtime, ++txId, "/MyRoot", R"(
                Name: "FirstView"
                QueryText: "Some query"
            )"
        );

        SetSchemeshardReadOnlyMode(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/FirstView"),
                           {NLs::Finished,
                            NLs::IsView}
        );

        TestCreateView(runtime, ++txId, "/MyRoot", R"(
                Name: "SecondView"
                QueryText: "Some query"
            )",
            {{NKikimrScheme::StatusReadOnly}}
        );

        SetSchemeshardReadOnlyMode(runtime, false);
        sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestCreateView(runtime, ++txId, "/MyRoot", R"(
                Name: "ThirdView"
                QueryText: "Some query"
            )"
        );
    }

    Y_UNIT_TEST(EmptyName) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateView(runtime, ++txId, "/MyRoot", R"(
                Name: ""
                QueryText: "Some query"
            )",
            {{NKikimrScheme::StatusSchemeError, "error: path part shouldn't be empty"}}
        );
    }

    Y_UNIT_TEST(EmptyQueryText) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateView(runtime, ++txId, "/MyRoot", R"(
                Name: "MyView"
                QueryText: ""
            )"
        );
    }
}
