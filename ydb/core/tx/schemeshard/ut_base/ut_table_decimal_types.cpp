#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardDecimalTypesInTables) {

    Y_UNIT_TEST(Parameterless) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Decimal" }
            Columns { Name: "value" Type: "Decimal" }
            KeyColumnNames: ["key"]
        )");
        TestModificationResults(runtime, txId, {TExpectedResult(NKikimrScheme::StatusAccepted)});
        env.TestWaitNotification(runtime, txId);

        AsyncAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "added" Type: "Decimal" }
        )");
        TestModificationResults(runtime, txId, {TExpectedResult(NKikimrScheme::StatusAccepted)});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
            NLs::PathExist,
            NLs::Finished,
            NLs::CheckColumnType(0, "Decimal")
        });
    }

    Y_UNIT_TEST_FLAG(Parameters_22_9, EnableParameterizedDecimal) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableParameterizedDecimal(EnableParameterizedDecimal));
        ui64 txId = 100;

        auto expectedResult = []() {
            if constexpr (EnableParameterizedDecimal) {
                return TExpectedResult(NKikimrScheme::StatusAccepted);
            } else {
                return TExpectedResult(NKikimrScheme::StatusSchemeError, "Type 'Decimal(22,9)' specified for column 'key', but support for parametrized decimal is disabled (EnableParameterizedDecimal feature flag is off)");
            }
        }();        

        AsyncCreateTable(runtime, ++txId, "/MyRoot", R"_(
            Name: "Table1"
            Columns { Name: "key"   Type: "Decimal(22,9)" }
            Columns { Name: "value" Type: "Decimal(22,9)" }
            KeyColumnNames: ["key"]
        )_");
        TestModificationResults(runtime, txId, {expectedResult});
        env.TestWaitNotification(runtime, txId);

        if constexpr (!EnableParameterizedDecimal)
            return;

        AsyncAlterTable(runtime, ++txId, "/MyRoot", R"_(
            Name: "Table1"
            Columns { Name: "added" Type: "Decimal(22,9)" }
        )_");
        TestModificationResults(runtime, txId, {TExpectedResult(NKikimrScheme::StatusAccepted)});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
            NLs::PathExist,
            NLs::Finished,
            NLs::CheckColumnType(0, "Decimal")
        });        
    }

    Y_UNIT_TEST_FLAG(Parameters_35_6, EnableParameterizedDecimal) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableParameterizedDecimal(EnableParameterizedDecimal));
        ui64 txId = 100;

        auto expectedResult = []() {
            if constexpr (EnableParameterizedDecimal) {
                return TExpectedResult(NKikimrScheme::StatusAccepted);
            } else {
                return TExpectedResult(NKikimrScheme::StatusSchemeError, "Type 'Decimal(35,6)' specified for column 'key', but support for parametrized decimal is disabled (EnableParameterizedDecimal feature flag is off)");
            }
        }();

        AsyncCreateTable(runtime, ++txId, "/MyRoot", R"_(
            Name: "Table1"
            Columns { Name: "key"   Type: "Decimal(35,6)" }
            Columns { Name: "value" Type: "Decimal(35,6)" }
            KeyColumnNames: ["key"]
        )_");
        TestModificationResults(runtime, txId, {expectedResult});
        env.TestWaitNotification(runtime, txId);

        if constexpr (!EnableParameterizedDecimal)
            return;

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
            NLs::PathExist,
            NLs::Finished,
            NLs::CheckColumnType(0, "Decimal(35,6)")
        });        
    }


}
