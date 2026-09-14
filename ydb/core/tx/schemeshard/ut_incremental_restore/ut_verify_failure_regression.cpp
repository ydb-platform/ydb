// Regression test for VERIFY failure in SchemeShard
// This test is designed to CRASH with current code to demonstrate an unfixed bug
// After the fix is applied, the test should PASS
//
// Issue: Missing IsUnderIncomingIncrementalRestore() in IsUnderOperation() sum check
// Crash location: schemeshard_path.cpp:1583 Y_VERIFY_S(sum == 1)

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TVerifyFailureRegressionTests) {

    // Test 1: IsUnderOperation() crash for EPathStateIncomingIncrementalRestore
    //
    // This test verifies that calling IsUnderOperation() on a path in
    // EPathStateIncomingIncrementalRestore state will crash with Y_VERIFY_S(sum == 1)
    // because IsUnderIncomingIncrementalRestore() is NOT included in the sum check.
    //
    // The bug is in schemeshard_path.cpp IsUnderOperation():
    //   ui32 sum = (ui32)IsUnderCreating()
    //            + (ui32)IsUnderAltering()
    //            + (ui32)IsUnderCopying()
    //            + (ui32)IsUnderBackingUp()
    //            + (ui32)IsUnderRestoring()
    //            + (ui32)IsUnderDeleting()
    //            + (ui32)IsUnderDomainUpgrade()
    //            + (ui32)IsUnderMoving()
    //            + (ui32)IsUnderOutgoingIncrementalRestore();
    //   // MISSING: + (ui32)IsUnderIncomingIncrementalRestore();
    //   Y_VERIFY_S(sum == 1, ...);  // CRASHES when state is EPathStateIncomingIncrementalRestore
    //
    Y_UNIT_TEST(IsUnderOperationCrashesForIncomingIncrementalRestore) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Create source table
        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "src1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");

        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId, txId-1});

        // Create destination table in EPathStateIncomingIncrementalRestore state
        // This uses TestConsistentCopyTables with TargetPathTargetState set
        TestConsistentCopyTables(runtime, ++txId, "/", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/DirA/src1"
                DstPath: "/MyRoot/DirA/dst1"
                TargetPathTargetState: EPathStateIncomingIncrementalRestore
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Verify the path is in EPathStateIncomingIncrementalRestore
        auto desc = DescribePath(runtime, "/MyRoot/DirA/dst1", true);
        UNIT_ASSERT_VALUES_EQUAL(
            desc.GetPathDescription().GetSelf().GetPathState(),
            NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore);

        // Now try to perform any operation that would call IsUnderOperation()
        // For example, trying to alter the table should trigger the path state check
        //
        // The crash will occur in TPath::IsUnderOperation() because:
        // - PathState == EPathStateIncomingIncrementalRestore (value 14)
        // - result = true (since PathState != EPathStateNoChanges)
        // - sum = 0 (because no IsUnder* function matches this state)
        // - Y_VERIFY_S(sum == 1) FAILS
        //
        // Note: This may only crash in debug builds where Y_VERIFY_S is active
        AsyncAlterTable(runtime, ++txId, "/MyRoot/DirA", R"(
            Name: "dst1"
            Columns { Name: "extra" Type: "Utf8" }
        )");

        // The test should crash before reaching here
        // If we reach here, the bug has been fixed
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, txId);
    }
}
