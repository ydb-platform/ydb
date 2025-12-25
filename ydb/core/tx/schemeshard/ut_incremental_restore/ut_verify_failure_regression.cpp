// Regression tests for VERIFY failure issues in SchemeShard
// These tests are designed to CRASH with current code to demonstrate unfixed bugs
// After fixes are applied, they should PASS
//
// Issues being tested:
// 1. Missing IsUnderIncomingIncrementalRestore() in IsUnderOperation() sum check
//    - Crash location: schemeshard_path.cpp:1583 Y_VERIFY_S(sum == 1)
// 2. No defensive check in DoDoneTransactions()
//    - Crash location: schemeshard__operation_side_effects.cpp:956 Y_ABORT_UNLESS

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>

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

    // Test 2: DoDoneTransactions() crash when path is removed from PathsById
    //
    // This test verifies that DoDoneTransactions() crashes with Y_ABORT_UNLESS
    // when a path registered in ReleasePathAtDone has been removed from PathsById.
    //
    // The bug is in schemeshard__operation_side_effects.cpp DoDoneTransactions():
    //   for (auto& item: operation->ReleasePathAtDone) {
    //       TPathId pathId = item.first;
    //       Y_ABORT_UNLESS(ss->PathsById.contains(pathId));  // CRASHES if path was removed
    //       ...
    //   }
    //
    // This can happen when:
    // 1. An operation registers paths in ReleasePathAtDone
    // 2. A concurrent operation or abort removes the path from PathsById
    // 3. The original operation tries to complete via DoDoneTransactions()
    //
    // NOTE: This test demonstrates the race condition scenario. The actual crash
    // depends on timing and may not occur deterministically. The test structure
    // shows how the bug can be triggered.
    //
    Y_UNIT_TEST(DoDoneTransactionsCrashesOnMissingPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Setup backup infrastructure
        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        env.TestWaitNotification(runtime, txId);

        // NOTE: Do NOT create SourceTable here - the restore will create it
        // This avoids the "path exist" error

        // Create backup collection pointing to a table that doesn't exist yet
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", R"(
            Name: "TestCollection"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/SourceTable"
                }
            }
            Cluster: {}
        )");
        env.TestWaitNotification(runtime, txId);

        // Create full backup structure (this is what will be restored)
        TestMkDir(runtime, ++txId, "/MyRoot/.backups/collections/TestCollection", "backup_001_full");
        env.TestWaitNotification(runtime, txId);

        AsyncCreateTable(runtime, ++txId, "/MyRoot/.backups/collections/TestCollection/backup_001_full", R"(
              Name: "SourceTable"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Create incremental backup structure
        TestMkDir(runtime, ++txId, "/MyRoot/.backups/collections/TestCollection", "backup_002_incremental");
        env.TestWaitNotification(runtime, txId);

        AsyncCreateTable(runtime, ++txId, "/MyRoot/.backups/collections/TestCollection/backup_002_incremental", R"(
              Name: "SourceTable"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value" Type: "Utf8" }
              Columns { Name: "__ydb_incrBackupImpl_changeMetadata" Type: "String" }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Start incremental restore operation (async)
        // This will:
        // 1. Create the table (copy from backup)
        // 2. Register paths in ReleasePathAtDone for state transitions
        AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", R"(
            Name: "TestCollection"
        )");
        ui64 restoreTxId = txId;

        // Immediately submit a drop on the table being created
        // This creates a race condition where the path might be removed
        // while still registered in ReleasePathAtDone
        AsyncDropTable(runtime, ++txId, "/MyRoot", "SourceTable");
        ui64 dropTxId = txId;

        // Check results - restore should succeed, drop should fail
        // The crash would occur when the restore operation tries to complete
        // and DoDoneTransactions() finds the path missing from PathsById
        TestModificationResult(runtime, restoreTxId, NKikimrScheme::StatusAccepted);

        // Drop should fail - table doesn't exist yet (restore is creating it)
        TestModificationResult(runtime, dropTxId, NKikimrScheme::StatusPathDoesNotExist);

        // Wait for completion - crash would occur here if bug exists
        env.TestWaitNotification(runtime, restoreTxId);

        // If we reach here without crash, either:
        // 1. The bug was fixed (defensive check added)
        // 2. The race condition didn't trigger in this run
        // 3. The operations were properly serialized
    }
}
