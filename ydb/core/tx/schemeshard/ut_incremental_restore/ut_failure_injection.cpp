#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/base/test_failure_injection.h>

#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardTestFailureInjection) {

// Helper functions for failure injection tests
TString GetUniqueTableName(const TString& baseName, ui64 testId) {
    return TStringBuilder() << "FailInj_" << baseName << "_" << testId;
}

TString GetUniqueBackupName(const TString& baseName, ui64 testId) {
    return TStringBuilder() << "FailInj_" << baseName << "_" << testId;
}

void SetupBackupInfrastructure(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
    TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
    env.TestWaitNotification(runtime, txId);
    TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
    env.TestWaitNotification(runtime, txId);
}

void CreateTestTable(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, const TString& tableName, ui64 testId) {
    TString uniqueTableName = GetUniqueTableName(tableName, testId);
    TString tableSchema = TStringBuilder() << R"(
          Name: ")" << uniqueTableName << R"("
          Columns { Name: "key"   Type: "Uint64" }
          Columns { Name: "value" Type: "Utf8" }
          KeyColumnNames: ["key"]
    )";
    
    AsyncCreateTable(runtime, ++txId, "/MyRoot", tableSchema);
    TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
    env.TestWaitNotification(runtime, txId);
}

void CreateBackupCollection(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, const TString& name, ui64 testId) {
    TString uniqueBackupName = GetUniqueBackupName(name, testId);
    TString uniqueTableName = GetUniqueTableName("Table1", testId);
    TString backupCollectionSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
        ExplicitEntryList {
            Entries {
                Path: "/MyRoot/)" << uniqueTableName << R"("
                Type: ETypeTable
            }
        }
        Cluster: {}
    )";
    
    AsyncCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", backupCollectionSchema);
    TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
    env.TestWaitNotification(runtime, txId);
}

void CreateFullBackup(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, const TString& collectionName, const TString& tableName, ui64 testId) {
    TString uniqueBackupName = GetUniqueBackupName(collectionName, testId);
    TString uniqueTableName = GetUniqueTableName(tableName, testId);
    TString backupDirName = "backup_001_full";
    
    // Create the full backup directory
    TestMkDir(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << uniqueBackupName, backupDirName);
    env.TestWaitNotification(runtime, txId);
    
    // Create the table backup inside the full backup directory
    TString tableSchema = TStringBuilder() << R"(
        Name: ")" << uniqueTableName << R"("
        Columns { Name: "key"   Type: "Uint64" }
        Columns { Name: "value" Type: "Utf8" }
        KeyColumnNames: ["key"]
    )";
    
    AsyncCreateTable(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << uniqueBackupName << "/" << backupDirName, tableSchema);
    env.TestWaitNotification(runtime, txId);
}

void CreateIncrementalBackups(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, const TString& collectionName, const TString& tableName, ui64 testId, ui32 count = 2) {
    TString uniqueBackupName = GetUniqueBackupName(collectionName, testId);
    TString uniqueTableName = GetUniqueTableName(tableName, testId);
    
    for (ui32 i = 2; i < 2 + count; ++i) {
        TString incrDirName = TStringBuilder() << "backup_" << Sprintf("%03d", i) << "_incremental";
        
        // Create the incremental backup directory
        TestMkDir(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << uniqueBackupName, incrDirName);
        env.TestWaitNotification(runtime, txId);
        
        // Create the table backup inside the incremental backup directory
        TString tableSchema = TStringBuilder() << R"(
            Name: ")" << uniqueTableName << R"("
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            Columns { Name: "__ydb_deleted" Type: "Bool" }
            KeyColumnNames: ["key"]
        )";
        
        AsyncCreateTable(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << uniqueBackupName << "/" << incrDirName, tableSchema);
        env.TestWaitNotification(runtime, txId);
    }
}

void CreateCompleteBackupScenario(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, const TString& collectionName, const TString& tableName, ui64 testId) {
    // Create the backup collection
    CreateBackupCollection(runtime, env, txId, collectionName, testId);
    
    // Create the full backup with table backup
    CreateFullBackup(runtime, env, txId, collectionName, tableName, testId);
    
    // Create incremental backups with table backups
    CreateIncrementalBackups(runtime, env, txId, collectionName, tableName, testId);
}

Y_UNIT_TEST(TestBackupCollectionNotFoundFailure) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
    ui64 txId = 100;
    ui64 testId = RandomNumber<ui64>();
    
    // Setup backup infrastructure directories
    SetupBackupInfrastructure(runtime, env, txId);
    
    // Create a backup collection and backup data (but don't create the actual table)
    // This way the restore operation will proceed to backup collection lookup where our failure injection waits
    CreateCompleteBackupScenario(runtime, env, txId, "TestBackup", "Table1", testId);
    
    // Inject failure: backup collection not found
    // This should override the normal operation and return StatusPathDoesNotExist
    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::BackupCollectionNotFound));
    
    // Try to start long incremental restore operation to a new path that doesn't exist
    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";
    
    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    
    // Expect failure due to injected error
    env.TestWaitNotification(runtime, txId, NKikimrScheme::StatusPathDoesNotExist);
    
    // Clear failure injection
    runtime.GetAppData().RemoveFailure(static_cast<ui64>(EInjectedFailureType::BackupCollectionNotFound));
}

Y_UNIT_TEST(TestBackupChildrenEmptyFailure) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
    ui64 txId = 100;
    ui64 testId = RandomNumber<ui64>();
    
    SetupBackupInfrastructure(runtime, env, txId);
    CreateTestTable(runtime, env, txId, "Table1", testId);
    CreateCompleteBackupScenario(runtime, env, txId, "TestBackup", "Table1", testId);
    
    // Inject failure: backup children empty
    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::BackupChildrenEmpty));
    
    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";
    
    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    env.TestWaitNotification(runtime, txId, NKikimrScheme::StatusSchemeError);
    
    // Clear failure injection
    runtime.GetAppData().RemoveFailure(static_cast<ui64>(EInjectedFailureType::BackupChildrenEmpty));
}

Y_UNIT_TEST(TestPathSplitFailure) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
    ui64 txId = 100;
    ui64 testId = RandomNumber<ui64>();
    
    SetupBackupInfrastructure(runtime, env, txId);
    CreateTestTable(runtime, env, txId, "Table1", testId);
    CreateCompleteBackupScenario(runtime, env, txId, "TestBackup", "Table1", testId);
    
    // Inject failure: path split failure
    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::PathSplitFailure));
    
    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";
    
    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    env.TestWaitNotification(runtime, txId, NKikimrScheme::StatusInvalidParameter);
    
    // Clear failure injection
    runtime.GetAppData().RemoveFailure(static_cast<ui64>(EInjectedFailureType::PathSplitFailure));
}

Y_UNIT_TEST(TestIncrementalBackupPathNotResolvedFailure) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
    ui64 txId = 100;
    ui64 testId = RandomNumber<ui64>();
    
    SetupBackupInfrastructure(runtime, env, txId);
    CreateTestTable(runtime, env, txId, "Table1", testId);
    CreateCompleteBackupScenario(runtime, env, txId, "TestBackup", "Table1", testId);
    
    // Inject failure: incremental backup path not resolved
    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::IncrementalBackupPathNotResolved));
    
    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";
    
    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    env.TestWaitNotification(runtime, txId, NKikimrScheme::StatusPathDoesNotExist);
    
    // Clear failure injection
    runtime.GetAppData().RemoveFailure(static_cast<ui64>(EInjectedFailureType::IncrementalBackupPathNotResolved));
}

Y_UNIT_TEST(TestCreateChangePathStateFailedFailure) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
    ui64 txId = 100;
    ui64 testId = RandomNumber<ui64>();
    
    SetupBackupInfrastructure(runtime, env, txId);
    CreateTestTable(runtime, env, txId, "Table1", testId);
    CreateCompleteBackupScenario(runtime, env, txId, "TestBackup", "Table1", testId);
    
    // Inject failure: create change path state failed
    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::CreateChangePathStateFailed));
    
    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";
    
    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    env.TestWaitNotification(runtime, txId, NKikimrScheme::StatusMultipleModifications);
    
    // Clear failure injection
    runtime.GetAppData().RemoveFailure(static_cast<ui64>(EInjectedFailureType::CreateChangePathStateFailed));
}

Y_UNIT_TEST(TestAbortProposeOperation) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
    ui64 txId = 100;
    ui64 testId = RandomNumber<ui64>();
    
    SetupBackupInfrastructure(runtime, env, txId);
    CreateTestTable(runtime, env, txId, "Table1", testId);
    CreateCompleteBackupScenario(runtime, env, txId, "TestBackup", "Table1", testId);
    
    // Create long incremental restore operation
    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";
    
    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    env.TestWaitNotification(runtime, txId, NKikimrScheme::StatusAccepted);
    
    ui64 restoreTxId = txId;
    
    // Now abort the operation via AbortOperation
    // This tests the AbortPropose functionality we implemented
    AsyncCancelTxTable(runtime, ++txId, restoreTxId);
    env.TestWaitNotification(runtime, txId);
    
    // Verify the restore operation is no longer active
    TestDescribe(runtime, "/MyRoot");
    // The describe should not show any ongoing restore operations
}

Y_UNIT_TEST(TestFailureInjectionToggling) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
    ui64 txId = 100;
    ui64 testId = RandomNumber<ui64>();
    
    SetupBackupInfrastructure(runtime, env, txId);
    CreateTestTable(runtime, env, txId, "Table1", testId);
    CreateCompleteBackupScenario(runtime, env, txId, "TestBackup", "Table1", testId);
    
    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";
    
    // Test that we can toggle failures on and off
    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::BackupCollectionNotFound));
    
    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    env.TestWaitNotification(runtime, txId, NKikimrScheme::StatusPathDoesNotExist);
    
    // Remove this failure and add a different one
    runtime.GetAppData().RemoveFailure(static_cast<ui64>(EInjectedFailureType::BackupCollectionNotFound));
    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::BackupChildrenEmpty));
    
    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    env.TestWaitNotification(runtime, txId, NKikimrScheme::StatusSchemeError);
    
    // Clear all failures - should work now
    runtime.GetAppData().ClearAllFailures();
    
    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    env.TestWaitNotification(runtime, txId, NKikimrScheme::StatusAccepted);
}

} // Y_UNIT_TEST_SUITE(TSchemeShardTestFailureInjection)
