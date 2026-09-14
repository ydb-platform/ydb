#include <ydb/core/base/test_failure_injection.h>
#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardTestFailureInjection) {

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

    TestMkDir(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << uniqueBackupName, backupDirName);
    env.TestWaitNotification(runtime, txId);

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

        TestMkDir(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << uniqueBackupName, incrDirName);
        env.TestWaitNotification(runtime, txId);

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
    CreateBackupCollection(runtime, env, txId, collectionName, testId);

    CreateFullBackup(runtime, env, txId, collectionName, tableName, testId);

    CreateIncrementalBackups(runtime, env, txId, collectionName, tableName, testId);
}

Y_UNIT_TEST(TestBackupCollectionNotFoundFailure) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
    ui64 txId = 100;
    ui64 testId = RandomNumber<ui64>();

    SetupBackupInfrastructure(runtime, env, txId);

    CreateCompleteBackupScenario(runtime, env, txId, "TestBackup", "Table1", testId);

    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::BackupCollectionNotFound));

    // Try to start long incremental restore operation to a new path that doesn't exist
    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";

    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);

    TestModificationResult(runtime, txId, NKikimrScheme::StatusPathDoesNotExist);

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

    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::BackupChildrenEmpty));

    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";

    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    TestModificationResult(runtime, txId, NKikimrScheme::StatusSchemeError);

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

    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::PathSplitFailure));

    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";

    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    TestModificationResult(runtime, txId, NKikimrScheme::StatusInvalidParameter);

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

    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::IncrementalBackupPathNotResolved));

    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";

    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    TestModificationResult(runtime, txId, NKikimrScheme::StatusPathDoesNotExist);

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

    runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::CreateChangePathStateFailed));

    TString uniqueBackupName = GetUniqueBackupName("TestBackup", testId);
    TString restoreOpSchema = TStringBuilder() << R"(
        Name: ")" << uniqueBackupName << R"("
    )";

    AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", restoreOpSchema);
    TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);

    runtime.GetAppData().RemoveFailure(static_cast<ui64>(EInjectedFailureType::CreateChangePathStateFailed));
}

} // Y_UNIT_TEST_SUITE(TSchemeShardTestFailureInjection)
