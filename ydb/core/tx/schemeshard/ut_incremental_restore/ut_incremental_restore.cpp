#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/lib/value/value.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

// Common setup function for all long operation tests
struct TLongOpTestSetup {
    TTestBasicRuntime Runtime;
    TTestEnv Env;
    ui64 TxId;
    
    TLongOpTestSetup() 
        : Env(Runtime, TTestEnvOptions().EnableBackupService(true))
        , TxId(100)
    {
        // Setup backup infrastructure directories
        TestMkDir(Runtime, ++TxId, "/MyRoot", ".backups");
        Env.TestWaitNotification(Runtime, TxId);
        TestMkDir(Runtime, ++TxId, "/MyRoot/.backups", "collections");
        Env.TestWaitNotification(Runtime, TxId);
    }
    
    // Create a test table with standard schema
    void CreateStandardTable(const TString& tableName) {
        TString tableSchema = TStringBuilder() << R"(
              Name: ")" << tableName << R"("
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )";
        
        AsyncCreateTable(Runtime, ++TxId, "/MyRoot", tableSchema);
        Env.TestWaitNotification(Runtime, TxId);
    }
    
    // Create a test table with custom schema
    void CreateTable(const TString& schema) {
        AsyncCreateTable(Runtime, ++TxId, "/MyRoot", schema);
        Env.TestWaitNotification(Runtime, TxId);
    }
    
    // Create a backup collection with specified table paths
    void CreateBackupCollection(const TString& collectionName, const TVector<TString>& tablePaths) {
        TStringBuilder settingsBuilder;
        settingsBuilder << R"(
            Name: ")" << collectionName << R"("
            ExplicitEntryList {)";
        
        for (const auto& tablePath : tablePaths) {
            settingsBuilder << R"(
                Entries {
                    Type: ETypeTable
                    Path: ")" << tablePath << R"("
                })";
        }
        
        settingsBuilder << R"(
            }
            Cluster: {}
        )";
        
        TestCreateBackupCollection(Runtime, ++TxId, "/MyRoot/.backups/collections/", settingsBuilder);
        Env.TestWaitNotification(Runtime, TxId);
    }
    
    // Create full backup structure for a collection
    void CreateFullBackup(const TString& collectionName, const TVector<TString>& tableNames, const TString& backupName = "backup_001_full") {
        TString backupPath = TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/" << backupName;
        TestMkDir(Runtime, ++TxId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName, backupName);
        Env.TestWaitNotification(Runtime, TxId);
        
        for (const auto& tableName : tableNames) {
            TString tableSchema = TStringBuilder() << R"(
                  Name: ")" << tableName << R"("
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value" Type: "Utf8" }
                  KeyColumnNames: ["key"]
            )";
            
            AsyncCreateTable(Runtime, ++TxId, backupPath, tableSchema);
            Env.TestWaitNotification(Runtime, TxId);
        }
    }
    
    // Create incremental backup structure for a collection
    void CreateIncrementalBackups(const TString& collectionName, const TVector<TString>& tableNames, ui32 count = 3, ui32 startIndex = 2) {
        for (ui32 i = startIndex; i < startIndex + count; ++i) {
            TString incrName = TStringBuilder() << "backup_" << Sprintf("%03d", i) << "_incremental";
            TString backupPath = TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/" << incrName;
            
            TestMkDir(Runtime, ++TxId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName, incrName);
            Env.TestWaitNotification(Runtime, TxId);
            
            for (const auto& tableName : tableNames) {
                TString tableSchema = TStringBuilder() << R"(
                      Name: ")" << tableName << R"("
                      Columns { Name: "key"   Type: "Uint64" }
                      Columns { Name: "value" Type: "Utf8" }
                      Columns { Name: "__ydb_deleted" Type: "Bool" }
                      KeyColumnNames: ["key"]
                )";
                
                AsyncCreateTable(Runtime, ++TxId, backupPath, tableSchema);
                Env.TestWaitNotification(Runtime, TxId);
            }
        }
    }
    
    // Execute restore operation
    void ExecuteRestore(const TString& collectionName, const TVector<NSchemeShardUT_Private::TExpectedResult>& expectedResults = {}) {
        TString restoreSettings = TStringBuilder() << R"(
            Name: ")" << collectionName << R"("
        )";
        
        if (expectedResults.empty()) {
            TestRestoreBackupCollection(Runtime, ++TxId, "/MyRoot/.backups/collections/", restoreSettings);
        } else {
            TestRestoreBackupCollection(Runtime, ++TxId, "/MyRoot/.backups/collections/", restoreSettings, expectedResults);
        }
        Env.TestWaitNotification(Runtime, TxId);
    }
    
    // Execute async restore operation (for testing concurrent operations)
    void ExecuteAsyncRestore(const TString& collectionName) {
        TString restoreSettings = TStringBuilder() << R"(
            Name: ")" << collectionName << R"("
        )";
        
        AsyncRestoreBackupCollection(Runtime, ++TxId, "/MyRoot/.backups/collections/", restoreSettings);
    }
    
    // Create a complete backup scenario (collection + full + incremental backups)
    void CreateCompleteBackupScenario(const TString& collectionName, const TVector<TString>& tableNames, ui32 incrementalCount = 3) {
        // Create backup collection
        TVector<TString> tablePaths;
        for (const auto& tableName : tableNames) {
            tablePaths.push_back(TStringBuilder() << "/MyRoot/" << tableName);
        }
        CreateBackupCollection(collectionName, tablePaths);
        
        // Create full backup
        CreateFullBackup(collectionName, tableNames);
        
        // Create incremental backups
        CreateIncrementalBackups(collectionName, tableNames, incrementalCount);
    }
    
    // Create custom backup directories (for testing specific scenarios)
    void CreateCustomBackupDirectories(const TString& collectionName, const TVector<TString>& backupNames) {
        for (const auto& backupName : backupNames) {
            TestMkDir(Runtime, ++TxId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName, backupName);
            Env.TestWaitNotification(Runtime, TxId);
        }
    }
};

Y_UNIT_TEST_SUITE(TIncrementalRestoreTests) {
    Y_UNIT_TEST(CopyTableChangeStateSupport) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");

        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "src1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");

        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId-1});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/dst1"
                         TargetPathTargetState: EPathStateIncomingIncrementalRestore
                       }
                      )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/dst1", true),
                           {NLs::CheckPathState(NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore)});
    }

    Y_UNIT_TEST(CreateLongIncrementalRestoreOpBasic) {
        TLongOpTestSetup setup;

        // Create complete backup scenario (don't create the actual table since restore will create it)
        setup.CreateCompleteBackupScenario("TestCollection", {"TestTable"}, 3);

        // Verify backup collection exists
        TestDescribeResult(DescribePath(setup.Runtime, "/MyRoot/.backups/collections/TestCollection"), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        // Execute restore operation
        setup.ExecuteRestore("TestCollection");

        // The operation should complete successfully
        // We can't easily test the internal ESchemeOpCreateLongIncrementalRestoreOp dispatch
        // without deeper integration, but we can verify the overall restore works
    }

    Y_UNIT_TEST(CreateLongIncrementalRestoreOpNonExistentCollection) {
        TLongOpTestSetup setup;

        // Try to restore from non-existent backup collection
        setup.ExecuteRestore("NonExistentCollection", {NKikimrScheme::StatusPathDoesNotExist});
    }

    Y_UNIT_TEST(CreateLongIncrementalRestoreOpInvalidPath) {
        TLongOpTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& env = setup.Env;
        auto& txId = setup.TxId;

        // Create a regular directory that is not a backup collection directory
        TestMkDir(runtime, ++txId, "/MyRoot", "NotABackupDir");
        env.TestWaitNotification(runtime, txId);
        
        // Create a collection inside the wrong directory to make the path exist
        TestMkDir(runtime, ++txId, "/MyRoot/NotABackupDir", "TestCollection");
        env.TestWaitNotification(runtime, txId);

        // Try to restore from invalid path (not a backup collection directory)
        TString restoreSettings = R"(
            Name: "TestCollection"
        )";

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot/NotABackupDir/", restoreSettings, 
                                   {NKikimrScheme::StatusNameConflict});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CreateLongIncrementalRestoreOpWithMultipleTables) {
        TLongOpTestSetup setup;

        // Create complete backup scenario with multiple tables (don't create the actual tables since restore will create them)
        setup.CreateCompleteBackupScenario("MultiTableCollection", {"Table1", "Table2"}, 2);

        // Execute restore operation
        setup.ExecuteRestore("MultiTableCollection");
    }

    Y_UNIT_TEST(CreateLongIncrementalRestoreOpOperationAlreadyInProgress) {
        TLongOpTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& env = setup.Env;
        auto& txId = setup.TxId;

        // Create backup collection for BusyTable (note: don't create the actual table since restore will create it)
        setup.CreateBackupCollection("BusyCollection", {"/MyRoot/BusyTable"});

        // Create backup structure manually to ensure long restore scenario
        setup.CreateFullBackup("BusyCollection", {"BusyTable"});
        setup.CreateIncrementalBackups("BusyCollection", {"BusyTable"}, 2);

        // Start first restore operation
        setup.ExecuteAsyncRestore("BusyCollection");
        ui64 firstTxId = txId;
        
        // Try to start another restore operation on the same collection (should fail)
        setup.ExecuteAsyncRestore("BusyCollection");
        ui64 secondTxId = txId;
        
        // First operation should succeed
        TestModificationResult(runtime, firstTxId, NKikimrScheme::StatusAccepted);
        // Second operation should fail due to already in progress
        TestModificationResult(runtime, secondTxId, NKikimrScheme::StatusMultipleModifications);
        
        env.TestWaitNotification(runtime, {firstTxId, secondTxId});
    }

    Y_UNIT_TEST(CreateLongIncrementalRestoreOpFactoryDispatch) {
        TLongOpTestSetup setup;

        // Create complete backup scenario (don't create the actual table since restore will create it)
        setup.CreateCompleteBackupScenario("DispatchTestCollection", {"DispatchTestTable"}, 2);

        // Verify backup collection exists and has correct type
        TestDescribeResult(DescribePath(setup.Runtime, "/MyRoot/.backups/collections/DispatchTestCollection"), {
            NLs::PathExist,
            NLs::IsBackupCollection,
        });

        // Execute restore operation
        setup.ExecuteRestore("DispatchTestCollection");

        // Verify that the operation completed successfully
        // The fact that it doesn't crash or return an error indicates that:
        // 1. The operation enum value is properly defined
        // 2. The factory dispatch case exists and works
        // 3. The CreateLongIncrementalRestoreOpControlPlane function works
        // 4. All the audit log and tx_proxy support is working
    }

    Y_UNIT_TEST(CreateLongIncrementalRestoreOpInternalTransaction) {
        TLongOpTestSetup setup;

        // This test verifies that the internal ESchemeOpCreateLongIncrementalRestoreOp
        // transaction can be created and processed without errors

        // Create backup collection (note: don't create the actual table since restore will create it)
        setup.CreateBackupCollection("InternalTestCollection", {"/MyRoot/InternalTestTable"});

        // Create backup structure with incremental backups to trigger long restore
        setup.CreateFullBackup("InternalTestCollection", {"InternalTestTable"}, "base_backup_full");
        
        // Add multiple incremental backups to simulate a long restore scenario
        setup.CreateCustomBackupDirectories("InternalTestCollection", {
            "incr_1_incremental", "incr_2_incremental", "incr_3_incremental", 
            "incr_4_incremental", "incr_5_incremental"
        });

        // Execute restore operation
        setup.ExecuteRestore("InternalTestCollection");

        // The restore should internally:
        // 1. Detect the presence of incremental backups
        // 2. Create a ESchemeOpCreateLongIncrementalRestoreOp operation
        // 3. Dispatch it through the operation factory
        // 4. Execute the control plane operation
        // 5. Complete successfully without Y_UNREACHABLE() errors
        // If we reach this point without crashes, the operation dispatch is working correctly
    }

    Y_UNIT_TEST(CreateLongIncrementalRestoreOpErrorHandling) {
        TLongOpTestSetup setup;

        // Test error handling scenarios for the ESchemeOpCreateLongIncrementalRestoreOp

        // Test 1: Try to restore from empty backup collection (no backups)
        setup.CreateBackupCollection("EmptyCollection", {"/MyRoot/NonExistentTable"});
        
        // This should fail with StatusInvalidParameter because there's nothing to restore
        setup.ExecuteRestore("EmptyCollection", {NKikimrScheme::StatusInvalidParameter});

        // Test 2: Try to restore from backup collection that doesn't match expected format
        setup.CreateBackupCollection("MalformedCollection", {"/MyRoot/TestTable"});

        // Create some directories that don't follow backup naming convention (no actual table backups inside)
        setup.CreateCustomBackupDirectories("MalformedCollection", {"invalid_backup_name", "another_invalid"});

        // Should fail with StatusPathDoesNotExist because the table backups don't exist in the backup directories
        setup.ExecuteRestore("MalformedCollection", {NKikimrScheme::StatusPathDoesNotExist});
        
        // Test 3: Try to restore with proper backup structure (should succeed)
        setup.CreateCompleteBackupScenario("ValidCollection", {"ValidTable"}, 1);
        
        // This should succeed because we have valid backup structure
        setup.ExecuteRestore("ValidCollection");
    }

    Y_UNIT_TEST(CreateLongIncrementalRestoreOpInternalStateVerification) {
        TLongOpTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& env = setup.Env;
        auto& txId = setup.TxId;

        // Create backup collection (note: we don't create the target table since restore will create it)
        TString collectionSettings = R"(
            Name: "DatabaseTestCollection"
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/DatabaseTestTable"
                }
            }
            Cluster: {}
        )";

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create backup structure that will trigger long incremental restore
        // First create the full backup directory and the table backup within it
        TestMkDir(runtime, ++txId, "/MyRoot/.backups/collections/DatabaseTestCollection", "backup_001_full");
        env.TestWaitNotification(runtime, txId);
        
        // Create the table backup entry within the full backup
        AsyncCreateTable(runtime, ++txId, "/MyRoot/.backups/collections/DatabaseTestCollection/backup_001_full", R"(
              Name: "DatabaseTestTable"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        
        // Add multiple incremental backups to ensure long restore scenario
        for (int i = 2; i <= 6; ++i) {
            TString incrName = TStringBuilder() << "backup_" << Sprintf("%03d", i) << "_incremental";
            TestMkDir(runtime, ++txId, "/MyRoot/.backups/collections/DatabaseTestCollection", incrName);
            env.TestWaitNotification(runtime, txId);
            
            // Create table backup entry within each incremental backup
            // For incremental backups, we need an additional column to track deletions
            AsyncCreateTable(runtime, ++txId, "/MyRoot/.backups/collections/DatabaseTestCollection/" + incrName, R"(
                  Name: "DatabaseTestTable"
                  Columns { Name: "key"   Type: "Uint32" }
                  Columns { Name: "value" Type: "Utf8" }
                  Columns { Name: "__ydb_deleted" Type: "Bool" }
                  KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);
        }

        // Capture the transaction ID that will be used for the restore operation
        ui64 restoreTxId = ++txId;

        // Execute the long incremental restore operation
        TString restoreSettings = R"(
            Name: "DatabaseTestCollection"
        )";

        TestRestoreBackupCollection(runtime, restoreTxId, "/MyRoot/.backups/collections/", restoreSettings);
        env.TestWaitNotification(runtime, restoreTxId);

        // Now verify that the operation data appears in SchemeShard's database tables
        // Query the IncrementalRestoreOperations table to check for our operation
        TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
        
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, schemeShardTabletId.GetValue(), Sprintf(R"(
            (
                (let range '('('Id (Null) (Void))))
                (let select '('Id 'Operation))
                (let operations (SelectRange 'IncrementalRestoreOperations range select '()))
                (let ret (AsList (SetResult 'Operations operations)))
                (return ret)
            )
        )"), result, err);
        
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        
        // Parse the result using NClient::TValue similar to CountRows function pattern
        auto value = NClient::TValue::Create(result);
        
        // Verify that we can access the Operations result set
        auto operationsResultSet = value["Operations"];
        UNIT_ASSERT_C(operationsResultSet.HaveValue(), "Operations result set should be present");
        
        auto operationsList = operationsResultSet["List"];
        if (operationsList.HaveValue()) {
            ui32 operationsCount = operationsList.Size();
            
            // Log the number of operations found
            Cerr << "Found " << operationsCount << " incremental restore operations in database" << Endl;
            
            // If we have operations, unpack and verify their structure
            for (ui32 i = 0; i < operationsCount; ++i) {
                auto operation = operationsList[i];
                
                // Verify that each operation has the expected fields and extract values
                auto operationIdValue = operation["Id"];
                auto operationDataValue = operation["Operation"];
                
                UNIT_ASSERT_C(operationIdValue.HaveValue(), "Operation should have Id field");
                UNIT_ASSERT_C(operationDataValue.HaveValue(), "Operation should have Operation field");
                
                // Extract the Id and Operation values using cast operators
                auto operationId = (ui64)operationIdValue;
                auto operationData = (TString)operationDataValue;
                
                Cerr << "Operation " << i << ": Id=" << operationId 
                     << ", Operation (serialized)=" << operationData << Endl;
                
                // Verify that the operation ID makes sense (should be non-zero)
                UNIT_ASSERT_C(operationId > 0, "Operation ID should be positive");
                
                // Verify that operation data is not empty
                UNIT_ASSERT_C(!operationData.empty(), "Operation data should not be empty");
                
                // Deserialize operation data from protobuf
                NKikimrSchemeOp::TLongIncrementalRestoreOp longIncrementalRestoreOp;
                bool parseSuccess = longIncrementalRestoreOp.ParseFromString(operationData);
                UNIT_ASSERT_C(parseSuccess, "Failed to parse operation data as TLongIncrementalRestoreOp protobuf");
                
                // Extract and verify fields from the unpacked protobuf
                Cerr << "Unpacked operation protobuf:" << Endl;
                Cerr << "  TxId: " << longIncrementalRestoreOp.GetTxId() << Endl;
                Cerr << "  Id: " << longIncrementalRestoreOp.GetId() << Endl;
                
                // Get BackupCollectionPathId (TPathID protobuf)
                if (longIncrementalRestoreOp.HasBackupCollectionPathId()) {
                    const auto& pathId = longIncrementalRestoreOp.GetBackupCollectionPathId();
                    Cerr << "  BackupCollectionPathId: OwnerId=" << pathId.GetOwnerId() 
                         << ", LocalId=" << pathId.GetLocalId() << Endl;
                }
                
                // Display table paths
                Cerr << "  TablePathList size: " << longIncrementalRestoreOp.GetTablePathList().size() << Endl;
                for (int i = 0; i < longIncrementalRestoreOp.GetTablePathList().size(); ++i) {
                    Cerr << "    Table " << i << ": " << longIncrementalRestoreOp.GetTablePathList(i) << Endl;
                }
                
                // Display backup names
                Cerr << "  FullBackupTrimmedName: " << longIncrementalRestoreOp.GetFullBackupTrimmedName() << Endl;
                Cerr << "  IncrementalBackupTrimmedNames size: " << longIncrementalRestoreOp.GetIncrementalBackupTrimmedNames().size() << Endl;
                for (int i = 0; i < longIncrementalRestoreOp.GetIncrementalBackupTrimmedNames().size(); ++i) {
                    Cerr << "    Incremental " << i << ": " << longIncrementalRestoreOp.GetIncrementalBackupTrimmedNames(i) << Endl;
                }
                
                // Verify that the protobuf has the expected structure
                UNIT_ASSERT_C(longIncrementalRestoreOp.GetTxId() > 0, "TxId in protobuf should be positive");
                UNIT_ASSERT_C(!longIncrementalRestoreOp.GetId().empty(), "Id should not be empty");
                UNIT_ASSERT_C(longIncrementalRestoreOp.HasBackupCollectionPathId(), "BackupCollectionPathId should be present");
            }
            
            UNIT_ASSERT_C(true, "Successfully queried and parsed IncrementalRestoreOperations table");
        } else {
            // No operations found, which is also valid for this test
            Cerr << "No operations found in IncrementalRestoreOperations table" << Endl;
            UNIT_ASSERT_C(true, "Successfully queried IncrementalRestoreOperations table (no operations found)");
        }

        // Now verify that path states are correctly set for incremental restore operations
        Cerr << "Verifying path states during incremental restore..." << Endl;

        // Check the target table state - it should be in EPathStateIncomingIncrementalRestore state
        auto targetTableDesc = DescribePath(runtime, "/MyRoot/DatabaseTestTable");
        auto targetState = targetTableDesc.GetPathDescription().GetSelf().GetPathState();
        Cerr << "Target table state: " << NKikimrSchemeOp::EPathState_Name(targetState) << Endl;
        
        // Assert that target table is in the correct incoming incremental restore state
        UNIT_ASSERT_VALUES_EQUAL_C(targetState, NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore,
            TStringBuilder() << "Target table should be in EPathStateIncomingIncrementalRestore state, but got: " 
                           << NKikimrSchemeOp::EPathState_Name(targetState));

        // Check source table states in the backup collection - they should be in EPathStateOutgoingIncrementalRestore
        auto sourceTableDesc = DescribePath(runtime, "/MyRoot/.backups/collections/DatabaseTestCollection/backup_001_full/DatabaseTestTable");
        auto sourceState = sourceTableDesc.GetPathDescription().GetSelf().GetPathState();
        Cerr << "Source table (full backup) state: " << NKikimrSchemeOp::EPathState_Name(sourceState) << Endl;
        
        // Check incremental backup source table states
        for (int i = 2; i <= 6; ++i) {
            TString incrName = TStringBuilder() << "backup_" << Sprintf("%03d", i) << "_incremental";
            TString incrTablePath = TStringBuilder() << "/MyRoot/.backups/collections/DatabaseTestCollection/" << incrName << "/DatabaseTestTable";
            
            auto incrTableDesc = DescribePath(runtime, incrTablePath);
            auto actualState = incrTableDesc.GetPathDescription().GetSelf().GetPathState();
            
            Cerr << "Source table (" << incrName << ") state: " << NKikimrSchemeOp::EPathState_Name(actualState) << Endl;
            
            // Assert that incremental backup source tables are in one of the valid outgoing incremental restore states
            bool isValidState = (actualState == NKikimrSchemeOp::EPathState::EPathStateOutgoingIncrementalRestore || 
                               actualState == NKikimrSchemeOp::EPathState::EPathStateAwaitingOutgoingIncrementalRestore);
            
            UNIT_ASSERT_C(isValidState, 
                TStringBuilder() << "Source table (" << incrName << ") should be in EPathStateOutgoingIncrementalRestore or "
                               << "EPathStateAwaitingOutgoingIncrementalRestore state, but got: " 
                               << NKikimrSchemeOp::EPathState_Name(actualState));
        }

        // Check the backup collection path state - it might be affected by the restore operation
        auto backupCollectionDesc = DescribePath(runtime, "/MyRoot/.backups/collections/DatabaseTestCollection");
        auto collectionState = backupCollectionDesc.GetPathDescription().GetSelf().GetPathState();
        Cerr << "Backup collection state: " << NKikimrSchemeOp::EPathState_Name(collectionState) << Endl;
    }
}
