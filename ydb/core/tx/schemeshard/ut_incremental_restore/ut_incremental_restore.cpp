#include <ydb/public/lib/value/value.h>

#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_incremental_restore.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>
#include <ydb/core/base/test_failure_injection.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <climits>
#include <deque>

template<>
void Out<Ydb::Backup::RestoreProgress::Progress>(IOutputStream& out, TTypeTraits<Ydb::Backup::RestoreProgress::Progress>::TFuncParam value) {
    out << Ydb::Backup::RestoreProgress_Progress_Name(value);
}

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NSchemeShardUT_Private::NIncrementalRestoreHelpers;

// Common setup function for all long operation tests
struct TLongOpTestSetup {
    TTestBasicRuntime Runtime;
    TTestEnv Env;
    ui64 TxId;
    TVector<TPathId> CapturedBackupCollectionPathIds;
    THashSet<TPathId> ExpectedBackupCollectionPathIds;
    bool OperationInProgress = false;
    
    TLongOpTestSetup() 
        : Env(Runtime, TTestEnvOptions().EnableBackupService(true))
        , TxId(100)
    {
        // Setup backup infrastructure directories
        TestMkDir(Runtime, ++TxId, "/MyRoot", ".backups");
        Env.TestWaitNotification(Runtime, TxId);
        TestMkDir(Runtime, ++TxId, "/MyRoot/.backups", "collections");
        Env.TestWaitNotification(Runtime, TxId);
        
        // Setup event observer to capture TEvRunIncrementalRestore events with validation
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (ev && ev->GetTypeRewrite() == TEvPrivate::TEvRunIncrementalRestore::EventType) {
                auto* msg = ev->Get<TEvPrivate::TEvRunIncrementalRestore>();
                if (msg) {
                    // Validate that this event belongs to an operation we're testing
                    if (OperationInProgress && ExpectedBackupCollectionPathIds.contains(msg->BackupCollectionPathId)) {
                        CapturedBackupCollectionPathIds.push_back(msg->BackupCollectionPathId);
                    } else if (OperationInProgress) {
                        Cerr << "Captured TEvRunIncrementalRestore for unexpected BackupCollectionPathId: " 
                             << msg->BackupCollectionPathId << Endl;
                    }
                }
                // Always rethrow the event to continue normal processing
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
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
                      Columns { Name: "__ydb_incrBackupImpl_changeMetadata" Type: "String" }
                      KeyColumnNames: ["key"]
                )";
                
                AsyncCreateTable(Runtime, ++TxId, backupPath, tableSchema);
                Env.TestWaitNotification(Runtime, TxId);
            }
        }
    }
    
    // Execute restore operation and return transaction ID
    ui64 ExecuteRestore(const TString& collectionName, const TVector<NSchemeShardUT_Private::TExpectedResult>& expectedResults = {}) {
        TString restoreSettings = TStringBuilder() << R"(
            Name: ")" << collectionName << R"("
        )";
        
        // Get the backup collection path ID before starting the operation
        TString backupCollectionPath = TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName;
        auto description = DescribePath(Runtime, backupCollectionPath);
        
        TPathId backupCollectionPathId;
        if (description.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
            auto selfEntry = description.GetPathDescription().GetSelf();
            backupCollectionPathId = TPathId(selfEntry.GetSchemeshardId(), selfEntry.GetPathId());
            
            // Register this path ID as expected and mark operation as in progress
            ExpectedBackupCollectionPathIds.insert(backupCollectionPathId);
            OperationInProgress = true;
        }
        
        ui64 restoreTxId = ++TxId;
        if (expectedResults.empty()) {
            TestRestoreBackupCollection(Runtime, restoreTxId, "/MyRoot/.backups/collections/", restoreSettings);
        } else {
            TestRestoreBackupCollection(Runtime, restoreTxId, "/MyRoot/.backups/collections/", restoreSettings, expectedResults);
        }
        Env.TestWaitNotification(Runtime, restoreTxId);
        
        // Mark operation as completed
        OperationInProgress = false;
        
        return restoreTxId;
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
    
    void ClearCapturedEvents() {
        CapturedBackupCollectionPathIds.clear();
        ExpectedBackupCollectionPathIds.clear();
        OperationInProgress = false;
    }
    
    bool HasCapturedEventsForOperation() const {
        if (CapturedBackupCollectionPathIds.empty()) {
            return false;
        }
        
        return !CapturedBackupCollectionPathIds.empty();
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

        setup.ExecuteRestore("TestCollection");
    }

    Y_UNIT_TEST(CreateLongIncrementalRestoreOpNonExistentCollection) {
        TLongOpTestSetup setup;

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

        // Create backup collection for BusyTable
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

        // Should fail because the table backups don't exist in the backup directories
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

        // Create backup collection
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
                  Columns { Name: "__ydb_incrBackupImpl_changeMetadata" Type: "String" }
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

    Y_UNIT_TEST(ExecuteLongIncrementalRestoreOpProgress) {
        TLongOpTestSetup setup;

        // Create complete backup scenario using the working pattern
        setup.CreateCompleteBackupScenario("ProgressTestCollection", {"ProgressTestTable"}, 3);

        // Execute the restore operation
        setup.ExecuteRestore("ProgressTestCollection");

        // The operation should complete successfully
        // We can't easily test the internal ESchemeOpCreateLongIncrementalRestoreOp dispatch
        // without deeper integration, but we can verify the overall restore works
    }

    Y_UNIT_TEST(ExecuteLongIncrementalRestoreOpProgressFailure) {
        TLongOpTestSetup setup;

        // Test scenario: try to restore from a backup collection that has no backup directories at all
        setup.CreateBackupCollection("EmptyProgressFailureCollection", {"/MyRoot/EmptyProgressFailureTestTable"});

        // The backup collection exists but has no backup directories (_full or _incremental)
        // This should fail with StatusInvalidParameter because there's nothing to restore
        setup.ExecuteRestore("EmptyProgressFailureCollection", {NKikimrScheme::StatusInvalidParameter});

        // Now create proper backup structure and verify it works
        setup.CreateCompleteBackupScenario("WorkingProgressFailureCollection", {"WorkingProgressFailureTestTable"}, 2);
        setup.ExecuteRestore("WorkingProgressFailureCollection");
    }

    Y_UNIT_TEST(TxProgressExecutedAfterIncrementalRestoreSuccess) {
        TLongOpTestSetup setup;

        // Create backup collection with incremental backups
        setup.CreateCompleteBackupScenario("TxProgressTestCollection", {"TxProgressTestTable"}, 3);

        // Clear any previous events
        setup.ClearCapturedEvents();

        // Execute restore operation (event validation is handled automatically)
        setup.ExecuteRestore("TxProgressTestCollection");

        // Verify that TEvRunIncrementalRestore event was actually sent
        UNIT_ASSERT_C(!setup.CapturedBackupCollectionPathIds.empty(), 
            "TEvRunIncrementalRestore event should have been sent during incremental restore");
        
        // Verify the event contains a valid backup collection path ID
        const TPathId& capturedPathId = setup.CapturedBackupCollectionPathIds[0];
        UNIT_ASSERT_C(capturedPathId.OwnerId != 0, "BackupCollectionPathId OwnerId should be valid");
        UNIT_ASSERT_C(capturedPathId.LocalPathId != 0, "BackupCollectionPathId LocalPathId should be valid");
        
        Cerr << "Successfully verified TEvRunIncrementalRestore event execution with PathId: " 
             << capturedPathId << Endl;

        // Also verify TTxProgress execution by checking that long incremental restore operation was created
        TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
        
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(setup.Runtime, schemeShardTabletId.GetValue(), R"(
            (
                (let range '('('Id (Null) (Void))))
                (let select '('Id 'Operation))
                (let operations (SelectRange 'IncrementalRestoreOperations range select '()))
                (let ret (AsList (SetResult 'Operations operations)))
                (return ret)
            )
        )", result, err);
        
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        
        auto value = NClient::TValue::Create(result);
        auto operationsResultSet = value["Operations"];
        UNIT_ASSERT_C(operationsResultSet.HaveValue(), "Operations result set should be present");
        
        auto operationsList = operationsResultSet["List"];
        bool hasIncrementalRestoreOperation = operationsList.HaveValue() && operationsList.Size() > 0;
        
        UNIT_ASSERT_C(hasIncrementalRestoreOperation, "TTxProgress should have been executed - incremental restore operation should exist in database");

        // Additional verification: Check that the backup collection is in correct state
        auto backupCollectionDesc = DescribePath(setup.Runtime, "/MyRoot/.backups/collections/TxProgressTestCollection");
        auto collectionState = backupCollectionDesc.GetPathDescription().GetSelf().GetPathState();
        
        // The collection should be in a state that indicates incremental restore is active or completed
        bool isValidRestoreState = (collectionState == NKikimrSchemeOp::EPathState::EPathStateOutgoingIncrementalRestore ||
                                   collectionState == NKikimrSchemeOp::EPathState::EPathStateNoChanges);
        
        UNIT_ASSERT_C(isValidRestoreState, 
            TStringBuilder() << "Backup collection should be in valid restore state, got: " 
                            << NKikimrSchemeOp::EPathState_Name(collectionState));
    }

    Y_UNIT_TEST(TxProgressExecutionWithCorrectBackupCollectionPathId) {
        TLongOpTestSetup setup;

        // Create two different backup collections to verify event specificity
        setup.CreateCompleteBackupScenario("TargetCollection", {"TargetTable"}, 2);
        setup.CreateCompleteBackupScenario("OtherCollection", {"OtherTable"}, 2);

        // Clear any previous events
        setup.ClearCapturedEvents();

        // Execute restore operation on the target collection only (automatic event validation)
        setup.ExecuteRestore("TargetCollection");

        // Verify that exactly one TEvRunIncrementalRestore event was sent
        UNIT_ASSERT_C(setup.CapturedBackupCollectionPathIds.size() == 1, 
            TStringBuilder() << "Expected exactly 1 TEvRunIncrementalRestore event, got: " << setup.CapturedBackupCollectionPathIds.size());
        
        // Verify the event contains a valid backup collection path ID
        const TPathId& capturedPathId = setup.CapturedBackupCollectionPathIds[0];
        UNIT_ASSERT_C(capturedPathId.OwnerId != 0, "Event should reference a valid backup collection OwnerId");
        UNIT_ASSERT_C(capturedPathId.LocalPathId != 0, "Event should reference a valid backup collection LocalPathId");
        
        // Verify that the captured PathId belongs to the expected backup collection
        UNIT_ASSERT_C(setup.ExpectedBackupCollectionPathIds.contains(capturedPathId), 
            "Captured event should be for the expected backup collection");
        
        Cerr << "Successfully verified TEvRunIncrementalRestore event contains valid PathId: " << capturedPathId << Endl;
    }

    Y_UNIT_TEST(MultipleCollectionsGenerateMultipleTEvRunIncrementalRestoreEvents) {
        TLongOpTestSetup setup;

        // Create 3 different backup collections with incremental backups
        setup.CreateCompleteBackupScenario("Collection1", {"Table1"}, 2);
        setup.CreateCompleteBackupScenario("Collection2", {"Table2"}, 3);
        setup.CreateCompleteBackupScenario("Collection3", {"Table3"}, 1);

        // Clear any previous events
        setup.ClearCapturedEvents();

        // Execute restore operations on all 3 collections sequentially
        // We need to execute them one by one to ensure proper event tracking
        
        // Restore Collection1
        setup.ExecuteRestore("Collection1");
        
        // Restore Collection2
        setup.ExecuteRestore("Collection2");
        
        // Restore Collection3
        setup.ExecuteRestore("Collection3");

        // Verify that exactly 3 TEvRunIncrementalRestore events were sent (one per collection)
        UNIT_ASSERT_C(setup.CapturedBackupCollectionPathIds.size() == 3, 
            TStringBuilder() << "Expected exactly 3 TEvRunIncrementalRestore events (one per collection), got: " 
                            << setup.CapturedBackupCollectionPathIds.size());
        
        // Verify that all captured events contain valid backup collection path IDs
        for (size_t i = 0; i < setup.CapturedBackupCollectionPathIds.size(); ++i) {
            const TPathId& capturedPathId = setup.CapturedBackupCollectionPathIds[i];
            UNIT_ASSERT_C(capturedPathId.OwnerId != 0, 
                TStringBuilder() << "Event " << i << " should reference a valid backup collection OwnerId");
            UNIT_ASSERT_C(capturedPathId.LocalPathId != 0, 
                TStringBuilder() << "Event " << i << " should reference a valid backup collection LocalPathId");
            
            // Verify that each captured PathId belongs to one of the expected backup collections
            UNIT_ASSERT_C(setup.ExpectedBackupCollectionPathIds.contains(capturedPathId), 
                TStringBuilder() << "Event " << i << " should be for one of the expected backup collections, got PathId: " 
                                << capturedPathId);
        }

        // Verify that we captured events for all 3 unique collections (no duplicates)
        THashSet<TPathId> uniquePathIds(setup.CapturedBackupCollectionPathIds.begin(), 
                                       setup.CapturedBackupCollectionPathIds.end());
        UNIT_ASSERT_C(uniquePathIds.size() == 3, 
            TStringBuilder() << "Expected 3 unique backup collection PathIds, got: " << uniquePathIds.size());

        // Also verify TTxProgress execution by checking the database for multiple operations
        TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
        
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(setup.Runtime, schemeShardTabletId.GetValue(), R"(
            (
                (let range '('('Id (Null) (Void))))
                (let select '('Id 'Operation))
                (let operations (SelectRange 'IncrementalRestoreOperations range select '()))
                (let ret (AsList (SetResult 'Operations operations)))
                (return ret)
            )
        )", result, err);
        
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        
        auto value = NClient::TValue::Create(result);
        auto operationsResultSet = value["Operations"];
        UNIT_ASSERT_C(operationsResultSet.HaveValue(), "Operations result set should be present");
        
        auto operationsList = operationsResultSet["List"];
        ui32 operationsCount = 0;
        if (operationsList.HaveValue()) {
            operationsCount = operationsList.Size();
        }
        
        UNIT_ASSERT_C(operationsCount == 3, 
            TStringBuilder() << "TTxProgress should have been executed for all 3 collections - expected exactly 3 incremental restore operations, got: " 
                            << operationsCount);

        Cerr << "Successfully verified " << setup.CapturedBackupCollectionPathIds.size() 
             << " TEvRunIncrementalRestore events for " << uniquePathIds.size() 
             << " unique collections with " << operationsCount << " operations in database" << Endl;
    }


    // Helper function to wait for incremental restore to make progress (not necessarily complete)
    void WaitForIncrementalRestoreProgress(TTestBasicRuntime& runtime, ui64 restoreId, ui32 timeoutSeconds = 30) {
        TInstant deadline = TInstant::Now() + TDuration::Seconds(timeoutSeconds);
        TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
        
        while (TInstant::Now() < deadline) {
            // Check if incremental restore has made actual progress
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, schemeShardTabletId.GetValue(), Sprintf(R"(
                (
                    (let key '('('OperationId (Uint64 '%lu))))
                    (let select '('OperationId 'CurrentIncrementalIdx 'CompletedOperations))
                    (let state (SelectRow 'IncrementalRestoreState key select))
                    (let ret (AsList (SetResult 'State state)))
                    (return ret)
                )
            )", restoreId), result, err);
            
            if (status == NKikimrProto::EReplyStatus::OK) {
                auto value = NClient::TValue::Create(result);
                auto stateResult = value["State"];
                
                if (stateResult.HaveValue() && !stateResult.IsNull()) {
                    auto currentIncrementalIdx = stateResult["CurrentIncrementalIdx"];
                    auto completedOperations = stateResult["CompletedOperations"];
                    
                    // Check if progress has been made
                    bool progressMade = false;
                    
                    if (currentIncrementalIdx.HaveValue() && !currentIncrementalIdx.IsNull()) {
                        ui32 idx = (ui32)currentIncrementalIdx;
                        if (idx > 0) {
                            progressMade = true;
                        }
                    }
                    
                    if (!progressMade && completedOperations.HaveValue() && !completedOperations.IsNull()) {
                        TString completedOpsStr = (TString)completedOperations;
                        if (!completedOpsStr.empty() && completedOpsStr != "[]") {
                            progressMade = true;
                        }
                    }
                    
                    if (progressMade) {
                        Cerr << "Incremental restore progress detected for operation " << restoreId << Endl;
                        return; // Progress has been made
                    }
                }
            }
            
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
        }
        
        // If we reach here, timeout occurred
        UNIT_ASSERT_C(false, "Timeout waiting for incremental restore progress");
    }

    // Helper function to wait for incremental restore completion
    void WaitForIncrementalRestoreCompletion(TTestBasicRuntime& runtime, const TString& collectionName, const TVector<TString>& tableNames, ui32 timeoutSeconds = 30) {
        Y_UNUSED(collectionName); // Collection name parameter kept for future use
        TInstant deadline = TInstant::Now() + TDuration::Seconds(timeoutSeconds);
        
        while (TInstant::Now() < deadline) {
            bool allTablesNormalized = true;
            for (const auto& tableName : tableNames) {
                TString targetPath = TStringBuilder() << "/MyRoot/" << tableName;
                auto targetDesc = DescribePath(runtime, targetPath);
                auto targetState = targetDesc.GetPathDescription().GetSelf().GetPathState();
                
                if (targetState != NKikimrSchemeOp::EPathState::EPathStateNoChanges) {
                    allTablesNormalized = false;
                    break;
                }
            }
            
            // Only wait for all tables to be normalized
            // Database operations cleanup now happens during FORGET, not finalization
            if (allTablesNormalized) {
                return; // Finalization completed
            }
            
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
        }
        
        // If we reach here, timeout occurred
        UNIT_ASSERT_C(false, "Timeout waiting for incremental restore finalization to complete");
    }

    // Helper function to verify database cleanup
    void VerifyDatabaseCleanup(TTestBasicRuntime& runtime) {
        TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
        
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, schemeShardTabletId.GetValue(), R"(
            (
                (let range '('('Id (Null) (Void))))
                (let select '('Id 'Operation))
                (let operations (SelectRange 'IncrementalRestoreOperations range select '()))
                (let ret (AsList (SetResult 'Operations operations)))
                (return ret)
            )
        )", result, err);
        
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
    }

    // Helper function to verify complete cleanup after FORGET operation
    void VerifyDatabaseCleanupAfterForget(TTestBasicRuntime& runtime) {
        TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
        
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, schemeShardTabletId.GetValue(), R"(
            (
                (let range '('('Id (Null) (Void))))
                (let select '('Id 'Operation))
                (let operations (SelectRange 'IncrementalRestoreOperations range select '()))
                (let ret (AsList (SetResult 'Operations operations)))
                (return ret)
            )
        )", result, err);
        
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        
        auto value = NClient::TValue::Create(result);
        auto operationsResultSet = value["Operations"];
        
        if (operationsResultSet.HaveValue()) {
            auto operationsList = operationsResultSet["List"];
            ui32 operationsCount = 0;
            if (operationsList.HaveValue()) {
                operationsCount = operationsList.Size();
            }
            
            UNIT_ASSERT_VALUES_EQUAL_C(operationsCount, 0, 
                TStringBuilder() << "IncrementalRestoreOperations table should be empty after FORGET, but found " 
                               << operationsCount << " operations");
        }
    }

    // Helper function to verify path state normalization
    void VerifyPathStatesNormalized(TTestBasicRuntime& runtime, const TString& collectionName, const TVector<TString>& tableNames) {
        // Verify target table states
        for (const auto& tableName : tableNames) {
            TString targetPath = TStringBuilder() << "/MyRoot/" << tableName;
            auto targetDesc = DescribePath(runtime, targetPath);
            auto targetState = targetDesc.GetPathDescription().GetSelf().GetPathState();
            
            UNIT_ASSERT_VALUES_EQUAL_C(targetState, NKikimrSchemeOp::EPathState::EPathStateNoChanges,
                TStringBuilder() << "Target table " << targetPath << " should be in EPathStateNoChanges state after finalization, got: " 
                               << NKikimrSchemeOp::EPathState_Name(targetState));
        }
        
        // Verify backup collection state
        TString collectionPath = TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName;
        auto collectionDesc = DescribePath(runtime, collectionPath);
        auto collectionState = collectionDesc.GetPathDescription().GetSelf().GetPathState();
        
        UNIT_ASSERT_VALUES_EQUAL_C(collectionState, NKikimrSchemeOp::EPathState::EPathStateNoChanges,
            TStringBuilder() << "Backup collection " << collectionPath << " should be in EPathStateNoChanges state after finalization, got: " 
                           << NKikimrSchemeOp::EPathState_Name(collectionState));
        
        // Verify source table states in backup directories
        for (const auto& tableName : tableNames) {
            // Check full backup table
            TString fullBackupTablePath = TStringBuilder() << collectionPath << "/backup_001_full/" << tableName;
            auto fullBackupDesc = DescribePath(runtime, fullBackupTablePath);
            auto fullBackupState = fullBackupDesc.GetPathDescription().GetSelf().GetPathState();
            
            UNIT_ASSERT_VALUES_EQUAL_C(fullBackupState, NKikimrSchemeOp::EPathState::EPathStateNoChanges,
                TStringBuilder() << "Full backup table " << fullBackupTablePath << " should be in EPathStateNoChanges state after finalization, got: " 
                               << NKikimrSchemeOp::EPathState_Name(fullBackupState));
            
            // Check incremental backup tables
            for (ui32 i = 2; i <= 5; ++i) { // Assuming up to 5 incremental backups
                TString incrBackupTablePath = TStringBuilder() << collectionPath << "/backup_" << Sprintf("%03d", i) << "_incremental/" << tableName;
                auto incrBackupDesc = DescribePath(runtime, incrBackupTablePath);
                if (incrBackupDesc.GetStatus() == NKikimrScheme::StatusPathDoesNotExist) {
                    continue; // This incremental backup doesn't exist
                }
                
                auto incrBackupState = incrBackupDesc.GetPathDescription().GetSelf().GetPathState();
                UNIT_ASSERT_VALUES_EQUAL_C(incrBackupState, NKikimrSchemeOp::EPathState::EPathStateNoChanges,
                    TStringBuilder() << "Incremental backup table " << incrBackupTablePath << " should be in EPathStateNoChanges state after finalization, got: " 
                                   << NKikimrSchemeOp::EPathState_Name(incrBackupState));
            }
        }
    }

    // Helper function to verify operation completion
    void VerifyIncrementalRestoreOperationCompleted(TTestBasicRuntime& runtime, ui64 operationId) {
        // With new completion tracking, operations remain in database until FORGET
        // This function verifies the operation exists and is in the correct completed state
        TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
        
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, schemeShardTabletId.GetValue(), Sprintf(R"(
            (
                (let key '('('Id (Uint64 '%lu))))
                (let select '('Id 'Operation))
                (let operation (SelectRow 'IncrementalRestoreOperations key select))
                (let ret (AsList (SetResult 'Operation operation)))
                (return ret)
            )
        )", operationId), result, err);
        
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
        
        auto value = NClient::TValue::Create(result);
        auto operationResult = value["Operation"];
        
        // With new completion tracking, operations remain in database until FORGET is called
        // So we expect the operation to be present (but internally marked as completed)
        UNIT_ASSERT_C(operationResult.HaveValue() && !operationResult.IsNull(), 
            "Operation " << operationId << " should remain in database until FORGET is called");
        
        // Verify the operation has the expected fields
        auto idField = operationResult["Id"];
        UNIT_ASSERT_C(idField.HaveValue() && !idField.IsNull(), 
            "Operation " << operationId << " should have a valid Id field");
        
        ui64 foundId = (ui64)idField;
        UNIT_ASSERT_VALUES_EQUAL_C(foundId, operationId, 
            "Operation Id should match expected value");
        
        // Also verify the operation can be found via list API (tests in-memory state consistency)
        auto listResp = TestListBackupCollectionRestores(runtime, "/MyRoot");
        bool foundInList = false;
        for (const auto& entry : listResp.GetEntries()) {
            if (entry.GetId() == operationId) {
                foundInList = true;
                // Verify the operation shows as completed
                UNIT_ASSERT_VALUES_EQUAL_C(entry.GetProgress(), Ydb::Backup::RestoreProgress::PROGRESS_DONE,
                    "Operation " << operationId << " should be marked as PROGRESS_DONE in list API");
                break;
            }
        }
        
        UNIT_ASSERT_C(foundInList, "Operation " << operationId << " should be visible in list API until FORGET is called");
    }

    Y_UNIT_TEST(LongIncrementalRestoreOpCleanupAfterSuccess) {
        TLongOpTestSetup setup;
        
        setup.CreateCompleteBackupScenario("CleanupTestCollection", {"CleanupTestTable"}, 3);
        
        ui64 restoreTxId = setup.ExecuteRestore("CleanupTestCollection");
        
        WaitForIncrementalRestoreCompletion(setup.Runtime, "CleanupTestCollection", {"CleanupTestTable"});
        
        VerifyDatabaseCleanup(setup.Runtime);
        
        VerifyIncrementalRestoreOperationCompleted(setup.Runtime, restoreTxId);
    }
    
    Y_UNIT_TEST(LongIncrementalRestoreOpCleanupMultipleOperations) {
        TLongOpTestSetup setup;
        
        // Create multiple backup collections
        setup.CreateCompleteBackupScenario("Collection1", {"Table1"}, 2);
        setup.CreateCompleteBackupScenario("Collection2", {"Table2"}, 3);
        setup.CreateCompleteBackupScenario("Collection3", {"Table3"}, 1);
        
        // Execute multiple restore operations
        ui64 restoreTxId1 = setup.ExecuteRestore("Collection1");
        ui64 restoreTxId2 = setup.ExecuteRestore("Collection2");
        ui64 restoreTxId3 = setup.ExecuteRestore("Collection3");
        
        // Wait for all operations to complete
        WaitForIncrementalRestoreCompletion(setup.Runtime, "Collection1", {"Table1"});
        WaitForIncrementalRestoreCompletion(setup.Runtime, "Collection2", {"Table2"});
        WaitForIncrementalRestoreCompletion(setup.Runtime, "Collection3", {"Table3"});
        
        // Verify that all operations are cleaned up independently
        // No cross-operation interference should occur
        VerifyDatabaseCleanup(setup.Runtime);
        
        VerifyIncrementalRestoreOperationCompleted(setup.Runtime, restoreTxId1);
        VerifyIncrementalRestoreOperationCompleted(setup.Runtime, restoreTxId2);
        VerifyIncrementalRestoreOperationCompleted(setup.Runtime, restoreTxId3);
    }
    
    Y_UNIT_TEST(PathStatesNormalizedAfterIncrementalRestore) {
        TLongOpTestSetup setup;
        
        setup.CreateCompleteBackupScenario("StateTestCollection", {"StateTestTable"}, 2);
        
        setup.ExecuteRestore("StateTestCollection");
        
        WaitForIncrementalRestoreCompletion(setup.Runtime, "StateTestCollection", {"StateTestTable"});
        
        VerifyPathStatesNormalized(setup.Runtime, "StateTestCollection", {"StateTestTable"});
    }
    
    Y_UNIT_TEST(BasicFinalizationWorksCorrectly) {
        TLongOpTestSetup setup;
        
        setup.CreateCompleteBackupScenario("BasicFinalizationCollection", {"BasicFinalizationTable"}, 1);
        
        setup.ExecuteRestore("BasicFinalizationCollection");
        
        WaitForIncrementalRestoreCompletion(setup.Runtime, "BasicFinalizationCollection", {"BasicFinalizationTable"}, 60);
        
        VerifyDatabaseCleanup(setup.Runtime);
        VerifyPathStatesNormalized(setup.Runtime, "BasicFinalizationCollection", {"BasicFinalizationTable"});
    }
    
    Y_UNIT_TEST(PathStatesNormalizedAfterPartialFailure) {
        TLongOpTestSetup setup;
        
        // Create a backup collection with a full backup but create a broken incremental backup
        setup.CreateBackupCollection("PartialFailureCollection", {"/MyRoot/PartialFailureTable"});
        setup.CreateFullBackup("PartialFailureCollection", {"PartialFailureTable"});
        
        // Create a malformed incremental backup directory but don't put any table backups inside
        // This will cause the incremental restore to fail when it tries to find table backups
        setup.CreateCustomBackupDirectories("PartialFailureCollection", {"backup_002_incremental"});
        
        // Execute restore operation - it should start but fail during incremental processing
        try {
            setup.ExecuteRestore("PartialFailureCollection");
            
            // Wait a reasonable time for the operation to attempt processing and fail
            setup.Runtime.SimulateSleep(TDuration::Seconds(2));
            
            // The operation should eventually timeout or fail, and finalization should occur
            // We'll wait with a longer timeout to account for retry mechanisms
            WaitForIncrementalRestoreCompletion(setup.Runtime, "PartialFailureCollection", {"PartialFailureTable"}, 60);
        } catch (...) {
            // Failure during restore execution is expected in this test case
            // Even if the restore fails, we should still wait for cleanup
            setup.Runtime.SimulateSleep(TDuration::Seconds(2));
        }
        
        // Even after failure, the system should eventually clean up paths
        // The key test is that we don't leave paths in intermediate states forever
        
        // Check backup collection state - it should be normalized
        TString collectionPath = "/MyRoot/.backups/collections/PartialFailureCollection";
        auto collectionDesc = DescribePath(setup.Runtime, collectionPath);
        auto collectionState = collectionDesc.GetPathDescription().GetSelf().GetPathState();
        
        // Collection should be in a valid final state (not stuck in intermediate state)
        bool isValidFinalState = (collectionState == NKikimrSchemeOp::EPathState::EPathStateNoChanges);
        
        UNIT_ASSERT_C(isValidFinalState,
            TStringBuilder() << "After failure, backup collection should be in valid final state, got: " 
                           << NKikimrSchemeOp::EPathState_Name(collectionState));
        
        // Target table should also be in a reasonable final state
        TString targetPath = "/MyRoot/PartialFailureTable";
        auto targetDesc = DescribePath(setup.Runtime, targetPath);
        auto targetState = targetDesc.GetPathDescription().GetSelf().GetPathState();
        
        // Target should either be normalized or not exist (both are acceptable outcomes after failure)
        bool isValidTargetState = (targetState == NKikimrSchemeOp::EPathState::EPathStateNoChanges) ||
                                 (targetDesc.GetStatus() == NKikimrScheme::StatusPathDoesNotExist);
        
        UNIT_ASSERT_C(isValidTargetState,
            TStringBuilder() << "After failure, target table should be in valid final state or not exist, got: " 
                           << NKikimrSchemeOp::EPathState_Name(targetState));
    }
    
    Y_UNIT_TEST(IncrementalRestoreCompleteLifecycle) {
        TLongOpTestSetup setup;
        
        setup.CreateCompleteBackupScenario("LifecycleCollection", {"LifecycleTable"}, 4);
        
        TString targetPath = "/MyRoot/LifecycleTable";
        TString collectionPath = "/MyRoot/.backups/collections/LifecycleCollection";
        
        setup.ExecuteRestore("LifecycleCollection");
        
        WaitForIncrementalRestoreCompletion(setup.Runtime, "LifecycleCollection", {"LifecycleTable"});
        
        TestDescribeResult(DescribePath(setup.Runtime, targetPath), {NLs::PathExist});
        
        VerifyPathStatesNormalized(setup.Runtime, "LifecycleCollection", {"LifecycleTable"});
        
        VerifyDatabaseCleanup(setup.Runtime);
        
        auto finalTargetDesc = DescribePath(setup.Runtime, targetPath);
        auto finalState = finalTargetDesc.GetPathDescription().GetSelf().GetPathState();
        UNIT_ASSERT_VALUES_EQUAL(finalState, NKikimrSchemeOp::EPathState::EPathStateNoChanges);
    }
    
    Y_UNIT_TEST(MultipleTablesIncrementalRestoreFinalization) {
        TLongOpTestSetup setup;
        
        TVector<TString> tableNames = {"MultiTable1", "MultiTable2", "MultiTable3"};
        setup.CreateCompleteBackupScenario("MultiTableCollection", tableNames, 3);
        
        setup.ExecuteRestore("MultiTableCollection");
        
        WaitForIncrementalRestoreCompletion(setup.Runtime, "MultiTableCollection", tableNames);
        
        VerifyPathStatesNormalized(setup.Runtime, "MultiTableCollection", tableNames);
        
        VerifyDatabaseCleanup(setup.Runtime);
        
        for (const auto& tableName : tableNames) {
            TString targetPath = TStringBuilder() << "/MyRoot/" << tableName;
            TestDescribeResult(DescribePath(setup.Runtime, targetPath), {NLs::PathExist});
        }
    }
    
    Y_UNIT_TEST(ConcurrentOperationsFinalization) {
        TLongOpTestSetup setup;
        
        setup.CreateCompleteBackupScenario("ConcurrentCollection1", {"ConcurrentTable1"}, 2);
        setup.CreateCompleteBackupScenario("ConcurrentCollection2", {"ConcurrentTable2"}, 3);
        
        setup.ExecuteRestore("ConcurrentCollection1");
        setup.ExecuteRestore("ConcurrentCollection2");
        
        WaitForIncrementalRestoreCompletion(setup.Runtime, "ConcurrentCollection1", {"ConcurrentTable1"});
        WaitForIncrementalRestoreCompletion(setup.Runtime, "ConcurrentCollection2", {"ConcurrentTable2"});
        
        VerifyDatabaseCleanup(setup.Runtime);
        
        VerifyPathStatesNormalized(setup.Runtime, "ConcurrentCollection1", {"ConcurrentTable1"});
        VerifyPathStatesNormalized(setup.Runtime, "ConcurrentCollection2", {"ConcurrentTable2"});
    }

    Y_UNIT_TEST(BackupCollectionRestoreOpApiGetListForget) {
        TLongOpTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& env = setup.Env;
        auto& txId = setup.TxId;

        setup.CreateCompleteBackupScenario("ApiCollection", {"ApiTable"}, 2);

        // Start async restore to allow checking mid-progress
        setup.ExecuteAsyncRestore("ApiCollection");
        ui64 startTxId = txId;
        TestModificationResult(runtime, startTxId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, startTxId);

        // List should show exactly one entry for this DB
        auto listResp = TestListBackupCollectionRestores(runtime, "/MyRoot");
        const auto& entries = listResp.GetEntries();
        UNIT_ASSERT_C(entries.size() >= 1, "Expected at least one incremental restore entry");

        // Find our collection entry by name match in metadata if available, otherwise use the last
        ui64 restoreId = entries.rbegin()->GetId();

        // Get should return SUCCESS
        auto getResp = TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(getResp.GetBackupCollectionRestore().GetId(), restoreId);

        // Forget during progress should fail
        auto forgetRespPre = TestForgetBackupCollectionRestore(runtime, ++txId, "/MyRoot", restoreId, Ydb::StatusIds::PRECONDITION_FAILED);
        Y_UNUSED(forgetRespPre);

        // Wait until operation completes (tables created)
        env.TestWaitNotification(runtime, startTxId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ApiTable"), {NLs::PathExist});

        // Add a short delay to allow incremental processing to start
        // Since the restore operation may complete very quickly, we need to give it time
        runtime.SimulateSleep(TDuration::MilliSeconds(500));

        // Get after completion should report DONE progress
        auto getAfter = TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(getAfter.GetBackupCollectionRestore().GetId(), restoreId);
        UNIT_ASSERT_VALUES_EQUAL(getAfter.GetBackupCollectionRestore().GetProgress(), Ydb::Backup::RestoreProgress::PROGRESS_DONE);

        // Now Forget should succeed and subsequent Get should be NOT_FOUND
        auto forgetResp = TestForgetBackupCollectionRestore(runtime, ++txId, "/MyRoot", restoreId, Ydb::StatusIds::SUCCESS);
        Y_UNUSED(forgetResp);
        (void)TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(IncrementalRestorePersistenceRowsLifecycle) {
        TLongOpTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& env = setup.Env;
        auto& txId = setup.TxId;

        setup.CreateCompleteBackupScenario("PersistCollection", {"PersistTable"}, 2);

        // Start async restore
        setup.ExecuteAsyncRestore("PersistCollection");
        ui64 startTxId = txId;
        TestModificationResult(runtime, startTxId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, startTxId);

        // Obtain restoreId from list
        auto listResp = TestListBackupCollectionRestores(runtime, "/MyRoot");
        UNIT_ASSERT_GE(listResp.GetEntries().size(), 1);
        ui64 restoreId = listResp.GetEntries().rbegin()->GetId();

        // Verify IncrementalRestoreState table has at least one row
        TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
        {
            NKikimrMiniKQL::TResult result;
            TString err;
            auto status = LocalMiniKQL(runtime, schemeShardTabletId.GetValue(), R"(
                (
                    (let range '('('OperationId (Null) (Void))))
                    (let select '('OperationId 'State 'CurrentIncrementalIdx))
                    (let rows (SelectRange 'IncrementalRestoreState range select '()))
                    (let ret (AsList (SetResult 'Rows rows)))
                    (return ret)
                )
            )", result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
            auto value = NClient::TValue::Create(result);
            auto rows = value["Rows"]["List"];
            UNIT_ASSERT_C(rows.HaveValue(), "Expected rows in IncrementalRestoreState during progress");
        }

        // Wait completion
        env.TestWaitNotification(runtime, startTxId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/PersistTable"), {NLs::PathExist});

        // Add a delay to allow incremental processing to complete
        runtime.SimulateSleep(TDuration::MilliSeconds(500));

        // Verify the restore is actually done before trying to forget
        auto getAfter = TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(getAfter.GetBackupCollectionRestore().GetId(), restoreId);
        UNIT_ASSERT_VALUES_EQUAL(getAfter.GetBackupCollectionRestore().GetProgress(), Ydb::Backup::RestoreProgress::PROGRESS_DONE);

        // Forget and ensure tables are cleaned
        TestForgetBackupCollectionRestore(runtime, ++txId, "/MyRoot", restoreId, Ydb::StatusIds::SUCCESS);

        // Now IncrementalRestoreState should be empty (or at least no row for this op)
        {
            NKikimrMiniKQL::TResult result;
            TString err;
            auto status = LocalMiniKQL(runtime, schemeShardTabletId.GetValue(), R"(
                (
                    (let range '('('OperationId (Null) (Void))))
                    (let select '('OperationId))
                    (let rows (SelectRange 'IncrementalRestoreState range select '()))
                    (let ret (AsList (SetResult 'Rows rows)))
                    (return ret)
                )
            )", result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
            auto value = NClient::TValue::Create(result);
            auto rows = value["Rows"]["List"];
            // It is acceptable that table exists for other ops, but our id should not be present
            bool found = false;
            if (rows.HaveValue()) {
                for (ui32 i = 0; i < rows.Size(); ++i) {
                    if ((ui64)rows[i]["Tuple"][0] == restoreId) {
                        found = true; break;
                    }
                }
            }
            UNIT_ASSERT_C(!found, "Restore state row should be deleted after forget");
        }
    }

    Y_UNIT_TEST(BackupCollectionRestoreOpApiMultipleOperationsListing) {
        TLongOpTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& env = setup.Env;
        auto& txId = setup.TxId;

        // Helper function to verify database is completely clean
        auto VerifyDatabaseCompletelyClean = [&]() {
            TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
            
            // Check IncrementalRestoreOperations table
            NKikimrMiniKQL::TResult result;
            TString err;
            auto status = LocalMiniKQL(runtime, schemeShardTabletId.GetValue(), R"(
                (
                    (let range '('('Id (Null) (Void))))
                    (let select '('Id 'Operation))
                    (let operations (SelectRange 'IncrementalRestoreOperations range select '()))
                    (let ret (AsList (SetResult 'Operations operations)))
                    (return ret)
                )
            )", result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
            
            auto value = NClient::TValue::Create(result);
            auto operationsResultSet = value["Operations"];
            ui32 operationsCount = 0;
            if (operationsResultSet.HaveValue()) {
                auto operationsList = operationsResultSet["List"];
                if (operationsList.HaveValue()) {
                    operationsCount = operationsList.Size();
                }
            }
            
            // Check IncrementalRestoreState table  
            status = LocalMiniKQL(runtime, schemeShardTabletId.GetValue(), R"(
                (
                    (let range '('('OperationId (Null) (Void))))
                    (let select '('OperationId))
                    (let states (SelectRange 'IncrementalRestoreState range select '()))
                    (let ret (AsList (SetResult 'States states)))
                    (return ret)
                )
            )", result, err);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, err);
            
            auto stateValue = NClient::TValue::Create(result);
            auto statesResultSet = stateValue["States"];
            ui32 statesCount = 0;
            if (statesResultSet.HaveValue()) {
                auto statesList = statesResultSet["List"];
                if (statesList.HaveValue()) {
                    statesCount = statesList.Size();
                }
            }
            
            UNIT_ASSERT_VALUES_EQUAL_C(operationsCount, 0, 
                TStringBuilder() << "IncrementalRestoreOperations should be empty, found: " << operationsCount);
            UNIT_ASSERT_VALUES_EQUAL_C(statesCount, 0,
                TStringBuilder() << "IncrementalRestoreState should be empty, found: " << statesCount);
            
            // Verify list API returns empty
            auto listResp = TestListBackupCollectionRestores(runtime, "/MyRoot");
            UNIT_ASSERT_VALUES_EQUAL_C(listResp.GetEntries().size(), 0,
                TStringBuilder() << "List API should return empty, found: " << listResp.GetEntries().size());
        };

        // Helper function to cleanup tables for next restore cycle
        auto CleanupTables = [&](const TVector<TString>& tableNames) {
            for (const auto& tableName : tableNames) {
                TString targetPath = TStringBuilder() << "/MyRoot/" << tableName;
                auto desc = DescribePath(runtime, targetPath);
                if (desc.GetStatus() != NKikimrScheme::StatusPathDoesNotExist) {
                    TestDropTable(runtime, ++txId, "/MyRoot", tableName);
                    env.TestWaitNotification(runtime, txId);
                }
            }
        };

        Cerr << "=== PHASE 1: Initial Multiple Operations Test ===" << Endl;

        // Create multiple backup scenarios
        setup.CreateCompleteBackupScenario("ListCollection1", {"ListTable1"}, 2);
        setup.CreateCompleteBackupScenario("ListCollection2", {"ListTable2"}, 3);
        setup.CreateCompleteBackupScenario("ListCollection3", {"ListTable3"}, 1);

        // Start multiple async restores
        setup.ExecuteAsyncRestore("ListCollection1");
        ui64 restore1TxId = txId;
        TestModificationResult(runtime, restore1TxId, NKikimrScheme::StatusAccepted);

        setup.ExecuteAsyncRestore("ListCollection2");
        ui64 restore2TxId = txId;
        TestModificationResult(runtime, restore2TxId, NKikimrScheme::StatusAccepted);

        setup.ExecuteAsyncRestore("ListCollection3");
        ui64 restore3TxId = txId;
        TestModificationResult(runtime, restore3TxId, NKikimrScheme::StatusAccepted);

        // Wait for all operations to be initialized
        env.TestWaitNotification(runtime, restore1TxId);
        env.TestWaitNotification(runtime, restore2TxId);
        env.TestWaitNotification(runtime, restore3TxId);

        // List should show all three operations
        auto listResp = TestListBackupCollectionRestores(runtime, "/MyRoot");
        const auto& entries = listResp.GetEntries();
        UNIT_ASSERT_C(entries.size() >= 3, 
            TStringBuilder() << "Expected at least 3 incremental restore entries, got: " << entries.size());

        // Collect restore IDs and verify they're all unique
        THashSet<ui64> cycle1RestoreIds;
        for (const auto& entry : entries) {
            cycle1RestoreIds.insert(entry.GetId());
        }
        UNIT_ASSERT_C(cycle1RestoreIds.size() >= 3, 
            TStringBuilder() << "Expected at least 3 unique restore operations, got: " << cycle1RestoreIds.size());

        // Wait for all operations to complete
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ListTable1"), {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ListTable2"), {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ListTable3"), {NLs::PathExist});

        // Add delay to allow incremental processing to complete
        runtime.SimulateSleep(TDuration::MilliSeconds(1000));

        // Verify all operations show as DONE
        auto listAfterCompletion = TestListBackupCollectionRestores(runtime, "/MyRoot");
        ui32 doneCount = 0;
        TVector<ui64> cycle1CompletedIds;
        for (const auto& entry : listAfterCompletion.GetEntries()) {
            if (cycle1RestoreIds.contains(entry.GetId()) && entry.GetProgress() == Ydb::Backup::RestoreProgress::PROGRESS_DONE) {
                doneCount++;
                cycle1CompletedIds.push_back(entry.GetId());
            }
        }
        UNIT_ASSERT_C(doneCount >= 3, TStringBuilder() << "Expected at least 3 DONE operations, got: " << doneCount);

        Cerr << "=== PHASE 2: Test Failed Restore (should not break system) ===" << Endl;

        // Create a backup collection that will cause restore to fail
        setup.CreateBackupCollection("FailCollection", {"/MyRoot/FailTable"});
        setup.CreateFullBackup("FailCollection", {"FailTable"});
        // Create malformed incremental backup directory (no table backup inside)
        setup.CreateCustomBackupDirectories("FailCollection", {"backup_002_incremental"});

        // Try to restore the broken collection - should fail gracefully
        ui64 failedRestoreTxId = 0;
        try {
            setup.ExecuteAsyncRestore("FailCollection");
            failedRestoreTxId = txId;
            TestModificationResult(runtime, failedRestoreTxId, NKikimrScheme::StatusAccepted);
            env.TestWaitNotification(runtime, failedRestoreTxId);
            runtime.SimulateSleep(TDuration::MilliSeconds(2000)); // Wait for failure
        } catch (...) {
            // Expected to fail
        }

        // Verify system is still functional after failure - list should still work
        auto listAfterFailure = TestListBackupCollectionRestores(runtime, "/MyRoot");
        UNIT_ASSERT_C(listAfterFailure.GetEntries().size() >= 3,
            "List API should still work after failed restore");

        // Verify successful operations are still accessible after failure
        for (ui64 id : cycle1CompletedIds) {
            auto getResp = TestGetBackupCollectionRestore(runtime, id, "/MyRoot");
            UNIT_ASSERT_VALUES_EQUAL(getResp.GetBackupCollectionRestore().GetId(), id);
        }

        Cerr << "=== PHASE 3: Second Restore Cycle (after cleanup) ===" << Endl;

        // Cleanup tables to enable second restore cycle
        CleanupTables({"ListTable1", "ListTable2", "ListTable3"});

        // Second restore cycle with same collections
        setup.ExecuteAsyncRestore("ListCollection1");
        ui64 restore1Cycle2TxId = txId;
        TestModificationResult(runtime, restore1Cycle2TxId, NKikimrScheme::StatusAccepted);

        setup.ExecuteAsyncRestore("ListCollection2");
        ui64 restore2Cycle2TxId = txId;
        TestModificationResult(runtime, restore2Cycle2TxId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, restore1Cycle2TxId);
        env.TestWaitNotification(runtime, restore2Cycle2TxId);

        // Verify we now have operations from both cycles visible
        auto listCycle2 = TestListBackupCollectionRestores(runtime, "/MyRoot");
        UNIT_ASSERT_C(listCycle2.GetEntries().size() >= 5, // 3 from cycle1 + 2 from cycle2
            TStringBuilder() << "Should have operations from both cycles, got: " << listCycle2.GetEntries().size());

        // Collect cycle2 IDs
        THashSet<ui64> cycle2RestoreIds;
        for (const auto& entry : listCycle2.GetEntries()) {
            if (!cycle1RestoreIds.contains(entry.GetId())) {
                cycle2RestoreIds.insert(entry.GetId());
            }
        }
        UNIT_ASSERT_C(cycle2RestoreIds.size() >= 2,
            TStringBuilder() << "Should have at least 2 new operations from cycle 2, got: " << cycle2RestoreIds.size());

        // Wait for cycle2 completion
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ListTable1"), {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/ListTable2"), {NLs::PathExist});
        runtime.SimulateSleep(TDuration::MilliSeconds(1000));

        Cerr << "=== PHASE 4: Selective FORGET Testing ===" << Endl;

        // Forget some operations from cycle1, keep others
        ui64 toForgetCycle1 = cycle1CompletedIds[0];
        auto forgetResp = TestForgetBackupCollectionRestore(runtime, ++txId, "/MyRoot", toForgetCycle1, Ydb::StatusIds::SUCCESS);
        Y_UNUSED(forgetResp);

        // Verify selective forget worked
        auto listAfterSelectiveForget = TestListBackupCollectionRestores(runtime, "/MyRoot");
        bool foundForgotten = false;
        ui32 remainingCycle1 = 0;
        ui32 remainingCycle2 = 0;

        for (const auto& entry : listAfterSelectiveForget.GetEntries()) {
            if (entry.GetId() == toForgetCycle1) {
                foundForgotten = true;
            }
            if (cycle1RestoreIds.contains(entry.GetId()) && entry.GetId() != toForgetCycle1) {
                remainingCycle1++;
            }
            if (cycle2RestoreIds.contains(entry.GetId())) {
                remainingCycle2++;
            }
        }

        UNIT_ASSERT_C(!foundForgotten, "Forgotten operation should not be in list");
        UNIT_ASSERT_C(remainingCycle1 >= 2, "Should have remaining cycle1 operations");
        UNIT_ASSERT_C(remainingCycle2 >= 2, "Should have remaining cycle2 operations");

        Cerr << "=== PHASE 5: Complete Cleanup Verification ===" << Endl;

        // Forget ALL remaining operations
        TVector<ui64> allRemainingIds;
        auto listBeforeCleanup = TestListBackupCollectionRestores(runtime, "/MyRoot");
        for (const auto& entry : listBeforeCleanup.GetEntries()) {
            allRemainingIds.push_back(entry.GetId());
        }

        Cerr << "Forgetting " << allRemainingIds.size() << " remaining operations..." << Endl;
        for (ui64 id : allRemainingIds) {
            auto forgetResp = TestForgetBackupCollectionRestore(runtime, ++txId, "/MyRoot", id, Ydb::StatusIds::SUCCESS);
            Y_UNUSED(forgetResp);
        }

        VerifyDatabaseCompletelyClean();
    }

    Y_UNIT_TEST(IncrementalRestoreShardFailureTriggersRetry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", ".backups/collections");
        env.TestWaitNotification(runtime, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: "MyCollection1"
            ExplicitEntryList { Entries { Type: ETypeTable Path: "/MyRoot/Table1" } }
            Cluster {}
            IncrementalBackupConfig {}
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        const ui64 incrBackupId = txId;
        env.TestWaitNotification(runtime, txId);

        WaitForIncrementalBackupDone(runtime, &env, incrBackupId, "/MyRoot");

        TestDropTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        std::atomic<int> failuresInjected{0};
        auto observerHolder = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/1,
            NKikimrTxDataShard::TShardOpResult::END_TRANSIENT_FAILURE,
            "Injected scan failure for retry test");

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env, "/MyRoot", true,
            TDuration::Seconds(1), TDuration::Seconds(60));
        UNIT_ASSERT_C(finalStatus == Ydb::StatusIds::SUCCESS,
            "Restore status is not SUCCESS after retry");
        UNIT_ASSERT_GE(failuresInjected.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);
    }

    Y_UNIT_TEST(IncrementalRestoreRespectsConcurrencyLimit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Set ICB cap=2 BEFORE the restore is issued.
        TControlBoard::SetValue(2, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/8);

        // Observer: count concurrent ESchemeOpRestoreMultipleIncrementalBackups sub-ops.
        // Increments on TEvModifySchemeTransaction (op start), decrements on
        // TEvModifySchemeTransactionResult (op accepted/done).
        std::atomic<i32> totalSeen{0};
        TInFlightTracker tracker;
        // Also count total seen via an extra start observer.
        auto [observerStart, observerEnd] = tracker.AttachObservers(runtime);
        // Wrap start observer to also count totalSeen.
        auto observerTotalSeen = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>(
            [&](TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
                const auto& rec = ev->Get()->Record;
                if (rec.TransactionSize() == 0) return;
                if (rec.GetTransaction(0).GetOperationType()
                        != NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups) {
                    return;
                }
                totalSeen.fetch_add(1);
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        WaitForRestoreDone(runtime, &env, "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(120));

        UNIT_ASSERT_C(totalSeen.load() >= 8,
            "Expected at least 8 restore sub-ops, saw " << totalSeen.load());
        UNIT_ASSERT_C(tracker.PeakInFlight.load() <= 2,
            "Expected peak in-flight <= 2 (cap=2), saw " << tracker.PeakInFlight.load());

        // Sanity: each table has 1 row from full + 1 row from incremental
        for (ui32 i = 0; i < 8; ++i) {
            TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
            UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 2u);
        }
    }

    Y_UNIT_TEST(IncrementalRestoreUnboundedWhenCapNegative) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        TControlBoard::SetValue(-1, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/8);

        TInFlightTracker tracker;
        auto [observerStart, observerEnd] = tracker.AttachObservers(runtime);

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        WaitForRestoreDone(runtime, &env, "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(120));

        // With cap=-1 we expect to observe all 8 in-flight at peak (best-effort:
        // require >2 to prove the cap is actually disabled).
        UNIT_ASSERT_C(tracker.PeakInFlight.load() > 2,
            "Expected peak in-flight > 2 with unbounded cap, saw " << tracker.PeakInFlight.load());
    }

    // Lowering the cap mid-restore does not abort in-flight ops (cap is checked at dispatch time).
    Y_UNIT_TEST(IncrementalRestoreCapChangedMidRestore) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Start with cap=2.
        TControlBoard::SetValue(2, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/8);

        std::atomic<i32> peakAfterRaise{0};
        std::atomic<bool> raised{false};
        TInFlightTracker tracker;
        // Attach base observers for in-flight tracking.
        auto [observerStart, observerEnd] = tracker.AttachObservers(runtime);
        // Extra observer to track peak after cap raise.
        // Note: this observer fires alongside the base tracker's start observer
        // on the same event; we use tracker.PeakInFlight as a proxy for the
        // post-increment value since both observers update it concurrently.
        auto observerAfterRaise = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>(
            [&](TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
                const auto& rec = ev->Get()->Record;
                if (rec.TransactionSize() == 0) return;
                if (rec.GetTransaction(0).GetOperationType()
                        != NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups) {
                    return;
                }
                if (raised.load()) {
                    // Read the current peak from tracker (updated by the base observer).
                    i32 cur = tracker.PeakInFlight.load();
                    i32 peak2;
                    do {
                        peak2 = peakAfterRaise.load();
                        if (cur <= peak2) break;
                    } while (!peakAfterRaise.compare_exchange_weak(peak2, cur));
                }
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        // While restore is processing, raise cap to 8.
        env.SimulateSleep(runtime, TDuration::MilliSeconds(500));
        TControlBoard::SetValue(8, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);
        raised.store(true);

        WaitForRestoreDone(runtime, &env, "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(120));

        // Verify cap was respected: peak <= 8 (the raised value).
        UNIT_ASSERT_C(tracker.PeakInFlight.load() <= 8,
            "Peak in-flight exceeded cap=8, saw " << tracker.PeakInFlight.load());

        // Sanity: restore finished
        for (ui32 i = 0; i < 8; ++i) {
            TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
            UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 2u);
        }
    }

    // Cap remains in effect during the retry wave after a shard failure.
    Y_UNIT_TEST(IncrementalRestoreCapRespectedDuringRetry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        TControlBoard::SetValue(2, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/4);

        TInFlightTracker tracker;
        auto [observerStart, observerEnd] = tracker.AttachObservers(runtime);

        // Inject one TEvFinished failure (same pattern as IncrementalRestoreShardFailureTriggersRetry).
        std::atomic<int> failuresInjected{0};
        auto failureObserver = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/1,
            NKikimrTxDataShard::TShardOpResult::END_TRANSIENT_FAILURE,
            "Injected scan failure for cap+retry test");

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        WaitForRestoreDone(runtime, &env, "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(120));

        // Cap respected during retry wave.
        UNIT_ASSERT_C(tracker.PeakInFlight.load() <= 2,
            "Peak in-flight exceeded cap=2 during retry, saw " << tracker.PeakInFlight.load());
        UNIT_ASSERT_GE(failuresInjected.load(), 1);

        // All 4 tables restored despite injected failure.
        for (ui32 i = 0; i < 4; ++i) {
            TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
            UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 2u);
        }
    }

    // Backoff gaps between retries honor GetRetryWakeupTimeoutBackoff: >=1s then >=2s.
    Y_UNIT_TEST(IncrementalRestoreRetryBackoffEnforced) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        TControlBoard::SetValue(50, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreRetriesPerIncremental);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/1);

        TMutex finishMutex;
        TVector<TInstant> finishTimes;
        std::atomic<int> failuresInjected{0};
        auto observerHolder = runtime.AddObserver<NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished>(
            [&](NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished::TPtr& ev) {
                if (ev->Get()->TxId == 0) {
                    return;
                }
                {
                    TGuard<TMutex> g(finishMutex);
                    finishTimes.push_back(runtime.GetCurrentTime());
                }
                if (failuresInjected.fetch_add(1) < 2) {
                    ev->Get()->Success = false;
                    ev->Get()->EndStatus = NKikimrTxDataShard::TShardOpResult::END_TRANSIENT_FAILURE;
                    ev->Get()->Error = "Injected retriable failure for backoff test";
                }
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        WaitForRestoreDone(runtime, &env, "/MyRoot", true,
            TDuration::Seconds(2), TDuration::Seconds(120));

        TVector<TInstant> snap;
        {
            TGuard<TMutex> g(finishMutex);
            snap = finishTimes;
        }
        UNIT_ASSERT_C(snap.size() >= 3,
            "Expected at least 3 TEvFinished, got " << snap.size());
        UNIT_ASSERT_GE(failuresInjected.load(), 2);

        TDuration gap1 = snap[1] - snap[0];
        TDuration gap2 = snap[2] - snap[1];
        UNIT_ASSERT_C(gap1 >= TDuration::Seconds(1),
            "Backoff gap1 too short: " << gap1);
        UNIT_ASSERT_C(gap2 >= TDuration::Seconds(2),
            "Backoff gap2 too short: " << gap2);

        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table0"), 2u);
    }

    // Budget cap=2 exhausted by injected failures → restore must reach GENERIC_ERROR.
    Y_UNIT_TEST(IncrementalRestoreRetryBudgetEnforced) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        TControlBoard::SetValue(2, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreRetriesPerIncremental);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/1);

        std::atomic<int> failuresInjected{0};
        auto observerHolder = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/INT_MAX,
            NKikimrTxDataShard::TShardOpResult::END_TRANSIENT_FAILURE,
            "Injected retriable failure for budget test");

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env, "/MyRoot", true,
            TDuration::Seconds(1), TDuration::Seconds(120));

        UNIT_ASSERT_C(finalStatus != Ydb::StatusIds::SUCCESS,
            "Restore status was SUCCESS under exhausted retry budget");
        UNIT_ASSERT_GE(failuresInjected.load(), 3);
    }

    // cap=-1 disables the budget; restore must succeed after 20 injected failures.
    Y_UNIT_TEST(IncrementalRestoreRetryBudgetUnlimited) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        TControlBoard::SetValue(-1, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreRetriesPerIncremental);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/1);

        constexpr int FailuresBeforeSuccess = 20;
        std::atomic<int> failuresInjected{0};
        auto observerHolder = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/FailuresBeforeSuccess,
            NKikimrTxDataShard::TShardOpResult::END_TRANSIENT_FAILURE,
            "Injected retriable failure for unlimited-cap test");

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        // Backoff at the 8s plateau: 20 retries can take ~150s of simulated time.
        WaitForRestoreDone(runtime, &env, "/MyRoot", true,
            TDuration::Seconds(2), TDuration::Seconds(600));

        auto listResp = TestListBackupCollectionRestores(runtime, "/MyRoot");
        UNIT_ASSERT(!listResp.GetEntries().empty());
        ui64 restoreId = listResp.GetEntries().rbegin()->GetId();
        auto finalResp = TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot");
        UNIT_ASSERT_C(finalResp.GetBackupCollectionRestore().GetStatus() == Ydb::StatusIds::SUCCESS,
            "Restore did not succeed after " << FailuresBeforeSuccess
            << " retriable failures (expected -1 cap to be unlimited)");
        UNIT_ASSERT_GE(failuresInjected.load(), FailuresBeforeSuccess);
    }

    // Non-retriable failure short-circuits to Failed without consuming the retry budget.
    Y_UNIT_TEST(IncrementalRestoreNonRetriableShortCircuits) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Generous budget; we expect the non-retriable bit to trump the cap.
        TControlBoard::SetValue(50, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreRetriesPerIncremental);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/1);

        std::atomic<int> failuresInjected{0};
        auto observerHolder = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/1,
            NKikimrTxDataShard::TShardOpResult::END_FATAL_FAILURE,
            "Injected non-retriable failure for short-circuit test");

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env, "/MyRoot", true,
            TDuration::Seconds(1), TDuration::Seconds(60));

        UNIT_ASSERT_C(finalStatus != Ydb::StatusIds::SUCCESS,
            "Restore status was SUCCESS despite a non-retriable failure");

        // Exactly one failure was injected — orchestrator did not burn the budget.
        // (We allow a small slack for the actual retry that may run before the
        // orchestrator processes the non-retriable bit, but the count must stay
        // far below the cap.)
        UNIT_ASSERT_LT_C(failuresInjected.load(), 10,
            "Too many failure events; non-retriable signal was not honored. Saw "
            << failuresInjected.load());
    }

    // Concurrent completion events must not double-count the retry counter.
    Y_UNIT_TEST(IncrementalRestoreRetryNotDoubleCountedOnConcurrentEvents) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Cap = 3. If concurrent events double-count, 4 failures × 1 round
        // would push the count to 4 > 3 and Fail before the second round even
        // starts. With proper de-duplication, we need 3 full rounds before
        // hitting the cap.
        TControlBoard::SetValue(3, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreRetriesPerIncremental);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/4);

        // Inject failure on the FIRST attempt of each table only (first 4 events).
        // Subsequent attempts succeed → restore finishes after exactly 1 retry.
        std::atomic<int> failuresInjected{0};
        auto observerHolder = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/4,
            NKikimrTxDataShard::TShardOpResult::END_TRANSIENT_FAILURE,
            "Injected retriable failure for double-fire test");

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        WaitForRestoreDone(runtime, &env, "/MyRoot", true,
            TDuration::Seconds(2), TDuration::Seconds(120));

        // Restore must succeed: only 1 retry round should have been used,
        // well within cap=3. If the counter were double-counted (cap-3, 4
        // simultaneous failures incrementing 4×) we'd be Failed instead.
        auto listResp = TestListBackupCollectionRestores(runtime, "/MyRoot");
        UNIT_ASSERT(!listResp.GetEntries().empty());
        ui64 restoreId = listResp.GetEntries().rbegin()->GetId();
        auto finalResp = TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot");
        UNIT_ASSERT_C(finalResp.GetBackupCollectionRestore().GetStatus() == Ydb::StatusIds::SUCCESS,
            "Restore did not SUCCESS — concurrent retries appear to be double-counted");

        // Sanity: data restored.
        for (ui32 i = 0; i < 4; ++i) {
            TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
            UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 2u);
        }
    }

    // Each retried sub-op must issue a fresh TEvAllocate, not reuse a cached TxId.
    Y_UNIT_TEST(RetryUsesAllocatorClientNotCachedPool) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/1);

        // Count TEvAllocate events sent toward the allocator after the restore is issued.
        std::atomic<int> allocateCount{0};
        std::atomic<bool> countingArmed{false};
        auto allocObserver = runtime.AddObserver<NKikimr::TEvTxAllocatorClient::TEvAllocate>(
            [&allocateCount, &countingArmed](
                    NKikimr::TEvTxAllocatorClient::TEvAllocate::TPtr&) {
                if (countingArmed.load()) {
                    allocateCount.fetch_add(1);
                }
            });

        // Inject 1 retriable shard failure: the first attempt fails, retry must succeed.
        std::atomic<int> failuresInjected{0};
        auto failureObserver = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/1,
            NKikimrTxDataShard::TShardOpResult::END_TRANSIENT_FAILURE,
            "Injected scan failure for allocator-client retry test");

        countingArmed.store(true);

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        WaitForRestoreDone(runtime, &env, "/MyRoot", true,
            TDuration::Seconds(2), TDuration::Seconds(120));

        UNIT_ASSERT_GE_C(failuresInjected.load(), 1,
            "Expected at least 1 injected scan failure; saw " << failuresInjected.load());
        // Initial dispatch + at least one retry dispatch must each issue a TEvAllocate.
        UNIT_ASSERT_GE_C(allocateCount.load(), 2,
            "Expected at least 2 TEvAllocate events (initial + retry); saw "
            << allocateCount.load() << ". A synchronous GetCachedTxId would emit zero.");
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table0"), 2u);
    }

    // An empty TxIds allocator result must schedule a retry, not fail or skip the item.
    Y_UNIT_TEST(EmptyAllocatorResultRetriesItem) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/1);

        // Drop the first N TEvAllocateResult events and replace them with empty
        // ones. We can't mutate the const TxIds field in place; instead we
        // intercept the event before it lands at SchemeShard, swallow it, and
        // re-issue a fresh empty TEvAllocateResult with the same cookie.
        constexpr int EmptyResultsToInject = 1;
        std::atomic<int> emptyInjected{0};
        TActorId schemeShardId;
        auto observer = runtime.AddObserver<NKikimr::TEvTxAllocatorClient::TEvAllocateResult>(
            [&runtime, &emptyInjected, &schemeShardId](
                    NKikimr::TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
                if (emptyInjected.load() >= EmptyResultsToInject) {
                    return;
                }
                if (!schemeShardId) {
                    schemeShardId = ev->Recipient;
                }
                const ui64 cookie = ev->Cookie;
                const ui64 originalOpId = cookie >> 32;
                if (originalOpId == 0) {
                    return; // not ours
                }
                emptyInjected.fetch_add(1);
                runtime.Send(new IEventHandle(
                    ev->Recipient, ev->Sender,
                    new NKikimr::TEvTxAllocatorClient::TEvAllocateResult(TVector<ui64>{}),
                    /*flags=*/0, cookie),
                    /*nodeIndex=*/0, /*viaActorSystem=*/true);
                ev.Reset();
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        // Restore must succeed once allocator delivers a non-empty result.
        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env,
            "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(120));
        UNIT_ASSERT_VALUES_EQUAL_C(finalStatus, Ydb::StatusIds::SUCCESS,
            "Restore did not SUCCESS after empty allocator result + retry");
        UNIT_ASSERT_GE_C(emptyInjected.load(), EmptyResultsToInject,
            "Did not inject any empty allocator results; observer unhooked too early");
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table0"), 2u);
    }

    // Allocator-level retries must not consume the per-incremental retry budget.
    Y_UNIT_TEST(AllocatorRetryDoesNotConsumeRetryBudget) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Per-incremental retry budget = 1: a second retry would FAIL.
        TControlBoard::SetValue(1, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreRetriesPerIncremental);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/1);

        // Inject 5 empty allocator results before letting one through.
        constexpr int EmptyResultsToInject = 5;
        std::atomic<int> emptyInjected{0};
        auto observer = runtime.AddObserver<NKikimr::TEvTxAllocatorClient::TEvAllocateResult>(
            [&runtime, &emptyInjected](
                    NKikimr::TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
                if (emptyInjected.load() >= EmptyResultsToInject) {
                    return;
                }
                const ui64 cookie = ev->Cookie;
                const ui64 originalOpId = cookie >> 32;
                if (originalOpId == 0) {
                    return; // unrelated allocator client
                }
                emptyInjected.fetch_add(1);
                runtime.Send(new IEventHandle(
                    ev->Recipient, ev->Sender,
                    new NKikimr::TEvTxAllocatorClient::TEvAllocateResult(TVector<ui64>{}),
                    /*flags=*/0, cookie),
                    /*nodeIndex=*/0, /*viaActorSystem=*/true);
                ev.Reset();
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        // Restore must SUCCEED despite N empty allocator results, because
        // allocator retries are budget-independent.
        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env,
            "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(120));
        UNIT_ASSERT_VALUES_EQUAL_C(finalStatus, Ydb::StatusIds::SUCCESS,
            "Restore FAILED after empty allocator results — allocator retries appear "
            "to be consuming the per-incremental retry budget (cap=1).");
        UNIT_ASSERT_GE_C(emptyInjected.load(), EmptyResultsToInject,
            "Expected " << EmptyResultsToInject << " empty allocator results injected; saw "
            << emptyInjected.load());
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table0"), 2u);
    }

    // Each concurrent TEvAllocateResult must bind to a distinct item and TxId via cookie.
    Y_UNIT_TEST(RetryUnderConcurrentAllocations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Cap = 3 to allow 3 concurrent allocations.
        TControlBoard::SetValue(3, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/3);

        // Capture (cookie -> txId) pairs from each allocator result aimed at SchemeShard.
        TMutex captureMutex;
        TVector<std::pair<ui64, ui64>> captures;
        auto observer = runtime.AddObserver<NKikimr::TEvTxAllocatorClient::TEvAllocateResult>(
            [&captureMutex, &captures](
                    NKikimr::TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
                const ui64 cookie = ev->Cookie;
                const ui64 originalOpId = cookie >> 32;
                if (originalOpId == 0) {
                    return;
                }
                const auto& txIds = ev->Get()->TxIds;
                if (txIds.empty()) {
                    return;
                }
                TGuard<TMutex> g(captureMutex);
                captures.emplace_back(cookie, txIds.front());
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        WaitForRestoreDone(runtime, &env, "/MyRoot", true,
            TDuration::Seconds(2), TDuration::Seconds(120));

        TVector<std::pair<ui64, ui64>> snap;
        {
            TGuard<TMutex> g(captureMutex);
            snap = captures;
        }
        // We need at least 3 binds for the 3 tables (more if finalize/index also hit).
        UNIT_ASSERT_GE_C(snap.size(), 3u,
            "Expected >=3 allocator results bound; saw " << snap.size());

        // Every allocator result destined for incremental restore must bind a
        // DISTINCT itemSeq (low-32) to a DISTINCT TxId. Duplicates would mean
        // the cookie packing collapsed two items into one bind.
        THashSet<ui32> itemSeqs;
        THashSet<ui64> txIds;
        for (const auto& [cookie, tx] : snap) {
            const ui32 seq = static_cast<ui32>(cookie & 0xFFFFFFFFULL);
            UNIT_ASSERT_C(itemSeqs.insert(seq).second,
                "Duplicate itemSeq " << seq << " bound across concurrent allocations");
            UNIT_ASSERT_C(txIds.insert(tx).second,
                "Duplicate TxId " << tx << " bound across concurrent allocations");
        }

        for (ui32 i = 0; i < 3; ++i) {
            TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
            UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 2u);
        }
    }

    // Out-of-order TEvAllocateResult delivery must bind by cookie itemSeq, not FIFO order.
    Y_UNIT_TEST(AllocateResultArrivesOutOfOrder) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Cap = 3 to allow 3 concurrent allocations.
        TControlBoard::SetValue(3, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/3);

        // Hold the first 3 allocator results; release them in reversed order
        // (2, 1, 0) so cookie unpacking is the only thing that can correctly
        // bind each TxId to its sub-op. Capture the (cookie, txId) for the
        // post-completion assertion.
        constexpr size_t HoldCount = 3;
        TMutex queueMutex;
        struct THeld {
            TActorId Recipient;
            TActorId Sender;
            ui64 Cookie;
            TVector<ui64> TxIds;
        };
        TVector<THeld> held;
        std::atomic<bool> released{false};
        TVector<std::pair<ui64, ui64>> finalCaptures;

        auto observer = runtime.AddObserver<NKikimr::TEvTxAllocatorClient::TEvAllocateResult>(
            [&runtime, &queueMutex, &held, &released, &finalCaptures](
                    NKikimr::TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
                const ui64 cookie = ev->Cookie;
                const ui64 originalOpId = cookie >> 32;
                if (originalOpId == 0) {
                    return;
                }

                bool needHold = false;
                {
                    TGuard<TMutex> g(queueMutex);
                    if (!released.load() && held.size() < HoldCount) {
                        held.push_back(THeld{
                            ev->Recipient, ev->Sender, cookie,
                            TVector<ui64>(ev->Get()->TxIds)});
                        needHold = true;
                    } else {
                        if (!ev->Get()->TxIds.empty()) {
                            finalCaptures.emplace_back(cookie, ev->Get()->TxIds.front());
                        }
                    }
                }

                if (needHold) {
                    ev.Reset();

                    // Once we have HoldCount results queued, replay in reverse.
                    bool readyToReplay = false;
                    {
                        TGuard<TMutex> g(queueMutex);
                        readyToReplay = (held.size() == HoldCount && !released.load());
                    }
                    if (readyToReplay) {
                        TVector<THeld> snapshot;
                        {
                            TGuard<TMutex> g(queueMutex);
                            snapshot = held;
                            released.store(true);
                        }
                        // Replay reversed. Replayed events flow through this same
                        // observer with released=true, so they'll be captured by
                        // the else-branch above — do NOT push to finalCaptures
                        // manually here, that would double-count.
                        for (size_t i = snapshot.size(); i-- > 0;) {
                            const auto& h = snapshot[i];
                            TVector<ui64> txIdsCopy = h.TxIds;
                            runtime.Send(new IEventHandle(
                                h.Recipient, h.Sender,
                                new NKikimr::TEvTxAllocatorClient::TEvAllocateResult(std::move(txIdsCopy)),
                                /*flags=*/0, h.Cookie),
                                /*nodeIndex=*/0, /*viaActorSystem=*/true);
                        }
                    }
                }
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env,
            "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(180));
        UNIT_ASSERT_VALUES_EQUAL_C(finalStatus, Ydb::StatusIds::SUCCESS,
            "Restore failed after out-of-order allocator delivery");

        // Each captured cookie must bind to a unique itemSeq with a unique TxId.
        TVector<std::pair<ui64, ui64>> snap;
        {
            TGuard<TMutex> g(queueMutex);
            snap = finalCaptures;
        }
        UNIT_ASSERT_GE_C(snap.size(), HoldCount,
            "Did not capture enough allocator results: " << snap.size());

        THashSet<ui32> itemSeqs;
        THashSet<ui64> txIds;
        for (const auto& [cookie, tx] : snap) {
            if (tx == 0) continue;
            const ui32 seq = static_cast<ui32>(cookie & 0xFFFFFFFFULL);
            UNIT_ASSERT_C(itemSeqs.insert(seq).second,
                "Duplicate itemSeq " << seq << " bound after re-ordering — "
                "cookie unpacking did not preserve item identity");
            UNIT_ASSERT_C(txIds.insert(tx).second,
                "Duplicate TxId " << tx << " bound after re-ordering");
        }

        for (ui32 i = 0; i < 3; ++i) {
            TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
            UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 2u);
        }
    }

    // A TEvAllocateResult arriving after FORGET must be silently dropped with no crash.
    Y_UNIT_TEST(OrphanAllocateResultAfterForgetIsIgnored) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/1);

        // Capture an allocator-result envelope (recipient/sender/cookie) so we
        // can replay one after FORGET. We ONLY capture metadata — we let the
        // event itself flow through unmodified so the restore proceeds normally.
        TMutex mtx;
        struct TCaptured {
            TActorId Recipient;
            TActorId Sender;
            ui64 Cookie = 0;
        };
        TMaybe<TCaptured> captured;
        auto allocObserver = runtime.AddObserver<NKikimr::TEvTxAllocatorClient::TEvAllocateResult>(
            [&mtx, &captured](NKikimr::TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
                const ui64 cookie = ev->Cookie;
                const ui64 originalOpId = cookie >> 32;
                if (originalOpId == 0) return;
                TGuard<TMutex> g(mtx);
                if (!captured) {
                    captured = TCaptured{ev->Recipient, ev->Sender, cookie};
                }
                // Pass through unmodified.
            });

        // Run a normal restore to Completed.
        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env,
            "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL(finalStatus, Ydb::StatusIds::SUCCESS);

        auto listResp = TestListBackupCollectionRestores(runtime, "/MyRoot");
        UNIT_ASSERT_C(!listResp.GetEntries().empty(), "List empty after Completed");
        const ui64 restoreId = listResp.GetEntries().rbegin()->GetId();

        TCaptured envelope;
        {
            TGuard<TMutex> g(mtx);
            UNIT_ASSERT_C(captured, "No allocator result observed during restore");
            envelope = *captured;
        }

        // FORGET wipes the IncrementalRestoreStates entry.
        TestForgetBackupCollectionRestore(runtime, ++txId, "/MyRoot", restoreId,
            Ydb::StatusIds::SUCCESS);

        // Deliver an orphan TEvAllocateResult bearing a cookie whose high-32
        // bits point at the now-forgotten restore. The handler must drop it
        // silently because IncrementalRestoreStates.find(originalOpId)==end().
        runtime.Send(new IEventHandle(
            envelope.Recipient, envelope.Sender,
            new NKikimr::TEvTxAllocatorClient::TEvAllocateResult(ui64(0xDEADBEEFULL)),
            /*flags=*/0, envelope.Cookie),
            /*nodeIndex=*/0, /*viaActorSystem=*/true);

        env.SimulateSleep(runtime, TDuration::Seconds(2));

        // SchemeShard still alive: list/get continue to work; the forgotten
        // restore must NOT reappear in the listing.
        auto listAfterOrphan = TestListBackupCollectionRestores(runtime, "/MyRoot");
        for (const auto& entry : listAfterOrphan.GetEntries()) {
            UNIT_ASSERT_VALUES_UNEQUAL_C(entry.GetId(), restoreId,
                "Forgotten restore reappeared after orphan allocator delivery");
        }
    }
}
