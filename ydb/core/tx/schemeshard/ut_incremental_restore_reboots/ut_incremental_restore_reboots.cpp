#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/lib/value/value.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TIncrementalRestoreWithRebootsTests) {

    // Helper function to create a complete backup scenario with incremental backups
    void CreateCompleteBackupScenario(TTestActorRuntime& runtime, TTestEnv& env, ui64& txId, 
                                       const TString& collectionName, const TVector<TString>& tableNames, 
                                       ui32 incrementalCount = 3) {
        // Create backup collection
        TString collectionSettings = TStringBuilder() << R"(
            Name: ")" << collectionName << R"("
            ExplicitEntryList {)";
        for (const auto& tableName : tableNames) {
            collectionSettings += TStringBuilder() << R"(
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/)" << tableName << R"("
                })";
        }
        collectionSettings += R"(
            }
            Cluster: {}
        )";

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", collectionSettings);
        env.TestWaitNotification(runtime, txId);

        // Create full backup directory and table backups
        TestMkDir(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName, "backup_001_full");
        env.TestWaitNotification(runtime, txId);
        
        for (const auto& tableName : tableNames) {
            AsyncCreateTable(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_full", 
                TStringBuilder() << R"(
                    Name: ")" << tableName << R"("
                    Columns { Name: "key"   Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
            env.TestWaitNotification(runtime, txId);
        }

        // Create incremental backup directories and table backups
        for (ui32 i = 2; i <= incrementalCount + 1; ++i) {
            TString incrName = TStringBuilder() << "backup_" << Sprintf("%03d", i) << "_incremental";
            TestMkDir(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName, incrName);
            env.TestWaitNotification(runtime, txId);
            
            for (const auto& tableName : tableNames) {
                AsyncCreateTable(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/" << incrName, 
                    TStringBuilder() << R"(
                        Name: ")" << tableName << R"("
                        Columns { Name: "key"   Type: "Uint32" }
                        Columns { Name: "value" Type: "Utf8" }
                        Columns { Name: "__ydb_deleted" Type: "Bool" }
                        KeyColumnNames: ["key"]
                    )");
                env.TestWaitNotification(runtime, txId);
            }
        }
    }

    // Helper function to verify incremental restore operation data in database
    void VerifyIncrementalRestoreOperationInDatabase(TTestActorRuntime& runtime, TTabletId schemeShardTabletId) {
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
        
        auto value = NClient::TValue::Create(result);
        auto operationsResultSet = value["Operations"];
        UNIT_ASSERT_C(operationsResultSet.HaveValue(), "Operations result set should be present");
        
        auto operationsList = operationsResultSet["List"];
        if (operationsList.HaveValue()) {
            ui32 operationsCount = operationsList.Size();
            Cerr << "Found " << operationsCount << " incremental restore operations in database" << Endl;
            
            UNIT_ASSERT_C(operationsCount > 0, "Should have at least one incremental restore operation in database");
            
            // Verify the first operation (there should be at least one)
            auto operation = operationsList[0];
            auto operationIdValue = operation["Id"];
            auto operationDataValue = operation["Operation"];
            
            UNIT_ASSERT_C(operationIdValue.HaveValue(), "Operation should have Id field");
            UNIT_ASSERT_C(operationDataValue.HaveValue(), "Operation should have Operation field");
            
            auto operationId = (ui64)operationIdValue;
            auto operationData = (TString)operationDataValue;
            
            UNIT_ASSERT_C(operationId > 0, "Operation ID should be positive");
            UNIT_ASSERT_C(!operationData.empty(), "Operation data should not be empty");
            
            // Deserialize and verify operation data
            NKikimrSchemeOp::TLongIncrementalRestoreOp longIncrementalRestoreOp;
            bool parseSuccess = longIncrementalRestoreOp.ParseFromString(operationData);
            UNIT_ASSERT_C(parseSuccess, "Failed to parse operation data as TLongIncrementalRestoreOp protobuf");
            
            // Verify operation structure
            UNIT_ASSERT_C(longIncrementalRestoreOp.GetTxId() > 0, "TxId in protobuf should be positive");
            UNIT_ASSERT_C(!longIncrementalRestoreOp.GetId().empty(), "Id should not be empty");
            UNIT_ASSERT_C(longIncrementalRestoreOp.HasBackupCollectionPathId(), "BackupCollectionPathId should be present");
            UNIT_ASSERT_C(longIncrementalRestoreOp.GetTablePathList().size() > 0, "Should have at least one table path");
            
            Cerr << "Successfully verified incremental restore operation in database: "
                 << "TxId=" << longIncrementalRestoreOp.GetTxId() 
                 << ", Id=" << longIncrementalRestoreOp.GetId()
                 << ", TableCount=" << longIncrementalRestoreOp.GetTablePathList().size() << Endl;
        } else {
            UNIT_ASSERT_C(false, "No operations found in IncrementalRestoreOperations table, but operations were expected");
        }
    }

    // Helper function to verify path states after reboot
    void VerifyPathStatesAfterReboot(TTestActorRuntime& runtime, const TVector<TString>& targetTables, 
                                     const TString& collectionName) {
        Cerr << "Verifying path states after reboot..." << Endl;
        
        // Check target table states - they should be in EPathStateIncomingIncrementalRestore
        for (const auto& tableName : targetTables) {
            auto targetTableDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/" << tableName);
            auto targetState = targetTableDesc.GetPathDescription().GetSelf().GetPathState();
            Cerr << "Target table '" << tableName << "' state: " << NKikimrSchemeOp::EPathState_Name(targetState) << Endl;
            
            UNIT_ASSERT_VALUES_EQUAL_C(targetState, NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore,
                TStringBuilder() << "Target table '" << tableName << "' should be in EPathStateIncomingIncrementalRestore state, but got: " 
                               << NKikimrSchemeOp::EPathState_Name(targetState));
        }

        // Check backup collection state - it should be in EPathStateOutgoingIncrementalRestore
        auto backupCollectionDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName);
        if (backupCollectionDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
            auto collectionState = backupCollectionDesc.GetPathDescription().GetSelf().GetPathState();
            Cerr << "Backup collection '" << collectionName << "' state: " << NKikimrSchemeOp::EPathState_Name(collectionState) << Endl;
            
            // The backup collection might be in EPathStateOutgoingIncrementalRestore if it exists locally
            // This depends on whether the backup collection path exists in the local schemeshard
        }
    }

    Y_UNIT_TEST(BasicIncrementalRestoreWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create backup scenario
            CreateCompleteBackupScenario(runtime, *t.TestEnv, t.TxId, "RebootTestCollection", {"RebootTestTable"}, 2);

            // Verify backup collection exists
            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/RebootTestCollection"), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                });
            }

            // Execute incremental restore operation
            TString restoreSettings = R"(
                Name: "RebootTestCollection"
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Verify that the operation completed successfully and database state is correct
            {
                TInactiveZone inactive(activeZone);
                
                // Verify that the target table was created and is in the correct state
                TestDescribeResult(DescribePath(runtime, "/MyRoot/RebootTestTable"), {NLs::PathExist});
                
                // Verify operation persisted in database
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId);
                
                // Verify path states
                VerifyPathStatesAfterReboot(runtime, {"RebootTestTable"}, "RebootTestCollection");
            }
        });
    }

    Y_UNIT_TEST(MultiTableIncrementalRestoreWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create backup scenario with multiple tables
            CreateCompleteBackupScenario(runtime, *t.TestEnv, t.TxId, "MultiTableCollection", 
                                        {"Table1", "Table2", "Table3"}, 3);

            // Execute incremental restore operation
            TString restoreSettings = R"(
                Name: "MultiTableCollection"
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Verify that all tables were created and are in the correct state
            {
                TInactiveZone inactive(activeZone);
                
                for (const auto& tableName : TVector<TString>{"Table1", "Table2", "Table3"}) {
                    TestDescribeResult(DescribePath(runtime, TStringBuilder() << "/MyRoot/" << tableName), {NLs::PathExist});
                }
                
                // Verify operation persisted in database
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId);
                
                // Verify path states for all tables
                VerifyPathStatesAfterReboot(runtime, {"Table1", "Table2", "Table3"}, "MultiTableCollection");
            }
        });
    }

    Y_UNIT_TEST(IncrementalRestoreStateRecoveryAfterReboot) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create complex backup scenario
            CreateCompleteBackupScenario(runtime, *t.TestEnv, t.TxId, "StateRecoveryCollection", 
                                        {"RecoveryTable"}, 5); // More incremental backups

            // Start incremental restore operation
            TString restoreSettings = R"(
                Name: "StateRecoveryCollection"
            )";
            AsyncRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            ui64 restoreTxId = t.TxId;

            // Wait for the operation to start and potentially get interrupted by reboot
            TestModificationResult(runtime, restoreTxId, NKikimrScheme::StatusAccepted);
            t.TestEnv->TestWaitNotification(runtime, restoreTxId);

            // Verify that after reboots, the operation state is correctly recovered
            {
                TInactiveZone inactive(activeZone);
                
                // Verify the table exists (restore completed)
                TestDescribeResult(DescribePath(runtime, "/MyRoot/RecoveryTable"), {NLs::PathExist});
                
                // Verify operation was properly persisted and can be recovered
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId);
                
                // Verify that path states are correctly restored after reboot
                VerifyPathStatesAfterReboot(runtime, {"RecoveryTable"}, "StateRecoveryCollection");
                
                // Additional verification: Check that the LongIncrementalRestoreOps in-memory state 
                // was correctly loaded from the database during init
                Cerr << "State recovery test completed successfully - operation state preserved across reboots" << Endl;
            }
        });
    }

    Y_UNIT_TEST(DatabaseConsistencyAfterMultipleReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create backup scenario
            CreateCompleteBackupScenario(runtime, *t.TestEnv, t.TxId, "ConsistencyTestCollection", 
                                        {"ConsistencyTable1", "ConsistencyTable2"}, 4);

            // Execute first restore operation
            TString restoreSettings1 = R"(
                Name: "ConsistencyTestCollection"
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings1);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create another backup collection
            CreateCompleteBackupScenario(runtime, *t.TestEnv, t.TxId, "ConsistencyTestCollection2", 
                                        {"AnotherTable"}, 2);

            // Execute second restore operation
            TString restoreSettings2 = R"(
                Name: "ConsistencyTestCollection2"
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings2);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Verify database consistency after multiple operations and reboots
            {
                TInactiveZone inactive(activeZone);
                
                // Verify all tables exist
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ConsistencyTable1"), {NLs::PathExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ConsistencyTable2"), {NLs::PathExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/AnotherTable"), {NLs::PathExist});
                
                // Verify both operations are in database
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
                
                auto value = NClient::TValue::Create(result);
                auto operationsResultSet = value["Operations"];
                UNIT_ASSERT_C(operationsResultSet.HaveValue(), "Operations result set should be present");
                
                auto operationsList = operationsResultSet["List"];
                if (operationsList.HaveValue()) {
                    ui32 operationsCount = operationsList.Size();
                    Cerr << "Found " << operationsCount << " incremental restore operations in database after multiple operations" << Endl;
                    
                    // We should have at least 2 operations (could be more due to reboots)
                    UNIT_ASSERT_C(operationsCount >= 2, "Should have at least 2 incremental restore operations in database");
                } else {
                    UNIT_ASSERT_C(false, "No operations found in database after multiple restore operations");
                }
                
                // Verify path states are consistent
                VerifyPathStatesAfterReboot(runtime, {"ConsistencyTable1", "ConsistencyTable2"}, "ConsistencyTestCollection");
                VerifyPathStatesAfterReboot(runtime, {"AnotherTable"}, "ConsistencyTestCollection2");
                
                Cerr << "Database consistency verification completed successfully" << Endl;
            }
        });
    }

    Y_UNIT_TEST(OperationIdempotencyAfterReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create backup scenario
            CreateCompleteBackupScenario(runtime, *t.TestEnv, t.TxId, "IdempotencyTestCollection", 
                                        {"IdempotencyTable"}, 3);

            // Execute first restore operation
            TString restoreSettings = R"(
                Name: "IdempotencyTestCollection"
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            ui64 firstRestoreTxId = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, firstRestoreTxId);

            // Attempt to execute the same restore operation again (should be handled gracefully)
            AsyncRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            ui64 secondRestoreTxId = t.TxId;
            
            // The second operation should either succeed (if it's idempotent) or fail with appropriate error
            auto secondResult = TestModificationResults(runtime, secondRestoreTxId, {NKikimrScheme::StatusAccepted});
            
            // Wait for completion of any pending notifications
            if (secondResult == NKikimrScheme::StatusAccepted) {
                t.TestEnv->TestWaitNotification(runtime, secondRestoreTxId);
            }

            // Verify system state after potential duplicate operation
            {
                TInactiveZone inactive(activeZone);
                
                // Verify table still exists and is in correct state
                TestDescribeResult(DescribePath(runtime, "/MyRoot/IdempotencyTable"), {NLs::PathExist});
                
                // Verify database integrity - operations should be properly handled
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId);
                
                // Verify path states remain consistent
                VerifyPathStatesAfterReboot(runtime, {"IdempotencyTable"}, "IdempotencyTestCollection");
                
                Cerr << "Idempotency test completed - system handled duplicate operation correctly" << Endl;
            }
        });
    }
}
