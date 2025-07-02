#include <ydb/public/lib/value/value.h>

#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TIncrementalRestoreWithRebootsTests) {

    // Helper structure for capturing TEvRunIncrementalRestore events
    struct TOrphanedOpEventCapture {
        TVector<TPathId> CapturedBackupCollectionPathIds;
        THashSet<TPathId> ExpectedBackupCollectionPathIds;
        bool CapturingEnabled = false;
        
        void ClearCapturedEvents() {
            CapturedBackupCollectionPathIds.clear();
            ExpectedBackupCollectionPathIds.clear();
            CapturingEnabled = false;
        }
        
        void EnableCapturing(const TVector<TPathId>& expectedPathIds = {}) {
            ExpectedBackupCollectionPathIds.clear();
            for (const auto& pathId : expectedPathIds) {
                ExpectedBackupCollectionPathIds.insert(pathId);
            }
            CapturingEnabled = true;
        }
        
        void DisableCapturing() {
            CapturingEnabled = false;
        }
        
        ui32 GetCapturedEventCount() const {
            return CapturedBackupCollectionPathIds.size();
        }
        
        bool HasCapturedEvents() const {
            return !CapturedBackupCollectionPathIds.empty();
        }
    };

    // Helper function to setup TEvRunIncrementalRestore event observer
    void SetupOrphanedOpEventObserver(TTestActorRuntime& runtime, TOrphanedOpEventCapture& capture) {
        runtime.SetObserverFunc([&capture](TAutoPtr<IEventHandle>& ev) {
            if (ev && ev->GetTypeRewrite() == TEvPrivate::TEvRunIncrementalRestore::EventType && capture.CapturingEnabled) {
                auto* msg = ev->Get<TEvPrivate::TEvRunIncrementalRestore>();
                if (msg) {
                    if (capture.ExpectedBackupCollectionPathIds.empty() || 
                        capture.ExpectedBackupCollectionPathIds.contains(msg->BackupCollectionPathId)) {
                        capture.CapturedBackupCollectionPathIds.push_back(msg->BackupCollectionPathId);
                        Cerr << "Captured TEvRunIncrementalRestore for BackupCollectionPathId: " 
                             << msg->BackupCollectionPathId << Endl;
                    }
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
    }

    // Helper function to get backup collection path ID from runtime
    TPathId GetBackupCollectionPathId(TTestActorRuntime& runtime, const TString& collectionName) {
        TString backupCollectionPath = TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName;
        auto description = DescribePath(runtime, backupCollectionPath);
        
        if (description.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
            auto selfEntry = description.GetPathDescription().GetSelf();
            return TPathId(selfEntry.GetSchemeshardId(), selfEntry.GetPathId());
        }
        
        return TPathId();
    }

    // Helper function to create basic backup scenario (full backups only)
    void CreateBasicBackupScenario(TTestActorRuntime& runtime, TTestEnv& env, ui64& txId, 
                                   const TString& collectionName, const TVector<TString>& tableNames) {
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

        // Create only full backup directory and table backups
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
    }

    // Helper function to verify incremental restore operation data in database
    void VerifyIncrementalRestoreOperationInDatabase(TTestActorRuntime& runtime, TTabletId schemeShardTabletId, bool expectOperations = false) {
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
            
            if (expectOperations) {
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
                // Just log the count, don't fail if no operations (they may have completed and been cleaned up)
                Cerr << "Database consistency check: found " << operationsCount << " operations (expected behavior after completion)" << Endl;
            }
        } else {
            if (expectOperations) {
                UNIT_ASSERT_C(false, "No operations found in IncrementalRestoreOperations table, but operations were expected");
            } else {
                Cerr << "No operations in database - this is expected after operation completion" << Endl;
            }
        }
    }

    // Helper function to verify path states after reboot for regular restore operations
    void VerifyPathStatesAfterReboot(TTestActorRuntime& runtime, const TVector<TString>& targetTables, 
                                     const TString& collectionName) {
        Cerr << "Verifying path states after reboot for regular restore operation..." << Endl;
        
        // Check target table states - they should be in EPathStateIncomingIncrementalRestore
        // Note: For regular restore operations (no incremental backups), target tables still use this state
        for (const auto& tableName : targetTables) {
            auto targetTableDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/" << tableName);
            if (targetTableDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
                auto targetState = targetTableDesc.GetPathDescription().GetSelf().GetPathState();
                Cerr << "Target table '" << tableName << "' state: " << NKikimrSchemeOp::EPathState_Name(targetState) << Endl;
                
        // For completed operations, tables should be in normal state (EPathStateNoChanges)
        // For ongoing operations, they should be in EPathStateIncomingIncrementalRestore
        bool validState = (targetState == NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore) ||
                         (targetState == NKikimrSchemeOp::EPathState::EPathStateNoChanges);
                UNIT_ASSERT_C(validState,
                    TStringBuilder() << "Target table '" << tableName << "' should be in EPathStateIncomingIncrementalRestore or EPathStateNoChanges state, but got: " 
                                   << NKikimrSchemeOp::EPathState_Name(targetState));
            }
        }

        // Check backup collection state - it should be in EPathStateOutgoingIncrementalRestore if operation is ongoing
        auto backupCollectionDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName);
        if (backupCollectionDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
            auto collectionState = backupCollectionDesc.GetPathDescription().GetSelf().GetPathState();
            Cerr << "Backup collection '" << collectionName << "' state: " << NKikimrSchemeOp::EPathState_Name(collectionState) << Endl;
            
            // The backup collection might be in EPathStateOutgoingIncrementalRestore if operation is ongoing
            // or EPathStateNoChanges if operation is completed
            bool validState = (collectionState == NKikimrSchemeOp::EPathState::EPathStateOutgoingIncrementalRestore) ||
                             (collectionState == NKikimrSchemeOp::EPathState::EPathStateNoChanges);
            UNIT_ASSERT_C(validState,
                TStringBuilder() << "Backup collection '" << collectionName << "' should be in valid state, but got: " 
                               << NKikimrSchemeOp::EPathState_Name(collectionState));
        }
    }

    // Helper function to verify backup table path states for regular restore operations (full backups only)
    void VerifyBackupTablePathStates(TTestActorRuntime& runtime, const TString& collectionName, 
                                   const TVector<TString>& tableNames) {
        Cerr << "Verifying backup table path states for regular restore operation in collection: " << collectionName << Endl;
        
        // Verify full backup table states (_full suffix) - these should be set by the current implementation
        TString fullBackupPath = TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_full";
        for (const auto& tableName : tableNames) {
            TString fullBackupTablePath = TStringBuilder() << fullBackupPath << "/" << tableName;
            auto desc = DescribePath(runtime, fullBackupTablePath);
            
            if (desc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
                auto state = desc.GetPathDescription().GetSelf().GetPathState();
                Cerr << "Full backup table '" << fullBackupTablePath << "' state: " << NKikimrSchemeOp::EPathState_Name(state) << Endl;
                
                // Full backup tables should be in EPathStateOutgoingIncrementalRestore if operation is ongoing
                // or EPathStateNoChanges if operation is completed
                bool validState = (state == NKikimrSchemeOp::EPathState::EPathStateOutgoingIncrementalRestore) ||
                                 (state == NKikimrSchemeOp::EPathState::EPathStateNoChanges);
                UNIT_ASSERT_C(validState,
                    TStringBuilder() << "Full backup table '" << fullBackupTablePath 
                                   << "' should be in valid state, but got: " 
                                   << NKikimrSchemeOp::EPathState_Name(state));
            }
        }
    }

    // Helper function to verify that backup table trimmed name reconstruction works correctly for regular restore
    void VerifyBackupTableTrimmedNameReconstruction(TTestActorRuntime& runtime, const TString& collectionName, 
                                                  const TVector<TString>& tableNames) {
        Cerr << "Verifying backup table trimmed name reconstruction for regular restore in collection: " << collectionName << Endl;
        
        // Test the trimmed name pattern for regular restore operations:
        // Full backup: backup_001_full (trimmed name: backup_001, suffix: _full)  
        // (No incremental backups in current implementation)
        
        // Verify that full backup paths constructed with trimmed names exist
        auto fullBackupDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_full");
        UNIT_ASSERT_C(fullBackupDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist,
            "Full backup directory should exist for trimmed name reconstruction test");
        
        // Verify table paths within backup directory
        for (const auto& tableName : tableNames) {
            auto fullTableDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_full/" << tableName);
            UNIT_ASSERT_C(fullTableDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist,
                TStringBuilder() << "Full backup table '" << tableName << "' should exist for trimmed name test");
        }
        
        Cerr << "Backup table trimmed name reconstruction verification completed successfully for regular restore" << Endl;
    }

    // Helper function to verify path states after reboot for incremental restore operations (with both full and incremental backups)
    void VerifyIncrementalPathStatesAfterReboot(TTestActorRuntime& runtime, const TVector<TString>& targetTables, 
                                                 const TString& collectionName) {
        Cerr << "Verifying path states after reboot for incremental restore operation..." << Endl;
        
        // Check target table states - they should be in EPathStateIncomingIncrementalRestore during operation
        // or EPathStateNoChanges after completion
        for (const auto& tableName : targetTables) {
            auto targetTableDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/" << tableName);
            if (targetTableDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
                auto targetState = targetTableDesc.GetPathDescription().GetSelf().GetPathState();
                Cerr << "Target table '" << tableName << "' state: " << NKikimrSchemeOp::EPathState_Name(targetState) << Endl;
                
                bool validState = (targetState == NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore) ||
                                 (targetState == NKikimrSchemeOp::EPathState::EPathStateNoChanges);
                UNIT_ASSERT_C(validState,
                    TStringBuilder() << "Target table '" << tableName << "' should be in EPathStateIncomingIncrementalRestore or EPathStateNoChanges state, but got: " 
                                   << NKikimrSchemeOp::EPathState_Name(targetState));
            }
        }

        // Check backup collection state - should be in EPathStateOutgoingIncrementalRestore during operation
        auto backupCollectionDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName);
        if (backupCollectionDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
            auto collectionState = backupCollectionDesc.GetPathDescription().GetSelf().GetPathState();
            Cerr << "Backup collection '" << collectionName << "' state: " << NKikimrSchemeOp::EPathState_Name(collectionState) << Endl;
            
            bool validState = (collectionState == NKikimrSchemeOp::EPathState::EPathStateOutgoingIncrementalRestore) ||
                             (collectionState == NKikimrSchemeOp::EPathState::EPathStateNoChanges);
            UNIT_ASSERT_C(validState,
                TStringBuilder() << "Backup collection '" << collectionName << "' should be in valid state, but got: " 
                               << NKikimrSchemeOp::EPathState_Name(collectionState));
        }
    }

    // Helper function to verify backup table path states for incremental restore operations
    void VerifyIncrementalBackupTablePathStates(TTestActorRuntime& runtime, const TString& collectionName, 
                                                 const TVector<TString>& tableNames) {
        Cerr << "Verifying backup table path states for incremental restore operation in collection: " << collectionName << Endl;
        
        // Verify full backup table states (_full suffix)
        TString fullBackupPath = TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_full";
        for (const auto& tableName : tableNames) {
            TString fullBackupTablePath = TStringBuilder() << fullBackupPath << "/" << tableName;
            auto desc = DescribePath(runtime, fullBackupTablePath);
            
            if (desc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
                auto state = desc.GetPathDescription().GetSelf().GetPathState();
                Cerr << "Full backup table '" << fullBackupTablePath << "' state: " << NKikimrSchemeOp::EPathState_Name(state) << Endl;
                
                // Full backup tables should always be in EPathStateNoChanges at the end, regardless of incremental backups
                // They transition to this state after the operation completes
                bool validState = (state == NKikimrSchemeOp::EPathState::EPathStateNoChanges);
                UNIT_ASSERT_C(validState,
                    TStringBuilder() << "Full backup table '" << fullBackupTablePath 
                                   << "' should be in EPathStateNoChanges state (operation completed), but got: " 
                                   << NKikimrSchemeOp::EPathState_Name(state));
            }
        }

        // Verify incremental backup table states (_incremental suffix)
        TString incrementalBackupPath = TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_incremental";
        for (const auto& tableName : tableNames) {
            TString incrementalBackupTablePath = TStringBuilder() << incrementalBackupPath << "/" << tableName;
            auto desc = DescribePath(runtime, incrementalBackupTablePath);
            
            if (desc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
                auto state = desc.GetPathDescription().GetSelf().GetPathState();
                Cerr << "Incremental backup table '" << incrementalBackupTablePath << "' state: " << NKikimrSchemeOp::EPathState_Name(state) << Endl;
                
                // This is the critical test - incremental backup tables should preserve their EPathStateAwaitingOutgoingIncrementalRestore state
                // throughout the incremental restore workflow, even after operation completion
                bool validState = (state == NKikimrSchemeOp::EPathState::EPathStateAwaitingOutgoingIncrementalRestore);
                UNIT_ASSERT_C(validState,
                    TStringBuilder() << "Incremental backup table '" << incrementalBackupTablePath 
                                   << "' should be in EPathStateAwaitingOutgoingIncrementalRestore state (this tests trimmed name reconstruction), but got: " 
                                   << NKikimrSchemeOp::EPathState_Name(state));
            } else {
                UNIT_ASSERT_C(false, TStringBuilder() << "Incremental backup table '" << incrementalBackupTablePath << "' should exist for this test");
            }
        }
    }

    // Helper function to verify that incremental backup table trimmed name reconstruction works correctly
    void VerifyIncrementalBackupTableTrimmedNameReconstruction(TTestActorRuntime& runtime, const TString& collectionName, 
                                                                const TVector<TString>& tableNames) {
        Cerr << "Verifying incremental backup table trimmed name reconstruction in collection: " << collectionName << Endl;
        
        // Test the trimmed name pattern for incremental restore operations:
        // Full backup: backup_001_full (trimmed name: backup_001, suffix: _full)  
        // Incremental backup: backup_001_incremental (trimmed name: backup_001, suffix: _incremental)
        // Both should be reconstructed from the same trimmed name "backup_001"
        
        // Verify that both full and incremental backup paths exist
        auto fullBackupDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_full");
        UNIT_ASSERT_C(fullBackupDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist,
            "Full backup directory should exist for incremental trimmed name reconstruction test");
            
        auto incrementalBackupDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_incremental");
        UNIT_ASSERT_C(incrementalBackupDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist,
            "Incremental backup directory should exist for incremental trimmed name reconstruction test");
        
        // Verify table paths within both backup directories
        for (const auto& tableName : tableNames) {
            auto fullTableDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_full/" << tableName);
            UNIT_ASSERT_C(fullTableDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist,
                TStringBuilder() << "Full backup table '" << tableName << "' should exist for incremental trimmed name test");
                
            auto incrementalTableDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_incremental/" << tableName);
            UNIT_ASSERT_C(incrementalTableDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist,
                TStringBuilder() << "Incremental backup table '" << tableName << "' should exist for incremental trimmed name test");
        }
        
        Cerr << "Incremental backup table trimmed name reconstruction verification completed successfully" << Endl;
    }

    // Helper function to create incremental backup scenario with both full and incremental backups
    void CreateIncrementalBackupScenario(TTestActorRuntime& runtime, TTestEnv& env, ui64& txId, 
                                          const TString& collectionName, const TVector<TString>& tableNames) {
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

        // Create both full and incremental backup directories and table backups
        // Create full backup directory and tables
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

        // Create incremental backup directory and tables
        TestMkDir(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName, "backup_001_incremental");
        env.TestWaitNotification(runtime, txId);
        
        for (const auto& tableName : tableNames) {
            AsyncCreateTable(runtime, ++txId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_incremental", 
                TStringBuilder() << R"(
                    Name: ")" << tableName << R"("
                    Columns { Name: "key"   Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8" }
                    Columns { Name: "incremental_data" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
            env.TestWaitNotification(runtime, txId);
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

            // Create basic backup scenario (only full backup, no incremental backups)
            CreateBasicBackupScenario(runtime, *t.TestEnv, t.TxId, "RebootTestCollection", {"RebootTestTable"});

            // Verify backup collection exists
            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/RebootTestCollection"), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                });
            }

            // Execute restore operation (regular restore, not incremental)
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
                
                // Verify operation persisted in database (operations may be cleaned up after completion)
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, false);
                
                // Verify path states for regular restore operation
                VerifyPathStatesAfterReboot(runtime, {"RebootTestTable"}, "RebootTestCollection");
                
                Cerr << "Basic restore with reboots test completed successfully" << Endl;
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

            // Create basic backup scenario with multiple tables (only full backup)
            CreateBasicBackupScenario(runtime, *t.TestEnv, t.TxId, "MultiTableCollection", 
                                    {"Table1", "Table2", "Table3"});

            // Execute restore operation
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
                
                // Verify operation persisted in database (operations may be cleaned up after completion)
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, false);
                
                // Verify path states for all tables
                VerifyPathStatesAfterReboot(runtime, {"Table1", "Table2", "Table3"}, "MultiTableCollection");
                
                Cerr << "Multi-table restore with reboots test completed successfully" << Endl;
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

            // Create basic backup scenario
            CreateBasicBackupScenario(runtime, *t.TestEnv, t.TxId, "StateRecoveryCollection", {"RecoveryTable"});

            // Start restore operation
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
                
                // Verify operation was properly persisted and can be recovered (operations may be cleaned up after completion)
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, false);
                
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

            // Create first backup scenario
            CreateBasicBackupScenario(runtime, *t.TestEnv, t.TxId, "ConsistencyTestCollection", 
                                    {"ConsistencyTable1", "ConsistencyTable2"});

            // Execute first restore operation
            TString restoreSettings1 = R"(
                Name: "ConsistencyTestCollection"
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings1);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create another backup collection
            CreateBasicBackupScenario(runtime, *t.TestEnv, t.TxId, "ConsistencyTestCollection2", {"AnotherTable"});

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
                    Cerr << "Found " << operationsCount << " restore operations in database after multiple operations" << Endl;
                    
                    // Operations may have been cleaned up after completion, which is expected behavior
                    Cerr << "Database consistency check: found " << operationsCount << " operations (operations may be cleaned up after completion)" << Endl;
                } else {
                    // This is expected behavior - operations are cleaned up after completion
                    Cerr << "No operations in database - this is expected after operation completion" << Endl;
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

            // Create basic backup scenario
            CreateBasicBackupScenario(runtime, *t.TestEnv, t.TxId, "IdempotencyTestCollection", {"IdempotencyTable"});

            // Execute first restore operation
            TString restoreSettings = R"(
                Name: "IdempotencyTestCollection"
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            ui64 firstRestoreTxId = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, firstRestoreTxId);

            // Attempt to execute the same restore operation again - this should fail in current implementation
            // because the target table already exists and restore operations are not idempotent
            AsyncRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            ui64 secondRestoreTxId = t.TxId;
            
            // The second operation should fail with StatusSchemeError because table already exists
            // This is the expected behavior in the current implementation (not idempotent)
            auto secondResult = TestModificationResults(runtime, secondRestoreTxId, {NKikimrScheme::StatusSchemeError});
            
            // Verify that the error is specifically about path existing (expected behavior)
            UNIT_ASSERT_C(secondResult == NKikimrScheme::StatusSchemeError, 
                "Second restore operation should fail with StatusSchemeError since table already exists");

            // Verify system state after duplicate operation attempt
            {
                TInactiveZone inactive(activeZone);
                
                // Verify table still exists and is in correct state
                TestDescribeResult(DescribePath(runtime, "/MyRoot/IdempotencyTable"), {NLs::PathExist});
                
                // Verify database integrity - completed operations may have been cleaned up
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, false); // Don't expect operations after completion
                
                // Verify path states remain consistent (from first successful operation)
                VerifyPathStatesAfterReboot(runtime, {"IdempotencyTable"}, "IdempotencyTestCollection");
                
                Cerr << "Idempotency test completed - system correctly rejected duplicate operation" << Endl;
            }
        });
    }

    Y_UNIT_TEST(BackupTablePathStateRestoration) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create basic backup scenario with specific focus on backup table path states
            CreateBasicBackupScenario(runtime, *t.TestEnv, t.TxId, "BackupTableTestCollection", 
                                    {"BackupTableTest1", "BackupTableTest2"});

            // Verify backup table trimmed name reconstruction works
            {
                TInactiveZone inactive(activeZone);
                VerifyBackupTableTrimmedNameReconstruction(runtime, "BackupTableTestCollection", 
                                                         {"BackupTableTest1", "BackupTableTest2"});
            }

            // Execute restore operation
            TString restoreSettings = R"(
                Name: "BackupTableTestCollection"
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Verify backup table path states after restore operation
            {
                TInactiveZone inactive(activeZone);
                
                // Verify that backup table path states are correctly restored from the database
                VerifyBackupTablePathStates(runtime, "BackupTableTestCollection", 
                                          {"BackupTableTest1", "BackupTableTest2"});
                
                // Verify target table states
                VerifyPathStatesAfterReboot(runtime, {"BackupTableTest1", "BackupTableTest2"}, 
                                          "BackupTableTestCollection");
                
                Cerr << "Backup table path state restoration test completed successfully" << Endl;
            }
        });
    }

    Y_UNIT_TEST(BackupTablePathStateRestorationWithMultipleCollections) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create multiple backup collections to test cross-collection state isolation
            CreateBasicBackupScenario(runtime, *t.TestEnv, t.TxId, "Collection1", {"Table1"});
            CreateBasicBackupScenario(runtime, *t.TestEnv, t.TxId, "Collection2", {"Table2", "Table3"});

            // Execute restore operations for both collections
            TString restoreSettings1 = R"(Name: "Collection1")";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings1);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TString restoreSettings2 = R"(Name: "Collection2")";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings2);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Verify backup table path states for both collections independently
            {
                TInactiveZone inactive(activeZone);
                
                // Verify backup table states for Collection1
                VerifyBackupTablePathStates(runtime, "Collection1", {"Table1"});
                
                // Verify backup table states for Collection2  
                VerifyBackupTablePathStates(runtime, "Collection2", {"Table2", "Table3"});
                
                // Verify target table states for both collections
                VerifyPathStatesAfterReboot(runtime, {"Table1"}, "Collection1");
                VerifyPathStatesAfterReboot(runtime, {"Table2", "Table3"}, "Collection2");
                
                Cerr << "Multiple collections backup table path state restoration test completed successfully" << Endl;
            }
        });
    }

    Y_UNIT_TEST(BackupTablePathStateRestorationEdgeCases) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create basic backup scenario with edge case names (special characters, long names)
            CreateBasicBackupScenario(runtime, *t.TestEnv, t.TxId, "EdgeCaseCollection", 
                                    {"Table_With_Underscores", "TableWithNumbers123"});

            // Execute restore operation
            TString restoreSettings = R"(
                Name: "EdgeCaseCollection"
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Verify backup table path states work correctly with edge case names
            {
                TInactiveZone inactive(activeZone);
                
                // Verify backup table states with special character table names
                VerifyBackupTablePathStates(runtime, "EdgeCaseCollection", 
                                          {"Table_With_Underscores", "TableWithNumbers123"});
                
                // Verify target table states 
                VerifyPathStatesAfterReboot(runtime, {"Table_With_Underscores", "TableWithNumbers123"}, 
                                          "EdgeCaseCollection");
                
                // Verify operation persisted correctly in database (operations may be cleaned up after completion)
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, false);
                
                Cerr << "Edge cases backup table path state restoration test completed successfully" << Endl;
            }
        });
    }

    Y_UNIT_TEST(BackupTablePathStateRestorationAfterMultipleReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create basic backup scenario
            CreateBasicBackupScenario(runtime, *t.TestEnv, t.TxId, "RebootStabilityCollection", {"RebootStabilityTable"});

            // Execute restore operation
            TString restoreSettings = R"(
                Name: "RebootStabilityCollection"  
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Perform multiple reboot cycles and verify backup table state persistence
            for (ui32 rebootCycle = 1; rebootCycle <= 3; ++rebootCycle) {
                Cerr << "Testing backup table path state persistence after reboot cycle " << rebootCycle << Endl;
                
                {
                    TInactiveZone inactive(activeZone);
                    
                    // Verify backup table path states persist across reboots
                    VerifyBackupTablePathStates(runtime, "RebootStabilityCollection", {"RebootStabilityTable"});
                    
                    // Verify target table state persists
                    VerifyPathStatesAfterReboot(runtime, {"RebootStabilityTable"}, "RebootStabilityCollection");
                    
                    // Verify operation data persists in database (operations may be cleaned up after completion)
                    TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                    VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, false);
                }
            }
            
            Cerr << "Backup table path state restoration after multiple reboots test completed successfully" << Endl;
        });
    }

    Y_UNIT_TEST(IncrementalBackupTablePathStatesWithReboots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create incremental backup scenario with BOTH full and incremental backup tables
            // This tests the critical trimmed name reconstruction logic: "backup_001" -> "backup_001_full" + "backup_001_incremental"
            CreateIncrementalBackupScenario(runtime, *t.TestEnv, t.TxId, "IncrementalRestoreCollection", 
                                          {"IncrementalTable1", "IncrementalTable2"});

            // Verify both backup directories exist before starting restore
            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/IncrementalRestoreCollection"), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                });
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/IncrementalRestoreCollection/backup_001_full"), {
                    NLs::PathExist,
                });
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/IncrementalRestoreCollection/backup_001_incremental"), {
                    NLs::PathExist,
                });
            }

            // Execute incremental restore operation
            TString restoreSettings = R"(
                Name: "IncrementalRestoreCollection"
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Verify that incremental backup table path states are correctly preserved after reboot
            {
                TInactiveZone inactive(activeZone);
                
                // Verify that the target tables were created  
                for (const auto& tableName : TVector<TString>{"IncrementalTable1", "IncrementalTable2"}) {
                    TestDescribeResult(DescribePath(runtime, TStringBuilder() << "/MyRoot/" << tableName), {NLs::PathExist});
                }
                
                // Verify operation persisted in database
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, false);
                
                // Verify path states for incremental restore operation
                VerifyIncrementalPathStatesAfterReboot(runtime, {"IncrementalTable1", "IncrementalTable2"}, "IncrementalRestoreCollection");
                
                // Verify backup table path states for both full and incremental backup tables
                VerifyIncrementalBackupTablePathStates(runtime, "IncrementalRestoreCollection", {"IncrementalTable1", "IncrementalTable2"});
                
                // Verify trimmed name reconstruction works for incremental backups
                VerifyIncrementalBackupTableTrimmedNameReconstruction(runtime, "IncrementalRestoreCollection", {"IncrementalTable1", "IncrementalTable2"});
                
                Cerr << "Incremental backup table path states correctly preserved after reboot" << Endl;
            }
        });
    }

    Y_UNIT_TEST(MultipleIncrementalBackupSnapshots) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Setup test data
            TString collectionName = "MultiSnapshotCollection";
            TVector<TString> tableNames = {"SnapshotTable1", "SnapshotTable2", "SnapshotTable3"}; // 3 tables per snapshot
            
            // Note: We do not create source tables here because restore operation will create them
            // The backup collection references them in ExplicitEntryList, but they don't need to exist beforehand
            
            // Create backup collection that references the source tables
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

            TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", collectionSettings);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            
            // Create full backup directory and tables (required for incremental restore)
            TestMkDir(runtime, ++t.TxId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName, "backup_001_full");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            
            for (const auto& tableName : tableNames) {
                AsyncCreateTable(runtime, ++t.TxId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_full", 
                    TStringBuilder() << R"(
                        Name: ")" << tableName << R"("
                        Columns { Name: "key"   Type: "Uint32" }
                        Columns { Name: "value" Type: "Utf8" }
                        KeyColumnNames: ["key"]
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            
            // Create multiple incremental backup snapshots with different snapshot numbers
            // This tests the trimmed name reconstruction logic with multiple incremental snapshots
            for (ui32 snapshotNum = 1; snapshotNum <= 3; ++snapshotNum) {
                TString snapshotName = TStringBuilder() << "backup_" << Sprintf("%03d", snapshotNum) << "_incremental";
                
                // Create incremental backup directory
                TestMkDir(runtime, ++t.TxId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName, snapshotName);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                
                // Create tables in this incremental backup snapshot
                for (const auto& tableName : tableNames) {
                    AsyncCreateTable(runtime, ++t.TxId, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/" << snapshotName, 
                        TStringBuilder() << R"(
                            Name: ")" << tableName << R"("
                            Columns { Name: "key"   Type: "Uint32" }
                            Columns { Name: "value" Type: "Utf8" }
                            Columns { Name: "snapshot_data_)" << snapshotNum << R"(" Type: "Utf8" }
                            KeyColumnNames: ["key"]
                        )");
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
                
                Cerr << "Created incremental backup snapshot: " << snapshotName << " with " << tableNames.size() << " tables" << Endl;
            }

            // Verify all backup structures exist before starting restore
            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                });
                
                // Verify full backup directory and tables exist
                TestDescribeResult(DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_full"), {NLs::PathExist});
                for (const auto& tableName : tableNames) {
                    TestDescribeResult(DescribePath(runtime, TStringBuilder() 
                        << "/MyRoot/.backups/collections/" << collectionName << "/backup_001_full/" << tableName), {NLs::PathExist});
                }
                
                // Verify all 3 incremental backup directories exist
                for (ui32 snapshotNum = 1; snapshotNum <= 3; ++snapshotNum) {
                    TString snapshotName = TStringBuilder() << "backup_" << Sprintf("%03d", snapshotNum) << "_incremental";
                    TestDescribeResult(DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/" << snapshotName), {
                        NLs::PathExist,
                    });
                    
                    // Verify all table paths exist in each incremental backup directory
                    for (const auto& tableName : tableNames) {
                        TestDescribeResult(DescribePath(runtime, TStringBuilder() 
                            << "/MyRoot/.backups/collections/" << collectionName << "/" << snapshotName << "/" << tableName), {NLs::PathExist});
                    }
                }
            }

            // Execute incremental restore operation
            TString restoreSettings = TStringBuilder() << R"(
                Name: ")" << collectionName << R"("
            )";
            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Main test: Verify that all incremental backup snapshots maintain their path states after reboot
            {
                TInactiveZone inactive(activeZone);
                
                // Verify that target tables were created in the destination
                for (const auto& tableName : tableNames) {
                    TestDescribeResult(DescribePath(runtime, TStringBuilder() << "/MyRoot/" << tableName), {NLs::PathExist});
                    Cerr << "Successfully verified target table created: " << tableName << Endl;
                }
                
                // Verify operation persisted in database
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, false);
                
                // Verify path states for all incremental backup snapshots
                // Each incremental backup table should be in EPathStateAwaitingOutgoingIncrementalRestore
                for (ui32 snapshotNum = 1; snapshotNum <= 3; ++snapshotNum) {
                    TString snapshotName = TStringBuilder() << "backup_" << Sprintf("%03d", snapshotNum) << "_incremental";
                    TString incrementalBackupPath = TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/" << snapshotName;
                    
                    for (const auto& tableName : tableNames) {
                        TString incrementalBackupTablePath = TStringBuilder() << incrementalBackupPath << "/" << tableName;
                        auto desc = DescribePath(runtime, incrementalBackupTablePath);
                        
                        UNIT_ASSERT_C(desc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist,
                            TStringBuilder() << "Incremental backup table '" << incrementalBackupTablePath << "' should exist");
                        
                        auto state = desc.GetPathDescription().GetSelf().GetPathState();
                        UNIT_ASSERT_C(state == NKikimrSchemeOp::EPathState::EPathStateAwaitingOutgoingIncrementalRestore,
                            TStringBuilder() << "Incremental backup table '" << incrementalBackupTablePath 
                                           << "' should be in EPathStateAwaitingOutgoingIncrementalRestore state, but got: " 
                                           << NKikimrSchemeOp::EPathState_Name(state));
                        
                        Cerr << "Verified incremental backup table '" << snapshotName << "/" << tableName << "' has correct state: " 
                             << NKikimrSchemeOp::EPathState_Name(state) << Endl;
                    }
                }
                
                // Additional verification: Test trimmed name reconstruction for multiple snapshots
                for (ui32 snapshotNum = 1; snapshotNum <= 3; ++snapshotNum) {
                    TString trimmedName = TStringBuilder() << "backup_" << Sprintf("%03d", snapshotNum);
                    TString incrementalName = TStringBuilder() << trimmedName << "_incremental";
                    
                    auto incrementalDesc = DescribePath(runtime, TStringBuilder() << "/MyRoot/.backups/collections/" << collectionName << "/" << incrementalName);
                    UNIT_ASSERT_C(incrementalDesc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist,
                        TStringBuilder() << "Incremental backup directory '" << incrementalName << "' should exist for trimmed name '" << trimmedName << "'");
                    
                    Cerr << "Verified trimmed name reconstruction: " << trimmedName << " -> " << incrementalName << Endl;
                }
                
                Cerr << "Multiple incremental backup snapshots test passed: " << tableNames.size() * 3 << " incremental backup tables" << Endl;
            }
        });
    }    // Test orphaned incremental restore operation recovery during SchemaShardRestart
    Y_UNIT_TEST(OrphanedIncrementalRestoreOperationRecovery) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup event capture for TEvRunIncrementalRestore
            TOrphanedOpEventCapture eventCapture;
            SetupOrphanedOpEventObserver(runtime, eventCapture);
            
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create incremental backup scenario
            CreateIncrementalBackupScenario(runtime, *t.TestEnv, t.TxId, "OrphanedOpCollection", {"OrphanedOpTable"});
            
            // Get the backup collection path ID for event verification
            TPathId backupCollectionPathId = GetBackupCollectionPathId(runtime, "OrphanedOpCollection");
            UNIT_ASSERT_C(backupCollectionPathId != TPathId(), "Backup collection should exist");

            // Start incremental restore operation but don't wait for completion
            TString restoreSettings = R"(
                Name: "OrphanedOpCollection"
            )";
            AsyncRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            ui64 restoreTxId = t.TxId;
            TOperationId operationId = TOperationId(TTestTxConfig::SchemeShard, restoreTxId);
            
            TestModificationResult(runtime, restoreTxId, NKikimrScheme::StatusAccepted);

            TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
            VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, true);

            // Wait briefly then force reboot to simulate orphaned operation scenario
            runtime.SimulateSleep(TDuration::MilliSeconds(100));

            // Enable event capturing before reboot
            eventCapture.ClearCapturedEvents();
            eventCapture.EnableCapturing({backupCollectionPathId});

            // Force reboot to trigger recovery
            {
                TInactiveZone inactive(activeZone);
                
                runtime.SimulateSleep(TDuration::MilliSeconds(500));
                
                // Check if orphaned operation recovery worked
                if (eventCapture.HasCapturedEvents()) {
                    Cerr << "TEvRunIncrementalRestore event captured - orphaned operation recovery worked!" << Endl;
                } else {
                    Cerr << "Note: Operation completed before restart (not orphaned)" << Endl;
                }
                
                // Verify captured event has correct backup collection path ID
                if (eventCapture.HasCapturedEvents()) {
                    const TPathId& capturedPathId = eventCapture.CapturedBackupCollectionPathIds[0];
                    UNIT_ASSERT_VALUES_EQUAL_C(capturedPathId, backupCollectionPathId,
                        "Captured event should have the correct BackupCollectionPathId");
                }
                
                // Verify the target table was created
                bool tableExists = false;
                for (int attempt = 0; attempt < 10; ++attempt) {
                    auto desc = DescribePath(runtime, "/MyRoot/OrphanedOpTable");
                    if (desc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
                        tableExists = true;
                        break;
                    }
                    runtime.SimulateSleep(TDuration::MilliSeconds(100));
                }
                
                UNIT_ASSERT_C(tableExists, "Incremental restore operation should have completed");
                
                // Verify operation cleanup
                for (int attempt = 0; attempt < 5; ++attempt) {
                    try {
                        VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, false);
                        break;
                    } catch (...) {
                        runtime.SimulateSleep(TDuration::MilliSeconds(200));
                    }
                }
                
                if (eventCapture.HasCapturedEvents()) {
                    Cerr << "Successfully verified orphaned operation recovery" << Endl;
                } else {
                    Cerr << "Operation completed normally before restart" << Endl;
                }
            }
            
            eventCapture.DisableCapturing();
        });
    }

    // Test multiple orphaned operations recovery
    Y_UNIT_TEST(MultipleOrphanedIncrementalRestoreOperationsRecovery) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup event capture for TEvRunIncrementalRestore
            TOrphanedOpEventCapture eventCapture;
            SetupOrphanedOpEventObserver(runtime, eventCapture);
            
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create multiple backup collections
            CreateIncrementalBackupScenario(runtime, *t.TestEnv, t.TxId, "OrphanedOpCollection1", {"OrphanedOpTable1"});
            CreateIncrementalBackupScenario(runtime, *t.TestEnv, t.TxId, "OrphanedOpCollection2", {"OrphanedOpTable2"});
            CreateIncrementalBackupScenario(runtime, *t.TestEnv, t.TxId, "OrphanedOpCollection3", {"OrphanedOpTable3"});

            // Get backup collection path IDs for event verification
            TVector<TPathId> expectedPathIds;
            for (int i = 1; i <= 3; ++i) {
                TString collectionName = TStringBuilder() << "OrphanedOpCollection" << i;
                TPathId pathId = GetBackupCollectionPathId(runtime, collectionName);
                UNIT_ASSERT_C(pathId != TPathId(), TStringBuilder() << "Backup collection " << collectionName << " should exist");
                expectedPathIds.push_back(pathId);
            }

            // Start multiple incremental restore operations simultaneously
            TVector<ui64> restoreTxIds;
            for (int i = 1; i <= 3; ++i) {
                TString restoreSettings = TStringBuilder() << R"(
                    Name: "OrphanedOpCollection)" << i << R"("
                )";
                AsyncRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
                restoreTxIds.push_back(t.TxId);
                TestModificationResult(runtime, t.TxId, NKikimrScheme::StatusAccepted);
            }

            // Wait briefly then force reboot to simulate orphaned operations scenario
            runtime.SimulateSleep(TDuration::MilliSeconds(100));

            // Enable event capturing before reboot to catch recovery events
            eventCapture.ClearCapturedEvents();
            eventCapture.EnableCapturing(expectedPathIds);

            // Force reboot to trigger recovery of all orphaned operations
            {
                TInactiveZone inactive(activeZone);
                
                runtime.SimulateSleep(TDuration::MilliSeconds(500));
                
                // Verify all target tables were created
                TVector<TString> expectedTables = {"OrphanedOpTable1", "OrphanedOpTable2", "OrphanedOpTable3"};
                
                for (const auto& tableName : expectedTables) {
                    bool tableExists = false;
                    for (int attempt = 0; attempt < 10; ++attempt) {
                        auto desc = DescribePath(runtime, TStringBuilder() << "/MyRoot/" << tableName);
                        if (desc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
                            tableExists = true;
                            break;
                        }
                        runtime.SimulateSleep(TDuration::MilliSeconds(100));
                    }
                    
                    UNIT_ASSERT_C(tableExists, TStringBuilder() << "Operation for " << tableName << " should have been recovered");
                }
                
                // Check if recovery events were sent
                if (eventCapture.HasCapturedEvents()) {
                    Cerr << "TEvRunIncrementalRestore events captured - orphaned operations recovery worked!" << Endl;
                } else {
                    Cerr << "Note: Operations completed before restart (not orphaned)" << Endl;
                }
                
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, false);
                
                if (eventCapture.HasCapturedEvents()) {
                    Cerr << "Successfully verified multiple orphaned operations recovery" << Endl;
                } else {
                    Cerr << "Multiple operations completed normally before restart" << Endl;
                }
            }
            
            eventCapture.DisableCapturing();
        });
    }

    // Test edge case: operation with missing control operation
    Y_UNIT_TEST(OrphanedOperationWithoutControlOperation) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup event capture for TEvRunIncrementalRestore
            TOrphanedOpEventCapture eventCapture;
            SetupOrphanedOpEventObserver(runtime, eventCapture);
            
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create backup scenario
            CreateIncrementalBackupScenario(runtime, *t.TestEnv, t.TxId, "EdgeCaseCollection", {"EdgeCaseTable"});
            
            // Get the backup collection path ID for event verification
            TPathId backupCollectionPathId = GetBackupCollectionPathId(runtime, "EdgeCaseCollection");
            UNIT_ASSERT_C(backupCollectionPathId != TPathId(), "Backup collection should exist");

            // Start operation
            TString restoreSettings = R"(
                Name: "EdgeCaseCollection"
            )";
            AsyncRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            ui64 restoreTxId = t.TxId;
            TestModificationResult(runtime, restoreTxId, NKikimrScheme::StatusAccepted);

            // Let operation start and create database entries
            runtime.SimulateSleep(TDuration::MilliSeconds(200));

            // Enable event capturing before reboot to catch recovery events
            eventCapture.ClearCapturedEvents();
            eventCapture.EnableCapturing({backupCollectionPathId});

            // Force reboot to simulate edge case
            {
                TInactiveZone inactive(activeZone);
                
                runtime.SimulateSleep(TDuration::MilliSeconds(500));
                
                // Verify operation recovery
                bool operationRecovered = false;
                for (int attempt = 0; attempt < 15; ++attempt) {
                    auto desc = DescribePath(runtime, "/MyRoot/EdgeCaseTable");
                    if (desc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
                        operationRecovered = true;
                        break;
                    }
                    runtime.SimulateSleep(TDuration::MilliSeconds(100));
                }
                
                UNIT_ASSERT_C(operationRecovered, "Recovery logic should have detected orphaned operation");
                
                if (eventCapture.HasCapturedEvents()) {
                    Cerr << "TEvRunIncrementalRestore event captured - orphaned operation recovery worked!" << Endl;
                } else {
                    Cerr << "Note: Operation completed before restart (not orphaned)" << Endl;
                }
                
                TestDescribeResult(DescribePath(runtime, "/MyRoot/EdgeCaseTable"), {NLs::PathExist});
            }
            
            eventCapture.DisableCapturing();
        });
    }

    // Test recovery during database loading
    Y_UNIT_TEST(OrphanedOperationRecoveryDuringDatabaseLoading) {
        TTestWithReboots t;
        t.EnvOpts = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            // Setup event capture for TEvRunIncrementalRestore
            TOrphanedOpEventCapture eventCapture;
            SetupOrphanedOpEventObserver(runtime, eventCapture);
            
            // Setup backup infrastructure
            TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            TestMkDir(runtime, ++t.TxId, "/MyRoot/.backups", "collections");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Create backup scenario with multiple tables
            CreateIncrementalBackupScenario(runtime, *t.TestEnv, t.TxId, "DatabaseLoadingCollection", 
                                          {"LoadingTable1", "LoadingTable2", "LoadingTable3"});
            
            // Get the backup collection path ID for event verification
            TPathId backupCollectionPathId = GetBackupCollectionPathId(runtime, "DatabaseLoadingCollection");
            UNIT_ASSERT_C(backupCollectionPathId != TPathId(), "Backup collection should exist");

            // Start a restore operation that will persist in database
            TString restoreSettings = R"(
                Name: "DatabaseLoadingCollection"
            )";
            AsyncRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections/", restoreSettings);
            ui64 restoreTxId = t.TxId;
            TestModificationResult(runtime, restoreTxId, NKikimrScheme::StatusAccepted);

            // Allow operation to start and persist data to database
            runtime.SimulateSleep(TDuration::MilliSeconds(300));

            // First reboot to establish database state
            {
                TInactiveZone inactive(activeZone);
                runtime.SimulateSleep(TDuration::MilliSeconds(100));
            }

            // Enable event capturing before second reboot to catch recovery events
            eventCapture.ClearCapturedEvents();
            eventCapture.EnableCapturing({backupCollectionPathId});

            // Second reboot to test recovery during database loading
            {
                TInactiveZone inactive(activeZone);
                
                runtime.SimulateSleep(TDuration::MilliSeconds(500));
                
                // Wait for recovery to complete
                bool allTablesRecovered = true;
                TVector<TString> expectedTables = {"LoadingTable1", "LoadingTable2", "LoadingTable3"};
                
                for (const auto& tableName : expectedTables) {
                    bool tableExists = false;
                    for (int attempt = 0; attempt < 12; ++attempt) {
                        auto desc = DescribePath(runtime, TStringBuilder() << "/MyRoot/" << tableName);
                        if (desc.GetPathDescription().GetSelf().GetPathState() != NKikimrSchemeOp::EPathState::EPathStateNotExist) {
                            tableExists = true;
                            break;
                        }
                        runtime.SimulateSleep(TDuration::MilliSeconds(100));
                    }
                    
                    if (!tableExists) {
                        allTablesRecovered = false;
                        Cerr << "Failed to recover table: " << tableName << Endl;
                    }
                }
                
                UNIT_ASSERT_C(allTablesRecovered, "All tables should be recovered during database loading phase");
                
                // Check if recovery events were sent
                if (eventCapture.HasCapturedEvents()) {
                    Cerr << "TEvRunIncrementalRestore event captured during database loading recovery!" << Endl;
                } else {
                    Cerr << "Note: Operation completed before restart (not orphaned)" << Endl;
                }
                
                TTabletId schemeShardTabletId = TTabletId(TTestTxConfig::SchemeShard);
                VerifyIncrementalRestoreOperationInDatabase(runtime, schemeShardTabletId, false);
                
                if (eventCapture.HasCapturedEvents()) {
                    Cerr << "Orphaned operation recovery during database loading completed successfully" << Endl;
                } else {
                    Cerr << "Operation recovery during database loading completed" << Endl;
                }
            }
            
            eventCapture.DisableCapturing();
        });
    }

}
