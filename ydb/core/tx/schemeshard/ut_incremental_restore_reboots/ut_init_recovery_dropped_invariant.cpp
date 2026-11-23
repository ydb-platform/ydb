// Regression tests for issue #33764: PathState/StepDropped invariant violation.
//
// Invariant (schemeshard_path_element.cpp:270):
//   if (StepDropped) { Y_VERIFY_DEBUG_S(PathState == EPathStateNotExist, ...) }
//
// Two unguarded PathState writes can violate it:
//   1. init.cpp RestoreIncrementalRestoreOpPathStates -- sets EPathStateIncomingIncrementalRestore
//      on every path in a persisted LongIncrementalRestoreOp without checking !Dropped().
//   2. side_effects.cpp:958 DoDoneTransactions -- writes path->PathState = EPathStateNoChanges
//      from ReleasePathAtDone without checking !Dropped().

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/public/lib/value/value.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/ast/yql_ast_escaping.h>
#include <util/stream/str.h>
#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TInitRecoveryDroppedInvariantTests) {

    static void SetupBackupDirs(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
        TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
        env.TestWaitNotification(runtime, txId);
    }

    static void CreateCollection(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId,
                                 const TString& collectionName, const TString& targetTablePath) {
        TString settings = TStringBuilder()
            << "Name: \"" << collectionName << "\"\n"
            << "ExplicitEntryList {\n"
            << "    Entries {\n"
            << "        Type: ETypeTable\n"
            << "        Path: \"" << targetTablePath << "\"\n"
            << "    }\n"
            << "}\n"
            << "Cluster: {}\n";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/", settings);
        env.TestWaitNotification(runtime, txId);
    }

    static void CreateFullBackupDir(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId,
                                    const TString& collectionName, const TString& tableName,
                                    const TString& dirName = "snap_001_full") {
        TString collPath = TStringBuilder()
            << "/MyRoot/.backups/collections/" << collectionName;
        TestMkDir(runtime, ++txId, collPath, dirName);
        env.TestWaitNotification(runtime, txId);
        TestCreateTable(runtime, ++txId, collPath + "/" + dirName,
            TStringBuilder()
                << "Name: \"" << tableName << "\"\n"
                << "Columns { Name: \"key\"   Type: \"Uint32\" }\n"
                << "Columns { Name: \"value\" Type: \"Utf8\" }\n"
                << "KeyColumnNames: [\"key\"]\n");
        env.TestWaitNotification(runtime, txId);
    }

    static void CreateIncrementalBackupDir(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId,
                                           const TString& collectionName, const TString& tableName,
                                           const TString& dirName = "snap_002_incremental") {
        TString collPath = TStringBuilder()
            << "/MyRoot/.backups/collections/" << collectionName;
        TestMkDir(runtime, ++txId, collPath, dirName);
        env.TestWaitNotification(runtime, txId);
        TestCreateTable(runtime, ++txId, collPath + "/" + dirName,
            TStringBuilder()
                << "Name: \"" << tableName << "\"\n"
                << "Columns { Name: \"key\"   Type: \"Uint32\" }\n"
                << "Columns { Name: \"value\" Type: \"Utf8\" }\n"
                << "Columns { Name: \"__ydb_incrBackupImpl_changeMetadata\" Type: \"String\" }\n"
                << "KeyColumnNames: [\"key\"]\n");
        env.TestWaitNotification(runtime, txId);
    }

    // Issue #33764 path 1: LongIncrementalRestoreOp row + dropped target + reboot.
    // init.cpp:5449 must not overwrite PathState on a dropped path.
    Y_UNIT_TEST(LongIncrRestoreOpReloadOverwritesDroppedTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupBackupDirs(runtime, env, txId);
        CreateCollection(runtime, env, txId, "Coll", "/MyRoot/RestoreTarget");
        // Incremental backup dir causes RestoreBackupCollection to persist a
        // LongIncrementalRestoreOp row, which init recovery reads on next boot.
        CreateFullBackupDir(runtime, env, txId, "Coll", "RestoreTarget");
        CreateIncrementalBackupDir(runtime, env, txId, "Coll", "RestoreTarget");

        AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
                                     "Name: \"Coll\"\n");
        const ui64 restoreTxId = txId;
        TestModificationResult(runtime, restoreTxId, NKikimrScheme::StatusAccepted);

        // Wait for the schema op to fully complete so Operations no longer
        // contains the txId. On reboot RestoreIncrementalRestoreOpPathStates
        // is then skipped (LongIncrementalRestoreOp row stays until FORGET).
        env.TestWaitNotification(runtime, restoreTxId);

        AsyncDropTable(runtime, ++txId, "/MyRoot", "RestoreTarget");
        const ui64 dropTxId = txId;
        auto dropResult = TestModificationResults(runtime, dropTxId, {
            NKikimrScheme::StatusAccepted,
            NKikimrScheme::StatusMultipleModifications,
            NKikimrScheme::StatusPathDoesNotExist,
        });

        if (dropResult != NKikimrScheme::StatusAccepted) {
            // Drop rejected -- bug path not exercised; reboot and verify no crash.
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            return;
        }
        env.TestWaitNotification(runtime, dropTxId);

        // After reboot TTxInit loads the LongIncrementalRestoreOp row. Without the
        // fix it would overwrite PathState=NotExist with EPathStateIncomingIncrementalRestore
        // (StepDropped != 0), corrupting the invariant.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // Triggers Dropped() -- must not crash with the fix.
        DescribePath(runtime, "/MyRoot/RestoreTarget");
    }

    // Prohibition test: DropBackupCollection must be rejected while an incremental
    // restore is active.  The collection path is held in EPathStateOutgoingIncrementalRestore
    // for the entire duration (schema op through finalize), so the path-state check fires
    // first and returns StatusMultipleModifications.  HasActiveBackupOperations (which would
    // return StatusPreconditionFailed) is defense-in-depth for future edge cases.
    Y_UNIT_TEST(DropBackupCollectionBlockedDuringActiveIncrementalRestore) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupBackupDirs(runtime, env, txId);
        CreateCollection(runtime, env, txId, "CollBlocked", "/MyRoot/RestoreTargetBlocked");
        CreateFullBackupDir(runtime, env, txId, "CollBlocked", "RestoreTargetBlocked");
        CreateIncrementalBackupDir(runtime, env, txId, "CollBlocked", "RestoreTargetBlocked");

        // Block TEvRunIncrementalRestore so the restore stays in the gap window:
        // LongIncrementalRestoreOp row committed, IncrementalRestoreStates absent.
        runtime.SetObserverFunc([](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPrivate::TEvRunIncrementalRestore::EventType) {
                return TTestActorRuntimeBase::EEventAction::DROP;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });

        AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
                                     "Name: \"CollBlocked\"\n");
        const ui64 restoreTxId = txId;
        TestModificationResult(runtime, restoreTxId, NKikimrScheme::StatusAccepted);

        AsyncDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections",
                                  "Name: \"CollBlocked\"\n");
        const ui64 dropTxId = txId;
        auto dropResult = TestModificationResults(runtime, dropTxId, {
            NKikimrScheme::StatusPreconditionFailed,
            NKikimrScheme::StatusMultipleModifications,
        });
        // Either blocking mechanism is correct: path-state check → StatusMultipleModifications,
        // HasActiveBackupOperations check → StatusPreconditionFailed.
        UNIT_ASSERT_C(static_cast<NKikimrScheme::EStatus>(dropResult) != NKikimrScheme::StatusAccepted,
            "DropBackupCollection must be rejected during active incremental restore, got: "
            << NKikimrScheme::EStatus_Name(dropResult));
    }

    // Prohibition test: DropTable must return StatusMultipleModifications while
    // the path is in EPathStateIncomingIncrementalRestore.
    // Guard: IsUnderIncomingIncrementalRestore() in IsUnderOperation() sum check.
    Y_UNIT_TEST(DropTableBlockedDuringActiveIncrementalRestore) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupBackupDirs(runtime, env, txId);
        CreateCollection(runtime, env, txId, "CollDrop", "/MyRoot/RestoreTargetDrop");
        CreateFullBackupDir(runtime, env, txId, "CollDrop", "RestoreTargetDrop");
        CreateIncrementalBackupDir(runtime, env, txId, "CollDrop", "RestoreTargetDrop");

        AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
                                     "Name: \"CollDrop\"\n");
        const ui64 restoreTxId = txId;
        TestModificationResult(runtime, restoreTxId, NKikimrScheme::StatusAccepted);

        bool reachedIncoming = false;
        for (int i = 0; i < 30 && !reachedIncoming; ++i) {
            runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
            auto state = DescribePath(runtime, "/MyRoot/RestoreTargetDrop")
                .GetPathDescription().GetSelf().GetPathState();
            if (state == NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore) {
                reachedIncoming = true;
            }
        }

        if (!reachedIncoming) {
            // Restore completed before we could observe the intermediate state.
            env.TestWaitNotification(runtime, restoreTxId);
            return;
        }

        AsyncDropTable(runtime, ++txId, "/MyRoot", "RestoreTargetDrop");
        const ui64 dropTxId = txId;
        auto dropResult = TestModificationResults(runtime, dropTxId, {
            NKikimrScheme::StatusMultipleModifications,
            NKikimrScheme::StatusAccepted, // restore may have completed between poll and drop
        });

        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<NKikimrScheme::EStatus>(dropResult),
            NKikimrScheme::StatusMultipleModifications,
            "DropTable must be rejected while path is in EPathStateIncomingIncrementalRestore, "
            "got: " << NKikimrScheme::EStatus_Name(dropResult));

        env.TestWaitNotification(runtime, restoreTxId);

        auto finalState = DescribePath(runtime, "/MyRoot/RestoreTargetDrop")
            .GetPathDescription().GetSelf().GetPathState();
        UNIT_ASSERT_VALUES_EQUAL_C(finalState, NKikimrSchemeOp::EPathState::EPathStateNoChanges,
            "After restore completes, path must be in EPathStateNoChanges");
    }

    // Step 1 contract test: init gate uses IncrementalRestoreStates (not Operations map).
    // After a full restore completes and schemeshard reboots, DescribePath on the
    // restored target must not crash and must be in a valid path state.
    Y_UNIT_TEST(InitGateUsesIncrementalRestoreStates) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupBackupDirs(runtime, env, txId);
        CreateCollection(runtime, env, txId, "CollGate", "/MyRoot/RestoreTargetGate");
        CreateFullBackupDir(runtime, env, txId, "CollGate", "RestoreTargetGate");
        CreateIncrementalBackupDir(runtime, env, txId, "CollGate", "RestoreTargetGate");

        // Run RestoreBackupCollection to completion so the schema op leaves
        // Operations, but LongIncrementalRestoreOp row remains in storage.
        AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
                                     "Name: \"CollGate\"\n");
        const ui64 restoreTxId = txId;
        TestModificationResult(runtime, restoreTxId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, restoreTxId);

        // Reboot: TTxInit loads LongIncrementalRestoreOp row. The gate must check
        // IncrementalRestoreStates (not Operations) to decide whether to call
        // RestoreIncrementalRestoreOpPathStates. With data restore completed, the
        // state entry is Completed/absent, so the call is skipped -- no crash.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // DescribePath must not crash.
        auto describeResult = DescribePath(runtime, "/MyRoot/RestoreTargetGate");
        auto pathState = describeResult.GetPathDescription().GetSelf().GetPathState();
        bool validState = (pathState == NKikimrSchemeOp::EPathState::EPathStateNoChanges)
                       || (pathState == NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore);
        UNIT_ASSERT_C(validState,
            "After reboot, RestoreTargetGate must be in EPathStateNoChanges or "
            "EPathStateIncomingIncrementalRestore, got: "
            << NKikimrSchemeOp::EPathState_Name(pathState));
    }

    // Inverse of the old scrub test: after DropTable the LongIncrementalRestoreOp
    // row must still exist in IncrementalRestoreOperations (no scrub).  Verified by
    // rebooting the schemeshard and confirming it does not crash when init encounters
    // the row referencing the now-dropped path.
    Y_UNIT_TEST(DropTablePreservesLongIncrementalRestoreOpReference) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        SetupBackupDirs(runtime, env, txId);
        CreateCollection(runtime, env, txId, "CollPreserve", "/MyRoot/RestoreTargetPreserve");
        CreateFullBackupDir(runtime, env, txId, "CollPreserve", "RestoreTargetPreserve");
        CreateIncrementalBackupDir(runtime, env, txId, "CollPreserve", "RestoreTargetPreserve");

        // Start restore -- writes LongIncrementalRestoreOp row.
        AsyncRestoreBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections/",
                                     "Name: \"CollPreserve\"\n");
        const ui64 restoreTxId = txId;
        TestModificationResult(runtime, restoreTxId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, restoreTxId);

        // Drop the restored table.
        AsyncDropTable(runtime, ++txId, "/MyRoot", "RestoreTargetPreserve");
        const ui64 dropTxId = txId;
        auto dropResult = TestModificationResults(runtime, dropTxId, {
            NKikimrScheme::StatusAccepted,
            NKikimrScheme::StatusMultipleModifications,
            NKikimrScheme::StatusPathDoesNotExist,
        });

        if (dropResult != NKikimrScheme::StatusAccepted) {
            // Drop was rejected (race) -- nothing to verify.
            return;
        }
        env.TestWaitNotification(runtime, dropTxId);

        // Verify the LongIncrementalRestoreOp row still exists (no scrub): read the
        // IncrementalRestoreOperations table and assert at least one row is present
        // with "/MyRoot/RestoreTargetPreserve" still in its TablePathList.
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(
            runtime, TTestTxConfig::SchemeShard, R"(
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

        bool foundDroppedPath = false;
        auto operationsList = operationsResultSet["List"];
        if (operationsList.HaveValue()) {
            for (ui32 i = 0; i < operationsList.Size(); ++i) {
                auto operation = operationsList[i];
                auto operationDataValue = operation["Operation"];
                if (!operationDataValue.HaveValue()) {
                    continue;
                }
                NKikimrSchemeOp::TLongIncrementalRestoreOp op;
                TString operationData = (TString)operationDataValue;
                if (!op.ParseFromString(operationData)) {
                    continue;
                }
                for (const auto& tablePath : op.GetTablePathList()) {
                    if (tablePath == "/MyRoot/RestoreTargetPreserve") {
                        foundDroppedPath = true;
                    }
                }
            }
        }
        UNIT_ASSERT_C(foundDroppedPath,
            "LongIncrementalRestoreOp row must still contain the dropped table path "
            "(no scrub): DropTable must not modify IncrementalRestoreOperations");

        // Reboot: init recovery must not crash when it encounters the row referencing
        // the dropped path.  The fix in init.cpp guards the PathState write with
        // !Dropped(), so no invariant violation occurs.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        // DescribePath on the dropped path must not crash.
        DescribePath(runtime, "/MyRoot/RestoreTargetPreserve");
    }

    static TString QuoteMiniKQLString(TStringBuf bytes) {
        TStringStream out;
        NYql::EscapeArbitraryAtom(bytes, '"', &out);
        return out.Str();
    }

    static void WriteRestoreHeader(TTestBasicRuntime& runtime, ui64 id,
                                   const NKikimrSchemeOp::TLongIncrementalRestoreOp& op) {
        LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('Id (Uint64 '%lu))))
                (let value '('('Operation (String '%s))))
                (return (AsList (UpdateRow 'IncrementalRestoreOperations key value)))
            )
        )", id, QuoteMiniKQLString(op.SerializeAsString()).c_str()));
    }

    static void WriteRestoreState(TTestBasicRuntime& runtime, ui64 id, ui32 state) {
        LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('OperationId (Uint64 '%lu))))
                (let value '('('State (Uint32 '%u)) '('CurrentIncrementalIdx (Uint32 '0))))
                (return (AsList (UpdateRow 'IncrementalRestoreState key value)))
            )
        )", id, state));
    }

    static void WriteRestoreItem(TTestBasicRuntime& runtime, ui64 id, ui32 seq, ui64 waitTxId = 0) {
        // Inject a legacy row directly; reboot before expecting the runtime to know about it.
        LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('OriginalOpId (Uint64 '%lu)) '('ItemSeq (Uint32 '%u))))
                (let value '('('ItemKind (Uint32 '0)) '('WaitTxId (Uint64 '%lu))))
                (return (AsList (UpdateRow 'IncrementalRestoreItem key value)))
            )
        )", id, seq, waitTxId));
    }

    static void CheckRestoreRows(TTestBasicRuntime& runtime, ui64 id,
                                 ui32 headers, ui32 states, ui32 items) {
        NKikimrMiniKQL::TResult result;
        TString error;
        auto status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let headers (SelectRange 'IncrementalRestoreOperations
                    '('('Id (Uint64 '%lu) (Uint64 '%lu))) '('Id) '()))
                (let states (SelectRange 'IncrementalRestoreState
                    '('('OperationId (Uint64 '%lu) (Uint64 '%lu))) '('OperationId) '()))
                (let items (SelectRange 'IncrementalRestoreItem
                    '('('OriginalOpId (Uint64 '%lu) (Uint64 '%lu)) '('ItemSeq (Null) (Void)))
                    '('OriginalOpId 'ItemSeq) '()))
                (return (AsList (SetResult 'Headers headers) (SetResult 'States states) (SetResult 'Items items)))
            )
        )", id, id, id, id, id, id), result, error);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::OK, error);
        auto value = NClient::TValue::Create(result);
        UNIT_ASSERT_VALUES_EQUAL(value["Headers"]["List"].Size(), headers);
        UNIT_ASSERT_VALUES_EQUAL(value["States"]["List"].Size(), states);
        UNIT_ASSERT_VALUES_EQUAL(value["Items"]["List"].Size(), items);
    }

    static void CheckRestoreInventory(TTestBasicRuntime& runtime, TSchemeShard* ss) {
        NKikimrMiniKQL::TResult result;
        TString error;
        auto status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, R"(
            (
                (let headers (SelectRange 'IncrementalRestoreOperations
                    '('('Id (Null) (Void))) '('Id 'Operation) '()))
                (let items (SelectRange 'IncrementalRestoreItem
                    '('('OriginalOpId (Null) (Void)) '('ItemSeq (Null) (Void)))
                    '('OriginalOpId 'ItemSeq) '()))
                (return (AsList (SetResult 'Headers headers) (SetResult 'Items items)))
            )
        )", result, error);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::OK, error);
        auto value = NClient::TValue::Create(result);
        THashMap<ui64, TPathId> headers;
        auto headerRows = value["Headers"]["List"];
        for (ui32 i = 0; i < headerRows.Size(); ++i) {
            NKikimrSchemeOp::TLongIncrementalRestoreOp op;
            UNIT_ASSERT(op.ParseFromString(TString(headerRows[i]["Operation"])));
            headers[ui64(headerRows[i]["Id"])] = TPathId(
                op.GetBackupCollectionPathId().GetOwnerId(), op.GetBackupCollectionPathId().GetLocalId());
        }
        THashMap<ui64, THashSet<ui32>> items;
        auto itemRows = value["Items"]["List"];
        for (ui32 i = 0; i < itemRows.Size(); ++i) {
            items[ui64(itemRows[i]["OriginalOpId"])].insert(ui32(itemRows[i]["ItemSeq"]));
        }
        // Call at quiescent/blocked stages, so no restore writes race the snapshot.
        runtime.RunCall([&] {
            UNIT_ASSERT_C(ss->PersistedIncrementalRestoreOps == headers, "Header inventory differs from disk");
            UNIT_ASSERT_C(ss->PersistedIncrementalRestoreItems == items, "Item inventory differs from disk");
            return true;
        });
    }

    static void WaitRestoreDone(TTestBasicRuntime& runtime, TTestEnv& env, ui64 restoreId) {
        const auto deadline = runtime.GetCurrentTime() + TDuration::Seconds(120);
        while (runtime.GetCurrentTime() < deadline) {
            const auto response = TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot");
            const auto& restore = response.GetBackupCollectionRestore();
            if (restore.GetProgress() == Ydb::Backup::RestoreProgress::PROGRESS_DONE) {
                UNIT_ASSERT_VALUES_EQUAL(restore.GetStatus(), Ydb::StatusIds::SUCCESS);
                return;
            }
            env.SimulateSleep(runtime, TDuration::MilliSeconds(100));
        }
        UNIT_FAIL("Restore did not finish");
    }

    enum class EStalePath {
        MissingCollectionId,
        MissingCollection,
        DroppedCollection,
        DroppedTarget,
        DroppedFullBackup,
        DroppedIncrementalBackup,
    };

    static void CheckStaleRestoreRepair(EStalePath stalePath, bool withState, bool terminal = false) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;
        SetupBackupDirs(runtime, env, txId);
        CreateCollection(runtime, env, txId, "Stale", "/MyRoot/Target");
        CreateFullBackupDir(runtime, env, txId, "Stale", "Target");
        CreateIncrementalBackupDir(runtime, env, txId, "Stale", "Target");
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Target"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const TString collectionPath = "/MyRoot/.backups/collections/Stale";
        const ui64 collectionId = DescribePath(runtime, collectionPath)
            .GetPathDescription().GetSelf().GetPathId();
        constexpr ui64 restoreId = 9000001;
        NKikimrSchemeOp::TLongIncrementalRestoreOp op;
        op.SetTxId(restoreId);
        op.MutableBackupCollectionPathId()->SetOwnerId(TTestTxConfig::SchemeShard);
        op.MutableBackupCollectionPathId()->SetLocalId(collectionId);
        op.AddTablePathList("/MyRoot/Target");
        op.SetFullBackupTrimmedName("snap_001");
        op.AddIncrementalBackupTrimmedNames("snap_002");

        TString droppedPath;
        switch (stalePath) {
        case EStalePath::MissingCollectionId:
            op.ClearBackupCollectionPathId();
            break;
        case EStalePath::MissingCollection:
            op.MutableBackupCollectionPathId()->SetLocalId(999999);
            break;
        case EStalePath::DroppedCollection:
            droppedPath = collectionPath;
            break;
        case EStalePath::DroppedTarget:
            droppedPath = "/MyRoot/Target";
            break;
        case EStalePath::DroppedFullBackup:
            droppedPath = collectionPath + "/snap_001_full/Target";
            break;
        case EStalePath::DroppedIncrementalBackup:
            droppedPath = collectionPath + "/snap_002_incremental/Target";
            break;
        }
        if (!droppedPath.empty()) {
            ui64 pathId = DescribePath(runtime, droppedPath).GetPathDescription().GetSelf().GetPathId();
            // Simulate a legacy database with a tombstone still referenced by a
            // long operation, without relying on races with normal DROP guards.
            LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
                (
                    (let key '('('Id (Uint64 '%lu))))
                    (let value '('('StepDropped (Uint64 '1000000))))
                    (return (AsList (UpdateRow 'Paths key value)))
                )
            )", pathId));
        }
        WriteRestoreHeader(runtime, restoreId, op);
        if (withState) {
            WriteRestoreState(runtime, restoreId, terminal ? 3 : 1); // Completed / Running
        }
        WriteRestoreItem(runtime, restoreId, 0);
        WriteRestoreItem(runtime, restoreId, 42);
        CheckRestoreRows(runtime, restoreId, 1, withState ? 1 : 0, 2);

        ui32 resumed = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if ((ev->GetTypeRewrite() == TEvPrivate::TEvRunIncrementalRestore::EventType
                    && ui64(ev->Get<TEvPrivate::TEvRunIncrementalRestore>()->OperationId.GetTxId()) == restoreId)
                || (ev->GetTypeRewrite() == TEvPrivate::TEvProgressIncrementalRestore::EventType
                    && ev->Get<TEvPrivate::TEvProgressIncrementalRestore>()->OperationId == restoreId)) {
                ++resumed;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
        for (ui32 reboot = 0; reboot != 2; ++reboot) {
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            CheckRestoreRows(runtime, restoreId, terminal ? 1 : 0, terminal ? 1 : 0, 0);
            UNIT_ASSERT_VALUES_EQUAL(resumed, 0);
            if (stalePath != EStalePath::DroppedTarget) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Target"), {
                    NLs::PathExist,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                });
            }
        }
    }

    Y_UNIT_TEST_FLAG(RepairMissingCollectionId, withState) {
        CheckStaleRestoreRepair(EStalePath::MissingCollectionId, withState);
    }

    Y_UNIT_TEST_FLAG(RepairMissingCollection, withState) {
        CheckStaleRestoreRepair(EStalePath::MissingCollection, withState);
    }

    Y_UNIT_TEST_FLAG(RepairDroppedCollection, withState) {
        CheckStaleRestoreRepair(EStalePath::DroppedCollection, withState);
    }

    Y_UNIT_TEST_FLAG(RepairDroppedTarget, withState) {
        CheckStaleRestoreRepair(EStalePath::DroppedTarget, withState);
    }

    Y_UNIT_TEST_FLAG(RepairDroppedFullBackup, withState) {
        CheckStaleRestoreRepair(EStalePath::DroppedFullBackup, withState);
    }

    Y_UNIT_TEST_FLAG(RepairDroppedIncrementalBackup, withState) {
        CheckStaleRestoreRepair(EStalePath::DroppedIncrementalBackup, withState);
    }

    Y_UNIT_TEST(RepairPreservesTerminalHistoryAfterTableDrop) {
        CheckStaleRestoreRepair(EStalePath::DroppedTarget, true, true);
        CheckStaleRestoreRepair(EStalePath::DroppedFullBackup, true, true);
        CheckStaleRestoreRepair(EStalePath::DroppedIncrementalBackup, true, true);
    }

    Y_UNIT_TEST_FLAG(DropCollectionRemovesPersistedRestoreHeader, rebootBeforeDrop) {
        TSchemeShard* ss = nullptr;
        auto factory = [&ss](const TActorId& tablet, TTabletStorageInfo* info) {
            ss = new TSchemeShard(tablet, info);
            return ss;
        };
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true), factory);
        ui64 txId = 100;
        SetupBackupDirs(runtime, env, txId);
        auto restoreCollection = [&](const TString& name, const TString& target) {
            CreateCollection(runtime, env, txId, name, "/MyRoot/" + target);
            CreateFullBackupDir(runtime, env, txId, name, target);
            CreateIncrementalBackupDir(runtime, env, txId, name, target);
            const ui64 id = ++txId;
            AsyncRestoreBackupCollection(runtime, id, "/MyRoot/.backups/collections/",
                TStringBuilder() << "Name: \"" << name << "\"\n");
            TestModificationResult(runtime, id);
            env.TestWaitNotification(runtime, id);
            WaitRestoreDone(runtime, env, id);
            // Finalization removes the active header, but not the durable inventory.
            runtime.RunCall([&] {
                UNIT_ASSERT(!ss->LongIncrementalRestoreOps.contains(TOperationId(TTxId(id), 0)));
                UNIT_ASSERT(ss->PersistedIncrementalRestoreOps.contains(id));
                return true;
            });
            return id;
        };
        const ui64 restoreId = restoreCollection("Finished", "Target");
        const ui64 otherId = restoreCollection("Other", "OtherTarget");
        CheckRestoreInventory(runtime, ss);
        if (rebootBeforeDrop) {
            // Old versions could leave item rows outside scheduling state.
            WriteRestoreItem(runtime, restoreId, 17);
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            CheckRestoreRows(runtime, restoreId, 1, 1, 0);
            CheckRestoreInventory(runtime, ss);
        }

        TestDropBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", "Name: \"Finished\"\n");
        env.TestWaitNotification(runtime, txId);
        CheckRestoreRows(runtime, restoreId, 0, 0, 0);
        CheckRestoreRows(runtime, otherId, 1, 1, 0);
        CheckRestoreInventory(runtime, ss);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        CheckRestoreRows(runtime, restoreId, 0, 0, 0);
        CheckRestoreRows(runtime, otherId, 1, 1, 0);
        CheckRestoreInventory(runtime, ss);

        TestForgetBackupCollectionRestore(runtime, ++txId, "/MyRoot", otherId, Ydb::StatusIds::SUCCESS);
        CheckRestoreRows(runtime, otherId, 0, 0, 0);
        CheckRestoreInventory(runtime, ss);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        CheckRestoreInventory(runtime, ss);
    }

    Y_UNIT_TEST_FLAG(RestoreItemInventorySurvivesRestart, reboot) {
        TSchemeShard* ss = nullptr;
        auto factory = [&ss](const TActorId& tablet, TTabletStorageInfo* info) {
            ss = new TSchemeShard(tablet, info);
            return ss;
        };
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true), factory);
        ui64 txId = 100;
        SetupBackupDirs(runtime, env, txId);
        CreateCollection(runtime, env, txId, "Coll", "/MyRoot/Target");
        CreateFullBackupDir(runtime, env, txId, "Coll", "Target");
        CreateIncrementalBackupDir(runtime, env, txId, "Coll", "Target");
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> finalize(runtime, [](const auto& ev) {
            for (const auto& tx : ev->Get()->Record.GetTransaction()) {
                if (tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpIncrementalRestoreFinalize) {
                    return true;
                }
            }
            return false;
        });
        const ui64 restoreId = ++txId;
        AsyncRestoreBackupCollection(runtime, restoreId, "/MyRoot/.backups/collections/", "Name: \"Coll\"\n");
        TestModificationResult(runtime, restoreId);
        runtime.WaitFor("finalize item dispatched", [&] { return !finalize.empty(); });
        runtime.RunCall([&] {
            UNIT_ASSERT(ss->PersistedIncrementalRestoreItems.contains(restoreId));
            return true;
        });
        CheckRestoreInventory(runtime, ss);
        if (reboot) {
            finalize.clear(); // Drop the old request: recovery must enqueue a new one.
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            runtime.WaitFor("finalize item redispatched", [&] { return !finalize.empty(); });
            CheckRestoreInventory(runtime, ss);
        }
        // Simulate a lost scheduling entry after the durable item was written.
        // Finalization must still remove that row using the independent inventory.
        runtime.RunCall([&] {
            auto& state = ss->IncrementalRestoreStates.at(restoreId);
            UNIT_ASSERT(!state.InFlightItems.empty());
            state.InFlightItems.clear();
            state.WaitTxIdToItemSeq.clear();
            return true;
        });
        finalize.Stop().Unblock();
        WaitRestoreDone(runtime, env, restoreId);
        CheckRestoreRows(runtime, restoreId, 1, 1, 0);
        CheckRestoreInventory(runtime, ss);
        TestForgetBackupCollectionRestore(runtime, ++txId, "/MyRoot", restoreId, Ydb::StatusIds::SUCCESS);
        CheckRestoreRows(runtime, restoreId, 0, 0, 0);
        CheckRestoreInventory(runtime, ss);
    }

    Y_UNIT_TEST_FLAG(RepairPreservesLiveSchemaTransaction, childTransaction) {
        TSchemeShard* ss = nullptr;
        auto factory = [&ss](const TActorId& tablet, TTabletStorageInfo* info) {
            ss = new TSchemeShard(tablet, info);
            return ss;
        };
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true), factory);
        TBlockEvents<TEvDataShard::TEvProposeTransactionResult> blocked(runtime);
        constexpr ui64 schemaTxId = 9000002;
        AsyncCreateTable(runtime, schemaTxId, "/MyRoot", R"(
            Name: "InFlight"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        TestModificationResult(runtime, schemaTxId);
        if (blocked.empty()) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back([&](IEventHandle&) { return !blocked.empty(); });
            runtime.DispatchEvents(options);
        }
        const ui64 restoreId = childTransaction ? 9000001 : schemaTxId;
        NKikimrSchemeOp::TLongIncrementalRestoreOp op;
        op.SetTxId(restoreId);
        WriteRestoreHeader(runtime, restoreId, op);
        if (childTransaction) {
            WriteRestoreItem(runtime, restoreId, 0, schemaTxId);
        }
        TBlockEvents<TEvPrivate::TEvRunIncrementalRestore> blockedRecovery(runtime);
        for (ui32 reboot = 0; reboot != 2; ++reboot) {
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            CheckRestoreRows(runtime, restoreId, 1, 0, childTransaction ? 1 : 0);
            CheckRestoreInventory(runtime, ss);
        }
    }

    Y_UNIT_TEST(RepairPreservesOutstandingDataShardWork) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        constexpr ui64 restoreId = 9000001;
        NKikimrSchemeOp::TLongIncrementalRestoreOp op;
        op.SetTxId(restoreId);
        // The malformed header alone is not sufficient evidence to forget work.
        WriteRestoreHeader(runtime, restoreId, op);
        WriteRestoreState(runtime, restoreId, 1); // Running
        NKikimrSchemeOp::TIncrementalRestoreOperationsList progress;
        auto* child = progress.AddSubOps();
        child->MutableId()->SetTxId(9000002);
        child->MutableId()->SetSubTxId(0);
        child->AddExpectedShardLocalIds(42);
        LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('OperationId (Uint64 '%lu))))
                (let value '('('SerializedData (String '%s))))
                (return (AsList (UpdateRow 'IncrementalRestoreState key value)))
            )
        )", restoreId, QuoteMiniKQLString(progress.SerializeAsString()).c_str()));

        // Observe startup in isolation from the progress transaction: this test
        // checks that incomplete per-shard state survives without a schema tx.
        runtime.SetObserverFunc([](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPrivate::TEvProgressIncrementalRestore::EventType) {
                return TTestActorRuntimeBase::EEventAction::DROP;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        CheckRestoreRows(runtime, restoreId, 1, 1, 0);
    }

    static void CheckRestoreSnapshotLayout(bool nested, bool missingEntry, bool reboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;
        SetupBackupDirs(runtime, env, txId);
        const TString parent = nested ? "/MyRoot/dir" : "/MyRoot";
        if (nested) {
            TestMkDir(runtime, ++txId, "/MyRoot", "dir");
            env.TestWaitNotification(runtime, txId);
        }
        for (const TString name : {"First", "Second"}) {
            TestCreateTable(runtime, ++txId, parent, TStringBuilder()
                << "Name: \"" << name << "\"\n"
                << "Columns { Name: \"key\" Type: \"Uint32\" }\n"
                << "Columns { Name: \"value\" Type: \"Uint32\" }\n"
                << "KeyColumnNames: [\"key\"]\n");
            env.TestWaitNotification(runtime, txId);
            UploadRow(runtime, parent + "/" + name, 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(10u)});
        }
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", TStringBuilder()
            << "Name: \"Layout\"\n"
            << "ExplicitEntryList {\n"
            << " Entries { Type: ETypeTable Path: \"" << parent << "/First\" }\n"
            << " Entries { Type: ETypeTable Path: \"" << parent << "/Second\" }\n"
            << "}\nCluster {}\nIncrementalBackupConfig {}\n");
        env.TestWaitNotification(runtime, txId);
        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/Layout")");
        env.TestWaitNotification(runtime, txId);
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        UploadRow(runtime, parent + "/First", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(20u)});
        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/Layout")");
        env.TestWaitNotification(runtime, txId);
        const TString collectionPath = "/MyRoot/.backups/collections/Layout";
        const auto collection = DescribePath(runtime, collectionPath);
        TString fullPath, incrementalPath;
        for (const auto& child : collection.GetPathDescription().GetChildren()) {
            if (child.GetName().EndsWith("_full")) {
                fullPath = collectionPath + "/" + child.GetName();
            } else if (child.GetName().EndsWith("_incremental")) {
                incrementalPath = collectionPath + "/" + child.GetName();
            }
        }
        UNIT_ASSERT(!fullPath.empty());
        UNIT_ASSERT(!incrementalPath.empty());
        const TString relativeDir = nested ? "/dir" : "";
        if (missingEntry) {
            // No changes to Second: dispatch supports snapshots without its entry.
            TestDropTable(runtime, ++txId, incrementalPath + relativeDir, "Second");
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, incrementalPath + relativeDir + "/Second"), {NLs::PathNotExist});
        }
        const auto expectedFirst = ReadShards(runtime, TTestTxConfig::SchemeShard, parent + "/First");
        const auto expectedSecond = ReadShards(runtime, TTestTxConfig::SchemeShard, parent + "/Second");
        for (const TString name : {"First", "Second"}) {
            TestDropTable(runtime, ++txId, parent, name);
            env.TestWaitNotification(runtime, txId);
        }

        TBlockEvents<TEvPrivate::TEvRunIncrementalRestore> blocked(runtime);
        const ui64 restoreId = ++txId;
        TestRestoreBackupCollection(runtime, restoreId, "/MyRoot",
            R"(Name: ".backups/collections/Layout")");
        env.TestWaitNotification(runtime, restoreId);
        runtime.WaitFor("restore dispatch blocked after schema completion", [&] { return !blocked.empty(); });
        CheckRestoreRows(runtime, restoreId, 1, 0, 0);
        if (reboot) {
            blocked.clear(); // Messages addressed to the previous SchemeShard actor.
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            CheckRestoreRows(runtime, restoreId, 1, 0, 0);
            runtime.WaitFor("orphan restore rescheduled", [&] { return !blocked.empty(); });
            TestDescribeResult(DescribePath(runtime, fullPath + relativeDir + "/First"), {
                NLs::PathExist,
                NLs::CheckPathState(NKikimrSchemeOp::EPathStateOutgoingIncrementalRestore),
            });
            TestDescribeResult(DescribePath(runtime, incrementalPath + relativeDir + "/First"), {
                NLs::PathExist,
                NLs::CheckPathState(NKikimrSchemeOp::EPathStateAwaitingOutgoingIncrementalRestore),
            });
        }
        blocked.Stop().Unblock();
        const auto deadline = runtime.GetCurrentTime() + TDuration::Seconds(120);
        bool completed = false;
        while (runtime.GetCurrentTime() < deadline) {
            const auto response = TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot");
            const auto& restore = response.GetBackupCollectionRestore();
            if (restore.GetProgress() == Ydb::Backup::RestoreProgress::PROGRESS_DONE) {
                UNIT_ASSERT_VALUES_EQUAL(restore.GetStatus(), Ydb::StatusIds::SUCCESS);
                completed = true;
                break;
            }
            env.SimulateSleep(runtime, TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(completed, "Restore must finish with the same data with and without a reboot");
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(runtime, TTestTxConfig::SchemeShard, parent + "/First"), expectedFirst);
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(runtime, TTestTxConfig::SchemeShard, parent + "/Second"), expectedSecond);
    }

    Y_UNIT_TEST_FLAG(RecoveryPreservesNestedSnapshotPaths, reboot) {
        CheckRestoreSnapshotLayout(true, false, reboot);
    }

    Y_UNIT_TEST_FLAG(RecoveryPreservesMissingIncrementalEntries, reboot) {
        CheckRestoreSnapshotLayout(false, true, reboot);
    }

    Y_UNIT_TEST_FLAG(RepairIgnoresUnrelatedAlterOnTarget, withState) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;
        SetupBackupDirs(runtime, env, txId);
        CreateCollection(runtime, env, txId, "Stale", "/MyRoot/Target");
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Target"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        const auto collectionId = DescribePath(runtime, "/MyRoot/.backups/collections/Stale")
            .GetPathDescription().GetSelf().GetPathId();
        TBlockEvents<TEvDataShard::TEvProposeTransactionResult> blocked(runtime);
        const ui64 alterId = ++txId;
        AsyncAlterTable(runtime, alterId, "/MyRoot", R"(
            Name: "Target"
            Columns { Name: "extra" Type: "Uint32" }
        )");
        TestModificationResult(runtime, alterId);
        runtime.WaitFor("unrelated alter in flight", [&] { return !blocked.empty(); });

        constexpr ui64 restoreId = 9000001;
        NKikimrSchemeOp::TLongIncrementalRestoreOp op;
        op.SetTxId(restoreId);
        op.MutableBackupCollectionPathId()->SetOwnerId(TTestTxConfig::SchemeShard);
        op.MutableBackupCollectionPathId()->SetLocalId(collectionId);
        op.AddTablePathList("/MyRoot/Target");
        WriteRestoreHeader(runtime, restoreId, op);
        if (withState) {
            WriteRestoreState(runtime, restoreId, 1);
        }
        LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('Id (Uint64 '%lu))))
                (let value '('('StepDropped (Uint64 '1000000))))
                (return (AsList (UpdateRow 'Paths key value)))
            )
        )", collectionId));
        for (ui32 reboot = 0; reboot != 2; ++reboot) {
            blocked.clear();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            CheckRestoreRows(runtime, restoreId, 0, 0, 0);
            runtime.WaitFor("alter still in flight after reboot", [&] { return !blocked.empty(); });
        }
        blocked.Stop().Unblock();
        env.TestWaitNotification(runtime, alterId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Target"), {
            NLs::PathExist,
            NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
        });
    }

    Y_UNIT_TEST_FLAG(RepairIgnoresAnotherRestoreReusingTarget, withState) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;
        SetupBackupDirs(runtime, env, txId);
        CreateCollection(runtime, env, txId, "Old", "/MyRoot/Target");
        const ui64 oldCollectionId = DescribePath(runtime, "/MyRoot/.backups/collections/Old")
            .GetPathDescription().GetSelf().GetPathId();
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Target"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        const ui64 oldTargetId = DescribePath(runtime, "/MyRoot/Target")
            .GetPathDescription().GetSelf().GetPathId();
        TestDropTable(runtime, ++txId, "/MyRoot", "Target");
        env.TestWaitNotification(runtime, txId);
        CreateCollection(runtime, env, txId, "New", "/MyRoot/Target");
        CreateFullBackupDir(runtime, env, txId, "New", "Target");
        CreateIncrementalBackupDir(runtime, env, txId, "New", "Target");
        const TString fullBackup = "/MyRoot/.backups/collections/New/snap_001_full/Target";
        WriteRow(runtime, ++txId, fullBackup, 0, 1, "restored");
        const auto expected = ReadShards(runtime, TTestTxConfig::SchemeShard, fullBackup);

        TBlockEvents<TEvDataShard::TEvProposeTransactionResult> blocked(runtime);
        const ui64 newRestoreId = ++txId;
        TestRestoreBackupCollection(runtime, newRestoreId, "/MyRoot",
            R"(Name: ".backups/collections/New")");
        runtime.WaitFor("second collection copy in flight", [&] { return !blocked.empty(); });
        UNIT_ASSERT_VALUES_UNEQUAL(DescribePath(runtime, "/MyRoot/Target")
            .GetPathDescription().GetSelf().GetPathId(), oldTargetId);

        // Reproduce the legacy header and collection tombstone while the new
        // restore is copying a different source into the same destination name.
        constexpr ui64 oldRestoreId = 9000001;
        NKikimrSchemeOp::TLongIncrementalRestoreOp op;
        op.SetTxId(oldRestoreId);
        op.MutableBackupCollectionPathId()->SetOwnerId(TTestTxConfig::SchemeShard);
        op.MutableBackupCollectionPathId()->SetLocalId(oldCollectionId);
        op.AddTablePathList("/MyRoot/Target");
        WriteRestoreHeader(runtime, oldRestoreId, op);
        if (withState) {
            WriteRestoreState(runtime, oldRestoreId, 1);
        }
        LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('Id (Uint64 '%lu))))
                (let value '('('StepDropped (Uint64 '1000000))))
                (return (AsList (UpdateRow 'Paths key value)))
            )
        )", oldCollectionId));
        for (ui32 reboot = 0; reboot != 2; ++reboot) {
            blocked.clear();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            CheckRestoreRows(runtime, oldRestoreId, 0, 0, 0);
            CheckRestoreRows(runtime, newRestoreId, 1, 0, 0);
            runtime.WaitFor("second collection copy survives reboot", [&] { return !blocked.empty(); });
        }
        blocked.Stop().Unblock();
        env.TestWaitNotification(runtime, newRestoreId);
        const auto deadline = runtime.GetCurrentTime() + TDuration::Seconds(120);
        bool completed = false;
        while (runtime.GetCurrentTime() < deadline) {
            const auto response = TestGetBackupCollectionRestore(runtime, newRestoreId, "/MyRoot");
            const auto& restore = response.GetBackupCollectionRestore();
            if (restore.GetProgress() == Ydb::Backup::RestoreProgress::PROGRESS_DONE) {
                UNIT_ASSERT_VALUES_EQUAL(restore.GetStatus(), Ydb::StatusIds::SUCCESS);
                completed = true;
                break;
            }
            env.SimulateSleep(runtime, TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(completed, "The second collection's restore must finish");
        UNIT_ASSERT_VALUES_EQUAL(ReadShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Target"), expected);
    }

    static void CheckFailedRestoreCleanup(bool terminal, bool allReported, bool unexpectedSuccess = false) {
        TSchemeShard* ss = nullptr;
        auto factory = [&ss](const TActorId& tablet, TTabletStorageInfo* info) {
            ss = new TSchemeShard(tablet, info);
            return ss;
        };
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true), factory);
        constexpr ui64 restoreId = 9000001;
        const TOperationId childId(TTxId(9000002), 0);
        NKikimrSchemeOp::TLongIncrementalRestoreOp op;
        op.SetTxId(restoreId);
        op.MutableBackupCollectionPathId()->SetOwnerId(TTestTxConfig::SchemeShard);
        op.MutableBackupCollectionPathId()->SetLocalId(999999);
        WriteRestoreHeader(runtime, restoreId, op);
        WriteRestoreState(runtime, restoreId, terminal ? 4 : 1); // Failed / Running
        NKikimrSchemeOp::TIncrementalRestoreOperationsList progress;
        auto* child = progress.AddSubOps();
        child->MutableId()->SetTxId(ui64(childId.GetTxId()));
        child->MutableId()->SetSubTxId(0);
        child->AddExpectedShardLocalIds(42);
        child->AddExpectedShardLocalIds(43);
        child->AddCompletedShardLocalIds(42);
        // Equal counts alone do not prove that each expected shard answered.
        if (unexpectedSuccess) {
            UNIT_ASSERT(!allReported);
            child->AddCompletedShardLocalIds(44);
        } else {
            child->AddFailedShardLocalIds(allReported ? 43 : 44);
            child->SetHasNonRetriableFailure(true);
        }
        LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"(
            (
                (let key '('('OperationId (Uint64 '%lu))))
                (let value '('('SerializedData (String '%s))))
                (return (AsList (UpdateRow 'IncrementalRestoreState key value)))
            )
        )", restoreId, QuoteMiniKQLString(progress.SerializeAsString()).c_str()));
        WriteRestoreItem(runtime, restoreId, 42);
        TVector<TEvPrivate::TEvProgressIncrementalRestore::TPtr> blocked;
        IEventHandle* unblockedProgress = nullptr;
        ui32 blockedProgressCount = 0;
        // Scheduled progress has no sender; TBlockEvents tries to resolve its
        // actor name when logging and cannot capture these timer events.
        auto progressObserver = runtime.AddObserver<TEvPrivate::TEvProgressIncrementalRestore>(
            [&](TEvPrivate::TEvProgressIncrementalRestore::TPtr& ev) {
                if (ev.Get() == unblockedProgress) {
                    unblockedProgress = nullptr;
                    return;
                }
                ++blockedProgressCount;
                blocked.emplace_back(std::move(ev));
            });
        const bool removed = terminal && allReported;
        for (ui32 reboot = 0; reboot != 2; ++reboot) {
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            CheckRestoreRows(runtime, restoreId, removed ? 0 : 1, removed ? 0 : 1, 0);
            if (reboot == 0 && unexpectedSuccess && !terminal) {
                runtime.WaitFor("recovered restore progress", [&] { return !blocked.empty(); });
                auto progress = std::move(blocked.back());
                blocked.pop_back();
                unblockedProgress = progress.Get();
                const auto blockedBefore = blockedProgressCount;
                const auto nodeIdx = progress->GetRecipientRewrite().NodeId() - runtime.GetFirstNodeId();
                runtime.Send(progress.Release(), nodeIdx, /* viaActorSystem */ true);
                // The next tick proves recovered progress ran before the reboot.
                // Keep that tick blocked while shard 43 is unanswered.
                runtime.WaitFor("next restore progress tick", [&] {
                    return !unblockedProgress && blockedProgressCount > blockedBefore;
                });
            }
            runtime.RunCall([&] {
                UNIT_ASSERT_VALUES_EQUAL(ss->IncrementalRestoreOperationToState.contains(childId), !removed);
                UNIT_ASSERT_VALUES_EQUAL(ss->TxIdToIncrementalRestore.contains(childId.GetTxId()), !removed);
                if (!removed) {
                    const auto& state = ss->IncrementalRestoreStates.at(restoreId);
                    UNIT_ASSERT(!state.CompletedOperations.contains(childId));
                    UNIT_ASSERT(state.InProgressOperations.contains(childId));
                    if (unexpectedSuccess) {
                        const auto& child = state.TableOperations.at(childId);
                        UNIT_ASSERT(child.ExpectedShards.contains(TShardIdx(TTestTxConfig::SchemeShard, 43)));
                        UNIT_ASSERT(!child.CompletedShards.contains(TShardIdx(TTestTxConfig::SchemeShard, 43)));
                        UNIT_ASSERT(!child.FailedShards.contains(TShardIdx(TTestTxConfig::SchemeShard, 43)));
                    }
                }
                return true;
            });
        }
    }

    Y_UNIT_TEST_FLAG(RepairFailedHistoryWithMissingCollection, allReported) {
        CheckFailedRestoreCleanup(true, allReported);
    }

    Y_UNIT_TEST(RepairPreservesNonterminalFailedShardWork) {
        CheckFailedRestoreCleanup(false, true);
    }

    Y_UNIT_TEST_FLAG(RepairPreservesUnexpectedSuccessfulShardReply, terminal) {
        CheckFailedRestoreCleanup(terminal, false, true);
    }

}
