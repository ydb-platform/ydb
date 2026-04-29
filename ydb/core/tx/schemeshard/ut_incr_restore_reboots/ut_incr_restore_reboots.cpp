#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/tx/datashard/incr_restore_scan.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TRestoreWithRebootsTests) {

    void WaitForIncrementalBackupDone(TTestActorRuntime& runtime, TTestEnv* testEnv, ui64 backupId, const TString& dbName,
            TDuration pollInterval = TDuration::Seconds(1), TDuration timeout = TDuration::Seconds(30)) {
        TInstant deadline = runtime.GetCurrentTime() + timeout;
        while (runtime.GetCurrentTime() < deadline) {
            auto resp = TestGetIncrementalBackup(runtime, backupId, dbName);
            if (resp.GetIncrementalBackup().GetProgress() == Ydb::Backup::BackupProgress::PROGRESS_DONE) {
                return;
            }
            testEnv->SimulateSleep(runtime, pollInterval);
        }
        auto resp = TestGetIncrementalBackup(runtime, backupId, dbName);
        UNIT_ASSERT_VALUES_EQUAL_C(resp.GetIncrementalBackup().GetProgress(),
            Ydb::Backup::BackupProgress::PROGRESS_DONE,
            "Incremental backup did not reach PROGRESS_DONE within timeout");
    }

    void WaitForRestoreDone(TTestActorRuntime& runtime, TTestEnv* testEnv, const TString& dbName,
            bool expectRegistered,
            TDuration pollInterval = TDuration::Seconds(1), TDuration timeout = TDuration::Seconds(30)) {
        auto listResp = TestListBackupCollectionRestores(runtime, dbName);
        const auto& entries = listResp.GetEntries();
        if (entries.empty()) {
            if (expectRegistered) {
                UNIT_ASSERT_C(false, "Restore was expected to be registered, but list returned no entries");
            }
            return;
        }
        ui64 restoreId = entries.rbegin()->GetId();

        TInstant deadline = runtime.GetCurrentTime() + timeout;
        while (runtime.GetCurrentTime() < deadline) {
            auto resp = TestGetBackupCollectionRestore(runtime, restoreId, dbName);
            if (resp.GetBackupCollectionRestore().GetProgress() == Ydb::Backup::RestoreProgress::PROGRESS_DONE) {
                return;
            }
            testEnv->SimulateSleep(runtime, pollInterval);
        }
        auto resp = TestGetBackupCollectionRestore(runtime, restoreId, dbName);
        UNIT_ASSERT_C(resp.GetBackupCollectionRestore().GetProgress() == Ydb::Backup::RestoreProgress::PROGRESS_DONE,
            "Restore did not reach PROGRESS_DONE within timeout");
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(RestoreFromFullShouldSucceed, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", R"(
                    Name: "MyCollection1"
                    ExplicitEntryList {
                        Entries {
                            Type: ETypeTable
                            Path: "/MyRoot/Table1"
                        }
                    }
                    Cluster {}
                    IncrementalBackupConfig {}
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});

                TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/MyCollection1")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                WaitForRestoreDone(runtime, t.TestEnv.Get(), "/MyRoot", false);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/MyCollection1"), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                    NLs::Finished,
                });

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });

                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(RestoreFromIncrementalShouldSucceed, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", R"(
                    Name: "MyCollection1"
                    ExplicitEntryList {
                        Entries {
                            Type: ETypeTable
                            Path: "/MyRoot/Table1"
                        }
                    }
                    Cluster {}
                    IncrementalBackupConfig {}
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});

                TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                runtime.AdvanceCurrentTime(TDuration::Seconds(1));

                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(2u)});

                TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                const ui64 incrBackupId = t.TxId;
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), incrBackupId, "/MyRoot");

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/MyCollection1")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                WaitForRestoreDone(runtime, t.TestEnv.Get(), "/MyRoot", true);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/MyCollection1"), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                    NLs::Finished,
                });

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });

                // After restore from incremental: key=1 updated to value=2, key=2 unchanged
                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(RestoreCycleWithDataShouldSucceed, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", R"(
                    Name: "MyCollection1"
                    ExplicitEntryList {
                        Entries {
                            Type: ETypeTable
                            Path: "/MyRoot/Table1"
                        }
                    }
                    Cluster {}
                    IncrementalBackupConfig {}
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});

                TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                runtime.AdvanceCurrentTime(TDuration::Seconds(1));

                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(2u)});

                TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                const ui64 incrBackupId1 = t.TxId;
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), incrBackupId1, "/MyRoot");

                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(3u)});

                TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                const ui64 incrBackupId2 = t.TxId;
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), incrBackupId2, "/MyRoot");

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/MyCollection1")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                WaitForRestoreDone(runtime, t.TestEnv.Get(), "/MyRoot", true);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });

                // After restore from full + 2 incremental: key=1 updated to value=3, key=2 unchanged
                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(MultiTableRestoreShouldSucceed, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", R"(
                    Name: "MyCollection1"
                    ExplicitEntryList {
                        Entries {
                            Type: ETypeTable
                            Path: "/MyRoot/Table1"
                        }
                        Entries {
                            Type: ETypeTable
                            Path: "/MyRoot/Table2"
                        }
                    }
                    Cluster {}
                    IncrementalBackupConfig {}
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table2"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});
                UploadRow(runtime, "/MyRoot/Table2", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                UploadRow(runtime, "/MyRoot/Table2", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});

                TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table2");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/MyCollection1")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                WaitForRestoreDone(runtime, t.TestEnv.Get(), "/MyRoot", false);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table2"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });

                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);
                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table2"), 2u);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(RestoreDataIntegrity, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TVector<TString> originalData;

            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", R"(
                    Name: "MyCollection1"
                    ExplicitEntryList {
                        Entries {
                            Type: ETypeTable
                            Path: "/MyRoot/Table1"
                        }
                    }
                    Cluster {}
                    IncrementalBackupConfig {}
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});

                TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                runtime.AdvanceCurrentTime(TDuration::Seconds(1));

                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(2u)});

                TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                const ui64 incrBackupId = t.TxId;
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), incrBackupId, "/MyRoot");

                // Capture table data before drop for comparison after restore
                originalData = ReadShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table1");

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/MyCollection1")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                WaitForRestoreDone(runtime, t.TestEnv.Get(), "/MyRoot", true);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });

                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);

                // Verify restored data matches the original (after incremental: key=1/value=2, key=2/value=2)
                auto restoredData = ReadShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table1");
                UNIT_ASSERT_VALUES_EQUAL(originalData.size(), restoredData.size());
                for (size_t i = 0; i < originalData.size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL_C(originalData[i], restoredData[i],
                        TStringBuilder() << "Shard " << i << " data mismatch after restore");
                }
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(TestIncrementalRestoreStateRecoveryAfterReboot, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", R"(
                    Name: "MyCollection1"
                    ExplicitEntryList {
                        Entries {
                            Type: ETypeTable
                            Path: "/MyRoot/Table1"
                        }
                    }
                    Cluster {}
                    IncrementalBackupConfig {}
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Insert initial 3 rows
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(3u)}, {TCell::Make(3u)});

                // Take full backup
                TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                runtime.AdvanceCurrentTime(TDuration::Seconds(1));

                // Add key=4,value=4 and take incremental backup #1
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(4u)}, {TCell::Make(4u)});

                TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                const ui64 incrBackupId1 = t.TxId;
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), incrBackupId1, "/MyRoot");

                runtime.AdvanceCurrentTime(TDuration::Seconds(1));

                // Add key=5,value=5 and take incremental backup #2
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(5u)}, {TCell::Make(5u)});

                TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                const ui64 incrBackupId2 = t.TxId;
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), incrBackupId2, "/MyRoot");

                // Drop the table so restore has something to do
                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Start an incremental restore — restore processing begins here
                TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Active zone: reboots are injected here while the restore is processing.
            // The bug manifests here: after reboot, IncrementalRestoreState is not
            // loaded from DB, so the restore actor loses its state and cannot complete.
            {
                TInactiveZone inactive(activeZone);

                // After reboots, the restore must still complete successfully.
                WaitForRestoreDone(runtime, t.TestEnv.Get(), "/MyRoot", true);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });

                // All 5 rows must be present after restore from full + 2 incrementals
                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 5u);
            }
        });
    }

    // Tests that TTxInit correctly resumes an incremental restore operation that is in the
    // Finalizing state after a SchemeShard reboot. With enough incrementals, the bucket
    // reboot mechanism is very likely to inject a reboot while the finalize sub-operation
    // is in flight (state == Finalizing), exercising the TTxInit Finalizing resume path
    // at schemeshard__init.cpp ("TTxInit resuming incremental restore operation", state: 2).
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(TestIncrementalRestoreFinalizingStateRecoveryAfterReboot, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", R"(
                    Name: "MyCollection1"
                    ExplicitEntryList {
                        Entries {
                            Type: ETypeTable
                            Path: "/MyRoot/Table1"
                        }
                    }
                    Cluster {}
                    IncrementalBackupConfig {}
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Uint32" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Insert 2 initial rows
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});

                // Take full backup
                TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Take 5 incremental backups: more steps means higher probability that
                // a bucket reboot lands while state == Finalizing (after last incremental
                // completes but before the finalize sub-operation finishes).
                for (int i = 3; i <= 7; ++i) {
                    runtime.AdvanceCurrentTime(TDuration::Seconds(1));
                    UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(ui32(i))}, {TCell::Make(ui32(i))});
                    TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                        R"(Name: ".backups/collections/MyCollection1")");
                    const ui64 incrBackupId = t.TxId;
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                    WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), incrBackupId, "/MyRoot");
                }

                // Drop the table so restore has something to do
                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Start restore — processing begins here; reboots will be injected during
                // the active zone below, including potentially during Finalizing state
                TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Active zone: bucket reboots injected here. With 5 incrementals the restore
            // goes through: Running(incr0) -> Running(incr1) -> ... -> Running(incr4) ->
            // Finalizing -> Completed. Reboots at any of these transitions exercise the
            // TTxInit resume path for both Running and Finalizing states.
            {
                TInactiveZone inactive(activeZone);

                WaitForRestoreDone(runtime, t.TestEnv.Get(), "/MyRoot", true);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });

                // Full backup: rows 1-2. Incrementals: rows 3-7. Total: 7 rows.
                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 7u);
            }
        });
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

        // Inject a single TEvFinished failure to exercise the retry path.
        // We rewrite the FIRST TEvFinished from the IncrementalRestoreScan to carry success=false
        // and let subsequent retries succeed.
        std::atomic<int> failuresInjected{0};
        auto observerHolder = runtime.AddObserver<NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished>(
            [&failuresInjected](NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished::TPtr& ev) {
                if (failuresInjected.fetch_add(1) == 0) {
                    // First TEvFinished: rewrite to failure
                    ev->Get()->Success = false;
                    ev->Get()->Error = "Injected scan failure for retry test";
                }
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        // Wait for restore to complete via the retry path
        auto listResp = TestListBackupCollectionRestores(runtime, "/MyRoot");
        UNIT_ASSERT_C(!listResp.GetEntries().empty(), "Restore was never registered");
        ui64 restoreId = listResp.GetEntries().rbegin()->GetId();

        TInstant deadline = runtime.GetCurrentTime() + TDuration::Seconds(60);
        while (runtime.GetCurrentTime() < deadline) {
            auto resp = TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot");
            if (resp.GetBackupCollectionRestore().GetProgress() == Ydb::Backup::RestoreProgress::PROGRESS_DONE) {
                break;
            }
            env.SimulateSleep(runtime, TDuration::Seconds(1));
        }

        auto finalResp = TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot");
        UNIT_ASSERT_C(finalResp.GetBackupCollectionRestore().GetProgress() == Ydb::Backup::RestoreProgress::PROGRESS_DONE,
            "Restore did not reach PROGRESS_DONE after retry");
        // Status must be SUCCESS — retry path recovered from the injected failure
        UNIT_ASSERT_C(finalResp.GetBackupCollectionRestore().GetStatus() == Ydb::StatusIds::SUCCESS,
            "Restore status is not SUCCESS after retry");

        // Ensure exactly 1 failure was injected (the retry must have succeeded on attempt 2)
        UNIT_ASSERT_GE(failuresInjected.load(), 1);

        // Verify table data was restored correctly despite the injected failure
        // Full backup: key=1,val=1. Incremental: key=2,val=2. Total: 2 rows.
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);
    }

    // Helper: create N tables in a backup collection, populate with data, take a
    // single full+incremental backup, drop the tables. Used by the cap tests.
    void SetupBackupCollectionWithNTables(TTestActorRuntime& runtime, TTestEnv& env,
            ui64& txId, ui32 numTables) {
        TestMkDir(runtime, ++txId, "/MyRoot", ".backups/collections");
        env.TestWaitNotification(runtime, txId);

        TStringBuilder bcRequest;
        bcRequest << "Name: \"MyCollection1\"\n"
                  << "ExplicitEntryList {\n";
        for (ui32 i = 0; i < numTables; ++i) {
            bcRequest << "  Entries { Type: ETypeTable Path: \"/MyRoot/Table" << i << "\" }\n";
        }
        bcRequest << "}\nCluster {}\nIncrementalBackupConfig {}\n";
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", bcRequest);
        env.TestWaitNotification(runtime, txId);

        for (ui32 i = 0; i < numTables; ++i) {
            TString tbl = TStringBuilder() << "Table" << i;
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "%s"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Uint32" }
                KeyColumnNames: ["key"]
            )", tbl.c_str()));
            env.TestWaitNotification(runtime, txId);

            TString fullPath = TStringBuilder() << "/MyRoot/" << tbl;
            UploadRow(runtime, fullPath, 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
        }

        TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        for (ui32 i = 0; i < numTables; ++i) {
            TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
            UploadRow(runtime, fullPath, 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});
        }

        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        const ui64 incrBackupId = txId;
        env.TestWaitNotification(runtime, txId);

        WaitForIncrementalBackupDone(runtime, &env, incrBackupId, "/MyRoot");

        for (ui32 i = 0; i < numTables; ++i) {
            TString tbl = TStringBuilder() << "Table" << i;
            TestDropTable(runtime, ++txId, "/MyRoot", tbl);
            env.TestWaitNotification(runtime, txId);
        }
    }

    // Test 2: With cap=2, never see >2 concurrent ESchemeOpRestoreMultipleIncrementalBackups
    // sub-ops in flight. All 8 tables eventually restored.
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
        std::atomic<i32> inFlight{0};
        std::atomic<i32> peakInFlight{0};
        std::atomic<i32> totalSeen{0};
        // Track which TxIds are restore sub-ops so we know which results to count.
        TMutex restoreTxIdsMutex;
        THashSet<ui64> restoreTxIds;

        auto observerStart = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>(
            [&](TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
                const auto& rec = ev->Get()->Record;
                if (rec.TransactionSize() == 0) return;
                if (rec.GetTransaction(0).GetOperationType()
                        != NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups) {
                    return;
                }
                {
                    TGuard<TMutex> g(restoreTxIdsMutex);
                    restoreTxIds.insert(rec.GetTxId());
                }
                i32 cur = inFlight.fetch_add(1) + 1;
                totalSeen.fetch_add(1);
                i32 peak;
                do {
                    peak = peakInFlight.load();
                    if (cur <= peak) break;
                } while (!peakInFlight.compare_exchange_weak(peak, cur));
            });

        auto observerEnd = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransactionResult>(
            [&](TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
                ui64 txId = ev->Get()->Record.GetTxId();
                bool isRestore;
                {
                    TGuard<TMutex> g(restoreTxIdsMutex);
                    isRestore = restoreTxIds.contains(txId);
                }
                if (isRestore) {
                    inFlight.fetch_sub(1);
                }
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        WaitForRestoreDone(runtime, &env, "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(120));

        UNIT_ASSERT_C(totalSeen.load() >= 8,
            "Expected at least 8 restore sub-ops, saw " << totalSeen.load());
        UNIT_ASSERT_C(peakInFlight.load() <= 2,
            "Expected peak in-flight <= 2 (cap=2), saw " << peakInFlight.load());

        // Sanity: each table has 1 row from full + 1 row from incremental
        for (ui32 i = 0; i < 8; ++i) {
            TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
            UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 2u);
        }
    }

    // Test 3: cap=-1 (unbounded sentinel) lets all 8 tables fan out at once.
    Y_UNIT_TEST(IncrementalRestoreUnboundedWhenCapNegative) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        TControlBoard::SetValue(-1, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/8);

        std::atomic<i32> inFlight{0};
        std::atomic<i32> peakInFlight{0};
        TMutex restoreTxIdsMutex;
        THashSet<ui64> restoreTxIds;

        auto observerStart = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>(
            [&](TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
                const auto& rec = ev->Get()->Record;
                if (rec.TransactionSize() == 0) return;
                if (rec.GetTransaction(0).GetOperationType()
                        != NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups) {
                    return;
                }
                {
                    TGuard<TMutex> g(restoreTxIdsMutex);
                    restoreTxIds.insert(rec.GetTxId());
                }
                i32 cur = inFlight.fetch_add(1) + 1;
                i32 peak;
                do {
                    peak = peakInFlight.load();
                    if (cur <= peak) break;
                } while (!peakInFlight.compare_exchange_weak(peak, cur));
            });

        auto observerEnd = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransactionResult>(
            [&](TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
                ui64 txId = ev->Get()->Record.GetTxId();
                bool isRestore;
                {
                    TGuard<TMutex> g(restoreTxIdsMutex);
                    isRestore = restoreTxIds.contains(txId);
                }
                if (isRestore) {
                    inFlight.fetch_sub(1);
                }
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        WaitForRestoreDone(runtime, &env, "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(120));

        // With cap=-1 we expect to observe all 8 in-flight at peak (best-effort:
        // require >2 to prove the cap is actually disabled).
        UNIT_ASSERT_C(peakInFlight.load() > 2,
            "Expected peak in-flight > 2 with unbounded cap, saw " << peakInFlight.load());
    }

    // Test 4: cap survives reboots. ICB lives in AppData which persists across
    // tablet restart in the test framework, so set it once in the inactive zone.
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(IncrementalRestoreCapRespectedAcrossReboots, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                // Set ICB cap=2 once; AppData survives test reboots.
                TControlBoard::SetValue(2, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TStringBuilder bcRequest;
                bcRequest << "Name: \"MyCollection1\"\n"
                          << "ExplicitEntryList {\n";
                for (ui32 i = 0; i < 8; ++i) {
                    bcRequest << "  Entries { Type: ETypeTable Path: \"/MyRoot/Table" << i << "\" }\n";
                }
                bcRequest << "}\nCluster {}\nIncrementalBackupConfig {}\n";
                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", bcRequest);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (ui32 i = 0; i < 8; ++i) {
                    TString tbl = TStringBuilder() << "Table" << i;
                    TestCreateTable(runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                        Name: "%s"
                        Columns { Name: "key" Type: "Uint32" }
                        Columns { Name: "value" Type: "Uint32" }
                        KeyColumnNames: ["key"]
                    )", tbl.c_str()));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);

                    TString fullPath = TStringBuilder() << "/MyRoot/" << tbl;
                    UploadRow(runtime, fullPath, 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});
                }

                TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Two incrementals to give the reboot bucket more chances to fire mid-restore
                for (int incIdx = 0; incIdx < 2; ++incIdx) {
                    runtime.AdvanceCurrentTime(TDuration::Seconds(1));
                    for (ui32 i = 0; i < 8; ++i) {
                        TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
                        ui32 v = ui32(2 + incIdx);
                        UploadRow(runtime, fullPath, 0, {1}, {2}, {TCell::Make(v)}, {TCell::Make(v)});
                    }
                    TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                        R"(Name: ".backups/collections/MyCollection1")");
                    const ui64 incrBackupId = t.TxId;
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                    WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), incrBackupId, "/MyRoot");
                }

                for (ui32 i = 0; i < 8; ++i) {
                    TString tbl = TStringBuilder() << "Table" << i;
                    TestDropTable(runtime, ++t.TxId, "/MyRoot", tbl);
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }

                TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Reboots inject here. Cap remains in effect across reboots.
            {
                TInactiveZone inactive(activeZone);

                WaitForRestoreDone(runtime, t.TestEnv.Get(), "/MyRoot", true,
                    TDuration::Seconds(2), TDuration::Seconds(120));

                for (ui32 i = 0; i < 8; ++i) {
                    TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
                    // Full backup: key=1,val=1 (1 row).
                    // Incr 0 (v=2): key=2,val=2 — new row.
                    // Incr 1 (v=3): key=3,val=3 — new row.
                    // After restore of both incrementals: 3 rows total.
                    UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 3u);
                }
            }
        });
    }

    // Test 5: change cap during restore. Raising lets dispatcher fill up; lowering
    // does not abort in-flight (cap is checked at dispatch time, not retroactively).
    Y_UNIT_TEST(IncrementalRestoreCapChangedMidRestore) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Start with cap=2.
        TControlBoard::SetValue(2, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/8);

        std::atomic<i32> inFlight{0};
        std::atomic<i32> peakInFlight{0};
        std::atomic<i32> peakAfterRaise{0};
        std::atomic<bool> raised{false};
        TMutex restoreTxIdsMutex;
        THashSet<ui64> restoreTxIds;

        auto observerStart = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>(
            [&](TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
                const auto& rec = ev->Get()->Record;
                if (rec.TransactionSize() == 0) return;
                if (rec.GetTransaction(0).GetOperationType()
                        != NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups) {
                    return;
                }
                {
                    TGuard<TMutex> g(restoreTxIdsMutex);
                    restoreTxIds.insert(rec.GetTxId());
                }
                i32 cur = inFlight.fetch_add(1) + 1;
                i32 peak;
                do {
                    peak = peakInFlight.load();
                    if (cur <= peak) break;
                } while (!peakInFlight.compare_exchange_weak(peak, cur));
                if (raised.load()) {
                    i32 peak2;
                    do {
                        peak2 = peakAfterRaise.load();
                        if (cur <= peak2) break;
                    } while (!peakAfterRaise.compare_exchange_weak(peak2, cur));
                }
            });

        auto observerEnd = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransactionResult>(
            [&](TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
                ui64 txId = ev->Get()->Record.GetTxId();
                bool isRestore;
                {
                    TGuard<TMutex> g(restoreTxIdsMutex);
                    isRestore = restoreTxIds.contains(txId);
                }
                if (isRestore) {
                    inFlight.fetch_sub(1);
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
        UNIT_ASSERT_C(peakInFlight.load() <= 8,
            "Peak in-flight exceeded cap=8, saw " << peakInFlight.load());

        // Sanity: restore finished
        for (ui32 i = 0; i < 8; ++i) {
            TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
            UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 2u);
        }
    }

    // Test 6: cap interacts cleanly with the retry path. After an injected shard
    // failure, the orchestrator clears PendingTables and re-enqueues; the cap
    // continues to apply during the retry wave.
    Y_UNIT_TEST(IncrementalRestoreCapRespectedDuringRetry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        TControlBoard::SetValue(2, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/4);

        std::atomic<i32> inFlight{0};
        std::atomic<i32> peakInFlight{0};
        TMutex restoreTxIdsMutex;
        THashSet<ui64> restoreTxIds;

        auto observerStart = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>(
            [&](TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
                const auto& rec = ev->Get()->Record;
                if (rec.TransactionSize() == 0) return;
                if (rec.GetTransaction(0).GetOperationType()
                        != NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups) {
                    return;
                }
                {
                    TGuard<TMutex> g(restoreTxIdsMutex);
                    restoreTxIds.insert(rec.GetTxId());
                }
                i32 cur = inFlight.fetch_add(1) + 1;
                i32 peak;
                do {
                    peak = peakInFlight.load();
                    if (cur <= peak) break;
                } while (!peakInFlight.compare_exchange_weak(peak, cur));
            });

        auto observerEnd = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransactionResult>(
            [&](TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
                ui64 txId = ev->Get()->Record.GetTxId();
                bool isRestore;
                {
                    TGuard<TMutex> g(restoreTxIdsMutex);
                    isRestore = restoreTxIds.contains(txId);
                }
                if (isRestore) {
                    inFlight.fetch_sub(1);
                }
            });

        // Inject one TEvFinished failure (same pattern as IncrementalRestoreShardFailureTriggersRetry).
        std::atomic<int> failuresInjected{0};
        auto failureObserver = runtime.AddObserver<NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished>(
            [&failuresInjected](NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished::TPtr& ev) {
                if (failuresInjected.fetch_add(1) == 0) {
                    ev->Get()->Success = false;
                    ev->Get()->Error = "Injected scan failure for cap+retry test";
                }
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        WaitForRestoreDone(runtime, &env, "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(120));

        // Cap respected during retry wave.
        UNIT_ASSERT_C(peakInFlight.load() <= 2,
            "Peak in-flight exceeded cap=2 during retry, saw " << peakInFlight.load());
        UNIT_ASSERT_GE(failuresInjected.load(), 1);

        // All 4 tables restored despite injected failure.
        for (ui32 i = 0; i < 4; ++i) {
            TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
            UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 2u);
        }
    }

} // Y_UNIT_TEST_SUITE(TRestoreWithRebootsTests)
