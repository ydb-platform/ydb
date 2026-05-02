#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_incremental_restore.h>
#include <ydb/core/tx/datashard/incr_restore_scan.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <climits>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NSchemeShardUT_Private::NIncrementalRestoreHelpers;

Y_UNIT_TEST_SUITE(TRestoreWithRebootsTests) {

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

            {
                TInactiveZone inactive(activeZone);

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

    // Verifies TTxInit resumes correctly when a reboot lands during the Finalizing state.
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

                // 3 incrementals: enough for a reboot bucket to land in Finalizing.
                for (int i = 3; i <= 5; ++i) {
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

                TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            {
                TInactiveZone inactive(activeZone);

                WaitForRestoreDone(runtime, t.TestEnv.Get(), "/MyRoot", true);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });

                // Full backup: rows 1-2. Incrementals: rows 3-5. Total: 5 rows.
                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 5u);
            }
        });
    }

    // The IncrementalRestoreState row must persist until FORGET, so Get reports
    // SUCCESS+PROGRESS_DONE regardless of where a reboot lands after finalize.
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(RestoreCompletedStatusSurvivesReboot, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", R"(
                    Name: "MyCollection1"
                    ExplicitEntryList { Entries { Type: ETypeTable Path: "/MyRoot/Table1" } }
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

                TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                runtime.AdvanceCurrentTime(TDuration::Seconds(1));
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});

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

                Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(
                    runtime, t.TestEnv.Get(), "/MyRoot", true,
                    TDuration::Seconds(2), TDuration::Seconds(120));
                UNIT_ASSERT_VALUES_EQUAL_C(finalStatus, Ydb::StatusIds::SUCCESS,
                    "Restore did not reach SUCCESS across reboot bucket");

                auto listResp = TestListBackupCollectionRestores(runtime, "/MyRoot");
                UNIT_ASSERT_C(!listResp.GetEntries().empty(),
                    "List returned no entries after Completed restore");
                ui64 restoreId = listResp.GetEntries().rbegin()->GetId();

                auto getResp = TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot");
                UNIT_ASSERT_VALUES_EQUAL_C(
                    getResp.GetBackupCollectionRestore().GetStatus(),
                    Ydb::StatusIds::SUCCESS,
                    "Get inner status not SUCCESS after Completed restore + reboot");
                UNIT_ASSERT_C(
                    getResp.GetBackupCollectionRestore().GetProgress() ==
                        Ydb::Backup::RestoreProgress::PROGRESS_DONE,
                    "Get progress not PROGRESS_DONE after Completed restore + reboot");
            }
        });
    }

    // After a Failed restore and a manual reboot, FinalStatus must surface as a
    // non-SUCCESS, non-UNSPECIFIED code; FinalStatus and FinalIssues are persisted.
    Y_UNIT_TEST(RestoreFailedStatusSurvivesReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        TControlBoard::SetValue(50, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreRetriesPerIncremental);

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
            NKikimrTxDataShard::TShardOpResult::END_FATAL_FAILURE,
            "Injected non-retriable failure for Failed-survives-reboot test");

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        Ydb::StatusIds::StatusCode preReboot =
            WaitForRestoreDone(runtime, &env, "/MyRoot", true,
                TDuration::Seconds(1), TDuration::Seconds(60));
        UNIT_ASSERT_C(preReboot != Ydb::StatusIds::SUCCESS,
            "Restore unexpectedly SUCCESS under non-retriable failure injection");
        UNIT_ASSERT_C(preReboot != Ydb::StatusIds::STATUS_CODE_UNSPECIFIED,
            "Restore status was STATUS_CODE_UNSPECIFIED before reboot");

        auto listBefore = TestListBackupCollectionRestores(runtime, "/MyRoot");
        UNIT_ASSERT_C(!listBefore.GetEntries().empty(), "List empty before reboot");
        ui64 restoreId = listBefore.GetEntries().rbegin()->GetId();

        // Force a reboot; the persisted FinalStatus must match what was reported pre-reboot.
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        auto listAfter = TestListBackupCollectionRestores(runtime, "/MyRoot");
        UNIT_ASSERT_C(!listAfter.GetEntries().empty(),
            "List returned no entries after Failed restore + reboot");
        // Expected inner status is the persisted FinalStatus (or GENERIC_ERROR default).
        auto getAfter = TestGetBackupCollectionRestore(runtime, restoreId, "/MyRoot", preReboot);
        UNIT_ASSERT_C(getAfter.GetBackupCollectionRestore().GetStatus() != Ydb::StatusIds::SUCCESS,
            "Get inner status flipped to SUCCESS after Failed restore + reboot");
        UNIT_ASSERT_C(getAfter.GetBackupCollectionRestore().GetStatus() !=
                Ydb::StatusIds::STATUS_CODE_UNSPECIFIED,
            "Get inner status was STATUS_CODE_UNSPECIFIED after Failed restore + reboot");
        UNIT_ASSERT_C(getAfter.GetBackupCollectionRestore().GetProgress() ==
            Ydb::Backup::RestoreProgress::PROGRESS_DONE,
            "Get progress not PROGRESS_DONE for Failed restore + reboot");
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(IncrementalRestoreCapRespectedAcrossReboots, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                // Set ICB cap=2 once; AppData survives test reboots.
                TControlBoard::SetValue(2, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreTablesInFlight);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // 4 tables (>cap=2) exercises the cap; larger counts time out under reboot replay.
                constexpr ui32 NumTables = 4;
                TStringBuilder bcRequest;
                bcRequest << "Name: \"MyCollection1\"\n"
                          << "ExplicitEntryList {\n";
                for (ui32 i = 0; i < NumTables; ++i) {
                    bcRequest << "  Entries { Type: ETypeTable Path: \"/MyRoot/Table" << i << "\" }\n";
                }
                bcRequest << "}\nCluster {}\nIncrementalBackupConfig {}\n";
                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", bcRequest);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (ui32 i = 0; i < NumTables; ++i) {
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

                runtime.AdvanceCurrentTime(TDuration::Seconds(1));
                for (ui32 i = 0; i < NumTables; ++i) {
                    TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
                    UploadRow(runtime, fullPath, 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});
                }
                TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                const ui64 incrBackupId = t.TxId;
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), incrBackupId, "/MyRoot");

                for (ui32 i = 0; i < NumTables; ++i) {
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

                constexpr ui32 NumTables = 4;
                for (ui32 i = 0; i < NumTables; ++i) {
                    TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
                    // Full backup: key=1,val=1 (1 row).
                    // Incr 0 (v=2): key=2,val=2 — new row.
                    // After restore: 2 rows.
                    UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 2u);
                }
            }
        });
    }

    // Transient backoff state is wiped on reboot; the next attempt fires immediately and restore succeeds.
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(IncrementalRestoreBackoffSurvivesReboot, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TControlBoard::SetValue(5, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreRetriesPerIncremental);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", R"(
                    Name: "MyCollection1"
                    ExplicitEntryList { Entries { Type: ETypeTable Path: "/MyRoot/Table1" } }
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

                TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                runtime.AdvanceCurrentTime(TDuration::Seconds(1));

                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});
                TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                const ui64 incrBackupId = t.TxId;
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), incrBackupId, "/MyRoot");

                TestDropTable(runtime, ++t.TxId, "/MyRoot", "Table1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Inject one retriable failure to drive the orchestrator into
                // its backoff window — then a reboot may land mid-window.
                // static is required here: the reboot-bucket lambda may re-enter
                // this scope, so the counter must survive across bucket iterations.
                static std::atomic<int> failuresInjected{0};
                failuresInjected.store(0);
                auto observerHolder = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/1,
                    NKikimrTxDataShard::TShardOpResult::END_TRANSIENT_FAILURE,
                    "Injected retriable failure for reboot-backoff test");

                TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Reboot bucket fires somewhere in here. After reboot, RetryScheduled
            // and NextRetryAttemptAt are wiped. The next attempt fires immediately;
            // restore eventually succeeds because the failure observer only runs
            // for the first event each time the test process starts.
            {
                TInactiveZone inactive(activeZone);

                WaitForRestoreDone(runtime, t.TestEnv.Get(), "/MyRoot", true,
                    TDuration::Seconds(2), TDuration::Seconds(180));

                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);
            }
        });
    }

    // A reboot landing between finalize-launch and finalize-complete must not strand
    // the restore: TTxInit resets orphaned Finalizing rows to Running and the
    // orchestrator re-launches finalize idempotently.
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(RestoreFinalizingResumesAfterReboot, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", R"(
                    Name: "MyCollection1"
                    ExplicitEntryList { Entries { Type: ETypeTable Path: "/MyRoot/Table1" } }
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

                TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                runtime.AdvanceCurrentTime(TDuration::Seconds(1));
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(2u)}, {TCell::Make(2u)});

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

                Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(
                    runtime, t.TestEnv.Get(), "/MyRoot", true,
                    TDuration::Seconds(2), TDuration::Seconds(120));
                UNIT_ASSERT_VALUES_EQUAL_C(finalStatus, Ydb::StatusIds::SUCCESS,
                    "Finalizing did not converge to SUCCESS across reboot bucket");

                // Sanity: the table has both rows from full + incremental.
                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);
            }
        });
    }

    // A full-only restore (no incremental backups) must reach SUCCESS across reboots;
    // a state row is always created so the restore is visible to Get/List.
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(FullOnlyRestoreReachesCompletedAcrossReboots, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestMkDir(runtime, ++t.TxId, "/MyRoot", ".backups/collections");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // No IncrementalBackupConfig — full-only collection.
                TestCreateBackupCollection(runtime, ++t.TxId, "/MyRoot/.backups/collections", R"(
                    Name: "MyCollection1"
                    ExplicitEntryList { Entries { Type: ETypeTable Path: "/MyRoot/Table1" } }
                    Cluster {}
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

                Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(
                    runtime, t.TestEnv.Get(), "/MyRoot", true,
                    TDuration::Seconds(2), TDuration::Seconds(120));
                UNIT_ASSERT_VALUES_EQUAL_C(finalStatus, Ydb::StatusIds::SUCCESS,
                    "Full-only restore did not reach SUCCESS across reboot bucket");

                // Sanity: the table is restored.
                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 1u);
            }
        });
    }

    // A reboot between TEvAllocate and TEvAllocateResult must not strand the restore; TTxInit re-issues the allocate.
    Y_UNIT_TEST(RebootDuringAllocateReissues) {
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
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        // Hold the FIRST allocator result destined for the incremental
        // restore: detach the incoming TEvAllocateResult, do not deliver,
        // and signal the test. We then reboot SchemeShard; the held result
        // is moot because the recipient mailbox dies on reboot.
        std::atomic<bool> firstAllocateSent{false};
        std::atomic<bool> firstResultHeld{false};
        std::atomic<bool> rebootHappened{false};

        auto sendObserver = runtime.AddObserver<NKikimr::TEvTxAllocatorClient::TEvAllocate>(
            [&firstAllocateSent](NKikimr::TEvTxAllocatorClient::TEvAllocate::TPtr&) {
                firstAllocateSent.store(true);
            });

        auto resultObserver = runtime.AddObserver<NKikimr::TEvTxAllocatorClient::TEvAllocateResult>(
            [&firstResultHeld, &rebootHappened](
                    NKikimr::TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
                if (rebootHappened.load()) return;
                if (ev->Cookie == 0) return;
                if (firstResultHeld.load()) return;
                firstResultHeld.store(true);
                ev.Reset();
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        // Wait for the held result to fire (i.e. the orchestrator allocated and
        // the result was intercepted). Bound by 60s of simulated time.
        TInstant deadline = runtime.GetCurrentTime() + TDuration::Seconds(60);
        while (runtime.GetCurrentTime() < deadline && !firstResultHeld.load()) {
            env.SimulateSleep(runtime, TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(firstAllocateSent.load(),
            "TEvAllocate never observed; orchestrator never reached enqueue");
        UNIT_ASSERT_C(firstResultHeld.load(),
            "TEvAllocateResult never reached the observer within 60s");

        // Reboot SchemeShard while the result is held undelivered.
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        rebootHappened.store(true);

        // Post-reboot, TTxInit must reload the IncrementalRestoreItem row,
        // push to PendingItems, and re-issue TEvAllocate. The natural
        // allocator response then drives restore to completion.
        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env,
            "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(180));
        UNIT_ASSERT_VALUES_EQUAL_C(finalStatus, Ydb::StatusIds::SUCCESS,
            "Restore did not SUCCESS after reboot during TEvAllocate; "
            "TTxInit may not be re-issuing allocate for orphan items.");

        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);
    }

} // Y_UNIT_TEST_SUITE(TRestoreWithRebootsTests)
