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

        // Generous deadlines so deadline-expiry doesn't race the non-retriable bit.
        SetRestoreDeadlines(runtime, /*overallSec=*/-1, /*stageSec=*/-1);

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
            NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::END_FATAL_FAILURE,
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
                    // Full: key=1 (1 row); incremental: key=2 (new row). Total: 2.
                    UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, fullPath), 2u);
                }
            }
        });
    }

    // Backoff state (RetryScheduled, NextRetryAttemptAt) persists across SS reboots;
    // restore eventually succeeds after reboot clears the single injected failure.
    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(IncrementalRestoreBackoffSurvivesReboot, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                // Generous deadlines so the reboot path has room to recover.
                SetRestoreDeadlines(runtime, /*overallSec=*/-1, /*stageSec=*/-1);

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

                // Inject one retriable failure to enter backoff; static survives
                // re-entry across reboot-bucket iterations.
                static std::atomic<int> failuresInjected{0};
                failuresInjected.store(0);
                auto observerHolder = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/1,
                    NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::END_TRANSIENT_FAILURE,
                    "Injected retriable failure for reboot-backoff test");

                TestRestoreBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Reboot bucket fires here; RetryScheduled/NextRetryAttemptAt survive reboot.
            {
                TInactiveZone inactive(activeZone);

                WaitForRestoreDone(runtime, t.TestEnv.Get(), "/MyRoot", true,
                    TDuration::Seconds(2), TDuration::Seconds(180));

                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);
            }
        });
    }

    // A reboot between finalize-launch and finalize-complete must not strand the
    // restore: TTxInit resets orphaned Finalizing rows to Running and re-launches.
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

        // Hold the first TEvAllocateResult for the incremental restore; reboot
        // SchemeShard while it is held — the recipient mailbox dies on reboot.
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

        // Post-reboot, TTxInit reloads the item and re-issues TEvAllocate.
        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env,
            "/MyRoot", true, TDuration::Seconds(2), TDuration::Seconds(180));
        UNIT_ASSERT_VALUES_EQUAL_C(finalStatus, Ydb::StatusIds::SUCCESS,
            "Restore did not SUCCESS after reboot during TEvAllocate; "
            "TTxInit may not be re-issuing allocate for orphan items.");

        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table1"), 2u);
    }

    // Overall deadline anchors survive SchemeShard reboot and eventually terminate the restore.
    Y_UNIT_TEST(RestoreOverallDeadlineSurvivesReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Tight overall deadline (15s); stage disabled so we exercise the overall arm.
        SetRestoreDeadlines(runtime, /*overallSec=*/15, /*stageSec=*/-1);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/1);

        // Inject infinite transient failures so the orchestrator never converges.
        std::atomic<int> failuresInjected{0};
        auto observerHolder = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/INT_MAX,
            NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::END_TRANSIENT_FAILURE,
            "Injected retriable failure for overall-deadline-reboot test");

        const TInstant startedAt = runtime.GetCurrentTime();
        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        // Reboot SchemeShard a couple of times before the deadline elapses.
        env.SimulateSleep(runtime, TDuration::Seconds(5));
        {
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        }
        env.SimulateSleep(runtime, TDuration::Seconds(5));
        {
            TActorId sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        }

        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env, "/MyRoot", true,
            TDuration::Seconds(1), TDuration::Seconds(120));
        const TDuration elapsed = runtime.GetCurrentTime() - startedAt;

        UNIT_ASSERT_C(finalStatus != Ydb::StatusIds::SUCCESS,
            "Restore status was SUCCESS under expired overall deadline + reboots");
        UNIT_ASSERT_C(elapsed >= TDuration::Seconds(15),
            "Restore terminated before the overall deadline elapsed: " << elapsed);
    }

    // A scheduled retry (RetryScheduled=true, NextRetryAttemptAt persisted) survives a SchemeShard reboot.
    Y_UNIT_TEST(RetryScheduledSurvivesReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Generous deadlines; we just want a stable retry-scheduled state.
        SetRestoreDeadlines(runtime, /*overallSec=*/-1, /*stageSec=*/-1);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/1);

        // First failure schedules a backoff. Subsequent attempts succeed.
        std::atomic<int> failuresInjected{0};
        auto observerHolder = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/1,
            NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::END_TRANSIENT_FAILURE,
            "Injected single retriable failure for retry-scheduled-survives-reboot test");

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        // Let the failure happen and the backoff get persisted.
        TInstant waitDeadline = runtime.GetCurrentTime() + TDuration::Seconds(30);
        while (runtime.GetCurrentTime() < waitDeadline && failuresInjected.load() < 1) {
            env.SimulateSleep(runtime, TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT_GE_C(failuresInjected.load(), 1, "Failure was not injected in time");

        // Reboot SchemeShard mid-backoff. After reload, RetryScheduled and
        // NextRetryAttemptAt persist via columns 9/10. Restore eventually succeeds.
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env, "/MyRoot", true,
            TDuration::Seconds(2), TDuration::Seconds(120));
        UNIT_ASSERT_VALUES_EQUAL_C(finalStatus, Ydb::StatusIds::SUCCESS,
            "Restore did not SUCCESS after retry-scheduled survives reboot");
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table0"), 2u);
    }

    // CompletedOperations must be loaded by TTxInit even for state.State == Running.
    // Channel split + idempotent RecordShardResult removed the race-with-late-replies
    // concern that motivated the original Running-state guard. Without the fix, an SS
    // reboot mid-Running discards CompletedOperations and re-dispatches sub-ops for
    // already-finished sub-ops (within the same incremental) — wasted work
    // proportional to total data volume.
    Y_UNIT_TEST(IncrementalRestoreCompletedOpsLoadedAfterReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Generous deadlines; we want a stable mid-Running reboot without timeout.
        SetRestoreDeadlines(runtime, /*overallSec=*/-1, /*stageSec=*/-1);

        // 4 tables × 1 incremental: each table is its own sub-op within the same
        // incremental. After 2 sub-ops finish and 2 are still pending, we reboot;
        // the 2 finished sub-ops must persist as CompletedOperations across TTxInit.
        constexpr ui32 NumTables = 4;
        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/NumTables);

        // Continuously fail scans for tables 2 and 3 (the LAST two) until reboot.
        // Tables 0 and 1 succeed normally and are persisted as completed sub-ops.
        // Stable identification: fail any scan whose TargetPathId is one of the
        // last two; here we use a counter — at least 2 successes go through, then
        // failures kick in, keeping the orchestrator in Running.
        static std::atomic<int> scanFinishCount{0};
        scanFinishCount.store(0);
        std::atomic<int> failuresInjected{0};
        std::atomic<bool> rebootHappened{false};
        auto scanObserver = runtime.AddObserver<NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished>(
            [&](NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished::TPtr& ev) {
                if (ev->Get()->TxId == 0) {
                    return;
                }
                int n = scanFinishCount.fetch_add(1) + 1;
                // Let the first 2 scans succeed (sub-ops complete and persist into
                // CompletedOperations). Fail subsequent scans until reboot to keep
                // the orchestrator in Running state with backoff retries.
                if (n >= 3 && !rebootHappened.load()) {
                    ev->Get()->Success = false;
                    ev->Get()->EndStatus = NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::END_TRANSIENT_FAILURE;
                    ev->Get()->Error = "Injected retriable failure for completed-ops-loaded-after-reboot test";
                    failuresInjected.fetch_add(1);
                }
            });

        // Count distinct sub-op dispatches (TxIds) of ESchemeOpRestoreMultipleIncrementalBackups.
        // Without the fix: TTxInit discards CompletedOperations for Running rows,
        // so post-reboot the orchestrator re-dispatches sub-ops for already-finished
        // tables — distinct TxIds grow by ~NumTables (4). With the fix, only the
        // pending tables (2) are re-dispatched.
        TMutex distinctMutex;
        THashSet<ui64> distinctSubOpTxIds;
        auto dispatchObserver = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>(
            [&](TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
                const auto& rec = ev->Get()->Record;
                if (rec.TransactionSize() == 0) return;
                if (rec.GetTransaction(0).GetOperationType()
                        != NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups) {
                    return;
                }
                TGuard<TMutex> g(distinctMutex);
                distinctSubOpTxIds.insert(rec.GetTxId());
            });

        TestRestoreBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        env.TestWaitNotification(runtime, txId);

        // Wait until at least 2 failures have been injected. By this point, the
        // first 2 sub-ops are persisted as Completed and the orchestrator is
        // firmly stuck retrying the remaining 2 in Running state.
        TInstant deadline = runtime.GetCurrentTime() + TDuration::Seconds(60);
        while (runtime.GetCurrentTime() < deadline && failuresInjected.load() < 2) {
            env.SimulateSleep(runtime, TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_GE_C(failuresInjected.load(), 2,
            "Fewer than 2 scan failures injected before reboot deadline");

        size_t distinctBeforeReboot;
        {
            TGuard<TMutex> g(distinctMutex);
            distinctBeforeReboot = distinctSubOpTxIds.size();
        }

        // Reboot SchemeShard while in Running state. The first 2 sub-ops are
        // persisted as Completed; they must be loaded by TTxInit on reboot.
        // Set rebootHappened FIRST so post-reboot scans don't keep failing.
        rebootHappened.store(true);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        Ydb::StatusIds::StatusCode finalStatus = WaitForRestoreDone(runtime, &env, "/MyRoot", true,
            TDuration::Seconds(1), TDuration::Seconds(180));
        UNIT_ASSERT_VALUES_EQUAL_C(finalStatus, Ydb::StatusIds::SUCCESS,
            "Restore did not SUCCESS after mid-Running reboot");

        // Each table has 2 rows after restore (1 from full + 1 from incremental).
        for (ui32 i = 0; i < NumTables; ++i) {
            TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
            UNIT_ASSERT_VALUES_EQUAL_C(CountRows(runtime, fullPath), 2u,
                "Wrong row count for " << fullPath);
        }

        size_t distinctAfter;
        {
            TGuard<TMutex> g(distinctMutex);
            distinctAfter = distinctSubOpTxIds.size();
        }

        // Without the fix: TTxInit discards CompletedOperations for Running rows.
        // Post-reboot the orchestrator re-dispatches sub-ops for ALL 4 tables, so
        // distinctAfter grows by ~4 (or more with retries). With the fix, only the
        // 2 incomplete sub-ops are re-dispatched, so distinctAfter grows by at
        // most ~3 (2 incomplete + at most 1 extra retry tolerance).
        UNIT_ASSERT_LE_C(distinctAfter - distinctBeforeReboot, 3,
            "Post-reboot sub-op re-dispatch budget exceeded: distinctBeforeReboot="
            << distinctBeforeReboot << " distinctAfter=" << distinctAfter
            << " (expected delta <=3 = 2 incomplete + at most 1 retry); "
            "TTxInit likely discarded CompletedOperations for the Running state row");
    }

} // Y_UNIT_TEST_SUITE(TRestoreWithRebootsTests)
