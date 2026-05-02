#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TBackupWithRebootsTests) {

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

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(FullBackupShouldSucceed, 2, 1, false) {
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
            }

            TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/MyCollection1")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TVector<TString> backups;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/MyCollection1"), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                    NLs::ChildrenCount(1),
                    NLs::ExtractChildren(&backups),
                    NLs::Finished,
                });

                for (const auto& b : backups) {
                    TString backupPath = TStringBuilder() << "/MyRoot/.backups/collections/MyCollection1/" << b;
                    TVector<TString> tables;
                    TestDescribeResult(DescribePath(runtime, backupPath), {
                        NLs::PathExist,
                        NLs::ChildrenCount(1),
                        NLs::ExtractChildren(&tables),
                        NLs::Finished,
                    });

                    for (const auto& tbl : tables) {
                        TString tablePath = TStringBuilder() << backupPath << "/" << tbl;
                        TestDescribeResult(DescribePath(runtime, tablePath), {
                            NLs::PathExist,
                            NLs::IsTable,
                            NLs::Finished,
                            NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                        });

                        ui32 rowCount = CountRows(runtime, tablePath);
                        UNIT_ASSERT_VALUES_EQUAL(rowCount, 2u);
                    }
                }

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(IncrementalBackupShouldSucceed, 2, 1, false) {
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
            }

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/MyCollection1")");
            const ui64 backupId = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), backupId, "/MyRoot");

                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/MyCollection1"), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                    NLs::ChildrenCount(2),
                    NLs::Finished,
                });

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(BackupCycleWithDataShouldSucceed, 2, 1, false) {
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
            }

            // Step 1: full backup
            TestBackupBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/MyCollection1")");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            // Step 2: modify data + incremental backup
            {
                TInactiveZone inactive(activeZone);
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(2u)});
            }

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/MyCollection1")");
            const ui64 backupId1 = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            runtime.AdvanceCurrentTime(TDuration::Seconds(1));

            // Step 3: modify data again + incremental backup
            {
                TInactiveZone inactive(activeZone);
                UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(3u)});
            }

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/MyCollection1")");
            const ui64 backupId2 = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), backupId1, "/MyRoot");
                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), backupId2, "/MyRoot");

                TVector<TString> backups;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/MyCollection1"), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                    NLs::ChildrenCount(3),
                    NLs::ExtractChildren(&backups),
                    NLs::Finished,
                });

                for (const auto& b : backups) {
                    TString backupPath = TStringBuilder() << "/MyRoot/.backups/collections/MyCollection1/" << b;
                    TVector<TString> tables;
                    TestDescribeResult(DescribePath(runtime, backupPath), {
                        NLs::PathExist,
                        NLs::ChildrenCount(1),
                        NLs::ExtractChildren(&tables),
                        NLs::Finished,
                    });

                    for (const auto& tbl : tables) {
                        TString tablePath = TStringBuilder() << backupPath << "/" << tbl;
                        TestDescribeResult(DescribePath(runtime, tablePath), {
                            NLs::PathExist,
                            NLs::IsTable,
                            NLs::Finished,
                            NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                        });

                        ui32 expectedRows = b.Contains("full") ? 2u : 1u;
                        ui32 foundRows = CountRows(runtime, tablePath);
                        UNIT_ASSERT_EQUAL_C(foundRows, expectedRows, TStringBuilder()
                            << "Backup table " << tablePath << " has " << foundRows << " rows, "
                            << "but expected " << expectedRows << " rows");
                    }
                }

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                    NLs::PathExist,
                    NLs::IsTable,
                    NLs::CheckPathState(NKikimrSchemeOp::EPathStateNoChanges),
                    NLs::Finished,
                });
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(IncrementalBackupForgetAfterDone, 2, 1, false) {
        t.GetTestEnvOptions() = TTestEnvOptions().EnableBackupService(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 backupId = 0;

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

                TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                    R"(Name: ".backups/collections/MyCollection1")");
                backupId = t.TxId;
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), backupId, "/MyRoot");
            }

            TestForgetIncrementalBackup(runtime, ++t.TxId, "/MyRoot", backupId);

            {
                TInactiveZone inactive(activeZone);

                TestGetIncrementalBackup(runtime, backupId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
            }
        });
    }

    Y_UNIT_TEST_WITH_REBOOTS_BUCKETS(MultiTableIncrementalBackupShouldSucceed, 2, 1, false) {
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

                runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            }

            TestBackupIncrementalBackupCollection(runtime, ++t.TxId, "/MyRoot",
                R"(Name: ".backups/collections/MyCollection1")");
            const ui64 backupId = t.TxId;
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                WaitForIncrementalBackupDone(runtime, t.TestEnv.Get(), backupId, "/MyRoot");

                TVector<TString> backups;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/.backups/collections/MyCollection1"), {
                    NLs::PathExist,
                    NLs::IsBackupCollection,
                    NLs::ChildrenCount(2),
                    NLs::ExtractChildren(&backups),
                    NLs::Finished,
                });

                // Find the incremental backup (non-full)
                for (const auto& b : backups) {
                    TString backupPath = TStringBuilder() << "/MyRoot/.backups/collections/MyCollection1/" << b;
                    if (!b.Contains("full")) {
                        TestDescribeResult(DescribePath(runtime, backupPath), {
                            NLs::PathExist,
                            NLs::ChildrenCount(2),
                            NLs::Finished,
                        });
                    }
                }

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
            }
        });
    }

} // TBackupWithRebootsTests
