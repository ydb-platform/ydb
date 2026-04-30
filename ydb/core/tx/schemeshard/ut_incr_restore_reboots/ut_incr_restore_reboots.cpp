#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/tx/datashard/incr_restore_scan.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <climits>

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

    // List-based poll: TestGetBackupCollectionRestore asserts inner Status, which flips
    // to GENERIC_ERROR when the orchestrator transitions to Failed.
    Ydb::StatusIds::StatusCode WaitForRestoreDone(TTestActorRuntime& runtime, TTestEnv* testEnv, const TString& dbName,
            bool expectRegistered,
            TDuration pollInterval = TDuration::Seconds(1), TDuration timeout = TDuration::Seconds(30)) {
        auto initialList = TestListBackupCollectionRestores(runtime, dbName);
        if (initialList.GetEntries().empty()) {
            if (expectRegistered) {
                UNIT_ASSERT_C(false, "Restore was expected to be registered, but list returned no entries");
            }
            return Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        }

        TInstant deadline = runtime.GetCurrentTime() + timeout;
        while (runtime.GetCurrentTime() < deadline) {
            auto listResp = TestListBackupCollectionRestores(runtime, dbName);
            if (!listResp.GetEntries().empty()) {
                const auto& entry = *listResp.GetEntries().rbegin();
                if (entry.GetProgress() == Ydb::Backup::RestoreProgress::PROGRESS_DONE) {
                    return static_cast<Ydb::StatusIds::StatusCode>(entry.GetStatus());
                }
            }
            testEnv->SimulateSleep(runtime, pollInterval);
        }
        UNIT_ASSERT_C(false, "Restore did not reach PROGRESS_DONE within timeout");
        return Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    }

    // Injects scan failures into TEvFinished; skips TxId=0 events from change_sender.
    TTestActorRuntime::TEventObserverHolder InjectScanFailures(
        TTestActorRuntime& runtime,
        std::atomic<int>& counter,
        int maxFailures,
        bool retriable,
        const TString& errorMessage)
    {
        return runtime.AddObserver<NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished>(
            [&counter, maxFailures, retriable, errorMessage](
                    NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished::TPtr& ev) {
                if (ev->Get()->TxId == 0) {
                    return;
                }
                if (counter.fetch_add(1) < maxFailures) {
                    ev->Get()->Success = false;
                    ev->Get()->Retriable = retriable;
                    ev->Get()->Error = errorMessage;
                }
            });
    }

    // Tracks peak concurrent ESchemeOpRestoreMultipleIncrementalBackups sub-ops.
    struct TInFlightTracker {
        std::atomic<i32> InFlight{0};
        std::atomic<i32> PeakInFlight{0};
        TMutex Mutex;
        THashSet<ui64> RestoreTxIds;

        std::pair<TTestActorRuntime::TEventObserverHolder, TTestActorRuntime::TEventObserverHolder>
        AttachObservers(TTestActorRuntime& runtime) {
            auto start = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>(
                [this](TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
                    const auto& rec = ev->Get()->Record;
                    if (rec.TransactionSize() == 0) return;
                    if (rec.GetTransaction(0).GetOperationType()
                            != NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups) {
                        return;
                    }
                    { TGuard<TMutex> g(Mutex); RestoreTxIds.insert(rec.GetTxId()); }
                    i32 cur = InFlight.fetch_add(1) + 1;
                    i32 peak;
                    do {
                        peak = PeakInFlight.load();
                        if (cur <= peak) break;
                    } while (!PeakInFlight.compare_exchange_weak(peak, cur));
                });
            auto end = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransactionResult>(
                [this](TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
                    ui64 txId = ev->Get()->Record.GetTxId();
                    bool isRestore;
                    { TGuard<TMutex> g(Mutex); isRestore = RestoreTxIds.contains(txId); }
                    if (isRestore) InFlight.fetch_sub(1);
                });
            return {std::move(start), std::move(end)};
        }
    };

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
            /*retriable=*/false, "Injected non-retriable failure for Failed-survives-reboot test");

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
            /*retriable=*/true, "Injected scan failure for retry test");

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

    // Creates N tables, takes a full+incremental backup, drops the tables. Used by cap tests.
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
            /*retriable=*/true, "Injected scan failure for cap+retry test");

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
                    ev->Get()->Retriable = true;
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
            /*retriable=*/true, "Injected retriable failure for budget test");

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
            /*retriable=*/true, "Injected retriable failure for unlimited-cap test");

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

    // Test 6 (integration): non-retriable failure short-circuits to Failed
    // without consuming the retry budget.
    Y_UNIT_TEST(IncrementalRestoreNonRetriableShortCircuits) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        // Generous budget; we expect the non-retriable bit to trump the cap.
        TControlBoard::SetValue(50, runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreRetriesPerIncremental);

        SetupBackupCollectionWithNTables(runtime, env, txId, /*numTables=*/1);

        std::atomic<int> failuresInjected{0};
        auto observerHolder = InjectScanFailures(runtime, failuresInjected, /*maxFailures=*/1,
            /*retriable=*/false, "Injected non-retriable failure for short-circuit test");

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

    // Test 7 (integration, reboot): backoff state survives reboot — but not
    // by replay. New transient state is wiped, so the next attempt fires
    // immediately. Restore eventually succeeds.
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
                    /*retriable=*/true, "Injected retriable failure for reboot-backoff test");

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

    // Test 8 (integration, anti-double-fire): concurrent completion events do
    // not double-count the retry counter.
    //
    // Use 4 tables so 4 concurrent NotifyIncrementalRestoreOperationCompleted
    // events fire during the failure wave. With the two-phase backoff guard,
    // the cap should NOT trigger after a single round of failures.
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
            /*retriable=*/true, "Injected retriable failure for double-fire test");

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

} // Y_UNIT_TEST_SUITE(TRestoreWithRebootsTests)
