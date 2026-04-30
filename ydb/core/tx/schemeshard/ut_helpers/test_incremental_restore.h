#pragma once

#include "helpers.h"
#include "test_env.h"

#include <ydb/core/tx/datashard/incr_restore_scan.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <utility>

namespace NSchemeShardUT_Private {
namespace NIncrementalRestoreHelpers {

// Polls TestGetIncrementalBackup until PROGRESS_DONE; fails on timeout.
inline void WaitForIncrementalBackupDone(NActors::TTestActorRuntime& runtime, TTestEnv* testEnv,
        ui64 backupId, const TString& dbName,
        TDuration pollInterval = TDuration::Seconds(1),
        TDuration timeout = TDuration::Seconds(30)) {
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
inline Ydb::StatusIds::StatusCode WaitForRestoreDone(NActors::TTestActorRuntime& runtime,
        TTestEnv* testEnv, const TString& dbName,
        bool expectRegistered,
        TDuration pollInterval = TDuration::Seconds(1),
        TDuration timeout = TDuration::Seconds(30)) {
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
inline NActors::TTestActorRuntime::TEventObserverHolder InjectScanFailures(
    NActors::TTestActorRuntime& runtime,
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

    std::pair<NActors::TTestActorRuntime::TEventObserverHolder,
              NActors::TTestActorRuntime::TEventObserverHolder>
    AttachObservers(NActors::TTestActorRuntime& runtime) {
        auto start = runtime.AddObserver<NKikimr::NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>(
            [this](NKikimr::NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
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
        auto end = runtime.AddObserver<NKikimr::NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(
            [this](NKikimr::NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
                ui64 txId = ev->Get()->Record.GetTxId();
                bool isRestore;
                { TGuard<TMutex> g(Mutex); isRestore = RestoreTxIds.contains(txId); }
                if (isRestore) InFlight.fetch_sub(1);
            });
        return {std::move(start), std::move(end)};
    }
};

// Creates N tables, takes a full+incremental backup, drops the tables. Used by cap tests.
inline void SetupBackupCollectionWithNTables(NActors::TTestActorRuntime& runtime, TTestEnv& env,
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
        UploadRow(runtime, fullPath, 0, {1}, {2}, {NKikimr::TCell::Make(1u)}, {NKikimr::TCell::Make(1u)});
    }

    TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
        R"(Name: ".backups/collections/MyCollection1")");
    env.TestWaitNotification(runtime, txId);

    runtime.AdvanceCurrentTime(TDuration::Seconds(1));

    for (ui32 i = 0; i < numTables; ++i) {
        TString fullPath = TStringBuilder() << "/MyRoot/Table" << i;
        UploadRow(runtime, fullPath, 0, {1}, {2}, {NKikimr::TCell::Make(2u)}, {NKikimr::TCell::Make(2u)});
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

} // namespace NIncrementalRestoreHelpers
} // namespace NSchemeShardUT_Private
