#pragma once

#include "helpers.h"
#include "test_env.h"

#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/tx/datashard/incr_restore_scan.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <utility>

namespace NSchemeShardUT_Private {
namespace NIncrementalRestoreHelpers {

// Sets both wall-clock deadline ICB controls. -1 disables either tier.
inline void SetRestoreDeadlines(NActors::TTestActorRuntime& runtime,
        i64 overallSec, i64 stageSec) {
    NKikimr::TControlBoard::SetValue(overallSec,
        runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreOverallDurationSeconds);
    NKikimr::TControlBoard::SetValue(stageSec,
        runtime.GetAppData().Icb->SchemeShardControls.MaxIncrementalRestoreStageDurationSeconds);
}

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
// `endStatus` is the DS-side cause; SS-side classification determines retry-or-fail.
inline NActors::TTestActorRuntime::TEventObserverHolder InjectScanFailures(
    NActors::TTestActorRuntime& runtime,
    std::atomic<int>& counter,
    int maxFailures,
    NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::EEndStatus endStatus,
    const TString& errorMessage)
{
    return runtime.AddObserver<NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished>(
        [&counter, maxFailures, endStatus, errorMessage](
                NKikimr::NDataShard::TEvIncrementalRestoreScan::TEvFinished::TPtr& ev) {
            if (ev->Get()->TxId == 0) {
                return;
            }
            if (counter.fetch_add(1) < maxFailures) {
                ev->Get()->Success = false;
                ev->Get()->EndStatus = endStatus;
                ev->Get()->Error = errorMessage;
            }
        });
}

// Tracks peak concurrent in-flight (subOpTxId, shardIdx) in-flight slots by observing
// TEvIncrementalRestoreSrcCreateRequest (start) and TEvIncrementalRestoreShardProgress (end).
struct TInFlightTracker {
    std::atomic<i32> InFlight{0};
    std::atomic<i32> PeakInFlight{0};
    TMutex Mutex;
    THashSet<std::pair<ui64, ui64>> InFlightKeys;  // (subOpTxId, shardIdx)

    std::pair<NActors::TTestActorRuntime::TEventObserverHolder,
              NActors::TTestActorRuntime::TEventObserverHolder>
    AttachObservers(NActors::TTestActorRuntime& runtime) {
        auto start = runtime.AddObserver<NKikimr::TEvDataShard::TEvIncrementalRestoreSrcCreateRequest>(
            [this](NKikimr::TEvDataShard::TEvIncrementalRestoreSrcCreateRequest::TPtr& ev) {
                const auto& rec = ev->Get()->Record;
                const ui64 subOpTxId = rec.GetSubOpTxId();
                // Dedup on (subOpTxId, shardIdx) so the request->reply pair shares the
                // same key (the reply's sender actor id differs from the request's
                // recipient actor id; both messages carry the same logical ShardIdx).
                const ui64 shardIdx = rec.GetShardIdx();
                const auto key = std::make_pair(subOpTxId, shardIdx);
                bool inserted = false;
                { TGuard<TMutex> g(Mutex); inserted = InFlightKeys.insert(key).second; }
                if (!inserted) return;
                i32 cur = InFlight.fetch_add(1) + 1;
                i32 peak;
                do {
                    peak = PeakInFlight.load();
                    if (cur <= peak) break;
                } while (!PeakInFlight.compare_exchange_weak(peak, cur));
            });
        auto end = runtime.AddObserver<NKikimr::TEvDataShard::TEvIncrementalRestoreShardProgress>(
            [this](NKikimr::TEvDataShard::TEvIncrementalRestoreShardProgress::TPtr& ev) {
                const auto& rec = ev->Get()->Record;
                const ui64 subOpTxId = rec.GetSubOpTxId();
                const ui64 shardIdx = rec.GetShardIdx();
                const auto key = std::make_pair(subOpTxId, shardIdx);
                bool removed = false;
                { TGuard<TMutex> g(Mutex); removed = InFlightKeys.erase(key) > 0; }
                if (removed) {
                    InFlight.fetch_sub(1);
                }
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

// Creates 1 table, takes a full backup, then K incremental backups. Used by stage-deadline tests.
inline void SetupBackupCollectionWithKIncrementals(NActors::TTestActorRuntime& runtime, TTestEnv& env,
        ui64& txId, ui32 numIncrementals) {
    TestMkDir(runtime, ++txId, "/MyRoot", ".backups/collections");
    env.TestWaitNotification(runtime, txId);

    TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
        Name: "MyCollection1"
        ExplicitEntryList { Entries { Type: ETypeTable Path: "/MyRoot/Table0" } }
        Cluster {}
        IncrementalBackupConfig {}
    )");
    env.TestWaitNotification(runtime, txId);

    TestCreateTable(runtime, ++txId, "/MyRoot", R"(
        Name: "Table0"
        Columns { Name: "key" Type: "Uint32" }
        Columns { Name: "value" Type: "Uint32" }
        KeyColumnNames: ["key"]
    )");
    env.TestWaitNotification(runtime, txId);
    UploadRow(runtime, "/MyRoot/Table0", 0, {1}, {2},
        {NKikimr::TCell::Make(1u)}, {NKikimr::TCell::Make(1u)});

    TestBackupBackupCollection(runtime, ++txId, "/MyRoot",
        R"(Name: ".backups/collections/MyCollection1")");
    env.TestWaitNotification(runtime, txId);

    for (ui32 i = 0; i < numIncrementals; ++i) {
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        UploadRow(runtime, "/MyRoot/Table0", 0, {1}, {2},
            {NKikimr::TCell::Make(2u + i)}, {NKikimr::TCell::Make(2u + i)});

        TestBackupIncrementalBackupCollection(runtime, ++txId, "/MyRoot",
            R"(Name: ".backups/collections/MyCollection1")");
        const ui64 incrBackupId = txId;
        env.TestWaitNotification(runtime, txId);
        WaitForIncrementalBackupDone(runtime, &env, incrBackupId, "/MyRoot");
    }

    TestDropTable(runtime, ++txId, "/MyRoot", "Table0");
    env.TestWaitNotification(runtime, txId);
}

} // namespace NIncrementalRestoreHelpers
} // namespace NSchemeShardUT_Private
