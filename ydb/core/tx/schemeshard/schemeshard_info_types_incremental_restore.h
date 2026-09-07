#pragma once

#include "schemeshard_info_types_base.h"

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr {
namespace NSchemeShard {

struct TIncrementalRestoreState {
    enum class EState : ui32 {
        Running = 1,
        Finalizing = 2,
        Completed = 3,
        Failed = 4,
    };

    EState State = EState::Running;

    TPathId BackupCollectionPathId;
    ui64 OriginalOperationId = 0;

    struct TIncrementalBackup {
        TPathId BackupPathId;
        TString BackupPath;
        ui64 Timestamp;
        bool Completed = false;

        TIncrementalBackup(const TPathId& pathId, const TString& path, ui64 timestamp)
            : BackupPathId(pathId), BackupPath(path), Timestamp(timestamp)
        {}
    };

    // Table operation state for tracking DataShard completion
    struct TTableOperationState {
        TOperationId OperationId;
        THashSet<TShardIdx> ExpectedShards;
        THashSet<TShardIdx> CompletedShards;
        THashSet<TShardIdx> FailedShards;

        // When true, completion is detected from per-shard reports rather than
        // Self->Operations.contains(txId) (no schema transaction was proposed).
        bool RequestsDispatched = false;

        TTableOperationState() = default;
        explicit TTableOperationState(const TOperationId& opId) : OperationId(opId) {}

        bool AllShardsComplete() const {
            return CompletedShards.size() + FailedShards.size() == ExpectedShards.size() &&
                    !ExpectedShards.empty();
        }

        bool HasFailures() const {
            return !FailedShards.empty();
        }

        bool HasNonRetriableFailure = false;

        // Idempotent against re-delivery: the first terminal report wins.
        bool RecordShardResult(TShardIdx shardIdx, bool success, bool retriable = true) {
            if (CompletedShards.contains(shardIdx) || FailedShards.contains(shardIdx)) {
                return false;
            }
            if (success) {
                CompletedShards.insert(shardIdx);
            } else {
                FailedShards.insert(shardIdx);
                if (!retriable) {
                    HasNonRetriableFailure = true;
                }
            }
            return true;
        }
    };

    // Pending sub-op dispatched in batches bounded by MaxIncrementalRestoreTablesInFlight.
    // Both Table and Index sub-ops share the queue so indexes don't fan out unbounded.
    struct TPendingRestoreOp {
        enum class EKind { Table, Index };
        EKind Kind = EKind::Table;

        TString BackupName;
        TString TablePath;

        TString IndexName;
        TString TargetTablePath;
        TString SpecificImplTableName;
    };

    // Sorted by timestamp.
    TVector<TIncrementalBackup> IncrementalBackups;
    ui32 CurrentIncrementalIdx = 0;
    bool CurrentIncrementalStarted = false;

    bool RetryNeeded = false;

    // Two-tier wall-clock deadline anchors. Persisted across reboots so the
    // budget cannot be defeated by a reboot loop.
    TInstant RestoreStartedAt;
    TInstant CurrentStageStartedAt;

    // Two-phase backoff guard: set when a retry is scheduled, cleared when it fires.
    // Prevents concurrent completion events from double-counting retries.
    bool RetryScheduled = false;
    TInstant NextRetryAttemptAt;

    bool NonRetriableFailure = false;

    // Set by TTxInit when retryNeeded=true. The first post-reboot retry-fire
    // absorbs failed sub-ops as completed rather than re-dispatching (pre-reboot
    // scan data is authoritative; re-dispatch won't get a fresh reply).
    bool FreshBootRetryAbsorbPending = false;

    THashSet<TOperationId> InProgressOperations;
    THashSet<TOperationId> CompletedOperations;

    THashMap<TOperationId, TTableOperationState> TableOperations;

    struct TPerShardDispatch {
        TPathId SrcPathId;
        TPathId DstPathId;
        ui64 SchemeShardGeneration = 0;
        THashMap<TShardIdx, TTabletId> ShardTablets;
    };
    THashMap<TOperationId, TPerShardDispatch> ShardDispatchByOp;

    // Populated by ProcessNextIncrementalBackup, drained by DispatchPendingIncrementalRestoreTables.
    // In-memory only; rebuilt after reboot from backup-collection contents.
    TDeque<TPendingRestoreOp> PendingTables;

    // ui32 maps to Ydb::StatusIds::StatusCode (0 == STATUS_CODE_UNSPECIFIED);
    // typed here to avoid pulling Ydb::StatusIds into the header.
    ui32 FinalStatus = 0;
    TString FinalIssues;

    // Per-sub-op tracking. Items move from PendingItems to InFlightItems once
    // TxAllocatorClient supplies a TxId.
    struct TItem {
        enum class EKind : ui32 {
            Table = 0,
            Index = 1,
            Finalize = 2,
        };
        ui32 ItemSeq = 0;
        EKind Kind = EKind::Table;
        TPathId TablePathId;
        TPathId SrcTablePathId; // 0/0 for Finalize items or unresolved src path
        // 0 == awaiting allocation.
        ui64 WaitTxId = 0;

        // Filled in and sent once TxAllocatorClient replies. In-memory only;
        // rebuilt on reboot via the orchestrator's re-entry path.
        TAutoPtr<NActors::IEventBase> PendingRequest;
    };

    ui32 NextItemSeq = 0;
    TDeque<TItem> PendingItems;
    THashMap<ui32 /*ItemSeq*/, TItem> InFlightItems;
    THashMap<ui64 /*WaitTxId*/, ui32 /*ItemSeq*/> WaitTxIdToItemSeq;

    // Returns progress within the current incremental as a fraction [0.0, 1.0]
    // based on per-shard completion across all table operations
    float CalcCurrentIncrementalProgress() const {
        ui32 totalShards = 0;
        ui32 doneShards = 0;
        for (const auto& [_, tableOp] : TableOperations) {
            totalShards += tableOp.ExpectedShards.size();
            doneShards += tableOp.CompletedShards.size() + tableOp.FailedShards.size();
        }
        if (totalShards == 0) {
            return 0.0f;
        }
        return static_cast<float>(doneShards) / totalShards;
    }

    bool AllIncrementsProcessed() const {
        return CurrentIncrementalIdx >= IncrementalBackups.size();
    }

    bool IsCurrentIncrementalComplete() const {
        return CurrentIncrementalIdx < IncrementalBackups.size() &&
                IncrementalBackups[CurrentIncrementalIdx].Completed;
    }

    bool AreAllCurrentOperationsComplete() const {
        // If we started processing the current incremental but there are no operations at all,
        // it means no table backups were found in this incremental backup, so consider it complete
        // TODO: probably have to ensure that empty backups are impossible
        if (CurrentIncrementalStarted && InProgressOperations.empty() && CompletedOperations.empty()
            && PendingTables.empty() && PendingItems.empty()) {
            return true;
        }
        // All in-progress ops completed and no sub-ops still queued or awaiting allocation.
        return InProgressOperations.empty()
            && PendingTables.empty()
            && PendingItems.empty()
            && !CompletedOperations.empty();
    }

    void MarkCurrentIncrementalComplete() {
        if (CurrentIncrementalIdx < IncrementalBackups.size()) {
            IncrementalBackups[CurrentIncrementalIdx].Completed = true;
        }
    }

    void MoveToNextIncremental() {
        if (CurrentIncrementalIdx < IncrementalBackups.size()) {
            CurrentIncrementalIdx++;
            CurrentIncrementalStarted = false;

            InProgressOperations.clear();
            CompletedOperations.clear();
            // TableOperations.clear() also clears HasNonRetriableFailure on each table.
            TableOperations.clear();
            PendingTables.clear();

            // Stage deadline anchor reset is owned by the caller (HandleAllOperationsComplete)
            // because it needs ctx.Now() and a paired db.Update.
            RetryScheduled = false;
            NextRetryAttemptAt = TInstant::Zero();
            NonRetriableFailure = false;
        }
    }

    const TIncrementalBackup* GetCurrentIncremental() const {
        if (CurrentIncrementalIdx < IncrementalBackups.size()) {
            return &IncrementalBackups[CurrentIncrementalIdx];
        }

        return nullptr;
    }

    void AddIncrementalBackup(const TPathId& pathId, const TString& path, ui64 timestamp) {
        IncrementalBackups.emplace_back(pathId, path, timestamp);

        // Sort by timestamp to ensure chronological order
        std::sort(IncrementalBackups.begin(), IncrementalBackups.end(),
                    [](const TIncrementalBackup& a, const TIncrementalBackup& b) {
                        return a.Timestamp < b.Timestamp;
                    });
    }

    void AddCurrentIncrementalOperation(const TOperationId& opId) {
        InProgressOperations.insert(opId);
    }

    void MarkOperationComplete(const TOperationId& opId) {
        InProgressOperations.erase(opId);
        CompletedOperations.insert(opId);
    }

    bool AllCurrentIncrementalOperationsComplete() const {
        return InProgressOperations.empty() && !CompletedOperations.empty();
    }
};

}
}
