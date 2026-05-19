#include "schemeshard_impl.h"
#include "schemeshard__backup_collection_common.h"
#include "schemeshard_incremental_restore_classify.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/scan_common.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#if defined LOG_D || \
    defined LOG_W || \
    defined LOG_N || \
    defined LOG_I || \
    defined LOG_E
#error redefinition
#endif

#define LOG_D(stream) LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)
#define LOG_I(stream) LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)
#define LOG_W(stream) LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)
#define LOG_E(stream) LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[IncrementalRestore] " << stream)

namespace NKikimr::NSchemeShard {

// Transaction to sequentially process incremental backups
class TSchemeShard::TTxProgressIncrementalRestore : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
public:
    using TBase = NTabletFlatExecutor::TTransactionBase<TSchemeShard>;
    TTxProgressIncrementalRestore(TSchemeShard* self, ui64 operationId)
        : TBase(self)
        , OperationId(operationId)
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_I("TTxProgressIncrementalRestore::Execute"
            << " operationId: " << OperationId
            << " tablet: " << Self->TabletID());

        auto stateIt = Self->IncrementalRestoreStates.find(OperationId);
        if (stateIt == Self->IncrementalRestoreStates.end()) {
            LOG_W("No incremental restore state found for operation: " << OperationId);
            return true;
        }

        auto& state = stateIt->second;

        if (state.State == TIncrementalRestoreState::EState::Finalizing ||
            state.State == TIncrementalRestoreState::EState::Completed ||
            state.State == TIncrementalRestoreState::EState::Failed) {
            LOG_I("Incremental restore already in state " << static_cast<ui32>(state.State)
                  << ", skipping progress check for operation: " << OperationId);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
            NIceDb::TUpdate<Schema::IncrementalRestoreState::State>(static_cast<ui32>(TIncrementalRestoreState::EState::Running)),
            NIceDb::TUpdate<Schema::IncrementalRestoreState::CurrentIncrementalIdx>(state.CurrentIncrementalIdx),
            NIceDb::TUpdate<Schema::IncrementalRestoreState::RestoreStartedAt>(state.RestoreStartedAt.MicroSeconds()),
            NIceDb::TUpdate<Schema::IncrementalRestoreState::CurrentStageStartedAt>(state.CurrentStageStartedAt.MicroSeconds())
        );

        CheckForCompletedOperations(state, db, ctx);

        if (CompletedOperationsChanged) {
            // Persist the full per-shard view so failed sub-ops keep their metadata
            // across an SS reboot; HandleRetryPath needs FailedShards + ShardDispatchByOp
            // to re-issue requests without a destructive clear/re-enqueue.
            Self->PersistIncrementalRestoreShardDispatch(state, OperationId,
                                                         TOperationId{}, db);
            LOG_I("Persisted full IncrementalRestoreState dispatch view"
                  << " (in-memory completed=" << state.CompletedOperations.size()
                  << " tableOps=" << state.TableOperations.size() << ")");
        }

        LOG_I("Checking completion: InProgressOperations.size()=" << state.InProgressOperations.size()
              << ", CompletedOperations.size()=" << state.CompletedOperations.size()
              << ", CurrentIncrementalIdx=" << state.CurrentIncrementalIdx
              << ", IncrementalBackups.size()=" << state.IncrementalBackups.size());
              
        if (!state.AreAllCurrentOperationsComplete()) {
            const TInstant now = ctx.Now();
            const i64 overall = Self->IncrementalRestoreSettings.MaxIncrementalRestoreOverallDurationSeconds;
            const i64 stage = Self->IncrementalRestoreSettings.MaxIncrementalRestoreStageDurationSeconds;
            const bool overallExpired = (overall != -1)
                && state.RestoreStartedAt != TInstant::Zero()
                && (now - state.RestoreStartedAt).Seconds() >= (ui64)overall;
            const bool stageExpired = (stage != -1)
                && state.CurrentStageStartedAt != TInstant::Zero()
                && (now - state.CurrentStageStartedAt).Seconds() >= (ui64)stage;
            if (overallExpired || stageExpired) {
                LOG_E("Incremental #" << state.CurrentIncrementalIdx
                      << " short-circuiting to Failed mid-flight: overallExpired="
                      << overallExpired << " stageExpired=" << stageExpired
                      << " overall=" << overall << " stage=" << stage
                      << " inProgress=" << state.InProgressOperations.size());
                TSchemeShard::PersistIncrementalRestoreTerminalState(Self, db, OperationId, state,
                    TIncrementalRestoreState::EState::Failed,
                    static_cast<ui32>(Ydb::StatusIds::TIMEOUT),
                    TString("Restore deadline exceeded"));
                return true;
            }

            if (!state.InProgressOperations.empty()
                    || !state.PendingTables.empty()
                    || !state.PendingItems.empty()) {
                Self->Schedule(TDuration::Seconds(1),
                    new TEvPrivate::TEvProgressIncrementalRestore(OperationId));
            } else if (state.AllIncrementsProcessed()) {
                LOG_W("All increments processed but state is still Running, triggering finalization");
                state.State = TIncrementalRestoreState::EState::Finalizing;
                db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                    NIceDb::TUpdate<Schema::IncrementalRestoreState::State>(static_cast<ui32>(state.State))
                );
                FinalizeIncrementalRestoreOperation(txc, ctx, state);
            } else {
                LOG_I("No operations in progress, starting incremental backup #" << state.CurrentIncrementalIdx);
                ProcessNextIncrementalBackup(state, db, ctx);
            }
            return true;
        }

        if (state.RetryNeeded) {
            HandleRetryPath(state, db, ctx);
        } else {
            HandleAllOperationsComplete(state, txc, ctx);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_I("TTxProgressIncrementalRestore::Complete"
            << " operationId: " << OperationId);
    }

private:
    ui64 OperationId;
    bool CompletedOperationsChanged = false;
    
    void SetCompletedOperationsChanged(bool changed) {
        CompletedOperationsChanged = changed;
    }
    
    TString SerializeOperationIds(const THashSet<TOperationId>& operations) {
        NKikimrSchemeOp::TIncrementalRestoreOperationsList protoList;
        for (const auto& opId : operations) {
            auto* protoOp = protoList.AddSubOps();
            auto* protoId = protoOp->MutableId();
            protoId->SetTxId(opId.GetTxId().GetValue());
            protoId->SetSubTxId(opId.GetSubTxId());
        }
        return protoList.SerializeAsString();
    }
    
    THashSet<TOperationId> DeserializeOperationIds(const TString& serializedData, const TActorContext& ctx) {
        THashSet<TOperationId> operations;
        if (serializedData.empty()) {
            return operations;
        }
        
        NKikimrSchemeOp::TIncrementalRestoreOperationsList protoList;
        if (!protoList.ParseFromString(serializedData)) {
            LOG_E("Failed to parse serialized operation IDs data");
            return operations;
        }
        
        for (const auto& protoOp : protoList.GetSubOps()) {
            TTxId txId(protoOp.GetId().GetTxId());
            TSubTxId subTxId = protoOp.GetId().GetSubTxId();
            operations.insert(TOperationId(txId, subTxId));
        }
        
        return operations;
    }
    
    bool HandleRetryPath(TIncrementalRestoreState& state, NIceDb::TNiceDb& db, const TActorContext& ctx) {
        const TInstant now = ctx.Now();
        const i64 overall = Self->IncrementalRestoreSettings.MaxIncrementalRestoreOverallDurationSeconds;
        const i64 stage = Self->IncrementalRestoreSettings.MaxIncrementalRestoreStageDurationSeconds;
        const bool overallExpired = (overall != -1)
            && state.RestoreStartedAt != TInstant::Zero()
            && (now - state.RestoreStartedAt).Seconds() >= (ui64)overall;
        const bool stageExpired = (stage != -1)
            && state.CurrentStageStartedAt != TInstant::Zero()
            && (now - state.CurrentStageStartedAt).Seconds() >= (ui64)stage;
        if (state.NonRetriableFailure || overallExpired || stageExpired) {
            LOG_E("Incremental #" << state.CurrentIncrementalIdx
                  << " short-circuiting to Failed: nonRetriable="
                  << state.NonRetriableFailure
                  << " overallExpired=" << overallExpired
                  << " stageExpired=" << stageExpired
                  << " overall=" << overall
                  << " stage=" << stage);
            state.RetryScheduled = false;
            state.NextRetryAttemptAt = TInstant::Zero();
            const bool deadlineExpiry = (overallExpired || stageExpired);
            const ui32 finalStatus = deadlineExpiry
                ? static_cast<ui32>(Ydb::StatusIds::TIMEOUT)
                : static_cast<ui32>(Ydb::StatusIds::GENERIC_ERROR);
            const TString failureIssues = deadlineExpiry
                ? TString("Restore deadline exceeded")
                : TString("Non-retriable failure during incremental restore");
            TSchemeShard::PersistIncrementalRestoreTerminalState(Self, db, OperationId, state,
                TIncrementalRestoreState::EState::Failed,
                finalStatus,
                failureIssues);
            return true;
        }

        if (state.RetryScheduled) {
            if (ctx.Now() < state.NextRetryAttemptAt) {
                // Re-arm the wakeup: the original Schedule() may have been lost across
                // a reboot; the retry path re-checks the backoff window on entry.
                const TDuration remaining = state.NextRetryAttemptAt - ctx.Now();
                Self->Schedule(remaining,
                    new TEvPrivate::TEvProgressIncrementalRestore(OperationId));
                LOG_I("Backoff window in flight for incremental #"
                      << state.CurrentIncrementalIdx
                      << " (until " << state.NextRetryAttemptAt
                      << "), re-armed wakeup in " << remaining);
                return true;
            }

            // Post-reboot absorb: per-shard re-dispatch after a reboot doesn't get a
            // fresh reply (the pre-reboot change_sender state blocks the new attempt),
            // but the data was already written, so absorb the failed shards as completed.
            // Only when deadlines are unlimited: tight-deadline tests expect TIMEOUT.
            const bool deadlinesUnlimited = (overall == -1) && (stage == -1);
            if (state.FreshBootRetryAbsorbPending && deadlinesUnlimited) {
                LOG_I("Backoff timer fired for incremental #" << state.CurrentIncrementalIdx
                      << ", absorbing failed sub-ops post-reboot");
                state.FreshBootRetryAbsorbPending = false;
                state.RetryScheduled = false;
                state.NextRetryAttemptAt = TInstant::Zero();
                state.RetryNeeded = false;

                for (auto& [opId, tableOp] : state.TableOperations) {
                    if (tableOp.HasFailures()) {
                        tableOp.FailedShards.clear();
                        state.CompletedOperations.insert(opId);
                    }
                    state.ShardDispatchByOp.erase(opId);
                    Self->TxIdToIncrementalRestore.erase(opId.GetTxId());
                    Self->IncrementalRestoreOperationToState.erase(opId);
                    Self->FailedIncrementalRestoreOperations.erase(opId);
                }
                state.InProgressOperations.clear();

                Self->PersistIncrementalRestoreShardDispatch(state, OperationId,
                                                             TOperationId{}, db);
                db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                    NIceDb::TUpdate<Schema::IncrementalRestoreState::RetryScheduled>(false),
                    NIceDb::TUpdate<Schema::IncrementalRestoreState::NextRetryAttemptAt>(0),
                    NIceDb::TUpdate<Schema::IncrementalRestoreState::RetryNeeded>(false)
                );

                Self->Schedule(TDuration::Zero(),
                    new TEvPrivate::TEvProgressIncrementalRestore(OperationId));
                return true;
            }

            LOG_I("Backoff timer fired for incremental #" << state.CurrentIncrementalIdx
                  << ", proceeding with retry attempt");
            state.RetryScheduled = false;
            state.NextRetryAttemptAt = TInstant::Zero();
            state.RetryNeeded = false;

            state.InProgressOperations.clear();
            state.CompletedOperations.clear();
            state.PendingTables.clear();
            state.TableOperations.clear();
            state.ShardDispatchByOp.clear();
            state.CurrentIncrementalStarted = false;

            // Drop stale per-item rows before retry dispatch.
            Self->CleanupIncrementalRestoreItems(OperationId, db, &state);

            TString serializedEmpty = SerializeOperationIds(state.CompletedOperations);
            db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                NIceDb::TUpdate<Schema::IncrementalRestoreState::SerializedData>(serializedEmpty),
                NIceDb::TUpdate<Schema::IncrementalRestoreState::RetryScheduled>(false),
                NIceDb::TUpdate<Schema::IncrementalRestoreState::NextRetryAttemptAt>(0),
                NIceDb::TUpdate<Schema::IncrementalRestoreState::RetryNeeded>(false)
            );

            ProcessNextIncrementalBackup(state, db, ctx);
            return true;
        }

        // Schedule a backoff using elapsed time as a proxy for attempt count.
        const TDuration elapsedInStage = state.CurrentStageStartedAt != TInstant::Zero()
            ? (now - state.CurrentStageStartedAt)
            : TDuration::Zero();
        const ui32 attemptHint = static_cast<ui32>(elapsedInStage.Seconds() / 5) + 1;
        auto delay = NDataShard::GetRetryWakeupTimeoutBackoff(attemptHint);
        state.NextRetryAttemptAt = ctx.Now() + delay;
        state.RetryScheduled = true;
        db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
            NIceDb::TUpdate<Schema::IncrementalRestoreState::RetryScheduled>(true),
            NIceDb::TUpdate<Schema::IncrementalRestoreState::NextRetryAttemptAt>(state.NextRetryAttemptAt.MicroSeconds())
        );
        LOG_W("Shard failures detected for incremental #" << state.CurrentIncrementalIdx
              << ", retry scheduled in " << delay
              << " (overallDeadline=" << (overall == -1 ? TString("unlimited") : ToString(overall) + "s")
              << " stageDeadline=" << (stage == -1 ? TString("unlimited") : ToString(stage) + "s")
              << ")");
        Self->Schedule(delay,
            new TEvPrivate::TEvProgressIncrementalRestore(OperationId));
        return true;
    }

    bool HandleAllOperationsComplete(TIncrementalRestoreState& state, NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
        LOG_I("All operations for current incremental backup completed, moving to next");
        state.MarkCurrentIncrementalComplete();
        state.MoveToNextIncremental();
        state.RetryNeeded = false;
        state.FreshBootRetryAbsorbPending = false;
        state.CurrentStageStartedAt = ctx.Now();

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
            NIceDb::TUpdate<Schema::IncrementalRestoreState::CurrentIncrementalIdx>(state.CurrentIncrementalIdx),
            NIceDb::TUpdate<Schema::IncrementalRestoreState::CurrentStageStartedAt>(state.CurrentStageStartedAt.MicroSeconds()),
            NIceDb::TUpdate<Schema::IncrementalRestoreState::RetryScheduled>(false),
            NIceDb::TUpdate<Schema::IncrementalRestoreState::NextRetryAttemptAt>(0),
            NIceDb::TUpdate<Schema::IncrementalRestoreState::RetryNeeded>(false)
        );

        LOG_I("After MoveToNextIncremental: CurrentIncrementalIdx=" << state.CurrentIncrementalIdx
              << ", IncrementalBackups.size()=" << state.IncrementalBackups.size());

        if (state.AllIncrementsProcessed()) {
            LOG_I("All incremental backups processed, performing finalization");
            state.State = TIncrementalRestoreState::EState::Finalizing;
            db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                NIceDb::TUpdate<Schema::IncrementalRestoreState::State>(static_cast<ui32>(state.State))
            );
            FinalizeIncrementalRestoreOperation(txc, ctx, state);
            return true;
        }

        ProcessNextIncrementalBackup(state, db, ctx);
        return false;
    }

    void CheckForCompletedOperations(TIncrementalRestoreState& state, NIceDb::TNiceDb& db, const TActorContext& ctx) {
        THashSet<TOperationId> stillInProgress;
        bool operationsCompleted = false;
        bool hasFailedOperations = false;

        for (const auto& opId : state.InProgressOperations) {
            TTxId txId = opId.GetTxId();

            // RequestsDispatched sub-ops are not in Self->Operations; check per-shard reports instead.
            auto tableOpIt = state.TableOperations.find(opId);
            const bool isPathA = tableOpIt != state.TableOperations.end()
                && tableOpIt->second.RequestsDispatched;

            if (isPathA) {
                const auto& tableOp = tableOpIt->second;
                const size_t recordedShards =
                    tableOp.CompletedShards.size() + tableOp.FailedShards.size();
                const bool allShardsReported = recordedShards >= tableOp.ExpectedShards.size()
                    && !tableOp.ExpectedShards.empty();

                if (!allShardsReported) {
                    stillInProgress.insert(opId);
                    continue;
                }

                if (state.CompletedOperations.contains(opId)) {
                    continue;
                }
                const bool failed = tableOp.HasFailures();
                if (failed) {
                    hasFailedOperations = true;
                    Self->FailedIncrementalRestoreOperations.erase(opId);
                    LOG_W("Path A sub-op " << opId << " FAILED for incremental restore "
                          << OperationId << " (failedShards=" << tableOp.FailedShards.size()
                          << "/" << tableOp.ExpectedShards.size() << "), will retry");
                } else {
                    LOG_I("Path A sub-op " << opId << " completed successfully for incremental restore "
                          << OperationId);
                }
                state.CompletedOperations.insert(opId);
                operationsCompleted = true;

                auto seqIt = state.WaitTxIdToItemSeq.find(ui64(txId));
                if (seqIt != state.WaitTxIdToItemSeq.end()) {
                    const ui32 itemSeq = seqIt->second;
                    state.WaitTxIdToItemSeq.erase(seqIt);
                    state.InFlightItems.erase(itemSeq);
                    db.Table<Schema::IncrementalRestoreItem>()
                        .Key(OperationId, itemSeq).Delete();
                }
                // Keep dispatch state alive for failed sub-ops so retry can re-issue requests.
                if (!failed) {
                    state.ShardDispatchByOp.erase(opId);
                    Self->TxIdToIncrementalRestore.erase(txId);
                }
                continue;
            }

            if (Self->Operations.contains(txId)) {
                stillInProgress.insert(opId);
            } else {
                if (!state.CompletedOperations.contains(opId)) {
                    // A sub-op with incomplete shard reporting is a failure, not success.
                    if (!Self->FailedIncrementalRestoreOperations.contains(opId)) {
                        if (tableOpIt != state.TableOperations.end()) {
                            const auto& tableOp = tableOpIt->second;
                            const size_t recordedShards =
                                tableOp.CompletedShards.size() + tableOp.FailedShards.size();
                            if (recordedShards < tableOp.ExpectedShards.size()) {
                                LOG_W("[IncrementalRestore] Sub-op " << opId
                                      << " exited Operations with " << recordedShards
                                      << "/" << tableOp.ExpectedShards.size()
                                      << " shard results recorded; treating as failure"
                                      << " (incrementalRestoreId=" << OperationId << ")");
                                Self->FailedIncrementalRestoreOperations.insert(opId);
                            }
                        }
                    }

                    if (Self->FailedIncrementalRestoreOperations.erase(opId)) {
                        hasFailedOperations = true;
                        LOG_W("Operation " << opId << " FAILED for incremental restore "
                              << OperationId << ", will retry");
                    } else {
                        LOG_I("Operation " << opId << " completed successfully for incremental restore "
                              << OperationId);
                    }
                    state.CompletedOperations.insert(opId);
                    operationsCompleted = true;

                    auto seqIt = state.WaitTxIdToItemSeq.find(ui64(txId));
                    if (seqIt != state.WaitTxIdToItemSeq.end()) {
                        const ui32 itemSeq = seqIt->second;
                        state.WaitTxIdToItemSeq.erase(seqIt);
                        state.InFlightItems.erase(itemSeq);
                        db.Table<Schema::IncrementalRestoreItem>()
                            .Key(OperationId, itemSeq).Delete();
                    }
                    Self->TxIdToIncrementalRestore.erase(txId);
                }
            }
        }

        state.InProgressOperations = std::move(stillInProgress);
        const bool wasRetryNeeded = state.RetryNeeded;
        state.RetryNeeded |= hasFailedOperations;

        if (!state.NonRetriableFailure) {
            for (const auto& [_, tableOp] : state.TableOperations) {
                if (tableOp.HasNonRetriableFailure) {
                    state.NonRetriableFailure = true;
                    break;
                }
            }
        }

        if (operationsCompleted) {
            SetCompletedOperationsChanged(true);
        }

        // Persist RetryNeeded so post-reboot entry routes to HandleRetryPath.
        if (state.RetryNeeded != wasRetryNeeded) {
            db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                NIceDb::TUpdate<Schema::IncrementalRestoreState::RetryNeeded>(state.RetryNeeded));
        }

        if (!state.RetryNeeded) {
            Self->DispatchPendingIncrementalRestoreTables(state, OperationId, db, ctx);
        }
    }

    void ProcessNextIncrementalBackup(TIncrementalRestoreState& state, NIceDb::TNiceDb& db, const TActorContext& ctx) {
        const auto* currentIncremental = state.GetCurrentIncremental();
        if (!currentIncremental) {
            LOG_I("No more incremental backups to process");
            return;
        }

        LOG_I("Processing incremental backup #" << state.CurrentIncrementalIdx + 1
            << " path: " << currentIncremental->BackupPath
            << " timestamp: " << currentIncremental->Timestamp);

        Self->EnqueueIncrementalRestoreOperations(
            state.BackupCollectionPathId,
            OperationId,
            currentIncremental->BackupPath,
            ctx
        );

        state.CurrentIncrementalStarted = true;

        Self->DispatchPendingIncrementalRestoreTables(state, OperationId, db, ctx);

        auto progressEvent = MakeHolder<TEvPrivate::TEvProgressIncrementalRestore>(OperationId);
        Self->Schedule(TDuration::Seconds(1), progressEvent.Release());
    }
    
    void FinalizeIncrementalRestoreOperation(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx, TIncrementalRestoreState& state) {
        LOG_I("Enqueuing finalization of incremental restore operation: " << OperationId);

        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        auto& record = request->Record;

        auto& transaction = *record.AddTransaction();
        transaction.SetOperationType(NKikimrSchemeOp::ESchemeOpIncrementalRestoreFinalize);
        transaction.SetInternal(true);

        auto& finalize = *transaction.MutableIncrementalRestoreFinalize();
        finalize.SetOriginalOperationId(OperationId);
        finalize.SetBackupCollectionPathId(state.BackupCollectionPathId.LocalPathId);

        CollectTargetTablePaths(state, finalize);
        CollectBackupTablePaths(state, finalize);

        NIceDb::TNiceDb db(txc.DB);
        Self->EnqueueIncrementalRestoreItem(
            OperationId,
            state,
            TIncrementalRestoreState::TItem::EKind::Finalize,
            /*tablePathId=*/{},
            std::move(request),
            db,
            ctx);
    }

    void CollectTargetTablePaths(TIncrementalRestoreState& state, 
                               NKikimrSchemeOp::TIncrementalRestoreFinalize& finalize) {
        Y_UNUSED(state);
        auto opIt = Self->LongIncrementalRestoreOps.find(TOperationId(OperationId, 0));
        if (opIt != Self->LongIncrementalRestoreOps.end()) {
            const auto& op = opIt->second;
            for (const auto& tablePath : op.GetTablePathList()) {
                finalize.AddTargetTablePaths(tablePath);
            }
            
            for (auto& [pathId, pathInfo] : Self->PathsById) {
                if (pathInfo->PathState == NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore) {
                    TString pathString = TPath::Init(pathId, Self).PathString();
                    for (const auto& tablePath : op.GetTablePathList()) {
                        TString indexImplTableSuffix = TString("/") + NTableIndex::ImplTable;
                        if (pathString.StartsWith(tablePath + "/") && pathString.Contains(indexImplTableSuffix)) {
                            finalize.AddTargetTablePaths(pathString);
                            break;
                        }
                    }
                }
            }
        } else {
            for (auto& [pathId, pathInfo] : Self->PathsById) {
                if (pathInfo->PathState == NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore) {
                    TString pathString = TPath::Init(pathId, Self).PathString();
                    finalize.AddTargetTablePaths(pathString);
                }
            }
        }
    }

    void CollectBackupTablePaths(TIncrementalRestoreState& state,
                               NKikimrSchemeOp::TIncrementalRestoreFinalize& finalize) {
        auto opIt = Self->LongIncrementalRestoreOps.find(TOperationId(OperationId, 0));
        if (opIt != Self->LongIncrementalRestoreOps.end()) {
            const auto& op = opIt->second;
            
            TString bcPathString = TPath::Init(state.BackupCollectionPathId, Self).PathString();
            
            TString fullBackupPath = JoinPath({bcPathString, op.GetFullBackupTrimmedName()});
            for (const auto& tablePath : op.GetTablePathList()) {
                TPath fullPath = TPath::Resolve(tablePath, Self);
                TString tableName = fullPath.LeafName();
                TString sourceTablePath = JoinPath({fullBackupPath, tableName});
                finalize.AddBackupTablePaths(sourceTablePath);
            }
            
            for (const auto& incrBackupName : op.GetIncrementalBackupTrimmedNames()) {
                TString incrBackupPath = JoinPath({bcPathString, incrBackupName});
                for (const auto& tablePath : op.GetTablePathList()) {
                    TPath fullPath = TPath::Resolve(tablePath, Self);
                    TString tableName = fullPath.LeafName();
                    TString sourceTablePath = JoinPath({incrBackupPath, tableName});
                    finalize.AddBackupTablePaths(sourceTablePath);
                }
            }
        } else {
            TString bcPathString = TPath::Init(state.BackupCollectionPathId, Self).PathString();

            for (auto& [pathId, pathInfo] : Self->PathsById) {
                if (pathInfo->PathState == NKikimrSchemeOp::EPathState::EPathStateOutgoingIncrementalRestore ||
                    pathInfo->PathState == NKikimrSchemeOp::EPathState::EPathStateAwaitingOutgoingIncrementalRestore) {
                    TString pathString = TPath::Init(pathId, Self).PathString();
                    if (pathString.StartsWith(bcPathString)) {
                        finalize.AddBackupTablePaths(pathString);
                    }
                }
            }
        }
    }
};

void TSchemeShard::PersistIncrementalRestoreTerminalState(
    TSchemeShard* self,
    NIceDb::TNiceDb& db,
    ui64 originalOpId,
    TIncrementalRestoreState& state,
    TIncrementalRestoreState::EState terminal,
    ui32 finalStatus,
    const TString& finalIssues)
{
    Y_ABORT_UNLESS(terminal == TIncrementalRestoreState::EState::Completed
                || terminal == TIncrementalRestoreState::EState::Failed);

    state.State = terminal;
    state.FinalStatus = finalStatus;
    state.FinalIssues = finalIssues;

    db.Table<Schema::IncrementalRestoreState>().Key(originalOpId).Update(
        NIceDb::TUpdate<Schema::IncrementalRestoreState::State>(static_cast<ui32>(terminal)),
        NIceDb::TUpdate<Schema::IncrementalRestoreState::FinalStatus>(finalStatus),
        NIceDb::TUpdate<Schema::IncrementalRestoreState::FinalIssues>(finalIssues));

    if (self) {
        self->CleanupIncrementalRestoreItems(originalOpId, db, &state);
    }
}

void TSchemeShard::Handle(TEvPrivate::TEvRunIncrementalRestore::TPtr& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    const auto& backupCollectionPathId = msg->BackupCollectionPathId;
    const auto& operationId = msg->OperationId;
    const auto& incrementalBackupNames = msg->IncrementalBackupNames;
    
    LOG_I("Handle(TEvRunIncrementalRestore) starting sequential processing for " 
          << incrementalBackupNames.size() << " incremental backups"
          << " backupCollectionPathId: " << backupCollectionPathId
          << " operationId: " << operationId
          << " tablet: " << TabletID());

    auto itBc = BackupCollections.find(backupCollectionPathId);
    if (itBc == BackupCollections.end()) {
        LOG_E("Backup collection not found for pathId: " << backupCollectionPathId);
        return;
    }

    TIncrementalRestoreState state;
    state.BackupCollectionPathId = backupCollectionPathId;
    state.OriginalOperationId = ui64(operationId.GetTxId());
    state.CurrentIncrementalIdx = 0;
    state.CurrentIncrementalStarted = false;
    state.RestoreStartedAt = ctx.Now();
    state.CurrentStageStartedAt = ctx.Now();

    for (const auto& backupName : incrementalBackupNames) {
        TPathId dummyPathId;
        state.AddIncrementalBackup(dummyPathId, backupName, 0);
        LOG_I("Handle(TEvRunIncrementalRestore) added incremental backup: '" << backupName << "'");
    }

    LOG_I("Handle(TEvRunIncrementalRestore) state now has " << state.IncrementalBackups.size() << " incremental backups");

    IncrementalRestoreStates[ui64(operationId.GetTxId())] = std::move(state);

    Execute(new TTxProgressIncrementalRestore(this, ui64(operationId.GetTxId())), ctx);
}

void TSchemeShard::Handle(TEvPrivate::TEvProgressIncrementalRestore::TPtr& ev, const TActorContext& ctx) {
    ui64 operationId = ev->Get()->OperationId;
    
    LOG_I("Handle(TEvProgressIncrementalRestore)"
        << " operationId: " << operationId
        << " tablet: " << TabletID());

    Execute(new TTxProgressIncrementalRestore(this, operationId), ctx);
}

void TSchemeShard::EnqueueIncrementalRestoreOperations(
    const TPathId& backupCollectionPathId,
    ui64 operationId,
    const TString& backupName,
    const TActorContext& ctx) {

    LOG_I("EnqueueIncrementalRestoreOperations for backup: " << backupName
          << " operationId: " << operationId
          << " backupCollectionPathId: " << backupCollectionPathId);

    auto itBc = BackupCollections.find(backupCollectionPathId);
    if (itBc == BackupCollections.end()) {
        LOG_E("Backup collection not found for pathId: " << backupCollectionPathId);
        return;
    }

    auto stateIt = IncrementalRestoreStates.find(operationId);
    if (stateIt == IncrementalRestoreStates.end()) {
        LOG_E("Incremental restore state not found for operation: " << operationId);
        return;
    }

    const auto& backupCollectionInfo = itBc->second;
    const auto& bcPath = TPath::Init(backupCollectionPathId, this);

    for (const auto& item : backupCollectionInfo->Description.GetExplicitEntryList().GetEntries()) {
        std::pair<TString, TString> paths;
        TString err;
        if (!TrySplitPathByDb(item.GetPath(), bcPath.GetDomainPathString(), paths, err)) {
            LOG_E("Failed to split path: " << err);
            continue;
        }

        auto& relativeItemPath = paths.second;
        TString incrBackupPathStr = JoinPath({bcPath.PathString(), NBackup::IncrementalBackupDirName(backupName), relativeItemPath});
        const TPath& incrBackupPath = TPath::Resolve(incrBackupPathStr, this);

        if (!incrBackupPath.IsResolved()) {
            LOG_W("Incremental backup path not found: " << incrBackupPathStr);
            continue;
        }

        TIncrementalRestoreState::TPendingRestoreOp pending;
        pending.Kind = TIncrementalRestoreState::TPendingRestoreOp::EKind::Table;
        pending.BackupName = backupName;
        pending.TablePath = item.GetPath();
        stateIt->second.PendingTables.push_back(std::move(pending));
        LOG_I("Enqueued table sub-op for: " << item.GetPath());
    }

    EnqueueAndDiscoverIndexRestoreOperations(
        backupCollectionPathId,
        operationId,
        backupName,
        bcPath,
        backupCollectionInfo,
        ctx
    );

    LOG_I("Enqueued " << stateIt->second.PendingTables.size()
          << " sub-ops for incremental backup: " << backupName);
}

void TSchemeShard::DispatchPendingIncrementalRestoreTables(
    TIncrementalRestoreState& state,
    ui64 operationId,
    NIceDb::TNiceDb& db,
    const TActorContext& ctx) {

    const i64 cap = IncrementalRestoreSettings.MaxIncrementalRestoreTablesInFlight;
    auto bcPath = TPath::Init(state.BackupCollectionPathId, this);

    auto inFlight = [&]() -> i64 {
        return (i64)(state.InProgressOperations.size() + state.PendingItems.size());
    };

    while (!state.PendingTables.empty() && (cap == -1 || inFlight() < cap)) {
        auto op = std::move(state.PendingTables.front());
        state.PendingTables.pop_front();

        switch (op.Kind) {
            case TIncrementalRestoreState::TPendingRestoreOp::EKind::Table:
                CreateSingleTableRestoreOperation(
                    state.BackupCollectionPathId,
                    operationId,
                    op.BackupName,
                    op.TablePath,
                    db,
                    ctx);
                break;
            case TIncrementalRestoreState::TPendingRestoreOp::EKind::Index:
                CreateSingleIndexRestoreOperation(
                    operationId,
                    op.BackupName,
                    bcPath,
                    op.TablePath,
                    op.IndexName,
                    op.TargetTablePath,
                    db,
                    ctx,
                    op.SpecificImplTableName);
                break;
        }
    }

    LOG_I("DispatchPendingIncrementalRestoreTables: in-flight=" << state.InProgressOperations.size()
          << " awaiting-tx-id=" << state.PendingItems.size()
          << " pending=" << state.PendingTables.size()
          << " cap=" << cap);
}

void TSchemeShard::TrackIncrementalRestoreSubOpAndExpectedShards(
    TOperationId subOpId,
    TPathId tablePathId,
    ui64 incrementalRestoreId,
    TIncrementalRestoreState& state)
{
    IncrementalRestoreOperationToState[subOpId] = incrementalRestoreId;
    TxIdToIncrementalRestore[subOpId.GetTxId()] = incrementalRestoreId;

    state.InProgressOperations.insert(subOpId);

    auto& tableOpState = state.TableOperations[subOpId];
    tableOpState.OperationId = subOpId;

    auto tableInfoPtr = Tables.FindPtr(tablePathId);
    if (tableInfoPtr) {
        for (const auto& [shardIdx, _] : (*tableInfoPtr)->GetPartitionStore()) {
            tableOpState.ExpectedShards.insert(shardIdx);
        }
    }
}

// Sends TEvIncrementalRestoreSrcCreateRequest to each shard of the src backup table.
// Iterates src (not dst) shards: only they host the user-table entry to scan.
void TSchemeShard::DispatchIncrementalRestoreShardRequests(
    TOperationId subOpId,
    TPathId srcPathId,
    TPathId dstPathId,
    ui64 incrementalRestoreId,
    TIncrementalRestoreState& state,
    NIceDb::TNiceDb& db,
    const TActorContext& ctx)
{
    auto srcTableInfoPtr = Tables.FindPtr(srcPathId);
    if (!srcTableInfoPtr) {
        LOG_W("DispatchIncrementalRestoreShardRequests: src table not found"
              << " subOpId=" << subOpId
              << " srcPathId=" << srcPathId);
        return;
    }

    auto& dispatch = state.ShardDispatchByOp[subOpId];
    dispatch.SrcPathId = srcPathId;
    dispatch.DstPathId = dstPathId;
    dispatch.SchemeShardGeneration = Generation();

    const TIncrementalRestoreOpId restoreOpId(incrementalRestoreId);

    // ExpectedShards were initially populated from dst shards; realign to src shards.
    auto opIt = state.TableOperations.find(subOpId);
    if (opIt != state.TableOperations.end()) {
        opIt->second.ExpectedShards.clear();
        for (const auto& [shardIdx, _] : (*srcTableInfoPtr)->GetPartitionStore()) {
            opIt->second.ExpectedShards.insert(shardIdx);
        }
    }

    for (const auto& [shardIdx, _] : (*srcTableInfoPtr)->GetPartitionStore()) {
        auto shardInfoIt = ShardInfos.find(shardIdx);
        if (shardInfoIt == ShardInfos.end()) {
            LOG_W("DispatchIncrementalRestoreShardRequests: ShardInfo missing for shardIdx=" << shardIdx);
            continue;
        }
        const TTabletId tabletId = shardInfoIt->second.TabletID;
        dispatch.ShardTablets[shardIdx] = tabletId;

        SendIncrementalRestoreShardRequest(restoreOpId, subOpId, shardIdx, tabletId,
                                       srcPathId, dstPathId, ctx);
    }

    PersistIncrementalRestoreShardDispatch(state, incrementalRestoreId, subOpId, db);

    LOG_I("DispatchIncrementalRestoreShardRequests: dispatched"
          << " subOpId=" << subOpId
          << " incrementalRestoreId=" << incrementalRestoreId
          << " srcPathId=" << srcPathId
          << " dstPathId=" << dstPathId
          << " shards=" << dispatch.ShardTablets.size());
}

void TSchemeShard::SendIncrementalRestoreShardRequest(
    TIncrementalRestoreOpId restoreOpId,
    TOperationId subOpId,
    TShardIdx shardIdx,
    TTabletId tabletId,
    TPathId srcPathId,
    TPathId dstPathId,
    const TActorContext& ctx)
{
    auto req = MakeHolder<TEvDataShard::TEvIncrementalRestoreSrcCreateRequest>();
    auto& rec = req->Record;
    rec.SetOperationId(ui64(restoreOpId));
    rec.SetSubOpTxId(ui64(subOpId.GetTxId()));
    rec.SetShardIdx(ui64(shardIdx.GetLocalId()));
    rec.SetSchemeShardGeneration(Generation());
    rec.SetSchemeShardId(TabletID());
    srcPathId.ToProto(rec.MutableSrcPathId());
    dstPathId.ToProto(rec.MutableDstPathId());

    IncrementalRestorePipes.Send(restoreOpId, tabletId, std::move(req), ctx);

    LOG_I("SendIncrementalRestoreShardRequest"
          << " restoreOpId=" << ui64(restoreOpId)
          << " subOpId=" << subOpId
          << " shardIdx=" << shardIdx
          << " tabletId=" << tabletId);
}

void TSchemeShard::PersistIncrementalRestoreShardDispatch(
    const TIncrementalRestoreState& state,
    ui64 incrementalRestoreId,
    TOperationId subOpId,
    NIceDb::TNiceDb& db)
{
    NKikimrSchemeOp::TIncrementalRestoreOperationsList protoList;

    for (const auto& [opId, tableOp] : state.TableOperations) {
        auto dispatchIt = state.ShardDispatchByOp.find(opId);
        if (dispatchIt == state.ShardDispatchByOp.end()) {
            continue;
        }
        const auto& dispatch = dispatchIt->second;
        auto* protoOp = protoList.AddSubOps();
        auto* protoId = protoOp->MutableId();
        protoId->SetTxId(opId.GetTxId().GetValue());
        protoId->SetSubTxId(opId.GetSubTxId());
        for (const auto& shardIdx : tableOp.ExpectedShards) {
            protoOp->AddExpectedShardLocalIds(ui64(shardIdx.GetLocalId()));
        }
        for (const auto& shardIdx : tableOp.CompletedShards) {
            protoOp->AddCompletedShardLocalIds(ui64(shardIdx.GetLocalId()));
        }
        for (const auto& shardIdx : tableOp.FailedShards) {
            protoOp->AddFailedShardLocalIds(ui64(shardIdx.GetLocalId()));
        }
        protoOp->SetHasNonRetriableFailure(tableOp.HasNonRetriableFailure);
        protoOp->SetSrcPathLocalId(dispatch.SrcPathId.LocalPathId);
        protoOp->SetDstPathLocalId(dispatch.DstPathId.LocalPathId);
    }

    for (const auto& opId : state.CompletedOperations) {
        auto tableOpIt = state.TableOperations.find(opId);
        if (tableOpIt != state.TableOperations.end() && tableOpIt->second.HasFailures()) {
            continue;
        }
        // Skip ops already included above.
        if (state.ShardDispatchByOp.contains(opId)) {
            continue;
        }
        auto* protoOp = protoList.AddSubOps();
        auto* protoId = protoOp->MutableId();
        protoId->SetTxId(opId.GetTxId().GetValue());
        protoId->SetSubTxId(opId.GetSubTxId());
    }

    Y_UNUSED(incrementalRestoreId);
    Y_UNUSED(subOpId);
    db.Table<Schema::IncrementalRestoreState>().Key(state.OriginalOperationId).Update(
        NIceDb::TUpdate<Schema::IncrementalRestoreState::SerializedData>(
            protoList.SerializeAsString()));
}

void TSchemeShard::ReDispatchPathAIncrementalRestoreOnInit(
    ui64 incrementalRestoreId,
    TIncrementalRestoreState& state,
    const TActorContext& ctx)
{
    const TIncrementalRestoreOpId restoreOpId(incrementalRestoreId);
    for (const auto& [subOpId, dispatch] : state.ShardDispatchByOp) {
        auto opIt = state.TableOperations.find(subOpId);
        if (opIt == state.TableOperations.end()) {
            continue;
        }
        const auto& tableOp = opIt->second;

        for (const auto& [shardIdx, tabletId] : dispatch.ShardTablets) {
            if (tableOp.CompletedShards.contains(shardIdx) ||
                tableOp.FailedShards.contains(shardIdx)) {
                continue;
            }
            SendIncrementalRestoreShardRequest(restoreOpId, subOpId, shardIdx, tabletId,
                                           dispatch.SrcPathId, dispatch.DstPathId, ctx);
        }
    }
}

void TSchemeShard::RetryIncrementalRestorePipe(
    TIncrementalRestoreOpId restoreOpId,
    TTabletId tabletId,
    const TActorContext& ctx)
{
    IncrementalRestorePipes.Close(restoreOpId, tabletId, ctx);

    auto stateIt = IncrementalRestoreStates.find(ui64(restoreOpId));
    if (stateIt == IncrementalRestoreStates.end()) {
        LOG_W("RetryIncrementalRestorePipe: no state for restoreOpId=" << ui64(restoreOpId));
        return;
    }
    auto& state = stateIt->second;

    int reissued = 0;
    for (const auto& [subOpId, dispatch] : state.ShardDispatchByOp) {
        auto opIt = state.TableOperations.find(subOpId);
        if (opIt == state.TableOperations.end()) continue;
        const auto& tableOp = opIt->second;
        for (const auto& [shardIdx, shardTablet] : dispatch.ShardTablets) {
            if (shardTablet != tabletId) continue;
            if (tableOp.CompletedShards.contains(shardIdx) ||
                tableOp.FailedShards.contains(shardIdx)) {
                continue;
            }
            SendIncrementalRestoreShardRequest(restoreOpId, subOpId, shardIdx, shardTablet,
                                           dispatch.SrcPathId, dispatch.DstPathId, ctx);
            ++reissued;
        }
    }

    LOG_I("RetryIncrementalRestorePipe: reissued=" << reissued
          << " restoreOpId=" << ui64(restoreOpId)
          << " tabletId=" << tabletId);
}

void TSchemeShard::CreateSingleTableRestoreOperation(
    const TPathId& backupCollectionPathId,
    ui64 operationId,
    const TString& backupName,
    const TString& targetTablePath,
    NIceDb::TNiceDb& db,
    const TActorContext& ctx) {

    auto bcPath = TPath::Init(backupCollectionPathId, this);

    std::pair<TString, TString> paths;
    TString err;
    if (!TrySplitPathByDb(targetTablePath, bcPath.GetDomainPathString(), paths, err)) {
        LOG_E("Failed to split path: " << err);
        return;
    }
    auto& relativeItemPath = paths.second;

    TString incrBackupPathStr = JoinPath({bcPath.PathString(), NBackup::IncrementalBackupDirName(backupName), relativeItemPath});
    const TPath& incrBackupPath = TPath::Resolve(incrBackupPathStr, this);

    if (!incrBackupPath.IsResolved()) {
        LOG_W("Incremental backup path not found at dispatch time: " << incrBackupPathStr);
        return;
    }

    LOG_I("Enqueuing separate restore operation for table: " << incrBackupPathStr << " -> " << targetTablePath);

    auto stateIt = IncrementalRestoreStates.find(operationId);
    if (stateIt == IncrementalRestoreStates.end()) {
        return;
    }
    auto& state = stateIt->second;

    auto tableRequest = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
    auto& tableRecord = tableRequest->Record;

    auto& tableTx = *tableRecord.AddTransaction();
    tableTx.SetOperationType(NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups);
    tableTx.SetInternal(true);
    tableTx.SetWorkingDir(bcPath.PathString());

    auto& tableRestore = *tableTx.MutableRestoreMultipleIncrementalBackups();
    tableRestore.AddSrcTablePaths(incrBackupPathStr);
    tableRestore.SetDstTablePath(targetTablePath);

    TPath itemPath = TPath::Resolve(targetTablePath, this);
    TPathId tablePathId = (itemPath.IsResolved() && itemPath.Base()->IsTable())
        ? itemPath.Base()->PathId
        : TPathId{};

    TPathId srcPathId = (incrBackupPath.IsResolved() && incrBackupPath.Base()->IsTable())
        ? incrBackupPath.Base()->PathId
        : TPathId{};

    EnqueueIncrementalRestoreItem(
        operationId, state,
        TIncrementalRestoreState::TItem::EKind::Table,
        tablePathId,
        std::move(tableRequest),
        db, ctx,
        srcPathId);
}

TString TSchemeShard::FindIncrementalRestoreTargetTablePath(
    const TBackupCollectionInfo::TPtr& backupCollectionInfo,
    const TString& relativeTablePath) {

    for (const auto& item : backupCollectionInfo->Description.GetExplicitEntryList().GetEntries()) {
        if (item.GetType() != NKikimrSchemeOp::TBackupCollectionDescription_TBackupEntry_EType_ETypeTable) {
            continue;
        }

        TString itemPath = item.GetPath();
        if (itemPath == relativeTablePath || itemPath.EndsWith("/" + relativeTablePath)) {
            return itemPath;
        }
    }

    return {};
}

void TSchemeShard::EnqueueIncrementalRestoreIndexesRecursive(
    ui64 operationId,
    const TString& backupName,
    const TBackupCollectionInfo::TPtr& backupCollectionInfo,
    const TPath& currentPath,
    const TString& accumulatedRelativePath,
    const TActorContext& ctx) {

    auto stateIt = IncrementalRestoreStates.find(operationId);
    if (stateIt == IncrementalRestoreStates.end()) {
        return;
    }

    TString targetTablePath = FindIncrementalRestoreTargetTablePath(backupCollectionInfo, accumulatedRelativePath);

    if (!targetTablePath.empty()) {
        LOG_I("Found table mapping: " << accumulatedRelativePath << " -> " << targetTablePath);

        for (const auto& [indexName, indexDirPathId] : currentPath.Base()->GetChildren()) {
            auto indexPathInBackup = TPath::Init(indexDirPathId, this);
            for (const auto& [implName, implPathId] : indexPathInBackup.Base()->GetChildren()) {
                TIncrementalRestoreState::TPendingRestoreOp pending;
                pending.Kind = TIncrementalRestoreState::TPendingRestoreOp::EKind::Index;
                pending.BackupName = backupName;
                pending.TablePath = accumulatedRelativePath;
                pending.IndexName = indexName;
                pending.TargetTablePath = targetTablePath;
                pending.SpecificImplTableName = implName;
                stateIt->second.PendingTables.push_back(std::move(pending));
                LOG_I("Enqueued index sub-op: " << indexName << "/" << implName << " on " << targetTablePath);
            }
        }
    } else {
        for (const auto& [childName, childPathId] : currentPath.Base()->GetChildren()) {
            auto childPath = TPath::Init(childPathId, this);
            TString newRelativePath = accumulatedRelativePath.empty()
                ? childName
                : accumulatedRelativePath + "/" + childName;

            EnqueueIncrementalRestoreIndexesRecursive(
                operationId,
                backupName,
                backupCollectionInfo,
                childPath,
                newRelativePath,
                ctx
            );
        }
    }
}

void TSchemeShard::EnqueueAndDiscoverIndexRestoreOperations(
    const TPathId& /*backupCollectionPathId*/,
    ui64 operationId,
    const TString& backupName,
    const TPath& bcPath,
    const TBackupCollectionInfo::TPtr& backupCollectionInfo,
    const TActorContext& ctx) {

    bool omitIndexes = backupCollectionInfo->Description.GetIncrementalBackupConfig().GetOmitIndexes();
    if (omitIndexes) {
        LOG_I("Indexes were omitted in backup, skipping index restore");
        return;
    }

    TString indexMetaBasePath = JoinPath({
        bcPath.PathString(),
        NBackup::IncrementalBackupDirName(backupName),
        "__ydb_backup_meta",
        "indexes"
    });

    const TPath& indexMetaPath = TPath::Resolve(indexMetaBasePath, this);
    if (!indexMetaPath.IsResolved()) {
        LOG_I("No index metadata found at: " << indexMetaBasePath << " (this is normal if no indexes were backed up)");
        return;
    }

    LOG_I("Discovering indexes for restore at: " << indexMetaBasePath);

    EnqueueIncrementalRestoreIndexesRecursive(
        operationId,
        backupName,
        backupCollectionInfo,
        indexMetaPath,
        "",
        ctx
    );
}

void TSchemeShard::CreateSingleIndexRestoreOperation(
    ui64 operationId,
    const TString& backupName,
    const TPath& bcPath,
    const TString& relativeTablePath,
    const TString& indexName,
    const TString& targetTablePath,
    NIceDb::TNiceDb& db,
    const TActorContext& ctx,
    const TString& specificImplTableName)
{
    LOG_I("CreateSingleIndexRestoreOperation: table=" << targetTablePath
          << " index=" << indexName
          << " relativeTablePath=" << relativeTablePath
          << " specificImplTableName=" << specificImplTableName);

    const TPath targetTablePathObj = TPath::Resolve(targetTablePath, this);
    if (!targetTablePathObj.IsResolved() || !targetTablePathObj.Base()->IsTable()) {
        LOG_W("Target table not found or invalid: " << targetTablePath);
        return;
    }

    TPathId indexPathId;
    TPathId indexImplTablePathId;
    bool indexFound = false;

    for (const auto& [childName, childPathId] : targetTablePathObj.Base()->GetChildren()) {
        if (childName == indexName) {
            auto childPath = PathsById.at(childPathId);
            if (childPath->PathType == NKikimrSchemeOp::EPathTypeTableIndex) {
                indexPathId = childPathId;

                auto indexInfoIt = Indexes.find(indexPathId);
                if (indexInfoIt == Indexes.end()) {
                    LOG_W("Index info not found for pathId: " << indexPathId);
                    return;
                }
                auto indexInfo = indexInfoIt->second;

                if (!IsSupportedIndex(indexPathId, this)) {
                    LOG_I("Skipping index with unsupported type: " << indexName << " (type=" << indexInfo->Type << ")");
                    return;
                }

                auto indexPath = TPath::Init(indexPathId, this);

                for (const auto& [implName, implPathId] : indexPath.Base()->GetChildren()) {
                    if (implName == specificImplTableName) {
                        indexImplTablePathId = implPathId;
                        indexFound = true;
                        LOG_I("Found index impl table: " << indexName << "/" << implName);
                        break;
                    }
                }
            }
            if (indexFound) break;
        }
    }

    if (!indexFound) {
        LOG_W("Index '" << indexName << "' (or specific table '" << specificImplTableName << "') not found on table " << targetTablePath
              << " - skipping (index may have been dropped)");
        return;
    }

    TString srcIndexBackupPath = JoinPath({
        bcPath.PathString(),
        NBackup::IncrementalBackupDirName(backupName),
        "__ydb_backup_meta",
        "indexes",
        relativeTablePath,
        indexName,
        specificImplTableName
    });

    const TPath& srcBackupPath = TPath::Resolve(srcIndexBackupPath, this);
    if (!srcBackupPath.IsResolved()) {
        LOG_W("Index backup not found at: " << srcIndexBackupPath);
        return;
    }

    auto indexImplTablePath = TPath::Init(indexImplTablePathId, this);
    TString dstIndexImplPath = indexImplTablePath.PathString();

    LOG_I("Enqueuing index restore operation: " << srcIndexBackupPath << " -> " << dstIndexImplPath);

    auto stateIt = IncrementalRestoreStates.find(operationId);
    if (stateIt == IncrementalRestoreStates.end()) {
        return;
    }
    auto& state = stateIt->second;

    auto indexRequest = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
    auto& indexRecord = indexRequest->Record;

    auto& indexTx = *indexRecord.AddTransaction();
    indexTx.SetOperationType(NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups);
    indexTx.SetInternal(true);
    indexTx.SetWorkingDir(bcPath.PathString());

    auto& indexRestore = *indexTx.MutableRestoreMultipleIncrementalBackups();
    indexRestore.AddSrcTablePaths(srcIndexBackupPath);
    indexRestore.SetDstTablePath(dstIndexImplPath);

    TPathId srcIndexPathId = (srcBackupPath.IsResolved() && srcBackupPath.Base()->IsTable())
        ? srcBackupPath.Base()->PathId
        : TPathId{};

    EnqueueIncrementalRestoreItem(
        operationId, state,
        TIncrementalRestoreState::TItem::EKind::Index,
        indexImplTablePathId,
        std::move(indexRequest),
        db, ctx,
        srcIndexPathId);
}

void TSchemeShard::NotifyIncrementalRestoreOperationCompleted(const TOperationId& operationId, const TActorContext& ctx) {
    auto it = IncrementalRestoreOperationToState.find(operationId);
    if (it != IncrementalRestoreOperationToState.end()) {
        ui64 incrementalRestoreId = it->second;

        LOG_I("Operation " << operationId << " completed, triggering progress check for incremental restore " << incrementalRestoreId);

        auto progressEvent = MakeHolder<TEvPrivate::TEvProgressIncrementalRestore>(incrementalRestoreId);
        ctx.Send(ctx.SelfID, progressEvent.Release());
    }
}

void TSchemeShard::EnqueueIncrementalRestoreItem(
    ui64 originalOpId,
    TIncrementalRestoreState& state,
    TIncrementalRestoreState::TItem::EKind kind,
    TPathId tablePathId,
    THolder<TEvSchemeShard::TEvModifySchemeTransaction> request,
    NIceDb::TNiceDb& db,
    const TActorContext& ctx,
    TPathId srcTablePathId)
{
    TIncrementalRestoreState::TItem item;
    item.ItemSeq = state.NextItemSeq++;
    item.Kind = kind;
    item.TablePathId = tablePathId;
    item.SrcTablePathId = srcTablePathId;
    item.WaitTxId = ui64(InvalidTxId);
    item.PendingRequest = request.Release();

    db.Table<Schema::IncrementalRestoreItem>()
        .Key(originalOpId, item.ItemSeq)
        .Update(
            NIceDb::TUpdate<Schema::IncrementalRestoreItem::ItemKind>(static_cast<ui32>(kind)),
            NIceDb::TUpdate<Schema::IncrementalRestoreItem::TablePathId>(tablePathId.LocalPathId),
            NIceDb::TUpdate<Schema::IncrementalRestoreItem::WaitTxId>(ui64(InvalidTxId)),
            NIceDb::TUpdate<Schema::IncrementalRestoreItem::SrcTablePathId>(srcTablePathId.LocalPathId));

    state.PendingItems.push_back(std::move(item));

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[IncrementalRestore] EnqueueIncrementalRestoreItem op=" << originalOpId
        << " itemSeq=" << state.PendingItems.back().ItemSeq
        << " kind=" << static_cast<ui32>(kind)
        << " tablePathId=" << tablePathId);
    ctx.Send(TxAllocatorClient,
        new TEvTxAllocatorClient::TEvAllocate(),
        /*flags=*/0,
        originalOpId);
}

void TSchemeShard::CleanupIncrementalRestoreItems(
    ui64 originalOpId,
    NIceDb::TNiceDb& db,
    TIncrementalRestoreState* state)
{
    THashSet<ui32> knownSeqs;
    if (state) {
        for (const auto& item : state->PendingItems) {
            knownSeqs.insert(item.ItemSeq);
        }
        for (const auto& [seq, _] : state->InFlightItems) {
            knownSeqs.insert(seq);
        }
    }
    for (ui32 seq : knownSeqs) {
        db.Table<Schema::IncrementalRestoreItem>()
            .Key(originalOpId, seq)
            .Delete();
    }

    TVector<TTxId> toErase;
    for (const auto& [txId, opId] : TxIdToIncrementalRestore) {
        if (opId == originalOpId) {
            toErase.push_back(txId);
        }
    }
    for (TTxId k : toErase) {
        TxIdToIncrementalRestore.erase(k);
    }

    if (state) {
        state->PendingItems.clear();
        state->InFlightItems.clear();
        state->WaitTxIdToItemSeq.clear();
        state->NextItemSeq = 0;
    }
}

class TSchemeShard::TTxProgressIncrementalRestoreAllocateResult : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
public:
    using TBase = NTabletFlatExecutor::TTransactionBase<TSchemeShard>;
    TTxProgressIncrementalRestoreAllocateResult(TSchemeShard* self,
            TEvTxAllocatorClient::TEvAllocateResult::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        const ui64 originalOpId = Ev->Cookie;

        LOG_I("TTxProgressIncrementalRestoreAllocateResult"
            << " originalOpId=" << originalOpId
            << " txIdsCount=" << Ev->Get()->TxIds.size());

        auto stateIt = Self->IncrementalRestoreStates.find(originalOpId);
        if (stateIt == Self->IncrementalRestoreStates.end()) {
            LOG_W("TTxProgressIncrementalRestoreAllocateResult: state for "
                  << originalOpId << " not found; dropping allocator result");
            return true;
        }
        auto& state = stateIt->second;

        if (state.PendingItems.empty()) {
            LOG_W("TTxProgressIncrementalRestoreAllocateResult: no PendingItems "
                  << "for op " << originalOpId
                  << "; dropping allocator result");
            return true;
        }
        const ui32 itemSeq = state.PendingItems.front().ItemSeq;

        if (Ev->Get()->TxIds.empty()) {
            LOG_W("TTxProgressIncrementalRestoreAllocateResult: empty TxIds; "
                  << "scheduling allocator retry for op " << originalOpId
                  << " itemSeq " << itemSeq);
            ScheduleAllocatorRetry(originalOpId, itemSeq, ctx);
            return true;
        }
        const TTxId allocatedTxId = TTxId(Ev->Get()->TxIds.front());

        TIncrementalRestoreState::TItem item = std::move(state.PendingItems.front());
        state.PendingItems.pop_front();
        item.WaitTxId = ui64(allocatedTxId);
        state.WaitTxIdToItemSeq[ui64(allocatedTxId)] = item.ItemSeq;

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::IncrementalRestoreItem>()
            .Key(originalOpId, item.ItemSeq)
            .Update(NIceDb::TUpdate<Schema::IncrementalRestoreItem::WaitTxId>(item.WaitTxId));

        Self->TxIdToIncrementalRestore[allocatedTxId] = originalOpId;

        const auto kind = item.Kind;
        const TPathId tablePathId = item.TablePathId;
        const TPathId srcTablePathId = item.SrcTablePathId;

        TAutoPtr<NActors::IEventBase> baseRequest = std::move(item.PendingRequest);

        if ((kind == TIncrementalRestoreState::TItem::EKind::Table
                || kind == TIncrementalRestoreState::TItem::EKind::Index)
            && srcTablePathId) {
            // Dispatch per-shard requests directly; TxId is a synthetic subOpId for bookkeeping.
            const TOperationId subOpId(allocatedTxId, 0);
            Self->TrackIncrementalRestoreSubOpAndExpectedShards(
                subOpId, srcTablePathId, originalOpId, state);

            auto tableOpIt = state.TableOperations.find(subOpId);
            if (tableOpIt != state.TableOperations.end()) {
                tableOpIt->second.RequestsDispatched = true;
            }

            baseRequest.Reset(); // drop the prepared ModifyScheme; we won't send it

            state.InFlightItems[item.ItemSeq] = std::move(item);

            LOG_I("TTxProgressIncrementalRestoreAllocateResult: dispatching "
                  << "Path A requests for op " << originalOpId
                  << " itemSeq " << itemSeq
                  << " subOpId " << subOpId
                  << " srcPathId " << srcTablePathId
                  << " dstPathId " << tablePathId);

            Self->DispatchIncrementalRestoreShardRequests(
                subOpId, srcTablePathId, tablePathId,
                originalOpId, state, db, ctx);
            return true;
        }

        // Fallback (Finalize items, or unresolved src path): use the legacy schema-op pipeline.
        if (!baseRequest) {
            LOG_E("TTxProgressIncrementalRestoreAllocateResult: missing "
                  << "PendingRequest for op " << originalOpId
                  << " itemSeq " << itemSeq);
            return true;
        }
        auto* request = static_cast<TEvSchemeShard::TEvModifySchemeTransaction*>(baseRequest.Release());
        request->Record.SetTxId(ui64(allocatedTxId));

        if (kind == TIncrementalRestoreState::TItem::EKind::Table
            || kind == TIncrementalRestoreState::TItem::EKind::Index) {
            TOperationId subOpId(allocatedTxId, 0);
            Self->TrackIncrementalRestoreSubOpAndExpectedShards(
                subOpId, tablePathId, originalOpId, state);
        }
        state.InFlightItems[item.ItemSeq] = std::move(item);

        LOG_I("TTxProgressIncrementalRestoreAllocateResult: dispatching "
              << "ModifyScheme for op " << originalOpId
              << " itemSeq " << itemSeq
              << " allocatedTxId " << allocatedTxId);
        Self->Send(Self->SelfId(), request);
        return true;
    }

    void Complete(const TActorContext&) override {}

private:
    TEvTxAllocatorClient::TEvAllocateResult::TPtr Ev;

    void ScheduleAllocatorRetry(ui64 originalOpId, ui32 itemSeq, const TActorContext& ctx) {
        Y_UNUSED(itemSeq);
        const TActorId txAllocator = Self->TxAllocatorClient;
        std::unique_ptr<IEventHandle> ev(new IEventHandle(
            txAllocator, Self->SelfId(),
            new TEvTxAllocatorClient::TEvAllocate(),
            /*flags=*/0, originalOpId));
        ctx.Schedule(TDuration::MilliSeconds(500), std::move(ev));
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestoreAllocateResult(
    TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev)
{
    return new TTxProgressIncrementalRestoreAllocateResult(this, ev);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(ui64 operationId) {
    return new TTxProgressIncrementalRestore(this, operationId);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvPrivate::TEvProgressIncrementalRestore::TPtr& ev) {
    auto* msg = ev->Get();
    return new TTxProgressIncrementalRestore(this, msg->OperationId);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    TTxId txId(msg->Record.GetTxId());
    
    auto txToIncrRestoreIt = TxIdToIncrementalRestore.find(txId);
    if (txToIncrRestoreIt != TxIdToIncrementalRestore.end()) {
        return new TTxProgressIncrementalRestore(this, txToIncrRestoreIt->second);
    }

    LOG_D("Transaction " << txId << " is not associated with incremental restore");
    return nullptr;
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TTxId completedTxId, const TActorContext& ctx) {
    auto txToIncrRestoreIt = TxIdToIncrementalRestore.find(completedTxId);
    if (txToIncrRestoreIt != TxIdToIncrementalRestore.end()) {
        return new TTxProgressIncrementalRestore(this, txToIncrRestoreIt->second);
    }

    LOG_D("Transaction " << completedTxId << " is not associated with incremental restore");
    return nullptr;
}

class TSchemeShard::TTxIncrementalRestoreShardProgress : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
public:
    using TBase = NTabletFlatExecutor::TTransactionBase<TSchemeShard>;
    explicit TTxIncrementalRestoreShardProgress(TSchemeShard* self,
            TEvDataShard::TEvIncrementalRestoreShardProgress::TPtr ev)
        : TBase(self)
        , Ev(std::move(ev))
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext&, const TActorContext& ctx) override {
        const auto& rec = Ev->Get()->Record;
        const ui64 subOpTxId = rec.GetSubOpTxId();
        const ui64 generation = rec.GetGeneration();
        const ui64 tabletId = rec.GetTabletId();
        const auto endStatus = rec.GetEndStatus();
        const bool success = rec.GetSuccess();

        if (generation != Self->Generation()) {
            LOG_W("[IncrementalRestore] TEvIncrementalRestoreShardProgress dropped: stale generation"
                  << " (got=" << generation << " current=" << Self->Generation() << ")"
                  << " subOpTxId=" << subOpTxId
                  << " tabletId=" << tabletId);
            return true;
        }

        const TOperationId opId(subOpTxId, 0);
        auto opStateIt = Self->IncrementalRestoreOperationToState.find(opId);
        if (opStateIt == Self->IncrementalRestoreOperationToState.end()) {
            LOG_W("[IncrementalRestore] TEvIncrementalRestoreShardProgress dropped: unknown SubOpTxId"
                  << " (subOpTxId=" << subOpTxId << " tabletId=" << tabletId << ")");
            return true;
        }
        const ui64 originalOpId = opStateIt->second;

        auto stateIt = Self->IncrementalRestoreStates.find(originalOpId);
        if (stateIt == Self->IncrementalRestoreStates.end()) {
            LOG_W("[IncrementalRestore] TEvIncrementalRestoreShardProgress dropped: no state for op"
                  << " (originalOpId=" << originalOpId << " subOpTxId=" << subOpTxId << ")");
            return true;
        }
        auto& state = stateIt->second;

        auto opIt = state.TableOperations.find(opId);
        if (opIt == state.TableOperations.end()) {
            LOG_W("[IncrementalRestore] TEvIncrementalRestoreShardProgress dropped: no TableOperationState"
                  << " (subOpTxId=" << subOpTxId << ")");
            return true;
        }
        auto& tableOp = opIt->second;

        auto* shardIdxPtr = Self->TabletIdToShardIdx.FindPtr(TTabletId(tabletId));
        if (!shardIdxPtr) {
            LOG_W("[IncrementalRestore] TEvIncrementalRestoreShardProgress dropped: unknown TabletId"
                  << " (tabletId=" << tabletId << ")");
            return true;
        }
        const TShardIdx shardIdx = *shardIdxPtr;

        const bool retriable = ShouldRetryIncrementalRestore(endStatus);
        const bool recorded = tableOp.RecordShardResult(shardIdx, success, retriable);

        LOG_I("[IncrementalRestore] TEvIncrementalRestoreShardProgress applied"
              << " originalOpId=" << originalOpId
              << " subOpTxId=" << subOpTxId
              << " shardIdx=" << shardIdx
              << " endStatus=" << static_cast<int>(endStatus)
              << " success=" << success
              << " retriable=" << retriable
              << " recorded=" << recorded);

        if (!success) {
            Self->FailedIncrementalRestoreOperations.insert(opId);
        }

        Self->Schedule(TDuration::Zero(),
            new TEvPrivate::TEvProgressIncrementalRestore(originalOpId));
        return true;
    }

    void Complete(const TActorContext&) override {}

private:
    TEvDataShard::TEvIncrementalRestoreShardProgress::TPtr Ev;
};

void TSchemeShard::Handle(TEvDataShard::TEvIncrementalRestoreShardProgress::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxIncrementalRestoreShardProgress(this, std::move(ev)), ctx);
}

} // namespace NKikimr::NSchemeShard
