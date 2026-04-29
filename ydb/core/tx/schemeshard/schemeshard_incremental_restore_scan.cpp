#include "schemeshard_impl.h"
#include "schemeshard__backup_collection_common.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/tx/datashard/scan_common.h>  // GetRetryWakeupTimeoutBackoff
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

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

        // Early exit if already finalizing, completed, or failed - no need to do more progress checks
        if (state.State == TIncrementalRestoreState::EState::Finalizing ||
            state.State == TIncrementalRestoreState::EState::Completed ||
            state.State == TIncrementalRestoreState::EState::Failed) {
            LOG_I("Incremental restore already in state " << static_cast<ui32>(state.State)
                  << ", skipping progress check for operation: " << OperationId);
            return true;
        }

        // Persist initial row if missing (idempotent update)
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
            NIceDb::TUpdate<Schema::IncrementalRestoreState::State>(static_cast<ui32>(TIncrementalRestoreState::EState::Running)),
            NIceDb::TUpdate<Schema::IncrementalRestoreState::CurrentIncrementalIdx>(state.CurrentIncrementalIdx)
        );
        
        CheckForCompletedOperations(state, ctx);
        
        // Persist the updated state including completed operations if they changed
        if (CompletedOperationsChanged) {
            TString serializedCompletedOperations = SerializeOperationIds(state.CompletedOperations);
            db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                NIceDb::TUpdate<Schema::IncrementalRestoreState::SerializedData>(serializedCompletedOperations)
            );
            LOG_I("Persisted CompletedOperations update: " << serializedCompletedOperations);
        }
        
        // Check if all operations for current incremental backup are complete
        LOG_I("Checking completion: InProgressOperations.size()=" << state.InProgressOperations.size() 
              << ", CompletedOperations.size()=" << state.CompletedOperations.size()
              << ", CurrentIncrementalIdx=" << state.CurrentIncrementalIdx
              << ", IncrementalBackups.size()=" << state.IncrementalBackups.size());
              
        if (state.AreAllCurrentOperationsComplete()) {
            if (state.RetryNeeded) {
                if (HandleRetryPath(state, db, ctx)) {
                    return true;
                }
            } else {
                if (HandleAllOperationsComplete(state, txc, ctx)) {
                    return true;
                }
            }
        } else if (!state.InProgressOperations.empty()) {
            // Fallback heartbeat. The hot path is the immediate ctx.Send in
            // NotifyIncrementalRestoreOperationCompleted (fires per completion);
            // this 1s heartbeat covers the case where a completion notification
            // was missed or the operation pipeline is otherwise quiet.
            auto progressEvent = MakeHolder<TEvPrivate::TEvProgressIncrementalRestore>(OperationId);
            Self->Schedule(TDuration::Seconds(1), progressEvent.Release());
        } else {
            // No operations in progress - check if we should start processing
            if (state.AllIncrementsProcessed()) {
                // All incrementals already processed but state wasn't set to Finalizing
                // This shouldn't happen normally, but handle it gracefully
                LOG_W("All increments processed but state is still Running, triggering finalization");
                state.State = TIncrementalRestoreState::EState::Finalizing;
                db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                    NIceDb::TUpdate<Schema::IncrementalRestoreState::State>(static_cast<ui32>(state.State))
                );
                FinalizeIncrementalRestoreOperation(txc, ctx, state);
            } else {
                // Start processing the current incremental backup
                LOG_I("No operations in progress, starting incremental backup #" << state.CurrentIncrementalIdx);
                ProcessNextIncrementalBackup(state, ctx);
            }
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
            auto* protoOp = protoList.AddOperations();
            protoOp->SetTxId(opId.GetTxId().GetValue());
            protoOp->SetSubTxId(opId.GetSubTxId());
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
        
        for (const auto& protoOp : protoList.GetOperations()) {
            TTxId txId(protoOp.GetTxId());
            TSubTxId subTxId = protoOp.GetSubTxId();
            operations.insert(TOperationId(txId, subTxId));
        }
        
        return operations;
    }
    
    // Returns true if Execute should also return true (retry handled this tick).
    bool HandleRetryPath(TIncrementalRestoreState& state, NIceDb::TNiceDb& db, const TActorContext& ctx) {
        const i64 cap = Self->IncrementalRestoreSettings.MaxIncrementalRestoreRetriesPerIncremental;
        // Skip budget check while a retry is in flight — count is
        // incremented at scheduling time, so re-entries between schedule
        // and run would otherwise fail prematurely.
        const bool budgetExceeded = (cap != -1)
            && !state.RetryScheduled
            && (i64)state.CurrentIncrementalRetryCount >= cap;
        if (state.NonRetriableFailure || budgetExceeded) {
            LOG_E("Incremental #" << state.CurrentIncrementalIdx
                  << " short-circuiting to Failed: nonRetriable="
                  << state.NonRetriableFailure
                  << " retryCount=" << state.CurrentIncrementalRetryCount
                  << " cap=" << cap);
            state.State = TIncrementalRestoreState::EState::Failed;
            state.RetryScheduled = false;
            state.NextRetryAttemptAt = TInstant::Zero();
            db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                NIceDb::TUpdate<Schema::IncrementalRestoreState::State>(static_cast<ui32>(state.State))
            );
            // State is persisted as Failed — callers polling via GetRestore/ListRestores
            // will see PROGRESS_DONE + GENERIC_ERROR. No finalization needed.
            return true;
        }

        // Two-phase backoff: Phase 1 (!RetryScheduled) increments counter +
        // schedules timer; Phase 2 (timer fired) re-dispatches; concurrent
        // completion events arriving mid-window bail out without double-counting.
        if (state.RetryScheduled) {
            if (ctx.Now() < state.NextRetryAttemptAt) {
                LOG_I("Backoff window in flight for incremental #"
                      << state.CurrentIncrementalIdx
                      << " (retry " << state.CurrentIncrementalRetryCount
                      << ", until " << state.NextRetryAttemptAt
                      << "), skipping concurrent retry trigger");
                return true;
            }

            // Phase 2: timer event has arrived. Drain into the retry body.
            LOG_I("Backoff timer fired for incremental #" << state.CurrentIncrementalIdx
                  << ", proceeding with retry attempt " << state.CurrentIncrementalRetryCount);
            state.RetryScheduled = false;
            state.NextRetryAttemptAt = TInstant::Zero();
            state.RetryNeeded = false;

            // Reset operation tracking for retry (don't advance index)
            state.InProgressOperations.clear();
            state.CompletedOperations.clear();
            state.PendingTables.clear();
            state.CurrentIncrementalStarted = false;

            // Persist cleared state
            TString serializedEmpty = SerializeOperationIds(state.CompletedOperations);
            db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                NIceDb::TUpdate<Schema::IncrementalRestoreState::SerializedData>(serializedEmpty)
            );

            // Retry: process same incremental again
            ProcessNextIncrementalBackup(state, ctx);
            return true;
        }

        // Phase 1: schedule the backoff.
        state.CurrentIncrementalRetryCount++;
        auto delay = NDataShard::GetRetryWakeupTimeoutBackoff(state.CurrentIncrementalRetryCount);
        state.NextRetryAttemptAt = ctx.Now() + delay;
        state.RetryScheduled = true;
        LOG_W("Shard failures detected for incremental #" << state.CurrentIncrementalIdx
              << ", retry attempt " << state.CurrentIncrementalRetryCount
              << "/" << (cap == -1 ? "unlimited" : ToString(cap))
              << " scheduled in " << delay);
        Self->Schedule(delay,
            new TEvPrivate::TEvProgressIncrementalRestore(OperationId));
        return true;
    }

    // Returns true if Execute should also return true (finalization fired).
    bool HandleAllOperationsComplete(TIncrementalRestoreState& state, NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
        LOG_I("All operations for current incremental backup completed, moving to next");
        state.MarkCurrentIncrementalComplete();
        state.MoveToNextIncremental();

        // Persist CurrentIncrementalIdx advance
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
            NIceDb::TUpdate<Schema::IncrementalRestoreState::CurrentIncrementalIdx>(state.CurrentIncrementalIdx)
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

        // Start processing next incremental backup
        ProcessNextIncrementalBackup(state, ctx);
        return false;
    }

    void CheckForCompletedOperations(TIncrementalRestoreState& state, const TActorContext& ctx) {
        THashSet<TOperationId> stillInProgress;
        bool operationsCompleted = false;
        bool hasFailedOperations = false;

        for (const auto& opId : state.InProgressOperations) {
            TTxId txId = opId.GetTxId();

            if (Self->Operations.contains(txId)) {
                stillInProgress.insert(opId);
            } else {
                // Check if we've already tracked this completion
                if (!state.CompletedOperations.contains(opId)) {
                    // Check if this operation failed at the shard level
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
                }
            }
        }

        state.InProgressOperations = std::move(stillInProgress);
        state.RetryNeeded |= hasFailedOperations;

        // Roll up the non-retriable bit from per-table state. Sticky within
        // the current incremental — once any table reports a non-retriable
        // failure, the orchestrator must short-circuit to Failed instead of
        // burning the retry budget.
        if (!state.NonRetriableFailure) {
            for (const auto& [_, tableOp] : state.TableOperations) {
                if (tableOp.HasNonRetriableFailure) {
                    state.NonRetriableFailure = true;
                    break;
                }
            }
        }

        // If operations were completed, update the persisted state
        if (operationsCompleted) {
            SetCompletedOperationsChanged(true);
        }

        // Top-up the in-flight set: any sub-ops that finished free up capacity
        // for queued PendingTables entries. Skip if we're about to retry
        // (retry path will clear and re-populate the queue).
        if (!state.RetryNeeded) {
            Self->DispatchPendingTables(state, OperationId, ctx);
        }
    }
    
    void ProcessNextIncrementalBackup(TIncrementalRestoreState& state, const TActorContext& ctx) {
        const auto* currentIncremental = state.GetCurrentIncremental();
        if (!currentIncremental) {
            LOG_I("No more incremental backups to process");
            return;
        }

        LOG_I("Processing incremental backup #" << state.CurrentIncrementalIdx + 1
            << " path: " << currentIncremental->BackupPath
            << " timestamp: " << currentIncremental->Timestamp);

        // Populate the dispatch queue with one TPendingRestoreOp per table (and per
        // index impl-table). Both kinds share the cap so indexes don't fan out
        // unbounded. The actual sub-op creation happens in DispatchPendingTables,
        // throttled by ICB SchemeShardControls.MaxIncrementalRestoreTablesInFlight.
        Self->EnqueueIncrementalRestoreOperations(
            state.BackupCollectionPathId,
            OperationId,
            currentIncremental->BackupPath,
            ctx
        );

        state.CurrentIncrementalStarted = true;

        Self->DispatchPendingTables(state, OperationId, ctx);

        auto progressEvent = MakeHolder<TEvPrivate::TEvProgressIncrementalRestore>(OperationId);
        Self->Schedule(TDuration::Seconds(1), progressEvent.Release());
    }
    
    void FinalizeIncrementalRestoreOperation(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx, TIncrementalRestoreState& state) {
        Y_UNUSED(txc);
        LOG_I("Starting finalization of incremental restore operation: " << OperationId);
        
        CreateFinalizationOperation(state, ctx);
    }

    void CreateFinalizationOperation(TIncrementalRestoreState& state, const TActorContext& ctx) {
        // Build the finalization request
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        auto& record = request->Record;
        
        TTxId finalizeTxId = Self->GetCachedTxId(ctx);
        record.SetTxId(ui64(finalizeTxId));
        
        auto& transaction = *record.AddTransaction();
        transaction.SetOperationType(NKikimrSchemeOp::ESchemeOpIncrementalRestoreFinalize);
        transaction.SetInternal(true);
        
        auto& finalize = *transaction.MutableIncrementalRestoreFinalize();
        finalize.SetOriginalOperationId(OperationId);
        finalize.SetBackupCollectionPathId(state.BackupCollectionPathId.LocalPathId);
        
        CollectTargetTablePaths(state, finalize);
        CollectBackupTablePaths(state, finalize);
        
        LOG_I("Sending finalization operation with txId: " << finalizeTxId);
        Self->Send(Self->SelfId(), request.Release());
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
            
            // Also collect index implementation tables that are in incoming restore state
            // These are restored separately but need to be finalized together with main tables
            for (auto& [pathId, pathInfo] : Self->PathsById) {
                if (pathInfo->PathState == NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore) {
                    TString pathString = TPath::Init(pathId, Self).PathString();
                    // Check if this is an index implementation table under one of our restored tables
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
            // For simple operations, collect paths directly from affected paths
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
            // For simple operations, collect backup paths directly
            TString bcPathString = TPath::Init(state.BackupCollectionPathId, Self).PathString();
            
            for (auto& [pathId, pathInfo] : Self->PathsById) {
                if (pathInfo->PathState == NKikimrSchemeOp::EPathState::EPathStateOutgoingIncrementalRestore ||
                    pathInfo->PathState == NKikimrSchemeOp::EPathState::EPathStateAwaitingOutgoingIncrementalRestore) {
                    TString pathString = TPath::Init(pathId, Self).PathString();
                    // Only add if it's under the backup collection
                    if (pathString.StartsWith(bcPathString)) {
                        finalize.AddBackupTablePaths(pathString);
                    }
                }
            }
        }
    }
};

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

    // Find the backup collection to get restore settings
    auto itBc = BackupCollections.find(backupCollectionPathId);
    if (itBc == BackupCollections.end()) {
        LOG_E("Backup collection not found for pathId: " << backupCollectionPathId);
        return;
    }

    if (incrementalBackupNames.empty()) {
        LOG_I("No incremental backups provided, nothing to restore");
        return;
    }

    // Initialize state for sequential processing of incremental backups
    TIncrementalRestoreState state;
    state.BackupCollectionPathId = backupCollectionPathId;
    state.OriginalOperationId = ui64(operationId.GetTxId());
    state.CurrentIncrementalIdx = 0;
    state.CurrentIncrementalStarted = false;
    
    // Add incremental backups (already sorted by timestamp based on backup names)
    for (const auto& backupName : incrementalBackupNames) {
        TPathId dummyPathId; // Will be filled when processing
        state.AddIncrementalBackup(dummyPathId, backupName, 0); // Timestamp will be inferred
        LOG_I("Handle(TEvRunIncrementalRestore) added incremental backup: '" << backupName << "'");
    }
    
    LOG_I("Handle(TEvRunIncrementalRestore) state now has " << state.IncrementalBackups.size() << " incremental backups");
    
    IncrementalRestoreStates[ui64(operationId.GetTxId())] = std::move(state);

    // Persist initial state row
    Execute(new TTxProgressIncrementalRestore(this, ui64(operationId.GetTxId())), ctx);
}

// Enhanced handler for TEvProgressIncrementalRestore  
void TSchemeShard::Handle(TEvPrivate::TEvProgressIncrementalRestore::TPtr& ev, const TActorContext& ctx) {
    ui64 operationId = ev->Get()->OperationId;
    
    LOG_I("Handle(TEvProgressIncrementalRestore)"
        << " operationId: " << operationId
        << " tablet: " << TabletID());

    Execute(new TTxProgressIncrementalRestore(this, operationId), ctx);
}

// Enumerate the per-table and per-index sub-ops for a single incremental backup
// and push them onto the orchestrator's PendingTables queue. Actual dispatch is
// throttled in DispatchPendingTables.
//
// Cap unit is per sub-op (one per table or per index impl-table), not per raw shard.
// This mirrors the Index Build orchestrator pattern at the unit incremental restore
// actually controls.
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

    // Enqueue table sub-ops
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

    // Enqueue index sub-ops (discovers indexes from the backup metadata tree)
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

// Pull PendingTables into in-flight up to the ICB cap. Moving-window dispatch:
// invoked at initial fan-out (ProcessNextIncrementalBackup) AND after every single
// completion (CheckForCompletedOperations, reached via the immediate
// NotifyIncrementalRestoreOperationCompleted -> TEvProgressIncrementalRestore
// path). Each call refills the window — there is no wave/batch boundary.
// The in-flight set stays at the cap until the queue drains.
void TSchemeShard::DispatchPendingTables(
    TIncrementalRestoreState& state,
    ui64 operationId,
    const TActorContext& ctx) {

    const i64 cap = IncrementalRestoreSettings.MaxIncrementalRestoreTablesInFlight;
    auto bcPath = TPath::Init(state.BackupCollectionPathId, this);

    while (!state.PendingTables.empty()
           && (cap == -1 || (i64)state.InProgressOperations.size() < cap)) {
        auto op = std::move(state.PendingTables.front());
        state.PendingTables.pop_front();

        switch (op.Kind) {
            case TIncrementalRestoreState::TPendingRestoreOp::EKind::Table:
                CreateSingleTableRestoreOperation(
                    state.BackupCollectionPathId,
                    operationId,
                    op.BackupName,
                    op.TablePath,
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
                    ctx,
                    op.SpecificImplTableName);
                break;
        }
    }

    LOG_I("DispatchPendingTables: in-flight=" << state.InProgressOperations.size()
          << " pending=" << state.PendingTables.size()
          << " cap=" << cap);
}

void TSchemeShard::TrackSubOpAndExpectedShards(
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
        for (const auto& [shardIdx, partitionIdx] : (*tableInfoPtr)->GetShard2PartitionIdx()) {
            tableOpState.ExpectedShards.insert(shardIdx);
            state.InvolvedShards.insert(shardIdx);
        }
    }
}

// Send the schemeshard MultiIncrementalRestore sub-op for one table entry.
// Extracted from the old CreateIncrementalRestoreOperation per-entry loop body.
void TSchemeShard::CreateSingleTableRestoreOperation(
    const TPathId& backupCollectionPathId,
    ui64 operationId,
    const TString& backupName,
    const TString& targetTablePath,
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

    LOG_I("Creating separate restore operation for table: " << incrBackupPathStr << " -> " << targetTablePath);

    auto tableRequest = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
    auto& tableRecord = tableRequest->Record;

    TTxId tableTxId = GetCachedTxId(ctx);
    tableRecord.SetTxId(ui64(tableTxId));

    auto& tableTx = *tableRecord.AddTransaction();
    tableTx.SetOperationType(NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups);
    tableTx.SetInternal(true);
    tableTx.SetWorkingDir(bcPath.PathString());

    auto& tableRestore = *tableTx.MutableRestoreMultipleIncrementalBackups();
    tableRestore.AddSrcTablePaths(incrBackupPathStr);
    tableRestore.SetDstTablePath(targetTablePath);

    TOperationId tableRestoreOpId(tableTxId, 0);

    auto stateIt = IncrementalRestoreStates.find(operationId);
    if (stateIt != IncrementalRestoreStates.end()) {
        TPath itemPath = TPath::Resolve(targetTablePath, this);
        TPathId tablePathId = (itemPath.IsResolved() && itemPath.Base()->IsTable())
            ? itemPath.Base()->PathId
            : TPathId{};
        TrackSubOpAndExpectedShards(tableRestoreOpId, tablePathId, operationId, stateIt->second);
        LOG_I("Table operation " << tableRestoreOpId << " expects "
              << stateIt->second.TableOperations[tableRestoreOpId].ExpectedShards.size() << " shards");
        LOG_I("Tracking operation " << tableRestoreOpId << " for incremental restore " << operationId);
    }

    LOG_I("Sending MultiIncrementalRestore operation for table: " << targetTablePath);
    Send(SelfId(), tableRequest.Release());
}

TString TSchemeShard::FindTargetTablePath(
    const TBackupCollectionInfo::TPtr& backupCollectionInfo,
    const TString& relativeTablePath) {

    // Map backup relative path to restore target path using backup collection's ExplicitEntryList
    for (const auto& item : backupCollectionInfo->Description.GetExplicitEntryList().GetEntries()) {
        if (item.GetType() != NKikimrSchemeOp::TBackupCollectionDescription_TBackupEntry_EType_ETypeTable) {
            continue;
        }

        // Extract the relative part of the item path
        // Item path is like /Root/db/table1, we need to extract the relative part
        TString itemPath = item.GetPath();

        // Only accept exact matches or suffixes preceded by path separator
        // to avoid false matches (e.g. "/Root/FooBar" should not match "Bar")
        if (itemPath == relativeTablePath || itemPath.EndsWith("/" + relativeTablePath)) {
            return itemPath;
        }
    }

    return {};
}

void TSchemeShard::EnqueueIndexesRecursive(
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

    // Try to find target table for current accumulated path
    TString targetTablePath = FindTargetTablePath(backupCollectionInfo, accumulatedRelativePath);

    if (!targetTablePath.empty()) {
        // Found target table, children are indexes
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
        // Not a table yet, descend into children to build up the path
        for (const auto& [childName, childPathId] : currentPath.Base()->GetChildren()) {
            auto childPath = TPath::Init(childPathId, this);
            TString newRelativePath = accumulatedRelativePath.empty()
                ? childName
                : accumulatedRelativePath + "/" + childName;

            EnqueueIndexesRecursive(
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

    // Check if indexes were backed up (OmitIndexes flag)
    bool omitIndexes = backupCollectionInfo->Description.GetIncrementalBackupConfig().GetOmitIndexes();
    if (omitIndexes) {
        LOG_I("Indexes were omitted in backup, skipping index restore");
        return;
    }

    // Path to index metadata: {backup}/__ydb_backup_meta/indexes
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

    // Start recursive discovery from the indexes root with empty accumulated path
    EnqueueIndexesRecursive(
        operationId,
        backupName,
        backupCollectionInfo,
        indexMetaPath,
        "", // Start with empty accumulated path
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
    const TActorContext& ctx,
    const TString& specificImplTableName)
{
    LOG_I("CreateSingleIndexRestoreOperation: table=" << targetTablePath
          << " index=" << indexName
          << " relativeTablePath=" << relativeTablePath
          << " specificImplTableName=" << specificImplTableName);

    // Validate target table exists
    const TPath targetTablePathObj = TPath::Resolve(targetTablePath, this);
    if (!targetTablePathObj.IsResolved() || !targetTablePathObj.Base()->IsTable()) {
        LOG_W("Target table not found or invalid: " << targetTablePath);
        return;
    }

    // Find the index and its impl table
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

    // Destination: {table}/{index}/indexImplTable
    auto indexImplTablePath = TPath::Init(indexImplTablePathId, this);
    TString dstIndexImplPath = indexImplTablePath.PathString();

    LOG_I("Creating index restore operation: " << srcIndexBackupPath << " -> " << dstIndexImplPath);

    // Create restore request (SAME structure as table restore)
    auto indexRequest = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
    auto& indexRecord = indexRequest->Record;

    TTxId indexTxId = GetCachedTxId(ctx);
    indexRecord.SetTxId(ui64(indexTxId));

    auto& indexTx = *indexRecord.AddTransaction();
    indexTx.SetOperationType(NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups);
    indexTx.SetInternal(true);
    indexTx.SetWorkingDir(bcPath.PathString());

    auto& indexRestore = *indexTx.MutableRestoreMultipleIncrementalBackups();
    indexRestore.AddSrcTablePaths(srcIndexBackupPath);
    indexRestore.SetDstTablePath(dstIndexImplPath);

    // Track this operation as part of incremental restore
    TOperationId indexRestoreOpId(indexTxId, 0);

    auto stateIt = IncrementalRestoreStates.find(operationId);
    if (stateIt != IncrementalRestoreStates.end()) {
        TrackSubOpAndExpectedShards(indexRestoreOpId, indexImplTablePathId, operationId, stateIt->second);
        LOG_I("Index operation " << indexRestoreOpId << " expects "
              << stateIt->second.TableOperations[indexRestoreOpId].ExpectedShards.size() << " shards");
        LOG_I("Tracking index operation " << indexRestoreOpId << " for incremental restore " << operationId);
    }

    // Send the request (parallel with table operations)
    LOG_I("Sending index restore operation for: " << dstIndexImplPath);
    Send(SelfId(), indexRequest.Release());
}

// SOLE notification path for incremental restore progress:
//
//   DataShard scan completes
//     → DataShard Tx completes (normal operation pipeline)
//     → SchemeShard RemoveTx() [schemeshard_impl.cpp]
//     → NotifyIncrementalRestoreOperationCompleted() (this function)
//     → schedules TEvProgressIncrementalRestore
//     → TTxProgressIncrementalRestore::Execute() advances state + persists to DB
//
// Required maps: IncrementalRestoreOperationToState, TxIdToIncrementalRestore
// These maps are populated by CreateSingleTableRestoreOperation() and
// CreateSingleIndexRestoreOperation() (both invoked by DispatchPendingTables).
// After reboot, maps are empty — TTxProgressIncrementalRestore handles this by
// re-calling ProcessNextIncrementalBackup() which repopulates the queue and maps.
// The actor model guarantees serialized execution, so no race conditions.
void TSchemeShard::NotifyIncrementalRestoreOperationCompleted(const TOperationId& operationId, const TActorContext& ctx) {
    auto it = IncrementalRestoreOperationToState.find(operationId);
    if (it != IncrementalRestoreOperationToState.end()) {
        ui64 incrementalRestoreId = it->second;

        LOG_I("Operation " << operationId << " completed, triggering progress check for incremental restore " << incrementalRestoreId);

        auto progressEvent = MakeHolder<TEvPrivate::TEvProgressIncrementalRestore>(incrementalRestoreId);
        ctx.Send(ctx.SelfID, progressEvent.Release());
    }
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
    
    // Find the incremental restore operation associated with this transaction
    auto txToIncrRestoreIt = TxIdToIncrementalRestore.find(txId);
    if (txToIncrRestoreIt != TxIdToIncrementalRestore.end()) {
        return new TTxProgressIncrementalRestore(this, txToIncrRestoreIt->second);
    }
    
    // Not an incremental restore operation, return nullptr to indicate no action needed
    LOG_D("Transaction " << txId << " is not associated with incremental restore");
    return nullptr;
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TTxId completedTxId, const TActorContext& ctx) {
    // Find the incremental restore operation associated with this transaction
    auto txToIncrRestoreIt = TxIdToIncrementalRestore.find(completedTxId);
    if (txToIncrRestoreIt != TxIdToIncrementalRestore.end()) {
        return new TTxProgressIncrementalRestore(this, txToIncrRestoreIt->second);
    }
    
    // Not an incremental restore operation, return nullptr
    LOG_D("Transaction " << completedTxId << " is not associated with incremental restore");
    return nullptr;
}

} // namespace NKikimr::NSchemeShard
