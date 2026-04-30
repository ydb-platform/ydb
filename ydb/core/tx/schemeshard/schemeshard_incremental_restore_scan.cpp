#include "schemeshard_impl.h"
#include "schemeshard__backup_collection_common.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/tx/datashard/scan_common.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
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
            NIceDb::TUpdate<Schema::IncrementalRestoreState::CurrentIncrementalIdx>(state.CurrentIncrementalIdx)
        );

        CheckForCompletedOperations(state, db, ctx);

        if (CompletedOperationsChanged) {
            TString serializedCompletedOperations = SerializeOperationIds(state.CompletedOperations);
            db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                NIceDb::TUpdate<Schema::IncrementalRestoreState::SerializedData>(serializedCompletedOperations)
            );
            LOG_I("Persisted CompletedOperations update: " << serializedCompletedOperations);
        }

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
            // Heartbeat: covers missed completion notifications.
            auto progressEvent = MakeHolder<TEvPrivate::TEvProgressIncrementalRestore>(OperationId);
            Self->Schedule(TDuration::Seconds(1), progressEvent.Release());
        } else {
            if (state.AllIncrementsProcessed()) {
                LOG_W("All increments processed but state is still Running, triggering finalization");
                // Per-item finalize is allocated asynchronously via TxAllocatorClient.
                // FinalizeTxId column is gone; the IncrementalRestoreItem row carries
                // the binding. The orchestrator only flips State=Finalizing.
                state.State = TIncrementalRestoreState::EState::Finalizing;
                db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                    NIceDb::TUpdate<Schema::IncrementalRestoreState::State>(static_cast<ui32>(state.State))
                );
                FinalizeIncrementalRestoreOperation(txc, ctx, state);
            } else {
                LOG_I("No operations in progress, starting incremental backup #" << state.CurrentIncrementalIdx);
                ProcessNextIncrementalBackup(state, db, ctx);
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
    
    bool HandleRetryPath(TIncrementalRestoreState& state, NIceDb::TNiceDb& db, const TActorContext& ctx) {
        const i64 cap = Self->IncrementalRestoreSettings.MaxIncrementalRestoreRetriesPerIncremental;
        // Skip budget check while a retry is in flight to avoid premature failure.
        const bool budgetExceeded = (cap != -1)
            && !state.RetryScheduled
            && (i64)state.CurrentIncrementalRetryCount >= cap;
        if (state.NonRetriableFailure || budgetExceeded) {
            LOG_E("Incremental #" << state.CurrentIncrementalIdx
                  << " short-circuiting to Failed: nonRetriable="
                  << state.NonRetriableFailure
                  << " retryCount=" << state.CurrentIncrementalRetryCount
                  << " cap=" << cap);
            state.RetryScheduled = false;
            state.NextRetryAttemptAt = TInstant::Zero();
            // Route through PersistIncrementalRestoreTerminalState so FinalStatus is durable across reboots.
            const TString failureIssues = state.NonRetriableFailure
                ? TString("Non-retriable failure during incremental restore")
                : TString("Retry budget exhausted during incremental restore");
            TSchemeShard::PersistIncrementalRestoreTerminalState(Self, db, OperationId, state,
                TIncrementalRestoreState::EState::Failed,
                static_cast<ui32>(Ydb::StatusIds::GENERIC_ERROR),
                failureIssues);
            return true;
        }

        if (state.RetryScheduled) {
            if (ctx.Now() < state.NextRetryAttemptAt) {
                LOG_I("Backoff window in flight for incremental #"
                      << state.CurrentIncrementalIdx
                      << " (retry " << state.CurrentIncrementalRetryCount
                      << ", until " << state.NextRetryAttemptAt
                      << "), skipping concurrent retry trigger");
                return true;
            }

            LOG_I("Backoff timer fired for incremental #" << state.CurrentIncrementalIdx
                  << ", proceeding with retry attempt " << state.CurrentIncrementalRetryCount);
            state.RetryScheduled = false;
            state.NextRetryAttemptAt = TInstant::Zero();
            state.RetryNeeded = false;

            state.InProgressOperations.clear();
            state.CompletedOperations.clear();
            state.PendingTables.clear();
            state.TableOperations.clear();
            state.CurrentIncrementalStarted = false;

            // Tier-A retry path: drop any stale per-item rows so the next
            // dispatch starts clean. The new ProcessNextIncrementalBackup
            // will rebuild PendingTables and re-enqueue items (each with a
            // fresh ItemSeq and a fresh TxAllocate request).
            Self->CleanupIncrementalRestoreItems(OperationId, db, &state);

            TString serializedEmpty = SerializeOperationIds(state.CompletedOperations);
            db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
                NIceDb::TUpdate<Schema::IncrementalRestoreState::SerializedData>(serializedEmpty)
            );

            ProcessNextIncrementalBackup(state, db, ctx);
            return true;
        }

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

    bool HandleAllOperationsComplete(TIncrementalRestoreState& state, NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
        LOG_I("All operations for current incremental backup completed, moving to next");
        state.MarkCurrentIncrementalComplete();
        state.MoveToNextIncremental();

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::IncrementalRestoreState>().Key(OperationId).Update(
            NIceDb::TUpdate<Schema::IncrementalRestoreState::CurrentIncrementalIdx>(state.CurrentIncrementalIdx)
        );

        LOG_I("After MoveToNextIncremental: CurrentIncrementalIdx=" << state.CurrentIncrementalIdx
              << ", IncrementalBackups.size()=" << state.IncrementalBackups.size());

        if (state.AllIncrementsProcessed()) {
            LOG_I("All incremental backups processed, performing finalization");
            // Per-item finalize: TxAllocatorClient asynchronously supplies a
            // TxId. The orchestrator transitions the state and the new
            // TTxProgressIncrementalRestoreAllocateResult dispatches the actual
            // finalize ModifyScheme once the TxId arrives.
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

            if (Self->Operations.contains(txId)) {
                stillInProgress.insert(opId);
            } else {
                if (!state.CompletedOperations.contains(opId)) {
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

                    // Per-item cleanup: drop the IncrementalRestoreItem row and
                    // mappings tied to this TxId. The orchestrator-side
                    // bookkeeping (CompletedOperations, TableOperations) is
                    // unchanged.
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
        state.RetryNeeded |= hasFailedOperations;

        // Non-retriable failure is sticky: short-circuit to Failed rather than burning the retry budget.
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

        // Top-up freed capacity; skip if a retry is pending (it will rebuild the queue).
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

        // Build the finalize request now (paths derived from current state)
        // and stash on the item; TxId is filled in once TxAllocatorClient
        // replies. CleanupIncrementalRestoreItems on terminal/forget removes
        // the row to keep the persistent set bounded.
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

    // Per-sub-op rows are no longer needed once the restore is terminal.
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

    // Register a state row even for full-only restores so Get/List have something
    // to report. The orchestrator drives the empty-increments case straight to Completed.
    TIncrementalRestoreState state;
    state.BackupCollectionPathId = backupCollectionPathId;
    state.OriginalOperationId = ui64(operationId.GetTxId());
    state.CurrentIncrementalIdx = 0;
    state.CurrentIncrementalStarted = false;

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

    // The cap now bounds (in-flight schema ops + items waiting for a TxId from
    // the allocator); both consume a slot so we don't overshoot the rate limit
    // when many allocator replies are pending. PendingItems holds items
    // awaiting allocator replies; InProgressOperations holds the modify-scheme
    // sub-ops the orchestrator is awaiting completion on.
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
        for (const auto& [shardIdx, partitionIdx] : (*tableInfoPtr)->GetShard2PartitionIdx()) {
            tableOpState.ExpectedShards.insert(shardIdx);
        }
    }
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
    // TxId is filled in by TTxProgressIncrementalRestoreAllocateResult once
    // TxAllocatorClient supplies a TxId.

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

    EnqueueIncrementalRestoreItem(
        operationId, state,
        TIncrementalRestoreState::TItem::EKind::Table,
        tablePathId,
        std::move(tableRequest),
        db, ctx);
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
    // TxId is filled in by TTxProgressIncrementalRestoreAllocateResult once
    // TxAllocatorClient supplies a TxId.

    auto& indexTx = *indexRecord.AddTransaction();
    indexTx.SetOperationType(NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups);
    indexTx.SetInternal(true);
    indexTx.SetWorkingDir(bcPath.PathString());

    auto& indexRestore = *indexTx.MutableRestoreMultipleIncrementalBackups();
    indexRestore.AddSrcTablePaths(srcIndexBackupPath);
    indexRestore.SetDstTablePath(dstIndexImplPath);

    EnqueueIncrementalRestoreItem(
        operationId, state,
        TIncrementalRestoreState::TItem::EKind::Index,
        indexImplTablePathId,
        std::move(indexRequest),
        db, ctx);
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
    const TActorContext& ctx)
{
    TIncrementalRestoreState::TItem item;
    item.ItemSeq = state.NextItemSeq++;
    item.Kind = kind;
    item.TablePathId = tablePathId;
    item.WaitTxId = ui64(InvalidTxId);
    item.PendingRequest = request.Release();

    db.Table<Schema::IncrementalRestoreItem>()
        .Key(originalOpId, item.ItemSeq)
        .Update(
            NIceDb::TUpdate<Schema::IncrementalRestoreItem::ItemKind>(static_cast<ui32>(kind)),
            NIceDb::TUpdate<Schema::IncrementalRestoreItem::TablePathId>(tablePathId.LocalPathId),
            NIceDb::TUpdate<Schema::IncrementalRestoreItem::WaitTxId>(ui64(InvalidTxId)));

    state.PendingItems.push_back(std::move(item));

    // Cookie packs (originalOpId<<32 | itemSeq) so per-item TxId binding is
    // unambiguous regardless of how many concurrent allocations are in flight.
    const ui64 cookie = (originalOpId << 32) | state.PendingItems.back().ItemSeq;
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[IncrementalRestore] EnqueueIncrementalRestoreItem op=" << originalOpId
        << " itemSeq=" << state.PendingItems.back().ItemSeq
        << " kind=" << static_cast<ui32>(kind)
        << " tablePathId=" << tablePathId
        << " cookie=" << cookie);
    ctx.Send(TxAllocatorClient,
        new TEvTxAllocatorClient::TEvAllocate(),
        /*flags=*/0,
        cookie);
}

void TSchemeShard::CleanupIncrementalRestoreItems(
    ui64 originalOpId,
    NIceDb::TNiceDb& db,
    TIncrementalRestoreState* state)
{
    // Walk through ItemSeq values we know about (in-memory) and DELETE rows.
    // For full DB-row coverage even after reboot loss of in-memory state, we
    // fall back to a Range scan for the originalOpId prefix.
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

    // Sweep the global TxId map for entries pointing here. THashMap::erase
    // returns void, so we collect keys and delete in a second pass.
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

// Consumes TEvAllocateResult, binds the allocated TxId to the exact item
// identified by the packed cookie, and sends the prebuilt ModifyScheme
// request. Empty TxIds (allocator transient failure) leave the item in
// PendingItems and schedule a re-allocate via TEvProgressIncrementalRestore.
class TSchemeShard::TTxProgressIncrementalRestoreAllocateResult : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
public:
    using TBase = NTabletFlatExecutor::TTransactionBase<TSchemeShard>;
    TTxProgressIncrementalRestoreAllocateResult(TSchemeShard* self,
            TEvTxAllocatorClient::TEvAllocateResult::TPtr ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        const ui64 cookie = Ev->Cookie;
        const ui64 originalOpId = cookie >> 32;
        const ui32 itemSeq = static_cast<ui32>(cookie & 0xFFFFFFFFULL);

        LOG_I("TTxProgressIncrementalRestoreAllocateResult"
            << " cookie=" << cookie
            << " originalOpId=" << originalOpId
            << " itemSeq=" << itemSeq
            << " txIdsCount=" << Ev->Get()->TxIds.size());

        auto stateIt = Self->IncrementalRestoreStates.find(originalOpId);
        if (stateIt == Self->IncrementalRestoreStates.end()) {
            // Restore was forgotten between Send and Recv; allocator TxId
            // simply leaks (allocator pool refills naturally). No SS-side
            // state mutation; TxIdToIncrementalRestore stays clean because it
            // is populated only AFTER state-found check passes.
            LOG_W("TTxProgressIncrementalRestoreAllocateResult: state for "
                  << originalOpId << " not found; dropping allocator result");
            return true;
        }
        auto& state = stateIt->second;

        // Match by exact ItemSeq (cookie packs it) — no FIFO-by-arrival
        // assumption needed. Concurrent allocations may complete in any order.
        auto pendingIt = std::find_if(state.PendingItems.begin(), state.PendingItems.end(),
            [itemSeq](const auto& it) { return it.ItemSeq == itemSeq; });
        if (pendingIt == state.PendingItems.end()) {
            LOG_W("TTxProgressIncrementalRestoreAllocateResult: itemSeq "
                  << itemSeq << " not found in PendingItems for op "
                  << originalOpId << "; dropping allocator result (item likely "
                  << "completed or canceled while allocate was in flight)");
            return true;
        }

        if (Ev->Get()->TxIds.empty()) {
            // Allocator transient failure. Re-send TEvAllocate for the same
            // (originalOpId, itemSeq) cookie after a small backoff. The item
            // stays in PendingItems and the persisted row stays at
            // WaitTxId=Invalid until the next allocator reply binds a TxId.
            // Tier-B retry: does NOT consume the per-incremental retry
            // budget (CurrentIncrementalRetryCount is unchanged).
            LOG_W("TTxProgressIncrementalRestoreAllocateResult: empty TxIds; "
                  << "scheduling allocator retry for op " << originalOpId
                  << " itemSeq " << itemSeq);
            ScheduleAllocatorRetry(originalOpId, itemSeq, ctx);
            return true;
        }
        const TTxId allocatedTxId = TTxId(Ev->Get()->TxIds.front());

        // Move from PendingItems -> InFlightItems with the bound TxId.
        TIncrementalRestoreState::TItem item = std::move(*pendingIt);
        state.PendingItems.erase(pendingIt);
        item.WaitTxId = ui64(allocatedTxId);
        state.WaitTxIdToItemSeq[ui64(allocatedTxId)] = item.ItemSeq;

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::IncrementalRestoreItem>()
            .Key(originalOpId, item.ItemSeq)
            .Update(NIceDb::TUpdate<Schema::IncrementalRestoreItem::WaitTxId>(item.WaitTxId));

        Self->TxIdToIncrementalRestore[allocatedTxId] = originalOpId;

        const auto kind = item.Kind;
        const TPathId tablePathId = item.TablePathId;

        // Stamp the TxId on the prebuilt request and send.
        TAutoPtr<NActors::IEventBase> baseRequest = std::move(item.PendingRequest);
        if (!baseRequest) {
            LOG_E("TTxProgressIncrementalRestoreAllocateResult: missing "
                  << "PendingRequest for op " << originalOpId
                  << " itemSeq " << itemSeq);
            return true;
        }
        // Safe upcast — PendingRequest is always TEvModifySchemeTransaction.
        auto* request = static_cast<TEvSchemeShard::TEvModifySchemeTransaction*>(baseRequest.Release());
        request->Record.SetTxId(ui64(allocatedTxId));

        if (kind == TIncrementalRestoreState::TItem::EKind::Table
            || kind == TIncrementalRestoreState::TItem::EKind::Index) {
            // Track sub-op for shard completion accounting BEFORE sending so
            // that the orchestrator's CheckForCompletedOperations sees
            // InProgressOperations as soon as the sub-op lands in
            // Self->Operations.
            TOperationId subOpId(allocatedTxId, 0);
            Self->TrackIncrementalRestoreSubOpAndExpectedShards(
                subOpId, tablePathId, originalOpId, state);
        }
        // Move into InFlightItems AFTER consuming PendingRequest.
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
        // Tier-B retry: re-send TEvAllocate for the same packed cookie after
        // a small backoff. We Schedule an IEventHandle directly to the
        // TxAllocatorClient mailbox so the response routes back to SS via the
        // same cookie path. CurrentIncrementalRetryCount is intentionally NOT
        // touched — tier-A retry budget is independent.
        const ui64 cookie = (originalOpId << 32) | itemSeq;
        const TActorId txAllocator = Self->TxAllocatorClient;
        std::unique_ptr<IEventHandle> ev(new IEventHandle(
            txAllocator, Self->SelfId(),
            new TEvTxAllocatorClient::TEvAllocate(),
            /*flags=*/0, cookie));
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

} // namespace NKikimr::NSchemeShard
