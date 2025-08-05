#include "schemeshard_impl.h"
#include "schemeshard_utils.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

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
        
        CheckForCompletedOperations(state, ctx);
        
        // Check if all operations for current incremental backup are complete
        LOG_I("Checking completion: InProgressOperations.size()=" << state.InProgressOperations.size() 
              << ", CompletedOperations.size()=" << state.CompletedOperations.size()
              << ", CurrentIncrementalIdx=" << state.CurrentIncrementalIdx
              << ", IncrementalBackups.size()=" << state.IncrementalBackups.size());
              
        if (state.AreAllCurrentOperationsComplete()) {
            LOG_I("All operations for current incremental backup completed, moving to next");
            state.MarkCurrentIncrementalComplete();
            state.MoveToNextIncremental();
            
            LOG_I("After MoveToNextIncremental: CurrentIncrementalIdx=" << state.CurrentIncrementalIdx
                  << ", IncrementalBackups.size()=" << state.IncrementalBackups.size());
            
            if (state.AllIncrementsProcessed()) {
                LOG_I("All incremental backups processed, performing finalization");
                FinalizeIncrementalRestoreOperation(txc, ctx, state);
                return true;
            }
            
            // Start processing next incremental backup
            ProcessNextIncrementalBackup(state, ctx);
        } else if (!state.InProgressOperations.empty()) {
            // Still have operations in progress, schedule another check
            auto progressEvent = MakeHolder<TEvPrivate::TEvProgressIncrementalRestore>(OperationId);
            Self->Schedule(TDuration::Seconds(1), progressEvent.Release());
        } else {
            // No operations in progress, start the first incremental backup
            LOG_I("No operations in progress, starting first incremental backup");
            ProcessNextIncrementalBackup(state, ctx);
        }
        
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_I("TTxProgressIncrementalRestore::Complete"
            << " operationId: " << OperationId);
    }

private:
    ui64 OperationId;
    
    void CheckForCompletedOperations(TIncrementalRestoreState& state, const TActorContext& ctx) {
        THashSet<TOperationId> stillInProgress;
        
        for (const auto& opId : state.InProgressOperations) {
            TTxId txId = opId.GetTxId();
            
            if (Self->Operations.contains(txId)) {
                stillInProgress.insert(opId);
            } else {
                state.CompletedOperations.insert(opId);
                LOG_I("Operation " << opId << " completed for incremental restore " << OperationId);
            }
        }
        
        state.InProgressOperations = std::move(stillInProgress);
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
        
        Self->CreateIncrementalRestoreOperation(
            state.BackupCollectionPathId,
            OperationId,
            currentIncremental->BackupPath,
            ctx
        );
        
        state.CurrentIncrementalStarted = true;
        
        auto progressEvent = MakeHolder<TEvPrivate::TEvProgressIncrementalRestore>(OperationId);
        Self->Schedule(TDuration::Seconds(1), progressEvent.Release());
    }
    
    void FinalizeIncrementalRestoreOperation(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx, TIncrementalRestoreState& state) {
        Y_UNUSED(txc);
        LOG_I("Starting finalization of incremental restore operation: " << OperationId);
        
        // Increment in-flight counter for finalization operation
        Self->TabletCounters->Simple()[TTxState::TxTypeInFlightCounter(TTxState::TxIncrementalRestoreFinalize)].Add(1);
        
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
        
        // Add metadata about what needs finalization
        auto& finalize = *transaction.MutableIncrementalRestoreFinalize();
        finalize.SetOriginalOperationId(OperationId);
        finalize.SetBackupCollectionPathId(state.BackupCollectionPathId.LocalPathId);
        
        // Add all paths that need normalization
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
            
            // Add full backup tables
            TString fullBackupPath = JoinPath({bcPathString, op.GetFullBackupTrimmedName()});
            for (const auto& tablePath : op.GetTablePathList()) {
                TPath fullPath = TPath::Resolve(tablePath, Self);
                TString tableName = fullPath.LeafName();
                TString sourceTablePath = JoinPath({fullBackupPath, tableName});
                finalize.AddBackupTablePaths(sourceTablePath);
            }
            
            // Add incremental backup tables
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
    
    void CleanupMappings(const TActorContext& ctx, ui64 operationId) {
        auto txIt = Self->TxIdToIncrementalRestore.begin();
        while (txIt != Self->TxIdToIncrementalRestore.end()) {
            if (txIt->second == operationId) {
                auto toErase = txIt++;
                Self->TxIdToIncrementalRestore.erase(toErase);
            } else {
                ++txIt;
            }
        }
        
        auto opIt = Self->IncrementalRestoreOperationToState.begin();
        while (opIt != Self->IncrementalRestoreOperationToState.end()) {
            if (opIt->second == operationId) {
                auto toErase = opIt++;
                Self->IncrementalRestoreOperationToState.erase(toErase);
            } else {
                ++opIt;
            }
        }
        
        Self->LongIncrementalRestoreOps.erase(TOperationId(operationId, 0));
        
        LOG_I("Cleaned up mappings for operation: " << operationId);
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
    
    // Execute progress transaction immediately instead of sending an event
    // This ensures the finalization logic gets triggered even for simple operations
    LOG_I("Handle(TEvRunIncrementalRestore) executing progress transaction directly");
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

// Enhanced handler for DataShard completion notifications
void TSchemeShard::Handle(TEvDataShard::TEvIncrementalRestoreResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    
    LOG_I("Handle(TEvIncrementalRestoreResponse)"
        << " txId: " << record.GetTxId()
        << " tableId: " << record.GetTableId()
        << " operationId: " << record.GetOperationId()
        << " shardIdx: " << record.GetShardIdx()
        << " incrementalIdx: " << record.GetIncrementalIdx()
        << " status: " << (int)record.GetRestoreStatus()
        << " from DataShard, tablet: " << TabletID());

    bool success = (record.GetRestoreStatus() == NKikimrTxDataShard::TEvIncrementalRestoreResponse::SUCCESS);
    
    if (!success) {
        LOG_W("DataShard reported incremental restore error: " << record.GetErrorMessage());
    }
    
    TTabletId shardId = TTabletId(ev->Sender.NodeId());
    TShardIdx shardIdx = GetShardIdx(shardId);
    TTxId txId = TTxId(record.GetTxId());
    TOperationId operationId(txId, 0);
    
    LOG_I("Processing DataShard response from shardId: " << shardId 
        << " shardIdx: " << shardIdx 
        << " operationId: " << operationId);
    
    auto opStateIt = IncrementalRestoreOperationToState.find(operationId);
    if (opStateIt == IncrementalRestoreOperationToState.end()) {
        LOG_W("No incremental restore state mapping found for operation: " << operationId);
        return;
    }
    
    ui64 globalOperationId = opStateIt->second;
    auto stateIt = IncrementalRestoreStates.find(globalOperationId);
    if (stateIt == IncrementalRestoreStates.end()) {
        LOG_W("No incremental restore state found for global operation: " << globalOperationId);
        return;
    }
    
    auto& state = stateIt->second;
    
    // Check if this operation is in progress
    if (state.InProgressOperations.find(operationId) == state.InProgressOperations.end()) {
        LOG_W("Operation " << operationId << " not found in InProgressOperations for global operation: " << globalOperationId);
        return;
    }
    
    // Find the table operation state
    auto tableOpIt = state.TableOperations.find(operationId);
    if (tableOpIt == state.TableOperations.end()) {
        LOG_W("Table operation " << operationId << " not found in TableOperations for global operation: " << globalOperationId);
        return;
    }
    
    auto& tableOpState = tableOpIt->second;
    
    if (success) {
        tableOpState.CompletedShards.insert(shardIdx);
        LOG_I("Marked shard " << shardIdx << " as completed for operation " << operationId);
    } else {
        tableOpState.FailedShards.insert(shardIdx);
        LOG_W("Marked shard " << shardIdx << " as failed for operation " << operationId);
    }
    
    if (tableOpState.AllShardsComplete()) {
        LOG_I("All shards completed for table operation " << operationId);
        
        state.InProgressOperations.erase(operationId);
        state.CompletedOperations.insert(operationId);
        
        IncrementalRestoreOperationToState.erase(operationId);
        TxIdToIncrementalRestore.erase(operationId.GetTxId());
        
        if (state.AreAllCurrentOperationsComplete()) {
            LOG_I("All table operations for current incremental backup completed, moving to next");
            state.MarkCurrentIncrementalComplete();
            state.MoveToNextIncremental();
            
            if (state.AllIncrementsProcessed()) {
                LOG_I("All incremental backups processed, cleaning up");
                IncrementalRestoreStates.erase(globalOperationId);
            } else {
                auto progressEvent = MakeHolder<TEvPrivate::TEvProgressIncrementalRestore>(globalOperationId);
                Schedule(TDuration::Seconds(1), progressEvent.Release());
            }
        }
    }
}

// Create a MultiIncrementalRestore operation for a single incremental backup
void TSchemeShard::CreateIncrementalRestoreOperation(
    const TPathId& backupCollectionPathId,
    ui64 operationId, 
    const TString& backupName,
    const TActorContext& ctx) {
    
    LOG_I("CreateIncrementalRestoreOperation for backup: " << backupName 
          << " operationId: " << operationId
          << " backupCollectionPathId: " << backupCollectionPathId);
    
    // Find the backup collection to get restore settings
    auto itBc = BackupCollections.find(backupCollectionPathId);
    if (itBc == BackupCollections.end()) {
        LOG_E("Backup collection not found for pathId: " << backupCollectionPathId);
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
        
        TString incrBackupPathStr = JoinPath({bcPath.PathString(), backupName + "_incremental", relativeItemPath});
        const TPath& incrBackupPath = TPath::Resolve(incrBackupPathStr, this);
        
        if (incrBackupPath.IsResolved()) {
            LOG_I("Creating separate restore operation for table: " << incrBackupPathStr << " -> " << item.GetPath());
            
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
            
            tableRestore.SetDstTablePath(item.GetPath());
            
            TOperationId tableRestoreOpId(tableTxId, 0);
            IncrementalRestoreOperationToState[tableRestoreOpId] = operationId;
            TxIdToIncrementalRestore[tableTxId] = operationId;
            
            auto stateIt = IncrementalRestoreStates.find(operationId);
            if (stateIt != IncrementalRestoreStates.end()) {
                stateIt->second.InProgressOperations.insert(tableRestoreOpId);
                
                auto& tableOpState = stateIt->second.TableOperations[tableRestoreOpId];
                tableOpState.OperationId = tableRestoreOpId;
                
                TPath itemPath = TPath::Resolve(item.GetPath(), this);
                if (itemPath.IsResolved() && itemPath.Base()->IsTable()) {
                    auto tableInfo = Tables.FindPtr(itemPath.Base()->PathId);
                    if (tableInfo) {
                        for (const auto& [shardIdx, partitionIdx] : (*tableInfo)->GetShard2PartitionIdx()) {
                            tableOpState.ExpectedShards.insert(shardIdx);
                        }
                        LOG_I("Table operation " << tableRestoreOpId << " expects " << tableOpState.ExpectedShards.size() << " shards");
                    }
                }
                
                LOG_I("Tracking operation " << tableRestoreOpId << " for incremental restore " << operationId);
            }
            
            LOG_I("Sending MultiIncrementalRestore operation for table: " << item.GetPath());
            Send(SelfId(), tableRequest.Release());
        } else {
            LOG_W("Incremental backup path not found: " << incrBackupPathStr);
        }
    }
    
    LOG_I("Created separate restore operations for incremental backup: " << backupName);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(ui64 operationId) {
    return new TTxProgressIncrementalRestore(this, operationId);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvPrivate::TEvRunIncrementalRestore::TPtr& ev) {
    return new TTxProgressIncrementalRestore(this, ev->Get()->BackupCollectionPathId.LocalPathId);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvPrivate::TEvProgressIncrementalRestore::TPtr& ev) {
    return new TTxProgressIncrementalRestore(this, ev->Get()->OperationId);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
    const auto& txIds = ev->Get()->TxIds;
    ui64 operationId = txIds.empty() ? 0 : txIds[0];
    return new TTxProgressIncrementalRestore(this, operationId);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
    ui64 operationId = ev->Get()->Record.GetTxId();
    return new TTxProgressIncrementalRestore(this, operationId);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TTxId txId) {
    ui64 operationId = ui64(txId);
    return new TTxProgressIncrementalRestore(this, operationId);
}

} // namespace NKikimr::NSchemeShard
