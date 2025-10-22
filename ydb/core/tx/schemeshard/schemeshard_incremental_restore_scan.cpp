#include "schemeshard_impl.h"
#include "schemeshard_utils.h"

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

enum class EIncrementalRestoreShardStatus : ui32 {
    Unknown = 0,
    Success = 1,
    Failed = 2
};

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
            LOG_I("All operations for current incremental backup completed, moving to next");
            state.MarkCurrentIncrementalComplete();
            state.MoveToNextIncremental();

            // Persist CurrentIncrementalIdx advance
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
    
    void CheckForCompletedOperations(TIncrementalRestoreState& state, const TActorContext& ctx) {
        THashSet<TOperationId> stillInProgress;
        bool operationsCompleted = false;
        
        for (const auto& opId : state.InProgressOperations) {
            TTxId txId = opId.GetTxId();
            
            if (Self->Operations.contains(txId)) {
                stillInProgress.insert(opId);
            } else {
                // Check if we've already tracked this completion
                if (!state.CompletedOperations.contains(opId)) {
                    state.CompletedOperations.insert(opId);
                    operationsCompleted = true;
                    LOG_I("Operation " << opId << " completed for incremental restore " << OperationId);
                }
            }
        }
        
        state.InProgressOperations = std::move(stillInProgress);
        
        // If operations were completed, update the persisted state
        if (operationsCompleted) {
            SetCompletedOperationsChanged(true);
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

// Transaction to persist per-shard progress of incremental restore
class TTxPersistIncrementalRestoreShardProgress : public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
public:
    using TBase = NTabletFlatExecutor::TTransactionBase<TSchemeShard>;
    TTxPersistIncrementalRestoreShardProgress(TSchemeShard* self, ui64 opId, ui64 shardIdx, EIncrementalRestoreShardStatus status)
        : TBase(self)
        , OpId(opId)
        , ShardIdx(shardIdx)
        , Status(status)
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::IncrementalRestoreShardProgress>().Key(OpId, ShardIdx).Update(
            NIceDb::TUpdate<Schema::IncrementalRestoreShardProgress::Status>(static_cast<ui32>(Status))
        );

        auto stateIt = Self->IncrementalRestoreStates.find(OpId);
        if (stateIt != Self->IncrementalRestoreStates.end()) {
            TShardIdx shardIdxObj = Self->GetShardIdx(TTabletId(ShardIdx));
            stateIt->second.InvolvedShards.insert(shardIdxObj);

            // Persist to long operation if exists
            auto longOpIt = Self->LongIncrementalRestoreOps.find(TOperationId(OpId, 0));
            if (longOpIt != Self->LongIncrementalRestoreOps.end()) {
                // Add shard to the protobuf if not already present
                auto& proto = longOpIt->second;
                bool shardAlreadyTracked = false;
                for (size_t i = 0; i < proto.InvolvedShardsSize(); ++i) {
                    if (proto.GetInvolvedShards(i) == ShardIdx) {
                        shardAlreadyTracked = true;
                        break;
                    }
                }
                if (!shardAlreadyTracked) {
                    proto.AddInvolvedShards(ShardIdx);
                    // Persist the updated long operation
                    db.Table<Schema::IncrementalRestoreOperations>().Key(OpId).Update(
                        NIceDb::TUpdate<Schema::IncrementalRestoreOperations::Operation>(proto.SerializeAsString())
                    );
                }
            }
        }

        return true;
    }

    void Complete(const TActorContext&) override {}

private:
    ui64 OpId;
    ui64 ShardIdx;
    EIncrementalRestoreShardStatus Status;
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

    // Persist shard progress row via tx
    {
        EIncrementalRestoreShardStatus status = success ? EIncrementalRestoreShardStatus::Success : EIncrementalRestoreShardStatus::Failed;
        Execute(new TTxPersistIncrementalRestoreShardProgress(this, record.GetOperationId(), record.GetShardIdx(), status), ctx);
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
                LOG_I("All incremental backups processed, operation complete but keeping in memory for list operations");
                
                NotifyIncrementalRestoreOperationCompleted(TOperationId(globalOperationId, 0), ctx);
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
                            stateIt->second.InvolvedShards.insert(shardIdx);
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

// Notification function for operation completion
void TSchemeShard::NotifyIncrementalRestoreOperationCompleted(const TOperationId& operationId, const TActorContext& ctx) {
    // Find which incremental restore this operation belongs to
    auto it = IncrementalRestoreOperationToState.find(operationId);
    if (it != IncrementalRestoreOperationToState.end()) {
        ui64 incrementalRestoreId = it->second;
        
        LOG_I("Operation " << operationId << " completed, triggering progress check for incremental restore " << incrementalRestoreId);
        
        // Trigger progress check immediately
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

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxProgressIncrementalRestore(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
    // For allocator results, we need to find the appropriate operation ID
    // For now, return a transaction that will find the right operation to process
    return new TTxProgressIncrementalRestore(this, 0);
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
