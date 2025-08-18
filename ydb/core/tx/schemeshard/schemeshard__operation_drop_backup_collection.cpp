#include "schemeshard__backup_collection_common.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard__operation.h"
#include "schemeshard_impl.h"
#include "schemeshard_utils.h"

#include <algorithm>

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace {

struct TDropPlan {
    struct TCdcStreamInfo {
        TPathId TablePathId;
        TString StreamName;
        TString TablePath;
    };
    
    struct TTableCdcStreams {
        TPathId TablePathId;
        TString TablePath;
        TVector<TString> StreamNames;
    };
    
    THashMap<TPathId, TTableCdcStreams> CdcStreamsByTable;  // Grouped by table
    TVector<TPath> BackupTables;
    TVector<TPath> BackupTopics;
    TPathId BackupCollectionId;
    
    bool HasExternalObjects() const {
        return !CdcStreamsByTable.empty() || !BackupTables.empty() || !BackupTopics.empty();
    }
};

THolder<TDropPlan> CollectExternalObjects(TOperationContext& context, const TPath& bcPath) {
    auto plan = MakeHolder<TDropPlan>();
    plan->BackupCollectionId = bcPath.Base()->PathId;
    
    LOG_I("DropPlan: Starting collection for backup collection: " << bcPath.PathString());
    
    // 1. Find CDC streams on source tables (these are OUTSIDE the backup collection)
    // Group them by table for efficient multi-stream drops
    for (const auto& [pathId, cdcStreamInfo] : context.SS->CdcStreams) {
        if (!context.SS->PathsById.contains(pathId)) {
            continue;
        }
        
        auto streamPath = context.SS->PathsById.at(pathId);
        if (!streamPath || streamPath->Dropped()) {
            continue;
        }
        
        if (streamPath->Name.EndsWith("_continuousBackupImpl")) {
            if (!context.SS->PathsById.contains(streamPath->ParentPathId)) {
                continue;
            }
            
            auto tablePath = context.SS->PathsById.at(streamPath->ParentPathId);
            if (!tablePath || !tablePath->IsTable() || tablePath->Dropped()) {
                continue;
            }
            
            TString tablePathStr = TPath::Init(streamPath->ParentPathId, context.SS).PathString();
            
            auto& tableEntry = plan->CdcStreamsByTable[streamPath->ParentPathId];
            if (tableEntry.StreamNames.empty()) {
                tableEntry.TablePathId = streamPath->ParentPathId;
                tableEntry.TablePath = tablePathStr;
            }
            tableEntry.StreamNames.push_back(streamPath->Name);
        }
    }
    
    // 2. Find backup tables and topics UNDER the collection path recursively
    TVector<TPath> toVisit = {bcPath};
    while (!toVisit.empty()) {
        TPath current = toVisit.back();
        toVisit.pop_back();
        
        for (const auto& [childName, childPathId] : current.Base()->GetChildren()) {
            TPath childPath = current.Child(childName);
            
            if (childPath.Base()->IsTable()) {
                plan->BackupTables.push_back(childPath);
            } else if (childPath.Base()->IsPQGroup()) {
                plan->BackupTopics.push_back(childPath);
            } else if (childPath.Base()->IsDirectory()) {
                toVisit.push_back(childPath);
            }
        }
    }
    
    return plan;
}

TTxTransaction CreateCdcDropTransaction(const TDropPlan::TTableCdcStreams& tableStreams, TOperationContext& context) {
    TTxTransaction cdcDropTx;
    TPath tablePath = TPath::Init(tableStreams.TablePathId, context.SS);
    cdcDropTx.SetWorkingDir(tablePath.Parent().PathString());
    cdcDropTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropCdcStream);
    
    auto* cdcDrop = cdcDropTx.MutableDropCdcStream();
    cdcDrop->SetTableName(tablePath.LeafName());
    
    // Add all streams for this table using the new repeated field functionality
    for (const auto& streamName : tableStreams.StreamNames) {
        cdcDrop->AddStreamName(streamName);
    }
    
    return cdcDropTx;
}

TTxTransaction CreateTableDropTransaction(const TPath& tablePath) {
    TTxTransaction tableDropTx;
    tableDropTx.SetWorkingDir(tablePath.Parent().PathString());
    tableDropTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
    
    auto* drop = tableDropTx.MutableDrop();
    drop->SetName(tablePath.LeafName());
    
    return tableDropTx;
}

// TODO: replace UGLY scan
// Clean up incremental restore state for a backup collection
void CleanupIncrementalRestoreState(const TPathId& backupCollectionPathId, TOperationContext& context, NIceDb::TNiceDb& db) {
    LOG_I("CleanupIncrementalRestoreState for backup collection pathId: " << backupCollectionPathId);
    
    // Find all incremental restore states for this backup collection
    TVector<ui64> statesToCleanup;
    
    for (auto it = context.SS->IncrementalRestoreStates.begin(); it != context.SS->IncrementalRestoreStates.end();) {
        if (it->second.BackupCollectionPathId == backupCollectionPathId) {
            const auto& stateId = it->first;  // it->first is ui64 (state ID)
            statesToCleanup.push_back(stateId);
            
            // Remove from memory
            auto toErase = it;
            ++it;
            context.SS->IncrementalRestoreStates.erase(toErase);
        } else {
            ++it;
        }
    }
    
    // Clean up database entries
    for (const auto& stateId : statesToCleanup) {
        // Delete from IncrementalRestoreState table
        db.Table<Schema::IncrementalRestoreState>().Key(stateId).Delete();
        
        // Delete all shard progress records for this state
        auto shardProgressRowset = db.Table<Schema::IncrementalRestoreShardProgress>().Range().Select();
        if (!shardProgressRowset.IsReady()) {
            return;
        }
        
        while (!shardProgressRowset.EndOfSet()) {
            ui64 operationId = shardProgressRowset.GetValue<Schema::IncrementalRestoreShardProgress::OperationId>();
            ui64 shardIdx = shardProgressRowset.GetValue<Schema::IncrementalRestoreShardProgress::ShardIdx>();
            
            if (operationId == stateId) {
                db.Table<Schema::IncrementalRestoreShardProgress>().Key(operationId, shardIdx).Delete();
            }
            
            if (!shardProgressRowset.Next()) {
                break;
            }
        }
    }
    
    // Clean up operation-to-state mappings
    for (auto opIt = context.SS->IncrementalRestoreOperationToState.begin(); 
         opIt != context.SS->IncrementalRestoreOperationToState.end();) {
        if (std::find(statesToCleanup.begin(), statesToCleanup.end(), opIt->second) != statesToCleanup.end()) {
            auto toErase = opIt;
            ++opIt;
            context.SS->IncrementalRestoreOperationToState.erase(toErase);
        } else {
            ++opIt;
        }
    }
    
    LOG_I("CleanupIncrementalRestoreState: Cleaned up " << statesToCleanup.size() << " incremental restore states");
}

class TPropose : public TSubOperationState {
public:
    explicit TPropose(TOperationId id)
        : OperationId(std::move(id))
    {}

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        const auto* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropBackupCollection);

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
        return false;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
        const TStepId step = TStepId(ev->Get()->StepId);
        LOG_I(DebugHint() << "HandleReply TEvOperationPlan: step# " << step);

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxDropBackupCollection);

        const TPathId& pathId = txState->TargetPathId;
        const TPathElement::TPtr pathPtr = context.SS->PathsById.at(pathId);
        const TPathElement::TPtr parentDirPtr = context.SS->PathsById.at(pathPtr->ParentPathId);

        NIceDb::TNiceDb db(context.GetDB());

        Y_ABORT_UNLESS(!pathPtr->Dropped());
        pathPtr->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, pathId, step, OperationId);
        context.SS->PersistRemoveBackupCollection(db, pathId);

        // Clean up incremental restore state for this backup collection
        CleanupIncrementalRestoreState(pathId, context, db);

        auto domainInfo = context.SS->ResolveDomainInfo(pathId);
        domainInfo->DecPathsInside(context.SS);
        DecAliveChildrenDirect(OperationId, parentDirPtr, context);
        context.SS->TabletCounters->Simple()[COUNTER_BACKUP_COLLECTION_COUNT].Sub(1);

        ++parentDirPtr->DirAlterVersion;
        context.SS->PersistPathDirAlterVersion(db, parentDirPtr);
        context.SS->ClearDescribePathCaches(parentDirPtr);
        context.SS->ClearDescribePathCaches(pathPtr);

        if (!context.SS->DisablePublicationsOfDropping) {
            context.OnComplete.PublishToSchemeBoard(OperationId, parentDirPtr->PathId);
            context.OnComplete.PublishToSchemeBoard(OperationId, pathId);
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }

private:
    TString DebugHint() const override {
        return TStringBuilder() << "TDropBackupCollection TPropose, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

class TDropBackupCollection : public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Propose;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Propose:
            return MakeHolder<TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

    void DropBackupCollectionPathElement(const TPath& dstPath) const {
        TPathElement::TPtr backupCollection = dstPath.Base();

        backupCollection->PathState = TPathElement::EPathState::EPathStateDrop;
        backupCollection->DropTxId = OperationId.GetTxId();
        backupCollection->LastTxId = OperationId.GetTxId();
    }

    void PersistDropBackupCollection(const TOperationContext& context, const TPath& dstPath) const {
        const TPathId& pathId = dstPath.Base()->PathId;

        context.MemChanges.GrabNewTxState(context.SS, OperationId);
        context.MemChanges.GrabPath(context.SS, pathId);
        context.MemChanges.GrabPath(context.SS, dstPath->ParentPathId);
        context.MemChanges.GrabBackupCollection(context.SS, pathId);

        context.DbChanges.PersistTxState(OperationId);
        context.DbChanges.PersistPath(pathId);
        context.DbChanges.PersistPath(dstPath->ParentPathId);
    }

    // TODO: replace UGLY scan
    bool HasActiveBackupOperations(const TPath& bcPath, TOperationContext& context) const {
        // Check if there are any active backup or restore operations for this collection
        const TPathId& bcPathId = bcPath.Base()->PathId;
        
        // Check all active transactions to see if any involve this backup collection
        for (const auto& [txId, txState] : context.SS->TxInFlight) {
            if (txState.TxType == TTxState::TxBackup ||
                txState.TxType == TTxState::TxRestore ||
                txState.TxType == TTxState::TxCopyTable) {
                
                // Check if the transaction target is this backup collection or a child path
                const TPathId& targetPathId = txState.TargetPathId;
                if (targetPathId == bcPathId) {
                    return true;
                }
                
                // Check if target is a child of this backup collection
                if (context.SS->PathsById.contains(targetPathId)) {
                    auto targetPath = context.SS->PathsById.at(targetPathId);
                    TPathId currentId = targetPathId;
                    
                    // Walk up the path hierarchy to check if bcPathId is an ancestor
                    while (currentId && context.SS->PathsById.contains(currentId)) {
                        if (currentId == bcPathId) {
                            return true;
                        }
                        auto currentPath = context.SS->PathsById.at(currentId);
                        currentId = currentPath->ParentPathId;
                    }
                }
            }
        }
        
        return false;
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TString& rootPathStr = Transaction.GetWorkingDir();
        const auto& dropDescription = Transaction.GetDropBackupCollection();
        const TString& name = dropDescription.GetName();
        LOG_N("TDropBackupCollection Propose: opId# " << OperationId << ", path# " << rootPathStr << "/" << name);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted,
                                                   static_cast<ui64>(OperationId.GetTxId()),
                                                   static_cast<ui64>(context.SS->SelfTabletId()));

        auto bcPaths = NBackup::ResolveBackupCollectionPaths(rootPathStr, name, false, context, result);
        if (!bcPaths) {
            return result;
        }

        auto& dstPath = bcPaths->DstPath;

        {
            auto checks = dstPath.Check();
            checks
                .NotEmpty()
                .NotUnderDomainUpgrade()
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsBackupCollection()
                .NotUnderOperation()
                .IsCommonSensePath();

            if (checks) {
                const TBackupCollectionInfo::TPtr backupCollection = context.SS->BackupCollections.Value(dstPath->PathId, nullptr);
                if (!backupCollection) {
                    result->SetError(NKikimrScheme::StatusSchemeError, "Backup collection doesn't exist");
                    return result;
                }

                // Check for active backup/restore operations
                if (HasActiveBackupOperations(dstPath, context)) {
                    result->SetError(NKikimrScheme::StatusPreconditionFailed, 
                                   "Cannot drop backup collection while backup or restore operations are active. Please wait for them to complete.");
                    return result;
                }
                
                // Check for concurrent operations on the same path
                // This catches the race condition where another drop operation is in progress
                // but hasn't yet marked the path as "under deleting"
                
                for (const auto& [txId, txState] : context.SS->TxInFlight) {
                    LOG_I("DropPlan: Found TxInFlight - txId: " << txId.GetTxId() 
                         << ", targetPathId: " << txState.TargetPathId 
                         << ", txType: " << (int)txState.TxType);
                         
                    if (txState.TargetPathId == dstPath.Base()->PathId && 
                        txId.GetTxId() != OperationId.GetTxId()) {
                        result->SetError(NKikimrScheme::StatusMultipleModifications, 
                                       TStringBuilder() << "Check failed: path: '" << dstPath.PathString() 
                                       << "', error: another operation is already in progress for this backup collection");
                        result->SetPathDropTxId(ui64(txId.GetTxId()));
                        result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                        return result;
                    }
                }
            }

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                if (dstPath.IsResolved() && dstPath.Base()->IsBackupCollection() && (dstPath.Base()->PlannedToDrop() || dstPath.Base()->Dropped())) {
                    result->SetPathDropTxId(ui64(dstPath.Base()->DropTxId));
                    result->SetPathId(dstPath.Base()->PathId.LocalPathId);
                }

                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }

        result->SetPathId(dstPath.Base()->PathId.LocalPathId);

        auto guard = context.DbGuard();
        PersistDropBackupCollection(context, dstPath);
        context.SS->CreateTx(
                OperationId,
                TTxState::TxDropBackupCollection,
                dstPath.Base()->PathId);

        DropBackupCollectionPathElement(dstPath);

        context.OnComplete.ActivateTx(OperationId);

        IncParentDirAlterVersionWithRepublishSafeWithUndo(OperationId, dstPath, context.SS, context.OnComplete);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TDropBackupCollection AbortPropose: opId# " << OperationId);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TDropBackupCollection AbortUnsafe: opId# " << OperationId << ", txId# " << forceDropTxId);
        context.OnComplete.DoneOperation(OperationId);
    }
};

// Cleanup operation for incremental restore state
class TIncrementalRestoreCleanup : public TSubOperationState {
public:
    explicit TIncrementalRestoreCleanup(TOperationId id, TPathId backupCollectionPathId)
        : OperationId(std::move(id))
        , BackupCollectionPathId(backupCollectionPathId)
    {}

    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        NIceDb::TNiceDb db(context.GetDB());
        
        // Clean up incremental restore state for this backup collection
        TVector<ui64> operationsToCleanup;
        
        for (const auto& [opId, restoreState] : context.SS->IncrementalRestoreStates) {
            if (restoreState.BackupCollectionPathId == BackupCollectionPathId) {
                operationsToCleanup.push_back(opId);
            }
        }
        
        for (ui64 opId : operationsToCleanup) {
            LOG_I(DebugHint() << "Cleaning up incremental restore state for operation: " << opId);
            
            // Remove from database
            db.Table<Schema::IncrementalRestoreOperations>()
                .Key(opId)
                .Delete();
            
            // Remove from in-memory state
            context.SS->IncrementalRestoreStates.erase(opId);
            
            // Clean up related mappings
            auto txIt = context.SS->TxIdToIncrementalRestore.begin();
            while (txIt != context.SS->TxIdToIncrementalRestore.end()) {
                if (txIt->second == opId) {
                    auto toErase = txIt;
                    ++txIt;
                    context.SS->TxIdToIncrementalRestore.erase(toErase);
                } else {
                    ++txIt;
                }
            }
            
            auto opIt = context.SS->IncrementalRestoreOperationToState.begin();
            while (opIt != context.SS->IncrementalRestoreOperationToState.end()) {
                if (opIt->second == opId) {
                    auto toErase = opIt;
                    ++opIt;
                    context.SS->IncrementalRestoreOperationToState.erase(toErase);
                } else {
                    ++opIt;
                }
            }
        }
        
        LOG_I(DebugHint() << "Cleaned up " << operationsToCleanup.size() << " incremental restore operations");
        
        return true;
    }

    bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr&, TOperationContext&) override {
        return true;
    }

private:
    TOperationId OperationId;
    TPathId BackupCollectionPathId;
    
    TString DebugHint() const override {
        return TStringBuilder()
            << "TIncrementalRestoreCleanup"
            << " operationId: " << OperationId;
    }
};

}  // anonymous namespace

// Create multiple suboperations for dropping backup collection
TVector<ISubOperation::TPtr> CreateDropBackupCollectionCascade(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropBackupCollection);

    auto dropOperation = tx.GetDropBackupCollection();
    const TString parentPathStr = tx.GetWorkingDir();
    
    // Check for empty backup collection name
    if (dropOperation.GetName().empty()) {
        return {CreateReject(nextId, NKikimrScheme::StatusInvalidParameter, "Backup collection name cannot be empty")};
    }
    
    // Use the same validation logic as ResolveBackupCollectionPaths to be consistent
    auto proposeResult = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, static_cast<ui64>(nextId.GetTxId()), static_cast<ui64>(context.SS->SelfTabletId()));
    auto bcPaths = NBackup::ResolveBackupCollectionPaths(parentPathStr, dropOperation.GetName(), false, context, proposeResult);
    if (!bcPaths) {
        return {CreateReject(nextId, proposeResult->Record.GetStatus(), proposeResult->Record.GetReason())};
    }

    auto& dstPath = bcPaths->DstPath;

    {
        TPath::TChecker checks = dstPath.Check();
        checks
            .NotEmpty()
            .IsResolved()
            .NotDeleted()
            .IsBackupCollection()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (!checks) {
            // TODO: is there more clean way to write it?
            // Handle the special case where the path is being deleted
            if (dstPath.IsResolved() && dstPath.Base()->IsBackupCollection() && 
                (dstPath.Base()->PlannedToDrop() || dstPath.Base()->Dropped())) {
                
                auto errorResult = MakeHolder<TProposeResponse>(checks.GetStatus(), static_cast<ui64>(nextId.GetTxId()), static_cast<ui64>(context.SS->SelfTabletId()));
                errorResult->SetError(checks.GetStatus(), checks.GetError());
                errorResult->SetPathDropTxId(ui64(dstPath.Base()->DropTxId));
                errorResult->SetPathId(dstPath.Base()->PathId.LocalPathId);
                return {CreateReject(nextId, std::move(errorResult))};
            } else {
                return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
            }
        }
    }

    const TPathId& pathId = dstPath.Base()->PathId;
    
    // Check if any backup or restore operations are active for this collection
    for (const auto& [txId, txState] : context.SS->TxInFlight) {
        if (txState.TargetPathId == pathId && 
            (txState.TxType == TTxState::TxBackup || 
             txState.TxType == TTxState::TxRestore)) {
            return {CreateReject(nextId, NKikimrScheme::StatusPreconditionFailed,
                "Cannot drop backup collection while backup or restore operations are active. Please wait for them to complete.")};
        }
    }
    
    // Check for active incremental restore operations in IncrementalRestoreStates
    for (const auto& [opId, restoreState] : context.SS->IncrementalRestoreStates) {
        if (restoreState.BackupCollectionPathId == pathId) {
            return {CreateReject(nextId, NKikimrScheme::StatusPreconditionFailed,
                "Cannot drop backup collection while incremental restore operations are active. Please wait for them to complete.")};
        }
    }

    TVector<ISubOperation::TPtr> result;

    auto cleanupOp = MakeSubOperation<TDropBackupCollection>(nextId, tx);
    result.push_back(cleanupOp);

    auto dropPlan = CollectExternalObjects(context, dstPath);
    if (dropPlan->HasExternalObjects()) {
        // Create suboperations for CDC streams - grouped by table for atomic multi-stream drops
        for (const auto& [tableId, tableStreams] : dropPlan->CdcStreamsByTable) {
            TStringBuilder streamList;
            for (size_t i = 0; i < tableStreams.StreamNames.size(); ++i) {
                if (i > 0) streamList << ", ";
                streamList << tableStreams.StreamNames[i];
            }
            
            TTxTransaction cdcDropTx = CreateCdcDropTransaction(tableStreams, context);
            if (!CreateDropCdcStream(nextId, cdcDropTx, context, result)) {
                return result;
            }
        }

        // Create suboperations for backup tables
        for (const auto& tablePath : dropPlan->BackupTables) {
            TTxTransaction tableDropTx = CreateTableDropTransaction(tablePath);
            if (!CreateDropTable(nextId, tableDropTx, context, result)) {
                return result;
            }
        }
    }
    
    return result;
}

ISubOperation::TPtr CreateDropBackupCollection(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropBackupCollection>(id, state);
}

}  // namespace NKikimr::NSchemeShard
