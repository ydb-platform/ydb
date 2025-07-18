#include "schemeshard__backup_collection_common.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard__operation.h"  // for NextPartId
#include "schemeshard_impl.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#include <algorithm>

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace {

// Helper function to create suboperations for dropping backup collection contents
ISubOperation::TPtr CascadeDropBackupCollection(TVector<ISubOperation::TPtr>& result, 
                                               const TOperationId& id, 
                                               const TPath& backupCollection,
                                               TOperationContext& context) {
    // For each backup directory in the collection
    for (const auto& [backupName, backupPathId] : backupCollection.Base()->GetChildren()) {
        TPath backupPath = backupCollection.Child(backupName);
        
        if (!backupPath.IsResolved() || backupPath.IsDeleted()) {
            continue;
        }

        // If this is a table (backup), drop it using CascadeDropTableChildren
        if (backupPath->IsTable()) {
            if (auto reject = CascadeDropTableChildren(result, id, backupPath)) {
                return reject;
            }
            
            // Then drop the table itself
            auto dropTable = TransactionTemplate(backupCollection.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
            dropTable.MutableDrop()->SetName(ToString(backupPath.Base()->Name));
            result.push_back(CreateDropTable(NextPartId(id, result), dropTable));
        } 
        // If this is a directory, recursively drop its contents
        else if (backupPath->IsDirectory()) {
            // Recursively handle directory contents
            if (auto reject = CascadeDropBackupCollection(result, id, backupPath, context)) {
                return reject;
            }
            
            // Then drop the directory itself
            auto dropDir = TransactionTemplate(backupCollection.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpRmDir);
            dropDir.MutableDrop()->SetName(ToString(backupPath.Base()->Name));
            result.push_back(CreateRmDir(NextPartId(id, result), dropDir));
        }
    }

    return nullptr;
}

class TDropParts : public TSubOperationState {
public:
    explicit TDropParts(TOperationId id)
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
        
        NIceDb::TNiceDb db(context.GetDB());

        // Recursively drop all backup collection contents
        if (!DropBackupCollectionContents(pathId, step, context, db)) {
            // If we couldn't drop everything, we'll retry next time
            LOG_I(DebugHint() << "Could not drop all contents, will retry");
            return false;
        }

        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);
        return true;
    }

private:
    bool DropBackupCollectionContents(const TPathId& bcPathId, TStepId step, 
                                     TOperationContext& context, NIceDb::TNiceDb& db) {
        if (!context.SS->PathsById.contains(bcPathId)) {
            LOG_I(DebugHint() << "Backup collection path not found: " << bcPathId);
            return true; // Path doesn't exist, consider it dropped
        }
        
        auto bcPath = context.SS->PathsById.at(bcPathId);

        // Drop CDC streams from source tables for incremental backup
        if (!DropSourceTableCdcStreams(bcPathId, step, context, db)) {
            LOG_I(DebugHint() << "Failed to drop CDC streams from source tables");
            return false; // Retry later
        }

        // First, drop all CDC streams and topics for incremental backups
        for (const auto& [childName, childPathId] : bcPath->GetChildren()) {
            if (!context.SS->PathsById.contains(childPathId)) {
                LOG_I(DebugHint() << "Child path not found: " << childPathId << ", skipping");
                continue;
            }
            
            auto childPath = context.SS->PathsById.at(childPathId);
            
            if (childPath->Dropped()) {
                continue; // Already dropped
            }

            // Check if this is an incremental backup directory
            if (childName.EndsWith("_incremental")) {
                // Drop CDC streams and topics for all tables in this incremental backup
                if (!DropIncrementalBackupCdcComponents(childPathId, step, context, db)) {
                    LOG_I(DebugHint() << "Failed to drop CDC components for incremental backup: " << childPathId);
                    return false; // Retry later
                }
            }
        }

        // Now drop all backup directories and their contents
        TVector<TPathId> pathsToRemove;
        CollectBackupPaths(bcPathId, pathsToRemove, context);

        // Sort paths by depth (deeper first) to ensure proper deletion order
        std::sort(pathsToRemove.begin(), pathsToRemove.end(), 
                 [&context](const TPathId& a, const TPathId& b) -> bool {
                     auto itA = context.SS->PathsById.find(a);
                     auto itB = context.SS->PathsById.find(b);
                     
                     if (itA == context.SS->PathsById.end() || itB == context.SS->PathsById.end()) {
                         return a < b; // Consistent ordering for missing paths
                     }
                     
                     // Use TPath to calculate depth
                     TPath pathA = TPath::Init(a, context.SS);
                     TPath pathB = TPath::Init(b, context.SS);
                     
                     if (!pathA.IsResolved() || !pathB.IsResolved()) {
                         return a < b; // Fallback ordering
                     }
                     
                     return pathA.Depth() > pathB.Depth();
                 });

        // Drop all collected paths
        for (const auto& pathId : pathsToRemove) {
            if (!context.SS->PathsById.contains(pathId)) {
                LOG_I(DebugHint() << "Path not found during deletion: " << pathId << ", skipping");
                continue;
            }
            
            auto path = context.SS->PathsById.at(pathId);
            
            if (path->Dropped()) {
                continue; // Already dropped
            }

            path->SetDropped(step, OperationId.GetTxId());
            context.SS->PersistDropStep(db, pathId, step, OperationId);
            
            // Update counters based on path type
            if (path->IsTable()) {
                context.SS->TabletCounters->Simple()[COUNTER_TABLE_COUNT].Sub(1);
            }
            
            // Clean up specific path type metadata
            if (path->IsTable() && context.SS->Tables.contains(pathId)) {
                context.SS->PersistRemoveTable(db, pathId, context.Ctx);
            }
            
            auto domainInfo = context.SS->ResolveDomainInfo(pathId);
            if (domainInfo) {
                domainInfo->DecPathsInside(context.SS);
            }
        }

        return true;
    }

    bool DropSourceTableCdcStreams(const TPathId& bcPathId, TStepId step, 
                                  TOperationContext& context, NIceDb::TNiceDb& db) {
        // Get the backup collection info to find source tables
        const TBackupCollectionInfo::TPtr backupCollection = context.SS->BackupCollections.Value(bcPathId, nullptr);
        if (!backupCollection) {
            LOG_I(DebugHint() << "Backup collection info not found: " << bcPathId);
            return true; // No backup collection, nothing to clean
        }

        // Iterate through all source tables defined in the backup collection
        for (const auto& entry : backupCollection->Description.GetExplicitEntryList().GetEntries()) {
            const TString& sourceTablePath = entry.GetPath();
            
            // Resolve the source table path
            TPath sourcePath = TPath::Resolve(sourceTablePath, context.SS);
            if (!sourcePath.IsResolved() || !sourcePath->IsTable() || sourcePath.IsDeleted()) {
                LOG_I(DebugHint() << "Source table not found or not a table: " << sourceTablePath);
                continue; // Source table doesn't exist, skip
            }

            // Look for CDC streams with the incremental backup naming pattern
            TVector<TString> cdcStreamsToDelete;
            for (const auto& [childName, childPathId] : sourcePath.Base()->GetChildren()) {
                if (!context.SS->PathsById.contains(childPathId)) {
                    continue;
                }
                
                auto childPath = context.SS->PathsById.at(childPathId);
                if (!childPath->IsCdcStream() || childPath->Dropped()) {
                    continue;
                }

                // Check if this CDC stream matches the incremental backup naming pattern
                if (childName.EndsWith("_continuousBackupImpl")) {
                    cdcStreamsToDelete.push_back(childName);
                    LOG_I(DebugHint() << "Found incremental backup CDC stream to delete: " << sourceTablePath << "/" << childName);
                }
            }

            // Drop all identified CDC streams from this source table
            for (const TString& streamName : cdcStreamsToDelete) {
                TPath cdcStreamPath = sourcePath.Child(streamName);
                if (cdcStreamPath.IsResolved() && !cdcStreamPath.IsDeleted()) {
                    if (!DropCdcStreamAndTopics(cdcStreamPath.Base()->PathId, step, context, db)) {
                        LOG_I(DebugHint() << "Failed to drop CDC stream: " << sourceTablePath << "/" << streamName);
                        return false; // Retry later
                    }
                }
            }
        }

        return true;
    }

    bool DropIncrementalBackupCdcComponents(const TPathId& incrBackupPathId, TStepId step, 
                                           TOperationContext& context, NIceDb::TNiceDb& db) {
        Y_ABORT_UNLESS(context.SS->PathsById.contains(incrBackupPathId));
        auto incrBackupPath = context.SS->PathsById.at(incrBackupPathId);

        // For each table in the incremental backup, drop associated CDC streams
        for (const auto& [tableName, tablePathId] : incrBackupPath->GetChildren()) {
            Y_ABORT_UNLESS(context.SS->PathsById.contains(tablePathId));
            auto tablePath = context.SS->PathsById.at(tablePathId);
            
            if (!tablePath->IsTable() || tablePath->Dropped()) {
                continue;
            }

            // Look for CDC streams associated with this table
            for (const auto& [streamName, streamPathId] : tablePath->GetChildren()) {
                Y_ABORT_UNLESS(context.SS->PathsById.contains(streamPathId));
                auto streamPath = context.SS->PathsById.at(streamPathId);
                
                if (!streamPath->IsCdcStream() || streamPath->Dropped()) {
                    continue;
                }

                // Drop CDC stream and its topics/partitions
                if (!DropCdcStreamAndTopics(streamPathId, step, context, db)) {
                    return false; // Retry later
                }
            }
        }

        return true;
    }

    bool DropCdcStreamAndTopics(const TPathId& streamPathId, TStepId step, 
                               TOperationContext& context, NIceDb::TNiceDb& db) {
        if (!context.SS->PathsById.contains(streamPathId)) {
            LOG_I(DebugHint() << "CDC stream path not found: " << streamPathId);
            return true; // Path doesn't exist, consider it dropped
        }
        
        auto streamPath = context.SS->PathsById.at(streamPathId);

        if (streamPath->Dropped()) {
            return true; // Already dropped
        }

        // First drop all PQ groups (topics) associated with this CDC stream
        for (const auto& [topicName, topicPathId] : streamPath->GetChildren()) {
            if (!context.SS->PathsById.contains(topicPathId)) {
                LOG_I(DebugHint() << "Topic path not found: " << topicPathId << ", skipping");
                continue;
            }
            
            auto topicPath = context.SS->PathsById.at(topicPathId);
            
            if (topicPath->IsPQGroup() && !topicPath->Dropped()) {
                topicPath->SetDropped(step, OperationId.GetTxId());
                context.SS->PersistDropStep(db, topicPathId, step, OperationId);
                
                if (context.SS->Topics.contains(topicPathId)) {
                    context.SS->PersistRemovePersQueueGroup(db, topicPathId);
                }
                
                auto domainInfo = context.SS->ResolveDomainInfo(topicPathId);
                if (domainInfo) {
                    domainInfo->DecPathsInside(context.SS);
                }
            }
        }

        // Then drop the CDC stream itself
        streamPath->SetDropped(step, OperationId.GetTxId());
        context.SS->PersistDropStep(db, streamPathId, step, OperationId);
        
        // Check if CDC stream metadata exists before removing
        if (context.SS->CdcStreams.contains(streamPathId)) {
            context.SS->PersistRemoveCdcStream(db, streamPathId);
            context.SS->TabletCounters->Simple()[COUNTER_CDC_STREAMS_COUNT].Sub(1);
        }
        
        auto domainInfo = context.SS->ResolveDomainInfo(streamPathId);
        if (domainInfo) {
            domainInfo->DecPathsInside(context.SS);
        }

        return true;
    }

    void CollectBackupPaths(const TPathId& rootPathId, TVector<TPathId>& paths, 
                           TOperationContext& context) {
        Y_ABORT_UNLESS(context.SS->PathsById.contains(rootPathId));
        auto rootPath = context.SS->PathsById.at(rootPathId);

        for (const auto& [childName, childPathId] : rootPath->GetChildren()) {
            Y_ABORT_UNLESS(context.SS->PathsById.contains(childPathId));
            auto childPath = context.SS->PathsById.at(childPathId);
            
            if (childPath->Dropped()) {
                continue;
            }

            // Recursively collect all children first
            CollectBackupPaths(childPathId, paths, context);
            
            // Add this path to be removed
            paths.push_back(childPathId);
        }
    }

    TString DebugHint() const override {
        return TStringBuilder() << "TDropBackupCollection TDropParts, operationId: " << OperationId << ", ";
    }

private:
    const TOperationId OperationId;
};

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
            return; // Will retry later
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
        return TTxState::DropParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::DropParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::DropParts:
            return MakeHolder<TDropParts>(OperationId);
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
            
            // Clean up related mappings using iterators
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

// Helper function to create incremental restore cleanup operation
ISubOperation::TPtr CreateIncrementalRestoreCleanup(TOperationId id, TPathId backupCollectionPathId) {
    class TIncrementalRestoreCleanupOperation : public TSubOperation {
    public:
        TIncrementalRestoreCleanupOperation(TOperationId id, TPathId pathId)
            : TSubOperation(id, TTxState::Waiting)
            , BackupCollectionPathId(pathId)
        {}

        THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
            auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), context.SS->TabletID());
            
            // Setup transaction state to proceed directly to cleanup
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->CreateTx(OperationId, TTxState::TxInvalid, BackupCollectionPathId);
            context.SS->ChangeTxState(db, OperationId, TTxState::Done);
            
            return result;
        }

        void AbortPropose(TOperationContext&) override {}
        void AbortUnsafe(TTxId, TOperationContext& context) override {
            context.OnComplete.DoneOperation(OperationId);
        }

        TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
            switch (state) {
            case TTxState::Waiting:
                return MakeHolder<TIncrementalRestoreCleanup>(OperationId, BackupCollectionPathId);
            default:
                return nullptr;
            }
        }

        TTxState::ETxState NextState(TTxState::ETxState state) const override {
            switch (state) {
            case TTxState::Waiting:
                return TTxState::Done;
            default:
                return TTxState::Invalid;
            }
        }

    private:
        TPathId BackupCollectionPathId;
    };
    
    return new TIncrementalRestoreCleanupOperation(id, backupCollectionPathId);
}

}  // anonymous namespace

// Create multiple suboperations for dropping backup collection
TVector<ISubOperation::TPtr> CreateDropBackupCollectionCascade(TOperationId nextId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropBackupCollection);

    auto dropOperation = tx.GetDropBackupCollection();
    const TString parentPathStr = tx.GetWorkingDir();
    
    TPath backupCollection = TPath::Resolve(parentPathStr + "/" + dropOperation.GetName(), context.SS);

    {
        TPath::TChecker checks = backupCollection.Check();
        checks
            .NotEmpty()
            .IsResolved()
            .NotDeleted()
            .IsBackupCollection()
            .NotUnderDeleting()
            .NotUnderOperation()
            .IsCommonSensePath();

        if (!checks) {
            return {CreateReject(nextId, checks.GetStatus(), checks.GetError())};
        }
    }

    // Check for active backup/restore operations
    const TPathId& pathId = backupCollection.Base()->PathId;
    
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
    
    // First, add incremental restore state cleanup operation
    auto cleanupOp = CreateIncrementalRestoreCleanup(NextPartId(nextId, result), backupCollection.Base()->PathId);
    result.push_back(cleanupOp);
    
    // Then use the cascade helper to generate all necessary suboperations
    if (auto reject = CascadeDropBackupCollection(result, nextId, backupCollection, context)) {
        return {reject};
    }

    return result;
}

ISubOperation::TPtr CreateDropBackupCollection(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TDropBackupCollection>(id, tx);
}

ISubOperation::TPtr CreateDropBackupCollection(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TDropBackupCollection>(id, state);
}

}  // namespace NKikimr::NSchemeShard
