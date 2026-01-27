#include "schemeshard__operation_incremental_restore_finalize.h"

#include "schemeshard__operation_base.h"
#include "schemeshard__operation_common.h"

#include <ydb/core/base/table_index.h>

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_W(stream) LOG_WARN_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

class TIncrementalRestoreFinalizeOp: public TSubOperationWithContext {
    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch(state) {
        case TTxState::Waiting:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::ConfigureParts:
            return MakeHolder<TConfigureParts>(OperationId, Transaction);
        case TTxState::Propose:
            return MakeHolder<TFinalizationPropose>(OperationId, Transaction);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

    class TConfigureParts: public TSubOperationState {
    private:
        TOperationId OperationId;
        TTxTransaction Transaction;

        TString DebugHint() const override {
            return TStringBuilder()
                << "TIncrementalRestoreFinalize TConfigureParts"
                << " operationId: " << OperationId;
        }

    public:
        TConfigureParts(TOperationId id, const TTxTransaction& tx)
            : OperationId(id), Transaction(tx)
        {
            IgnoreMessages(DebugHint(), {TEvHive::TEvCreateTabletReply::EventType});
        }

        bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
            TTabletId ssId = context.SS->SelfTabletId();

            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint() << " HandleReply TEvProposeTransactionResult"
                                   << ", at schemeshard: " << ssId
                                   << ", message: " << ev->Get()->Record.ShortDebugString());

            return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
        }

        bool ProgressState(TOperationContext& context) override {
            TTabletId ssId = context.SS->SelfTabletId();

            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       DebugHint() << " ProgressState"
                                   << ", at schemeshard: " << ssId);

            TTxState* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState);

            const auto& finalize = Transaction.GetIncrementalRestoreFinalize();

            // Collect all index impl tables that need schema version updates
            THashSet<TPathId> implTablesToUpdate;
            CollectIndexImplTables(finalize, context, implTablesToUpdate);

            if (implTablesToUpdate.empty()) {
                LOG_I(DebugHint() << " No index impl tables to update, skipping ConfigureParts");
                return true;
            }

            // Prepare AlterData for each table and add shards to txState
            NIceDb::TNiceDb db(context.GetDB());
            txState->ClearShardsInProgress();

            for (const auto& tablePathId : implTablesToUpdate) {
                if (!context.SS->Tables.contains(tablePathId)) {
                    LOG_W(DebugHint() << " Table not found: " << tablePathId);
                    continue;
                }

                auto table = context.SS->Tables.at(tablePathId);

                // InitAlterData() sets AlterVersion = CurrentVersion + 1 and also sets CoordinatedSchemaVersion
                table->InitAlterData(OperationId);

                context.SS->PersistAddAlterTable(db, tablePathId, table->AlterData);

                LOG_I(DebugHint() << " Preparing ALTER for table " << tablePathId
                      << " version: " << table->AlterVersion << " -> " << table->AlterData->AlterVersion);

                // Note: Parent index version update is deferred to TFinalizationPropose::SyncIndexSchemaVersions()
                // which runs after coordinator confirmation, ensuring consistency if datashard proposals fail.

                // Add all shards of this table to txState
                for (const auto& shard : table->GetPartitions()) {
                    auto shardIdx = shard.ShardIdx;
                    if (!txState->ShardsInProgress.contains(shardIdx)) {
                        txState->Shards.emplace_back(shardIdx, ETabletType::DataShard, TTxState::ConfigureParts);
                        txState->ShardsInProgress.insert(shardIdx);
                        
                        LOG_I(DebugHint() << " Added shard " << shardIdx 
                              << " (tablet: " << context.SS->ShardInfos[shardIdx].TabletID << ") to txState");
                    }
                }
            }

            context.SS->PersistTxState(db, OperationId);

            // Send ALTER TABLE transactions to all datashards
            for (const auto& shard : txState->Shards) {
                auto shardIdx = shard.Idx;
                auto datashardId = context.SS->ShardInfos[shardIdx].TabletID;

                LOG_I(DebugHint() << " Propose ALTER to datashard " << datashardId 
                      << " shardIdx: " << shardIdx << " txid: " << OperationId);

                const auto seqNo = context.SS->StartRound(*txState);
                
                // Find which table this shard belongs to
                TPathId tablePathId;
                for (const auto& pathId : implTablesToUpdate) {
                    auto table = context.SS->Tables.at(pathId);
                    for (const auto& partition : table->GetPartitions()) {
                        if (partition.ShardIdx == shardIdx) {
                            tablePathId = pathId;
                            break;
                        }
                    }
                    if (tablePathId) break;
                }

                if (!tablePathId) {
                    LOG_W(DebugHint() << " Could not find table for shard " << shardIdx);
                    continue;
                }

                const auto txBody = context.SS->FillAlterTableTxBody(tablePathId, shardIdx, seqNo);
                auto event = context.SS->MakeDataShardProposal(tablePathId, OperationId, txBody, context.Ctx);
                context.OnComplete.BindMsgToPipe(OperationId, datashardId, shardIdx, event.Release());
            }

            txState->UpdateShardsInProgress();
            return false;
        }

    private:
        void CollectIndexImplTables(const NKikimrSchemeOp::TIncrementalRestoreFinalize& finalize,
                                   TOperationContext& context,
                                   THashSet<TPathId>& implTables) {
            for (const auto& tablePath : finalize.GetTargetTablePaths()) {
                // Check if this path looks like an index implementation table
                TString indexImplTableSuffix = TString("/") + NTableIndex::ImplTable;
                if (!tablePath.Contains(indexImplTableSuffix)) {
                    continue;
                }

                TPath path = TPath::Resolve(tablePath, context.SS);
                if (!path.IsResolved()) {
                    LOG_W("CollectIndexImplTables: Table not resolved: " << tablePath);
                    continue;
                }

                if (path.Base()->PathType != NKikimrSchemeOp::EPathType::EPathTypeTable) {
                    continue;
                }

                TPathId implTablePathId = path.Base()->PathId;
                if (context.SS->Tables.contains(implTablePathId)) {
                    implTables.insert(implTablePathId);
                    LOG_I("CollectIndexImplTables: Found index impl table: " << tablePath 
                          << " pathId: " << implTablePathId);
                }
            }
        }
    };

    class TFinalizationPropose: public TSubOperationState {
    private:
        TOperationId OperationId;
        TTxTransaction Transaction;

        TString DebugHint() const override {
            return TStringBuilder()
                << "TIncrementalRestoreFinalize TPropose"
                << " operationId: " << OperationId;
        }

    public:
        TFinalizationPropose(TOperationId id, const TTxTransaction& tx) 
            : OperationId(id), Transaction(tx) {}

        bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
            return TSubOperationState::HandleReply(ev, context);
        }

        bool ProgressState(TOperationContext& context) override {
            LOG_I(DebugHint() << " ProgressState");

            TTxState* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState);

            const auto& finalize = Transaction.GetIncrementalRestoreFinalize();

            // Sync schema versions for restored indexes before releasing path states
            SyncIndexSchemaVersions(finalize, context);

            // Release all affected path states to EPathStateNoChanges
            TVector<TPathId> pathsToNormalize;
            CollectPathsToNormalize(finalize, context, pathsToNormalize);

            for (const auto& pathId : pathsToNormalize) {
                context.OnComplete.ReleasePathState(OperationId, pathId, 
                    TPathElement::EPathState::EPathStateNoChanges);
            }

            PerformFinalCleanup(finalize, context);

            {
                ui64 originalOpId = finalize.GetOriginalOperationId();
                NIceDb::TNiceDb db(context.GetDB());
                
                auto stateIt = context.SS->IncrementalRestoreStates.find(originalOpId);
                if (stateIt != context.SS->IncrementalRestoreStates.end()) {
                    const auto& involvedShards = stateIt->second.InvolvedShards;

                    LOG_I("Cleaning up " << involvedShards.size() << " shard progress entries for operation " << originalOpId);

                    for (const auto& shardIdx : involvedShards) {
                        db.Table<Schema::IncrementalRestoreShardProgress>()
                            .Key(originalOpId, ui64(shardIdx.GetLocalId()))
                            .Delete();
                    }
                }
                
                db.Table<Schema::IncrementalRestoreState>().Key(originalOpId).Delete();
            }

            context.OnComplete.DoneOperation(OperationId);
            return true;
        }

    private:
        void SyncIndexSchemaVersions(const NKikimrSchemeOp::TIncrementalRestoreFinalize& finalize,
                                     TOperationContext& context) {
            LOG_I("SyncIndexSchemaVersions: Starting schema version sync for restored indexes");
            LOG_I("SyncIndexSchemaVersions: Processing " << finalize.GetTargetTablePaths().size() << " target table paths");
            
            NIceDb::TNiceDb db(context.GetDB());
            THashSet<TPathId> publishedMainTables;

            // Iterate through all target table paths and finalize their alters
            for (const auto& tablePath : finalize.GetTargetTablePaths()) {
                // Check if this path looks like an index implementation table
                TString indexImplTableSuffix = TString("/") + NTableIndex::ImplTable;
                if (!tablePath.Contains(indexImplTableSuffix)) {
                    continue;
                }

                TPath path = TPath::Resolve(tablePath, context.SS);
                if (!path.IsResolved()) {
                    LOG_W("SyncIndexSchemaVersions: Table not resolved: " << tablePath);
                    continue;
                }

                if (path.Base()->PathType != NKikimrSchemeOp::EPathType::EPathTypeTable) {
                    continue;
                }

                TPathId implTablePathId = path.Base()->PathId;
                if (!context.SS->Tables.contains(implTablePathId)) {
                    LOG_W("SyncIndexSchemaVersions: Table not found: " << implTablePathId);
                    continue;
                }

                auto table = context.SS->Tables.at(implTablePathId);
                if (!table->AlterData) {
                    LOG_W("SyncIndexSchemaVersions: No AlterData for table: " << implTablePathId);
                    continue;
                }

                // Store coordinated version BEFORE calling FinishAlter (which resets AlterData)
                ui64 coordVersion = table->AlterData->CoordinatedSchemaVersion.GetOrElse(table->AlterVersion + 1);

                // Finalize the alter - this commits AlterData to the main table state
                LOG_I("SyncIndexSchemaVersions: Finalizing ALTER for table " << implTablePathId
                      << " version: " << table->AlterVersion << " -> " << table->AlterData->AlterVersion);

                // Release AlterData tracking before FinishAlter resets it
                if (table->ReleaseAlterData(OperationId)) {
                    context.SS->PersistClearAlterTableFull(db, implTablePathId);
                }

                table->FinishAlter();
                context.SS->PersistTableAltered(db, implTablePathId, table);

                // Clear describe path caches and publish to scheme board
                context.SS->ClearDescribePathCaches(path.Base());
                context.OnComplete.PublishToSchemeBoard(OperationId, implTablePathId);

                LOG_I("SyncIndexSchemaVersions: Finalized schema version for: " << tablePath);

                // Also update the parent index version
                TPath indexPath = path.Parent();
                if (indexPath.IsResolved() && indexPath.Base()->PathType == NKikimrSchemeOp::EPathTypeTableIndex) {
                    TPathId indexPathId = indexPath.Base()->PathId;
                    if (context.SS->Indexes.contains(indexPathId)) {
                        auto oldVersion = context.SS->Indexes[indexPathId]->AlterVersion;

                        // Use the coordinated version stored before FinishAlter
                        ui64 targetVersion = coordVersion;

                        if (context.SS->Indexes[indexPathId]->AlterVersion < targetVersion) {
                            auto index = context.SS->Indexes[indexPathId];
                            index->AlterVersion = targetVersion;
                            if (index->AlterData && index->AlterData->AlterVersion < targetVersion) {
                                index->AlterData->AlterVersion = targetVersion;
                                context.SS->PersistTableIndexAlterData(db, indexPathId);
                            }
                            context.SS->PersistTableIndexAlterVersion(db, indexPathId, index);

                            LOG_I("SyncIndexSchemaVersions: Index AlterVersion updated from "
                                  << oldVersion << " to " << context.SS->Indexes[indexPathId]->AlterVersion);

                            context.OnComplete.PublishToSchemeBoard(OperationId, indexPathId);

                            TPath mainTablePath = indexPath.Parent();
                            if (mainTablePath.IsResolved() && mainTablePath.Base()->PathType == NKikimrSchemeOp::EPathTypeTable) {
                                TPathId mainTablePathId = mainTablePath.Base()->PathId;
                                if (!publishedMainTables.contains(mainTablePathId)) {
                                    publishedMainTables.insert(mainTablePathId);
                                    context.SS->ClearDescribePathCaches(mainTablePath.Base());
                                    context.OnComplete.PublishToSchemeBoard(OperationId, mainTablePathId);
                                    LOG_I("SyncIndexSchemaVersions: Published main table: " << mainTablePathId);
                                }
                            }
                        }
                    }
                }
            }

            LOG_I("SyncIndexSchemaVersions: Finished schema version sync");
        }

        void CollectPathsToNormalize(const NKikimrSchemeOp::TIncrementalRestoreFinalize& finalize, 
                                   TOperationContext& context,
                                   TVector<TPathId>& pathsToNormalize) {
            
            // Collect target table paths
            for (const auto& tablePath : finalize.GetTargetTablePaths()) {
                TPath path = TPath::Resolve(tablePath, context.SS);
                if (path.IsResolved()) {
                    TPathId pathId = path.Base()->PathId;
                    if (auto* pathInfo = context.SS->PathsById.FindPtr(pathId)) {
                        if ((*pathInfo)->PathState == NKikimrSchemeOp::EPathState::EPathStateIncomingIncrementalRestore) {
                            pathsToNormalize.push_back(pathId);
                            LOG_I("Adding target table path to normalize: " << tablePath);
                        }
                    }
                }
            }
            
            // Collect backup table paths
            for (const auto& backupTablePath : finalize.GetBackupTablePaths()) {
                TPath path = TPath::Resolve(backupTablePath, context.SS);
                if (path.IsResolved()) {
                    TPathId pathId = path.Base()->PathId;
                    if (auto* pathInfo = context.SS->PathsById.FindPtr(pathId)) {
                        if ((*pathInfo)->PathState == NKikimrSchemeOp::EPathState::EPathStateOutgoingIncrementalRestore ||
                            (*pathInfo)->PathState == NKikimrSchemeOp::EPathState::EPathStateAwaitingOutgoingIncrementalRestore) {
                            pathsToNormalize.push_back(pathId);
                            LOG_I("Adding backup table path to normalize: " << backupTablePath);
                        }
                    }
                }
            }
        }

        void PerformFinalCleanup(const NKikimrSchemeOp::TIncrementalRestoreFinalize& finalize, 
                                TOperationContext& context) {
            ui64 originalOpId = finalize.GetOriginalOperationId();
            
            NIceDb::TNiceDb db(context.GetDB());
            
            auto stateIt = context.SS->IncrementalRestoreStates.find(originalOpId);
            if (stateIt != context.SS->IncrementalRestoreStates.end()) {
                auto& state = stateIt->second;
                state.State = TIncrementalRestoreState::EState::Completed;
                
                LOG_I("Marked incremental restore state as completed for operation: " << originalOpId);
            }
            
            LOG_I("Keeping IncrementalRestoreOperations entry for operation: " << originalOpId << " - will be cleaned up on FORGET");
            
            context.SS->LongIncrementalRestoreOps.erase(TOperationId(originalOpId, 0));
            LOG_I("Cleaned up long incremental restore ops for operation: " << originalOpId);
            
            CleanupMappings(context.SS, originalOpId, context);
        }

        void CleanupMappings(TSchemeShard* ss, ui64 operationId, TOperationContext& context) {
            auto txIt = ss->TxIdToIncrementalRestore.begin();
            while (txIt != ss->TxIdToIncrementalRestore.end()) {
                if (txIt->second == operationId) {
                    auto toErase = txIt++;
                    ss->TxIdToIncrementalRestore.erase(toErase);
                } else {
                    ++txIt;
                }
            }
            
            auto opIt = ss->IncrementalRestoreOperationToState.begin();
            while (opIt != ss->IncrementalRestoreOperationToState.end()) {
                if (opIt->second == operationId) {
                    auto toErase = opIt++;
                    ss->IncrementalRestoreOperationToState.erase(toErase);
                } else {
                    ++opIt;
                }
            }
            
            LOG_I("Cleaned up mappings for operation: " << operationId);
        }
    };

    class TDone: public TSubOperationState {
    private:
        TOperationId OperationId;

        TString DebugHint() const override {
            return TStringBuilder()
                << "TIncrementalRestoreFinalize TDone"
                << " operationId: " << OperationId;
        }

    public:
        TDone(TOperationId id) : OperationId(id) {}

        bool ProgressState(TOperationContext& context) override {
            LOG_I(DebugHint() << " ProgressState");
            // Operation is already complete, nothing to do
            return true;
        }
    };

public:
    using TSubOperationWithContext::TSubOperationWithContext;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& tx = Transaction;
        const TTabletId schemeshardTabletId = context.SS->SelfTabletId();
        
        LOG_I("TIncrementalRestoreFinalizeOp Propose"
            << ", opId: " << OperationId
        );

        const auto& finalize = tx.GetIncrementalRestoreFinalize();
        ui64 originalOpId = finalize.GetOriginalOperationId();

        // Validate that we have the restore state
        auto stateIt = context.SS->IncrementalRestoreStates.find(originalOpId);
        if (stateIt == context.SS->IncrementalRestoreStates.end()) {
            return MakeHolder<TProposeResponse>(NKikimrScheme::StatusPreconditionFailed, 
                ui64(OperationId.GetTxId()), ui64(schemeshardTabletId),
                "Incremental restore state not found for operation: " + ToString(originalOpId));
        }

        Y_VERIFY_S(!context.SS->FindTx(OperationId), 
            "TIncrementalRestoreFinalizeOp Propose: operation already exists"
            << ", opId: " << OperationId);

        // Use backup collection path as domain path
        TPathId backupCollectionPathId(context.SS->TabletID(), finalize.GetBackupCollectionPathId());
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxIncrementalRestoreFinalize, backupCollectionPathId);
        
        txState.TargetPathId = backupCollectionPathId;
        
        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(schemeshardTabletId));

        txState.State = TTxState::Waiting;
        context.DbChanges.PersistTxState(OperationId);
        context.OnComplete.ActivateTx(OperationId);

        SetState(NextState(TTxState::Waiting), context);
        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TIncrementalRestoreFinalizeOp AbortPropose"
            << ", opId: " << OperationId);
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TIncrementalRestoreFinalizeOp AbortUnsafe"
            << ", opId: " << OperationId
            << ", forceDropId: " << forceDropTxId);
        
        // TODO: Handle abort if needed
    }
};

ISubOperation::TPtr CreateIncrementalRestoreFinalize(TOperationId opId, const TTxTransaction& tx) {
    return MakeSubOperation<TIncrementalRestoreFinalizeOp>(opId, tx);
}

ISubOperation::TPtr CreateIncrementalRestoreFinalize(TOperationId opId, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TIncrementalRestoreFinalizeOp>(opId, state);
}

}
