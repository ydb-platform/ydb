#include "schemeshard__operation_incremental_restore_finalize.h"

#include "schemeshard__operation_base.h"
#include "schemeshard__operation_common.h"

#include <ydb/core/base/table_index.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

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

            YDB_LOG_CTX_INFO(context.Ctx, "HandleReply TEvProposeTransactionResult",
                {"DebugHint", DebugHint()},
                {"at_schemeshard", ssId},
                {"message", ev->Get()->Record.ShortDebugString()});

            return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
        }

        bool ProgressState(TOperationContext& context) override {
            TTabletId ssId = context.SS->SelfTabletId();

            YDB_LOG_CTX_INFO(context.Ctx, "ProgressState",
                {"DebugHint", DebugHint()},
                {"at_schemeshard", ssId});

            TTxState* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState);

            const auto& finalize = Transaction.GetIncrementalRestoreFinalize();

            // Collect all index impl tables that need schema version updates
            THashSet<TPathId> implTablesToUpdate;
            CollectIndexImplTables(finalize, context, implTablesToUpdate);

            if (implTablesToUpdate.empty()) {
                YDB_LOG_CTX_INFO(context.Ctx, "No index impl tables to update, skipping ConfigureParts",
                    {"TabletID", context.SS->TabletID()},
                    {"DebugHint", DebugHint()});
                return true;
            }

            // Prepare AlterData for each table and add shards to txState
            NIceDb::TNiceDb db(context.GetDB());
            txState->ClearShardsInProgress();

            for (const auto& tablePathId : implTablesToUpdate) {
                if (!context.SS->Tables.contains(tablePathId)) {
                    YDB_LOG_CTX_WARN(context.Ctx, "Table not",
                        {"TabletID", context.SS->TabletID()},
                        {"DebugHint", DebugHint()},
                        {"found", tablePathId});
                    continue;
                }

                auto table = context.SS->Tables.at(tablePathId);

                // InitAlterData() sets AlterVersion = CurrentVersion + 1 and also sets CoordinatedSchemaVersion
                table->InitAlterData(OperationId);

                context.SS->PersistAddAlterTable(db, tablePathId, table->AlterData);

                YDB_LOG_CTX_INFO(context.Ctx, "Preparing ALTER for table ->",
                    {"TabletID", context.SS->TabletID()},
                    {"DebugHint", DebugHint()},
                    {"tablePathId", tablePathId},
                    {"version", table->AlterVersion},
                    {"#_table->AlterData->AlterVersion", table->AlterData->AlterVersion});

                // Note: Parent index version update is deferred to TFinalizationPropose::SyncIndexSchemaVersions()
                // which runs after coordinator confirmation, ensuring consistency if datashard proposals fail.

                // Add all shards of this table to txState
                for (const auto* shard : table->GetPartitions()) {
                    auto shardIdx = shard->ShardIdx;
                    if (!txState->ShardsInProgress.contains(shardIdx)) {
                        txState->Shards.emplace_back(shardIdx, ETabletType::DataShard, TTxState::ConfigureParts);
                        txState->ShardsInProgress.insert(shardIdx);
                        
                        YDB_LOG_CTX_INFO(context.Ctx, "Added shard ) to txState",
                            {"TabletID", context.SS->TabletID()},
                            {"DebugHint", DebugHint()},
                            {"shardIdx", shardIdx},
                            {"(tablet", context.SS->ShardInfos[shardIdx].TabletID});
                    }
                }
            }

            context.SS->PersistTxState(db, OperationId);

            // Send ALTER TABLE transactions to all datashards
            for (const auto& shard : txState->Shards) {
                auto shardIdx = shard.Idx;
                auto datashardId = context.SS->ShardInfos[shardIdx].TabletID;

                YDB_LOG_CTX_INFO(context.Ctx, "Propose ALTER to datashard",
                    {"TabletID", context.SS->TabletID()},
                    {"DebugHint", DebugHint()},
                    {"datashardId", datashardId},
                    {"shardIdx", shardIdx},
                    {"txid", OperationId});

                const auto seqNo = context.SS->StartRound(*txState);
                
                // Find which table this shard belongs to
                TPathId tablePathId;
                for (const auto& pathId : implTablesToUpdate) {
                    auto table = context.SS->Tables.at(pathId);
                    for (const auto* partition : table->GetPartitions()) {
                        if (partition->ShardIdx == shardIdx) {
                            tablePathId = pathId;
                            break;
                        }
                    }
                    if (tablePathId) break;
                }

                if (!tablePathId) {
                    YDB_LOG_CTX_WARN(context.Ctx, "Could not find table for shard",
                        {"TabletID", context.SS->TabletID()},
                        {"DebugHint", DebugHint()},
                        {"shardIdx", shardIdx});
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
                    YDB_LOG_CTX_WARN(context.Ctx, "CollectIndexImplTables: Table not",
                        {"TabletID", context.SS->TabletID()},
                        {"resolved", tablePath});
                    continue;
                }

                if (path.Base()->PathType != NKikimrSchemeOp::EPathType::EPathTypeTable) {
                    continue;
                }

                TPathId implTablePathId = path.Base()->PathId;
                if (context.SS->Tables.contains(implTablePathId)) {
                    implTables.insert(implTablePathId);
                    YDB_LOG_CTX_INFO(context.Ctx, "CollectIndexImplTables: Found index impl",
                        {"TabletID", context.SS->TabletID()},
                        {"table", tablePath},
                        {"pathId", implTablePathId});
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
            YDB_LOG_CTX_INFO(context.Ctx, "ProgressState",
                {"TabletID", context.SS->TabletID()},
                {"DebugHint", DebugHint()});

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

            context.OnComplete.DoneOperation(OperationId);
            return true;
        }

    private:
        void SyncIndexSchemaVersions(const NKikimrSchemeOp::TIncrementalRestoreFinalize& finalize,
                                     TOperationContext& context) {
            YDB_LOG_CTX_INFO(context.Ctx, "SyncIndexSchemaVersions: Starting schema version sync for restored indexes",
                {"TabletID", context.SS->TabletID()});
            LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " <<"SyncIndexSchemaVersions: Processing " << finalize.GetTargetTablePaths().size() << " target table paths");
            
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
                    YDB_LOG_CTX_WARN(context.Ctx, "SyncIndexSchemaVersions: Table not",
                        {"TabletID", context.SS->TabletID()},
                        {"resolved", tablePath});
                    continue;
                }

                if (path.Base()->PathType != NKikimrSchemeOp::EPathType::EPathTypeTable) {
                    continue;
                }

                TPathId implTablePathId = path.Base()->PathId;
                if (!context.SS->Tables.contains(implTablePathId)) {
                    YDB_LOG_CTX_WARN(context.Ctx, "SyncIndexSchemaVersions: Table not",
                        {"TabletID", context.SS->TabletID()},
                        {"found", implTablePathId});
                    continue;
                }

                auto table = context.SS->Tables.at(implTablePathId);
                if (!table->AlterData) {
                    YDB_LOG_CTX_WARN(context.Ctx, "SyncIndexSchemaVersions: No AlterData for",
                        {"TabletID", context.SS->TabletID()},
                        {"table", implTablePathId});
                    continue;
                }

                // Store coordinated version BEFORE calling FinishAlter (which resets AlterData)
                ui64 coordVersion = table->AlterData->CoordinatedSchemaVersion.GetOrElse(table->AlterVersion + 1);

                // Finalize the alter - this commits AlterData to the main table state
                YDB_LOG_CTX_INFO(context.Ctx, "SyncIndexSchemaVersions: Finalizing ALTER for table ->",
                    {"TabletID", context.SS->TabletID()},
                    {"implTablePathId", implTablePathId},
                    {"version", table->AlterVersion},
                    {"#_table->AlterData->AlterVersion", table->AlterData->AlterVersion});

                // Release AlterData tracking before FinishAlter resets it
                if (table->ReleaseAlterData(OperationId)) {
                    context.SS->PersistClearAlterTableFull(db, implTablePathId);
                }

                table->FinishAlter();
                context.SS->PersistTableAltered(db, implTablePathId, table);

                // Clear describe path caches and publish to scheme board
                context.SS->ClearDescribePathCaches(path.Base());
                context.OnComplete.PublishToSchemeBoard(OperationId, implTablePathId);

                YDB_LOG_CTX_INFO(context.Ctx, "SyncIndexSchemaVersions: Finalized schema version",
                    {"TabletID", context.SS->TabletID()},
                    {"for", tablePath});

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

                            YDB_LOG_CTX_INFO(context.Ctx, "SyncIndexSchemaVersions: Index AlterVersion updated from to",
                                {"TabletID", context.SS->TabletID()},
                                {"oldVersion", oldVersion},
                                {"#_context.SS->Indexes[indexPathId]->AlterVersion", context.SS->Indexes[indexPathId]->AlterVersion});

                            context.OnComplete.PublishToSchemeBoard(OperationId, indexPathId);

                            TPath mainTablePath = indexPath.Parent();
                            if (mainTablePath.IsResolved() && mainTablePath.Base()->PathType == NKikimrSchemeOp::EPathTypeTable) {
                                TPathId mainTablePathId = mainTablePath.Base()->PathId;
                                if (!publishedMainTables.contains(mainTablePathId)) {
                                    publishedMainTables.insert(mainTablePathId);
                                    context.SS->ClearDescribePathCaches(mainTablePath.Base());
                                    context.OnComplete.PublishToSchemeBoard(OperationId, mainTablePathId);
                                    YDB_LOG_CTX_INFO(context.Ctx, "SyncIndexSchemaVersions: Published main",
                                        {"TabletID", context.SS->TabletID()},
                                        {"table", mainTablePathId});
                                }
                            }
                        }
                    }
                }
            }

            YDB_LOG_CTX_INFO(context.Ctx, "SyncIndexSchemaVersions: Finished schema version sync",
                {"TabletID", context.SS->TabletID()});
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
                            YDB_LOG_CTX_INFO(context.Ctx, "Adding target table path",
                                {"TabletID", context.SS->TabletID()},
                                {"to_normalize", tablePath});
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
                            YDB_LOG_CTX_INFO(context.Ctx, "Adding backup table path",
                                {"TabletID", context.SS->TabletID()},
                                {"to_normalize", backupTablePath});
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
                // Persist terminal Completed state in the same db tx that releases path states.
                // Row remains for Get/List to surface SUCCESS until FORGET clears it.
                TSchemeShard::PersistIncrementalRestoreTerminalState(context.SS, db, originalOpId, state,
                    TIncrementalRestoreState::EState::Completed,
                    static_cast<ui32>(Ydb::StatusIds::SUCCESS));

                YDB_LOG_CTX_INFO(context.Ctx, "Persisted incremental restore state as Completed for",
                    {"TabletID", context.SS->TabletID()},
                    {"operation", originalOpId});
            }

            YDB_LOG_CTX_INFO(context.Ctx, "Keeping IncrementalRestoreOperations entry for - will be cleaned up on FORGET",
                {"TabletID", context.SS->TabletID()},
                {"operation", originalOpId});

            context.SS->LongIncrementalRestoreOps.erase(TOperationId(originalOpId, 0));
            YDB_LOG_CTX_INFO(context.Ctx, "Cleaned up long incremental restore ops for",
                {"TabletID", context.SS->TabletID()},
                {"operation", originalOpId});

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
            
            YDB_LOG_CTX_INFO(context.Ctx, "Cleaned up mappings for",
                {"TabletID", context.SS->TabletID()},
                {"operation", operationId});
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
            YDB_LOG_CTX_INFO(context.Ctx, "ProgressState",
                {"TabletID", context.SS->TabletID()},
                {"DebugHint", DebugHint()});
            // Operation is already complete, nothing to do
            return true;
        }
    };

public:
    using TSubOperationWithContext::TSubOperationWithContext;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& tx = Transaction;
        const TTabletId schemeshardTabletId = context.SS->SelfTabletId();
        
        YDB_LOG_CTX_INFO(context.Ctx, "TIncrementalRestoreFinalizeOp Propose",
            {"TabletID", context.SS->TabletID()},
            {"opId", OperationId});

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
        YDB_LOG_CTX_NOTICE(context.Ctx, "TIncrementalRestoreFinalizeOp AbortPropose",
            {"TabletID", context.SS->TabletID()},
            {"opId", OperationId});
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDB_LOG_CTX_NOTICE(context.Ctx, "TIncrementalRestoreFinalizeOp AbortUnsafe",
            {"TabletID", context.SS->TabletID()},
            {"opId", OperationId},
            {"forceDropId", forceDropTxId});
        
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
