#include "schemeshard__operation_incremental_restore_finalize.h"

#include "schemeshard__operation_base.h"
#include "schemeshard__operation_common.h"

#include <ydb/core/base/table_index.h>

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
    public:
        virtual const char* Name() const override final { return "TConfigureParts"; }

    private:
        TOperationId OperationId;
        TTxTransaction Transaction;

    public:
        TConfigureParts(TOperationId id, const TTxTransaction& tx)
            : OperationId(id), Transaction(tx)
        {
            IgnoreMessages({TEvHive::TEvCreateTabletReply::EventType});
        }

        bool HandleReply(TEvDataShard::TEvProposeTransactionResult::TPtr& ev, TOperationContext& context) override {
            YDB_LOG_INFO_CTX(context.Ctx, "",
                {"message", ev->Get()->Record.ShortDebugString()},
            );

            return NTableState::CollectProposeTransactionResults(OperationId, ev, context);
        }

        bool ProgressState(TOperationContext& context) override {
            YDB_LOG_INFO_CTX(context.Ctx, "");

            TTxState* txState = context.SS->FindTx(OperationId);
            Y_ABORT_UNLESS(txState);

            const auto& finalize = Transaction.GetIncrementalRestoreFinalize();

            // Collect all index impl tables that need schema version updates
            THashSet<TPathId> implTablesToUpdate;
            CollectIndexImplTables(finalize, context, implTablesToUpdate);

            if (implTablesToUpdate.empty()) {
                YDB_LOG_INFO_CTX(context.Ctx, "No index impl tables to update, skipping ConfigureParts"
                );
                return true;
            }

            // Prepare AlterData for each table and add shards to txState
            NIceDb::TNiceDb db(context.GetDB());
            txState->ClearShardsInProgress();

            for (const auto& tablePathId : implTablesToUpdate) {
                if (!context.SS->Tables.contains(tablePathId)) {
                    YDB_LOG_WARN_CTX(context.Ctx, "Table not found",
                        {"tablePathId", tablePathId},
                    );
                    continue;
                }

                auto table = context.SS->Tables.at(tablePathId);

                // InitAlterData() sets AlterVersion = CurrentVersion + 1 and also sets CoordinatedSchemaVersion
                table->InitAlterData(OperationId);

                context.SS->PersistAddAlterTable(db, tablePathId, table->AlterData);

                YDB_LOG_INFO_CTX(context.Ctx, "Preparing ALTER for table",
                    {"tablePathId", tablePathId},
                    {"version", table->AlterVersion},
                    {"newVersion", table->AlterData->AlterVersion},
                );

                // Note: Parent index version update is deferred to TFinalizationPropose::SyncIndexSchemaVersions()
                // which runs after coordinator confirmation, ensuring consistency if datashard proposals fail.

                // Add all shards of this table to txState
                for (const auto* shard : table->GetPartitions()) {
                    auto shardIdx = shard->ShardIdx;
                    if (!txState->ShardsInProgress.contains(shardIdx)) {
                        txState->Shards.emplace_back(shardIdx, ETabletType::DataShard, TTxState::ConfigureParts);
                        txState->ShardsInProgress.insert(shardIdx);

                        YDB_LOG_INFO_CTX(context.Ctx, "Added shard to txState",
                            {"shardIdx", shardIdx},
                            {"tabletId", context.SS->ShardInfos[shardIdx].TabletID},
                        );
                    }
                }
            }

            context.SS->PersistTxState(db, OperationId);

            // Send ALTER TABLE transactions to all datashards
            for (const auto& shard : txState->Shards) {
                auto shardIdx = shard.Idx;
                auto datashardId = context.SS->ShardInfos[shardIdx].TabletID;

                YDB_LOG_INFO_CTX(context.Ctx, "Propose ALTER to datashard",
                    {"datashardId", datashardId},
                    {"shardIdx", shardIdx},
                    {"txId", OperationId},
                );

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
                    YDB_LOG_WARN_CTX(context.Ctx, "Could not find table for shard",
                        {"shardIdx", shardIdx},
                    );
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
                    YDB_LOG_WARN_CTX(context.Ctx, "CollectIndexImplTables: Table not resolved",
                        {"tablePath", tablePath},
                    );
                    continue;
                }

                if (path.Base()->PathType != NKikimrSchemeOp::EPathType::EPathTypeTable) {
                    continue;
                }

                TPathId implTablePathId = path.Base()->PathId;
                if (context.SS->Tables.contains(implTablePathId)) {
                    implTables.insert(implTablePathId);
                    YDB_LOG_INFO_CTX(context.Ctx, "CollectIndexImplTables: Found index impl table",
                        {"tablePath", tablePath},
                        {"pathId", implTablePathId},
                    );
                }
            }
        }
    };

    class TFinalizationPropose: public TSubOperationState {
    public:
        virtual const char* Name() const override final { return "TFinalizationPropose"; }

    private:
        TOperationId OperationId;
        TTxTransaction Transaction;

    public:
        TFinalizationPropose(TOperationId id, const TTxTransaction& tx)
            : OperationId(id), Transaction(tx) {}

        bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
            return TSubOperationState::HandleReply(ev, context);
        }

        bool ProgressState(TOperationContext& context) override {
            YDB_LOG_INFO_CTX(context.Ctx, "");

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
            YDB_LOG_INFO_CTX(context.Ctx, "SyncIndexSchemaVersions: Starting schema version sync for restored indexes"
            );
            YDB_LOG_INFO_CTX(context.Ctx, "SyncIndexSchemaVersions: Processing target table paths",
                {"count", finalize.GetTargetTablePaths().size()},
            );

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
                    YDB_LOG_WARN_CTX(context.Ctx, "SyncIndexSchemaVersions: Table not resolved",
                        {"tablePath", tablePath},
                    );
                    continue;
                }

                if (path.Base()->PathType != NKikimrSchemeOp::EPathType::EPathTypeTable) {
                    continue;
                }

                TPathId implTablePathId = path.Base()->PathId;
                if (!context.SS->Tables.contains(implTablePathId)) {
                    YDB_LOG_WARN_CTX(context.Ctx, "SyncIndexSchemaVersions: Table not found",
                        {"tablePathId", implTablePathId},
                    );
                    continue;
                }

                auto table = context.SS->Tables.at(implTablePathId);
                if (!table->AlterData) {
                    YDB_LOG_WARN_CTX(context.Ctx, "SyncIndexSchemaVersions: No AlterData for table",
                        {"tablePathId", implTablePathId},
                    );
                    continue;
                }

                // Store coordinated version BEFORE calling FinishAlter (which resets AlterData)
                ui64 coordVersion = table->AlterData->CoordinatedSchemaVersion.GetOrElse(table->AlterVersion + 1);

                // Finalize the alter - this commits AlterData to the main table state
                YDB_LOG_INFO_CTX(context.Ctx, "SyncIndexSchemaVersions: Finalizing ALTER for table",
                    {"tablePathId", implTablePathId},
                    {"version", table->AlterVersion},
                    {"newVersion", table->AlterData->AlterVersion},
                );

                // Release AlterData tracking before FinishAlter resets it
                if (table->ReleaseAlterData(OperationId)) {
                    context.SS->PersistClearAlterTableFull(db, implTablePathId);
                }

                table->FinishAlter();
                context.SS->PersistTableAltered(db, implTablePathId, table);

                // Clear describe path caches and publish to scheme board
                context.SS->ClearDescribePathCaches(path.Base());
                context.OnComplete.PublishToSchemeBoard(OperationId, implTablePathId);

                YDB_LOG_INFO_CTX(context.Ctx, "SyncIndexSchemaVersions: Finalized schema version",
                    {"tablePath", tablePath},
                );

                // Also update the parent index version
                TPath indexPath = path.Parent();
                if (indexPath.IsResolved() && indexPath.Base()->PathType == NKikimrSchemeOp::EPathTypeTableIndex) {
                    TPathId indexPathId = indexPath.Base()->PathId;
                    if (context.SS->Indexes.contains(indexPathId)) {
                        auto oldVersion = context.SS->Indexes.at(indexPathId)->AlterVersion;

                        // Use the coordinated version stored before FinishAlter
                        ui64 targetVersion = coordVersion;

                        if (context.SS->Indexes.at(indexPathId)->AlterVersion < targetVersion) {
                            auto index = context.SS->Indexes.at(indexPathId);
                            index->AlterVersion = targetVersion;
                            if (index->AlterData && index->AlterData->AlterVersion < targetVersion) {
                                index->AlterData->AlterVersion = targetVersion;
                                context.SS->PersistTableIndexAlterData(db, indexPathId);
                            }
                            context.SS->PersistTableIndexAlterVersion(db, indexPathId, index);

                            YDB_LOG_INFO_CTX(context.Ctx, "SyncIndexSchemaVersions: Index AlterVersion updated",
                                {"oldVersion", oldVersion},
                                {"newVersion", context.SS->Indexes.at(indexPathId)->AlterVersion},
                            );

                            context.OnComplete.PublishToSchemeBoard(OperationId, indexPathId);

                            TPath mainTablePath = indexPath.Parent();
                            if (mainTablePath.IsResolved() && mainTablePath.Base()->PathType == NKikimrSchemeOp::EPathTypeTable) {
                                TPathId mainTablePathId = mainTablePath.Base()->PathId;
                                if (!publishedMainTables.contains(mainTablePathId)) {
                                    publishedMainTables.insert(mainTablePathId);
                                    context.SS->ClearDescribePathCaches(mainTablePath.Base());
                                    context.OnComplete.PublishToSchemeBoard(OperationId, mainTablePathId);
                                    YDB_LOG_INFO_CTX(context.Ctx, "SyncIndexSchemaVersions: Published main table",
                                        {"tablePathId", mainTablePathId},
                                    );
                                }
                            }
                        }
                    }
                }
            }

            YDB_LOG_INFO_CTX(context.Ctx, "SyncIndexSchemaVersions: Finished schema version sync"
            );
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
                            YDB_LOG_INFO_CTX(context.Ctx, "Adding target table path to normalize",
                                {"tablePath", tablePath},
                            );
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
                            YDB_LOG_INFO_CTX(context.Ctx, "Adding backup table path to normalize",
                                {"tablePath", backupTablePath},
                            );
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

                YDB_LOG_INFO_CTX(context.Ctx, "Persisted incremental restore state as Completed for operation",
                    {"operationId", originalOpId},
                );
            }

            YDB_LOG_INFO_CTX(context.Ctx, "Keeping IncrementalRestoreOperations entry for operation - will be cleaned up on FORGET",
                {"operationId", originalOpId},
            );

            context.SS->LongIncrementalRestoreOps.erase(TOperationId(originalOpId, 0));
            YDB_LOG_INFO_CTX(context.Ctx, "Cleaned up long incremental restore ops for operation",
                {"operationId", originalOpId},
            );

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

            YDB_LOG_INFO_CTX(context.Ctx, "Cleaned up mappings for operation",
                {"operationId", operationId},
            );
        }
    };

    class TDone: public TSubOperationState {
    public:
        virtual const char* Name() const override final { return "TDone"; }

    private:
        TOperationId OperationId;

    public:
        TDone(TOperationId id) : OperationId(id) {}

        bool ProgressState(TOperationContext& context) override {
            YDB_LOG_INFO_CTX(context.Ctx, "");
            // Operation is already complete, nothing to do
            return true;
        }
    };

public:
    using TSubOperationWithContext::TSubOperationWithContext;

    virtual const char* Name() const override final { return "TIncrementalRestoreFinalizeOp"; }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const auto& tx = Transaction;
        const TTabletId schemeshardTabletId = context.SS->SelfTabletId();

        YDB_LOG_INFO_CTX(context.Ctx, "");

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
        YDB_LOG_NOTICE_CTX(context.Ctx, "");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        YDB_LOG_NOTICE_CTX(context.Ctx, "TIncrementalRestoreFinalizeOp AbortUnsafe",
            {"operationId", OperationId},
            {"forceDropId", forceDropTxId},
            {"schemeshard", context.SS->SelfTabletId()},
        );

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

#undef YDB_LOG_THIS_FILE_COMPONENT
