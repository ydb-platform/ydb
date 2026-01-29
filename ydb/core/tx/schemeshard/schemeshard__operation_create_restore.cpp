#include "schemeshard__operation_backup_restore_common.h"
#include "schemeshard_billing_helpers.h"

namespace NKikimr {
namespace NSchemeShard {

struct TRestore {
    static constexpr TStringBuf Name() {
        return "TRestore";
    }

    static constexpr bool NeedSnapshotTime() {
        return false;
    }

    static bool HasTask(const TTxTransaction& tx) {
        return tx.HasRestore();
    }

    static TString GetTableName(const TTxTransaction& tx) {
        return tx.GetRestore().GetTableName();
    }

    static void ProposeTx(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        const auto& pathId = txState.TargetPathId;
        const TPath sourcePath = TPath::Init(pathId, context.SS);
        if (sourcePath->IsColumnTable()) {
            return ProposeColumnTableTx(opId, txState, context);
        } else {
            return ProposeTableTx(opId, txState, context);
        }
    }

    static void ProposeColumnTableTx(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        const auto& pathId = txState.TargetPathId;
        Y_ABORT_UNLESS(context.SS->ColumnTables.contains(pathId));
        TColumnTableInfo::TPtr table = context.SS->ColumnTables.at(pathId).GetPtr();

        const auto seqNo = context.SS->StartRound(txState);
        for (ui32 i = 0; i < txState.Shards.size(); ++i) {
            const auto& idx = txState.Shards[i].Idx;
            const auto& columnShardId = context.SS->ShardInfos[idx].TabletID;

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Propose restore"
                            << ", columnshard: " << columnShardId
                            << ", opId: " <<  opId
                            << ", at schemeshard: " << context.SS->TabletID());
            
            NKikimrTxColumnShard::TRestoreTxBody txBodyRestore;
            auto& restore = *txBodyRestore.MutableRestoreTask();
            restore.CopyFrom(table->RestoreSettings);
            restore.SetTableId(pathId.LocalPathId);
            restore.SetShardNum(i);
            auto ev = context.SS->MakeColumnShardProposal(pathId, opId, seqNo, txBodyRestore.SerializeAsString(), context.Ctx, NKikimrTxColumnShard::TX_KIND_RESTORE);
            context.OnComplete.BindMsgToPipe(opId, columnShardId, idx, ev.Release());
        }
    }

    static void ProposeTableTx(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        const auto& pathId = txState.TargetPathId;
        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        TTableInfo::TPtr table = context.SS->Tables.at(pathId);

        const auto seqNo = context.SS->StartRound(txState);
        for (ui32 i = 0; i < txState.Shards.size(); ++i) {
            const auto& idx = txState.Shards[i].Idx;
            const auto& datashardId = context.SS->ShardInfos[idx].TabletID;

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Propose restore"
                            << ", datashard: " << datashardId
                            << ", opId: " <<  opId
                            << ", at schemeshard: " << context.SS->TabletID());

            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            context.SS->FillSeqNo(tx, seqNo);
            auto& restore = *tx.MutableRestore();
            restore.CopyFrom(table->RestoreSettings);
            restore.SetTableId(pathId.LocalPathId);
            restore.SetShardNum(i);

            auto ev = context.SS->MakeDataShardProposal(pathId, opId, tx.SerializeAsString(), context.Ctx);
            context.OnComplete.BindMsgToPipe(opId, datashardId, idx, ev.Release());
        }
    }

    static ui64 RequestUnits(ui64 bytes, ui64 rows) {
        return TRUCalculator::BulkUpsert(bytes, rows);
    }

    static void Finish(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        const auto& pathId = txState.TargetPathId;
        const TPath sourcePath = TPath::Init(pathId, context.SS);
        if (sourcePath->IsColumnTable()) {
            return ColumnTableFinish(opId, txState, context);
        } else {
            return TableFinish(opId, txState, context);
        }
    }
    
    static void ColumnTableFinish(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        if (txState.TxType != TTxState::TxRestore) {
            return;
        }

        Y_ABORT_UNLESS(TAppData::TimeProvider.Get() != nullptr);
        const ui64 ts = TAppData::TimeProvider->Now().Seconds();

        Y_ABORT_UNLESS(context.SS->ColumnTables.contains(txState.TargetPathId));
        TColumnTableInfo::TPtr table = context.SS->ColumnTables.at(txState.TargetPathId).GetPtr();

        auto& restoreInfo = table->RestoreHistory[opId.GetTxId()];

        restoreInfo.StartDateTime = txState.StartTime.Seconds();
        restoreInfo.CompletionDateTime = ts;
        restoreInfo.TotalShardCount = table->GetColumnShards().size();
        restoreInfo.SuccessShardCount = CountIf(txState.ShardStatuses, [](const auto& kv) {
            return kv.second.Success;
        });
        restoreInfo.ShardStatuses = std::move(txState.ShardStatuses);
        restoreInfo.DataTotalSize = txState.DataTotalSize;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistCompletedRestore(db, opId.GetTxId(), txState, restoreInfo);

        table->IsRestore = false;
        context.SS->PersistTableIsRestore(db, txState.TargetPathId, table);
    }

    static void TableFinish(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        if (txState.TxType != TTxState::TxRestore) {
            return;
        }

        Y_ABORT_UNLESS(TAppData::TimeProvider.Get() != nullptr);
        const ui64 ts = TAppData::TimeProvider->Now().Seconds();

        Y_ABORT_UNLESS(context.SS->Tables.contains(txState.TargetPathId));
        TTableInfo::TPtr table = context.SS->Tables[txState.TargetPathId];

        auto& restoreInfo = table->RestoreHistory[opId.GetTxId()];

        restoreInfo.StartDateTime = txState.StartTime.Seconds();
        restoreInfo.CompletionDateTime = ts;
        restoreInfo.TotalShardCount = table->GetPartitions().size();
        restoreInfo.SuccessShardCount = CountIf(txState.ShardStatuses, [](const auto& kv) {
            return kv.second.Success;
        });
        restoreInfo.ShardStatuses = std::move(txState.ShardStatuses);
        restoreInfo.DataTotalSize = txState.DataTotalSize;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistCompletedRestore(db, opId.GetTxId(), txState, restoreInfo);

        table->IsRestore = false;
        context.SS->PersistTableIsRestore(db, txState.TargetPathId, table);
    }

    static void PersistTask(const TPathId& pathId, const TTxTransaction& tx, TOperationContext& context) {
        const TPath sourcePath = TPath::Init(pathId, context.SS);
        if (sourcePath->IsColumnTable()) {
            return PersistColumnTableTask(pathId, tx, context);
        } else {
            return PersistTableTask(pathId, tx, context);
        }
    }
    
    static void PersistColumnTableTask(const TPathId& pathId, const TTxTransaction& tx, TOperationContext& context) {
        const auto& restore = tx.GetRestore();

        Y_ABORT_UNLESS(context.SS->ColumnTables.contains(pathId));
        TColumnTableInfo::TPtr table = context.SS->ColumnTables.at(pathId).GetPtr();
        table->RestoreSettings = restore;

        NIceDb::TNiceDb db(context.GetDB());
        db.Table<Schema::RestoreTasks>()
            .Key(pathId.OwnerId, pathId.LocalPathId)
            .Update(NIceDb::TUpdate<Schema::RestoreTasks::Task>(restore.SerializeAsString()));
    }

    static void PersistTableTask(const TPathId& pathId, const TTxTransaction& tx, TOperationContext& context) {
        const auto& restore = tx.GetRestore();

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        TTableInfo::TPtr table = context.SS->Tables.at(pathId);
        table->RestoreSettings = restore;

        NIceDb::TNiceDb db(context.GetDB());
        db.Table<Schema::RestoreTasks>()
            .Key(pathId.OwnerId, pathId.LocalPathId)
            .Update(NIceDb::TUpdate<Schema::RestoreTasks::Task>(restore.SerializeAsString()));
    }

    static void PersistDone(const TPathId& pathId, TOperationContext& context) {
        NIceDb::TNiceDb db(context.GetDB());
        db.Table<Schema::RestoreTasks>()
            .Key(pathId.OwnerId, pathId.LocalPathId)
            .Delete();
    }

    static bool NeedToBill(const TPathId&, TOperationContext&) {
        return true;
    }
};

ISubOperation::TPtr CreateRestore(TOperationId id, const TTxTransaction& tx) {
    return new TBackupRestoreOperationBase<TRestore, TEvDataShard::TEvCancelRestore>(
        TTxState::TxRestore, TPathElement::EPathState::EPathStateRestore, id, tx
    );
}

ISubOperation::TPtr CreateRestore(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return new TBackupRestoreOperationBase<TRestore, TEvDataShard::TEvCancelRestore>(
        TTxState::TxRestore, TPathElement::EPathState::EPathStateRestore, id, state
    );
}

} // NSchemeShard
} // NKikimr
