#include "schemeshard__operation_backup_restore_common.h"
#include "schemeshard_billing_helpers.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

struct TRestore {
    static constexpr const char* Name() {
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
        if (sourcePath->IsTable()) {
            Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
            TTableInfo::TPtr table = context.SS->Tables.at(pathId);
            return ProposeTableTx(table->RestoreSettings, opId, txState, context);
        } else {
            Y_ABORT_UNLESS(context.SS->ColumnTables.contains(pathId));
            TColumnTableInfo::TPtr table = context.SS->ColumnTables.at(pathId).GetPtr();
            return ProposeTableTx(table->RestoreSettings, opId, txState, context);
        }
    }

    static void ProposeTableTx(const NKikimrSchemeOp::TRestoreTask& restoreSettings, const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        const auto& pathId = txState.TargetPathId;
        const TPath sourcePath = TPath::Init(pathId, context.SS);
        const auto seqNo = context.SS->StartRound(txState);

        // The body differs across shards only by the ShardNum scalar: build the
        // invariant common piece once and append a tiny per-shard delta (see
        // schemeshard_proposal_body.h for the wire-concatenation rules).
        TString txBodyCommon;
        if (sourcePath->IsTable()) {
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            context.SS->FillSeqNo(tx, seqNo);
            auto& restore = *tx.MutableRestore();
            restore.CopyFrom(restoreSettings);
            restore.SetTableId(pathId.LocalPathId);
            Y_ABORT_UNLESS(tx.SerializeToString(&txBodyCommon));
        } else {
            NKikimrTxColumnShard::TRestoreTxBody txBodyRestore;
            auto& restore = *txBodyRestore.MutableRestoreTask();
            restore.CopyFrom(restoreSettings);
            restore.SetTableId(pathId.LocalPathId);
            Y_ABORT_UNLESS(txBodyRestore.SerializeToString(&txBodyCommon));
        }

        for (ui32 i = 0; i < txState.Shards.size(); ++i) {
            const auto& idx = txState.Shards[i].Idx;
            const auto& shardId = context.SS->ShardInfos[idx].TabletID;

            YDB_LOG_DEBUG_CTX(context.Ctx, "Propose restore",
                {"shard", shardId},
                {"operationId", opId},
                {"schemeshard", context.SS->TabletID()},
            );

            // Per-shard delta: only the ShardNum scalar, wrapped in the same
            // outer message type so the wire tags match on concatenation.
            TString deltaPiece;
            if (sourcePath->IsTable()) {
                NKikimrTxDataShard::TFlatSchemeTransaction deltaTx;
                deltaTx.MutableRestore()->SetShardNum(i);
                Y_ABORT_UNLESS(deltaTx.SerializeToString(&deltaPiece));
            } else {
                NKikimrTxColumnShard::TRestoreTxBody deltaBody;
                deltaBody.MutableRestoreTask()->SetShardNum(i);
                Y_ABORT_UNLESS(deltaBody.SerializeToString(&deltaPiece));
            }

            if (sourcePath->IsTable()) {
                auto ev = context.SS->MakeDataShardProposal(pathId, opId, context.Ctx);
                TString& txBody = *ev->Record.MutableTxBody();
                txBody.append(txBodyCommon);
                txBody.append(deltaPiece);
                context.OnComplete.BindMsgToPipe(opId, shardId, idx, ev.Release());
            } else {
                auto ev = context.SS->MakeColumnShardProposal(pathId, opId, seqNo, context.Ctx, NKikimrTxColumnShard::TX_KIND_RESTORE);
                TString& txBody = *ev->Record.MutableTxBody();
                txBody.append(txBodyCommon);
                txBody.append(deltaPiece);
                context.OnComplete.BindMsgToPipe(opId, shardId, idx, ev.Release());
            }
        }
    }

    static ui64 RequestUnits(ui64 bytes, ui64 rows) {
        return TRUCalculator::BulkUpsert(bytes, rows);
    }

    static void Finish(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        const auto& pathId = txState.TargetPathId;
        const TPath sourcePath = TPath::Init(pathId, context.SS);
        if (sourcePath->IsTable()) {
            Y_ABORT_UNLESS(context.SS->Tables.contains(txState.TargetPathId));
            TTableInfo::TPtr table = context.SS->Tables.at(txState.TargetPathId);
            return TableFinish(table, opId, txState, context);
        } else {
            Y_ABORT_UNLESS(context.SS->ColumnTables.contains(txState.TargetPathId));
            TColumnTableInfo::TPtr table = context.SS->ColumnTables.at(txState.TargetPathId).GetPtr();
            return TableFinish(table, opId, txState, context);
        }
    }

    template <typename TTableInfo>
    static void TableFinish(const TTableInfo& table, const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        if (txState.TxType != TTxState::TxRestore) {
            return;
        }

        Y_ABORT_UNLESS(TAppData::TimeProvider.Get() != nullptr);
        const ui64 ts = TAppData::TimeProvider->Now().Seconds();

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
        if (sourcePath->IsTable()) {
            Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
            TTableInfo::TPtr table = context.SS->Tables.at(pathId);
            return PersistTableTask(table, pathId, tx, context);
        } else {
            Y_ABORT_UNLESS(context.SS->ColumnTables.contains(pathId));
            TColumnTableInfo::TPtr table = context.SS->ColumnTables.at(pathId).GetPtr();
            return PersistTableTask(table, pathId, tx, context);
        }
    }

    template <typename TTableInfo>
    static void PersistTableTask(const TTableInfo& table, const TPathId& pathId, const TTxTransaction& tx, TOperationContext& context) {
        const auto& restore = tx.GetRestore();
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

#undef YDB_LOG_THIS_FILE_COMPONENT
