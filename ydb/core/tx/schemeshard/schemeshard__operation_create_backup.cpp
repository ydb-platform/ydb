#include "schemeshard__operation_backup_restore_common.h"
#include "schemeshard_billing_helpers.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

struct TBackup {
    static constexpr const char* Name() {
        return "TBackup";
    }

    static constexpr bool NeedSnapshotTime() {
        return true;
    }

    static bool HasTask(const TTxTransaction& tx) {
        return tx.HasBackup();
    }

    static TString GetTableName(const TTxTransaction& tx) {
        return tx.GetBackup().GetTableName();
    }

    static void ProposeTx(const TOperationId& opId, TTxState& txState, TOperationContext& context, TVirtualTimestamp snapshotTime) {
        const auto& pathId = txState.TargetPathId;
        const TPath sourcePath = TPath::Init(pathId, context.SS);
        if (sourcePath->IsColumnTable()) {
            return ProposeColumnTableTx(opId, txState, context, snapshotTime);
        } else {
            return ProposeTableTx(opId, txState, context, snapshotTime);
        }
    }

    static void ProposeColumnTableTx(const TOperationId& opId, TTxState& txState, TOperationContext& context, TVirtualTimestamp snapshotTime) {
        const auto& pathId = txState.TargetPathId;
        Y_ABORT_UNLESS(context.SS->ColumnTables.contains(pathId));
        auto table = context.SS->ColumnTables.at(pathId);
        NKikimrSchemeOp::TBackupTask backup = table->BackupSettings;
        backup.SetSnapshotStep(snapshotTime.Step);
        backup.SetSnapshotTxId(snapshotTime.TxId);

        const auto seqNo = context.SS->StartRound(txState);

        // The body is identical across shards except ShardNum; shard 0 additionally
        // carries the Table/ChangefeedUnderlyingTopics fields (cleared from the
        // task after the first shard in the original per-shard build). Assemble by
        // concatenation: a common piece serialized once + a tiny per-shard delta.
        NKikimrTxColumnShard::TBackupTxBody txBodyCommon;
        {
            NKikimrSchemeOp::TBackupTask commonTask = backup;
            commonTask.ClearTable();
            commonTask.ClearChangefeedUnderlyingTopics();
            commonTask.SetTableId(pathId.LocalPathId);
            *txBodyCommon.MutableBackupTask() = std::move(commonTask);
        }
        const TString commonPiece = txBodyCommon.SerializeAsString();

        NKikimrTxColumnShard::TBackupTxBody txBodyShard0Extra;
        TString shard0ExtraPiece;
        {
            auto* task = txBodyShard0Extra.MutableBackupTask();
            *task->MutableTable() = backup.GetTable();
            *task->MutableChangefeedUnderlyingTopics() = backup.GetChangefeedUnderlyingTopics();
            shard0ExtraPiece = txBodyShard0Extra.SerializeAsString();
        }

        NKikimrTxColumnShard::TBackupTxBody txBodyDelta;
        for (ui32 i = 0; i < txState.Shards.size(); ++i) {
            auto idx = txState.Shards[i].Idx;
            auto columnShardId = context.SS->ShardInfos[idx].TabletID;

            YDB_LOG_DEBUG_CTX(context.Ctx, "Propose backup to columnshard",
                {"columnShard", columnShardId},
                {"operationId", opId},
                {"schemeshard", context.SS->SelfTabletId()},
            );

            txBodyDelta.Clear();
            txBodyDelta.MutableBackupTask()->SetShardNum(i);
            auto event = context.SS->MakeColumnShardProposal(pathId, opId, seqNo, context.Ctx, NKikimrTxColumnShard::TX_KIND_BACKUP);
            TString& body = *event->Record.MutableTxBody();
            body.append(commonPiece);
            if (i == 0) {
                body.append(shard0ExtraPiece);
            }
            body.append(txBodyDelta.SerializeAsString());
            context.OnComplete.BindMsgToPipe(opId, columnShardId, idx, event.Release());
        }
    }

    static void ProposeTableTx(const TOperationId& opId, TTxState& txState, TOperationContext& context, TVirtualTimestamp snapshotTime) {
        const auto& pathId = txState.TargetPathId;
        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        TTableInfo::TPtr table = context.SS->Tables.at(pathId);
        NKikimrSchemeOp::TBackupTask backup = table->BackupSettings;
        backup.SetSnapshotStep(snapshotTime.Step);
        backup.SetSnapshotTxId(snapshotTime.TxId);

        const auto seqNo = context.SS->StartRound(txState);
        // The body is identical across shards except ShardNum; shard 0 additionally
        // carries the Table/ChangefeedUnderlyingTopics fields (the original per-shard
        // build cleared them from the task after the first shard). Assemble by
        // concatenation: a common piece serialized once + a shard-0 extra piece + a
        // tiny per-shard delta.
        NKikimrSchemeOp::TBackupTask commonTask = backup;
        commonTask.ClearTable();
        commonTask.ClearChangefeedUnderlyingTopics();
        const TString txBodyCommon = context.SS->FillBackupTxBodyCommon(pathId, commonTask, seqNo);

        NKikimrTxDataShard::TFlatSchemeTransaction txBodyShard0Extra;
        {
            auto* task = txBodyShard0Extra.MutableBackup();
            *task->MutableTable() = backup.GetTable();
            *task->MutableChangefeedUnderlyingTopics() = backup.GetChangefeedUnderlyingTopics();
        }
        const TString shard0ExtraPiece = txBodyShard0Extra.SerializeAsString();
        for (ui32 i = 0; i < txState.Shards.size(); ++i) {
            auto idx = txState.Shards[i].Idx;
            auto datashardId = context.SS->ShardInfos[idx].TabletID;

            YDB_LOG_DEBUG_CTX(context.Ctx, "Propose backup to datashard",
                {"datashard", datashardId},
                {"operationId", opId},
                {"schemeshard", context.SS->SelfTabletId()},
            );

            auto event = context.SS->MakeDataShardProposal(pathId, opId, context.Ctx);
            TString& txBody = *event->Record.MutableTxBody();
            txBody.append(txBodyCommon);
            if (i == 0) {
                txBody.append(shard0ExtraPiece);
            }
            context.SS->AppendBackupTxBodyDelta(i, txBody);
            context.OnComplete.BindMsgToPipe(opId, datashardId, idx, event.Release());
        }
    }

    static ui64 RequestUnits(ui64 bytes, ui64 rows) {
        Y_UNUSED(rows);
        return TRUCalculator::ReadTable(bytes);
    }

    static void Finish(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        const auto& pathId = txState.TargetPathId;
        const TPath sourcePath = TPath::Init(pathId, context.SS);
        if (sourcePath->IsColumnTable()) {
            return FinishColumnTable(opId, txState, context);
        } else {
            return FinishTable(opId, txState, context);
        }
    }

    static void FinishColumnTable(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        if (txState.TxType != TTxState::TxBackup) {
            return;
        }

        Y_ABORT_UNLESS(TAppData::TimeProvider.Get() != nullptr);
        const ui64 ts = TAppData::TimeProvider->Now().Seconds();

        Y_ABORT_UNLESS(context.SS->ColumnTables.contains(txState.TargetPathId));
        auto table = context.SS->ColumnTables.at(txState.TargetPathId);

        auto& backupInfo = table.GetPtr()->BackupHistory[opId.GetTxId()];

        backupInfo.StartDateTime = txState.StartTime.Seconds();
        backupInfo.CompletionDateTime = ts;
        backupInfo.TotalShardCount = table->GetColumnShards().size();
        backupInfo.SuccessShardCount = CountIf(txState.ShardStatuses, [](const auto& kv) {
            return kv.second.Success;
        });
        backupInfo.ShardStatuses = std::move(txState.ShardStatuses);
        backupInfo.DataTotalSize = txState.DataTotalSize;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistCompletedBackup(db, opId.GetTxId(), txState, backupInfo);
    }

    static void FinishTable(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        if (txState.TxType != TTxState::TxBackup) {
            return;
        }

        Y_ABORT_UNLESS(TAppData::TimeProvider.Get() != nullptr);
        const ui64 ts = TAppData::TimeProvider->Now().Seconds();

        Y_ABORT_UNLESS(context.SS->Tables.contains(txState.TargetPathId));
        TTableInfo::TPtr table = context.SS->Tables.at(txState.TargetPathId);

        auto& backupInfo = table->BackupHistory[opId.GetTxId()];

        backupInfo.StartDateTime = txState.StartTime.Seconds();
        backupInfo.CompletionDateTime = ts;
        backupInfo.TotalShardCount = table->GetPartitions().size();
        backupInfo.SuccessShardCount = CountIf(txState.ShardStatuses, [](const auto& kv) {
            return kv.second.Success;
        });
        backupInfo.ShardStatuses = std::move(txState.ShardStatuses);
        backupInfo.DataTotalSize = txState.DataTotalSize;

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistCompletedBackup(db, opId.GetTxId(), txState, backupInfo);
    }

    static void PersistTask(const TPathId& pathId, const TTxTransaction& tx, TOperationContext& context) {
        const TPath path = TPath::Init(pathId, context.SS);
        if (path->IsColumnTable()) {
            return PersistColumnTableTask(pathId, tx, context);
        } else {
            return PersistTableTask(pathId, tx, context);
        }
    }

    static void PersistColumnTableTask(const TPathId& pathId, const TTxTransaction& tx, TOperationContext& context) {
        Y_ABORT_UNLESS(context.SS->ColumnTables.contains(pathId));
        auto table = context.SS->ColumnTables.at(pathId);

        table.GetPtr()->BackupSettings = tx.GetBackup();

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistBackupSettings(db, pathId, tx.GetBackup());
    }

    static void PersistTableTask(const TPathId& pathId, const TTxTransaction& tx, TOperationContext& context) {
        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        TTableInfo::TPtr table = context.SS->Tables.at(pathId);

        table->BackupSettings = tx.GetBackup();

        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistBackupSettings(db, pathId, table->BackupSettings);
    }

    static void PersistDone(const TPathId& pathId, TOperationContext& context) {
        NIceDb::TNiceDb db(context.GetDB());
        context.SS->PersistBackupDone(db, pathId);
    }

    static bool NeedToBill(const TPathId& pathId, TOperationContext& context) {
        if (context.SS->ColumnTables.contains(pathId)) {
            auto table = context.SS->ColumnTables.at(pathId);
            return table->BackupSettings.GetNeedToBill();
        }

        Y_ABORT_UNLESS(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);
        return table->BackupSettings.GetNeedToBill();
    }
};

ISubOperation::TPtr CreateBackup(TOperationId id, const TTxTransaction& tx) {
    return new TBackupRestoreOperationBase<TBackup, TEvDataShard::TEvCancelBackup>(
        TTxState::TxBackup, TPathElement::EPathState::EPathStateBackup, id, tx
    );
}

ISubOperation::TPtr CreateBackup(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return new TBackupRestoreOperationBase<TBackup, TEvDataShard::TEvCancelBackup>(
        TTxState::TxBackup, TPathElement::EPathState::EPathStateBackup, id, state
    );
}

}
}

#undef YDB_LOG_THIS_FILE_COMPONENT
