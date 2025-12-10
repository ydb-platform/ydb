#include "schemeshard__operation_backup_restore_common.h"
#include "schemeshard_billing_helpers.h"

namespace NKikimr {
namespace NSchemeShard {

struct TBackup {
    static constexpr TStringBuf Name() {
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
        for (ui32 i = 0; i < txState.Shards.size(); ++i) {
            auto idx = txState.Shards[i].Idx;
            auto columnShardId = context.SS->ShardInfos[idx].TabletID;

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Propose backup"
                            << " to columnshard " << columnShardId
                            << " txid " <<  opId
                            << " at schemeshard " << context.SS->SelfTabletId());

            NKikimrTxColumnShard::TBackupTxBody txBodyBackup;
            *txBodyBackup.MutableBackupTask() = backup;
            txBodyBackup.MutableBackupTask()->SetTableId(pathId.LocalPathId);
            txBodyBackup.MutableBackupTask()->SetShardNum(i);
            auto event = context.SS->MakeColumnShardProposal(pathId, opId, seqNo, txBodyBackup.SerializeAsString(), context.Ctx, NKikimrTxColumnShard::TX_KIND_BACKUP);
            context.OnComplete.BindMsgToPipe(opId, columnShardId, idx, event.Release());

            backup.ClearTable();
            backup.ClearChangefeedUnderlyingTopics();
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
        for (ui32 i = 0; i < txState.Shards.size(); ++i) {
            auto idx = txState.Shards[i].Idx;
            auto datashardId = context.SS->ShardInfos[idx].TabletID;

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Propose backup"
                            << " to datashard " << datashardId
                            << " txid " <<  opId
                            << " at schemeshard " << context.SS->SelfTabletId());

            const auto txBody = context.SS->FillBackupTxBody(pathId, backup, i, seqNo);
            auto event = context.SS->MakeDataShardProposal(pathId, opId, txBody, context.Ctx);
            context.OnComplete.BindMsgToPipe(opId, datashardId, idx, event.Release());

            backup.ClearTable();
            backup.ClearChangefeedUnderlyingTopics();
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
        TTableInfo::TPtr table = context.SS->Tables[txState.TargetPathId];

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
