#include "schemeshard__operation_backup_restore_common.h"
#include "schemeshard_billing_helpers.h"

namespace NKikimr {
namespace NSchemeShard {

struct TBackup {
    static constexpr TStringBuf Name() {
        return "TBackup";
    }

    static bool HasTask(const TTxTransaction& tx) {
        return tx.HasBackup();
    }

    static TString GetTableName(const TTxTransaction& tx) {
        return tx.GetBackup().GetTableName();
    }

    static void ProposeTx(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        auto seqNo = context.SS->StartRound(txState);
        const auto& processingParams = context.SS->SelectProcessingParams(txState.TargetPathId);

        Y_VERIFY(context.SS->Tables.contains(txState.TargetPathId));
        TTableInfo::TPtr table = context.SS->Tables.at(txState.TargetPathId);
        NKikimrSchemeOp::TBackupTask backup = table->BackupSettings;

        for (ui32 i = 0; i < txState.Shards.size(); ++i) {
            auto idx = txState.Shards[i].Idx;
            auto datashardId = context.SS->ShardInfos[idx].TabletID;

            LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Propose backup"
                            << " to datashard " << datashardId
                            << " txid " <<  opId
                            << " at schemeshard " << context.SS->SelfTabletId());

            TString txBody = context.SS->FillBackupTxBody(txState.TargetPathId, backup, i, seqNo);
            THolder<TEvDataShard::TEvProposeTransaction> event =
                THolder(new TEvDataShard::TEvProposeTransaction(NKikimrTxDataShard::TX_KIND_SCHEME,
                                                        context.SS->TabletID(),
                                                        context.Ctx.SelfID,
                                                        ui64(opId.GetTxId()),
                                                        txBody,
                                                        processingParams));

            context.OnComplete.BindMsgToPipe(opId, datashardId, idx, event.Release());

            backup.ClearTable();
        }
    }

    static ui64 RequestUnits(ui64 bytes, ui64 rows) {
        Y_UNUSED(rows);
        return TRUCalculator::ReadTable(bytes);
    }

    static void FinishStats(const TOperationId& opId, TTxState& txState, TOperationContext& context) {
        if (txState.TxType != TTxState::TxBackup) {
            return;
        }

        Y_VERIFY(TAppData::TimeProvider.Get() != nullptr);
        const ui64 ts = TAppData::TimeProvider->Now().Seconds();

        Y_VERIFY(context.SS->Tables.contains(txState.TargetPathId));
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
        Y_VERIFY(context.SS->Tables.contains(pathId));
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
        Y_VERIFY(context.SS->Tables.contains(pathId));
        auto table = context.SS->Tables.at(pathId);
        return table->BackupSettings.GetNeedToBill();
    }
};

ISubOperationBase::TPtr CreateBackup(TOperationId id, const TTxTransaction& tx) {
    return new TBackupRestoreOperationBase<TBackup, TEvDataShard::TEvCancelBackup>(
        TTxState::TxBackup, TPathElement::EPathState::EPathStateBackup, id, tx
    );
}

ISubOperationBase::TPtr CreateBackup(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TBackupRestoreOperationBase<TBackup, TEvDataShard::TEvCancelBackup>(
        TTxState::TxBackup, TPathElement::EPathState::EPathStateBackup, id, state
    );
}

}
}
