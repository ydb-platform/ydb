#include "datashard_write_operation.h"
#include "datashard_pipeline.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"
#include "datashard_user_db.h"

#include <ydb/core/engine/mkql_engine_flat_host.h>

namespace NKikimr {
namespace NDataShard {

class TWriteUnit : public TExecutionUnit {
public:
    TWriteUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::ExecuteWrite, true, self, pipeline)
    {
    }

    ~TWriteUnit()
    {
    }

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        if (op->HasRuntimeConflicts() || op->HasWaitingForGlobalTxIdFlag()) {
            return false;
        }

        if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && WillRejectDataTx(op)) {
            return true;
        }

        if (DataShard.IsStopping()) {
            // Avoid doing any new work when datashard is stopping
            return false;
        }

        return !op->HasRuntimeConflicts();
    }

    void DoExecute(TDataShard* self, TWriteOperation* writeOp, TTransactionContext& txc, const TActorContext& ctx) {
        TValidatedWriteTx::TPtr& writeTx = writeOp->GetWriteTx();

        const ui64 tableId = writeTx->GetTableId().PathId.LocalPathId;
        const TTableId fullTableId(self->GetPathOwnerId(), tableId);
        const ui64 localTableId = self->GetLocalTableId(fullTableId);
        if (localTableId == 0) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, TStringBuilder() << "Unknown table id " << tableId);
            return;
        }
        const ui64 shadowTableId = self->GetShadowTableId(fullTableId);
        const TUserTable& TableInfo_ = *self->GetUserTables().at(tableId);
        Y_ABORT_UNLESS(TableInfo_.LocalTid == localTableId);
        Y_ABORT_UNLESS(TableInfo_.ShadowTid == shadowTableId);

        const NTable::TScheme& scheme = txc.DB.GetScheme();
        const NTable::TScheme::TTableInfo* tableInfo = scheme.GetTableInfo(localTableId);

        auto [readVersion, writeVersion] = self->GetReadWriteVersions(writeOp);
        writeTx->SetReadVersion(readVersion);
        writeTx->SetWriteVersion(writeVersion);

        TSmallVec<TRawTypeValue> key;
        TSmallVec<NTable::TUpdateOp> ops;

        const TSerializedCellMatrix& matrix = writeTx->GetMatrix();

        for (ui32 rowIdx = 0; rowIdx < matrix.GetRowCount(); ++rowIdx)
        {
            key.clear();
            key.reserve(TableInfo_.KeyColumnIds.size());
            for (ui16 keyColIdx = 0; keyColIdx < TableInfo_.KeyColumnIds.size(); ++keyColIdx) {
                const TCell& cell = matrix.GetCell(rowIdx, keyColIdx);
                ui32 keyCol = tableInfo->KeyColumns[keyColIdx];
                if (cell.IsNull()) {
                    key.emplace_back();
                } else {
                    NScheme::TTypeInfo vtypeInfo = scheme.GetColumnInfo(tableInfo, keyCol)->PType;
                    key.emplace_back(cell.Data(), cell.Size(), vtypeInfo);
                }
            }

            ops.clear();
            Y_ABORT_UNLESS(matrix.GetColCount() >= TableInfo_.KeyColumnIds.size());
            ops.reserve(matrix.GetColCount() - TableInfo_.KeyColumnIds.size());

            for (ui16 valueColIdx = TableInfo_.KeyColumnIds.size(); valueColIdx < matrix.GetColCount(); ++valueColIdx) {
                ui32 columnTag = writeTx->RecordOperation().GetColumnIds(valueColIdx);
                const TCell& cell = matrix.GetCell(rowIdx, valueColIdx);

                NScheme::TTypeInfo vtypeInfo = scheme.GetColumnInfo(tableInfo, columnTag)->PType;
                ops.emplace_back(columnTag, NTable::ECellOp::Set, cell.IsNull() ? TRawTypeValue() : TRawTypeValue(cell.Data(), cell.Size(), vtypeInfo));
            }

            writeTx->GetUserDb().UpdateRow(fullTableId, key, ops);
        }

        self->IncCounter(COUNTER_WRITE_ROWS, matrix.GetRowCount());
        self->IncCounter(COUNTER_WRITE_BYTES, matrix.GetBuffer().size());

        writeOp->SetWriteResult(NEvents::TDataEvents::TEvWriteResult::BuildCommited(self->TabletID(), writeOp->GetTxId()));

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Executed write operation for " << *writeOp << " at " << self->TabletID());
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Executing write operation for " << *op << " at " << DataShard.TabletID());

        if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && CheckRejectDataTx(op, ctx)) {
            return EExecutionStatus::Executed;
        }

        if (op->HasWaitingForGlobalTxIdFlag()) {
            return EExecutionStatus::Continue;
        }

        if (op->IsImmediate()) {
            // Every time we execute immediate transaction we may choose a new mvcc version
            op->MvccReadWriteVersion.reset();
        }
        else {
            //TODO: Prepared
            writeOp->SetWriteResult(NEvents::TDataEvents::TEvWriteResult::BuildPrepared(DataShard.TabletID(), op->GetTxId(), {0, 0, {}}));
            return EExecutionStatus::DelayCompleteNoMoreRestarts;
        }

        TDataShardLocksDb locksDb(DataShard, txc);
        TSetupSysLocks guardLocks(op, DataShard, &locksDb);

        const TValidatedWriteTx::TPtr& writeTx = writeOp->GetWriteTx();

        if (op->IsImmediate() && !writeOp->ReValidateKeys()) {
            // Immediate transactions may be reordered with schema changes and become invalid
            Y_ABORT_UNLESS(!writeTx->Ready());
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, writeTx->GetErrStr());
            return EExecutionStatus::Executed;
        }

        if (writeTx->CheckCancelled()) {
            writeOp->ReleaseTxData(txc, ctx);
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED, "Tx was cancelled");
            DataShard.IncCounter(COUNTER_WRITE_CANCELLED);
            return EExecutionStatus::Executed;
        }

        try {
            DoExecute(&DataShard, writeOp, txc, ctx);
        } catch (const TNeedGlobalTxId&) {
            Y_VERIFY_S(op->GetGlobalTxId() == 0,
                "Unexpected TNeedGlobalTxId exception for write operation with TxId# " << op->GetGlobalTxId());
            Y_VERIFY_S(op->IsImmediate(),
                "Unexpected TNeedGlobalTxId exception for a non-immediate write operation with TxId# " << op->GetTxId());

            ctx.Send(MakeTxProxyID(),
                new TEvTxUserProxy::TEvAllocateTxId(),
                0, op->GetTxId());
            op->SetWaitingForGlobalTxIdFlag();

            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }
            return EExecutionStatus::Continue;
        }

        if (Pipeline.AddLockDependencies(op, guardLocks)) {
            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }
            return EExecutionStatus::Continue;
        }

        op->ChangeRecords() = std::move(writeOp->GetWriteTx()->GetCollectedChanges());

        DataShard.SysLocksTable().ApplyLocks();
        DataShard.SubscribeNewLocks(ctx);
        Pipeline.AddCommittingOp(op);

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Pipeline.RemoveCommittingOp(op);
        DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));
        DataShard.EmitHeartbeats(ctx);

        TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

        const auto& status = writeOp->GetWriteResult()->Record.status();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Completed write operation for " << *op << " at " << DataShard.TabletID() << ", status " << status);

        DataShard.IncCounter(writeOp->GetWriteResult()->Record.status() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED ?
            COUNTER_WRITE_SUCCESS : COUNTER_WRITE_ERROR);

        ctx.Send(writeOp->GetEv()->Sender, writeOp->ReleaseWriteResult().release(), 0, writeOp->GetEv()->Cookie);
    }

};  // TWriteUnit

THolder<TExecutionUnit> CreateWriteUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TWriteUnit(self, pipeline));
}

} // NDataShard
} // NKikimr
