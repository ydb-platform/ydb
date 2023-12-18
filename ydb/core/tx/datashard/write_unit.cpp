#include "datashard_write_operation.h"
#include "datashard_pipeline.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"
#include "datashard_user_db.h"

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

    void DoExecute(TDataShard* self, TWriteOperation* tx, TTransactionContext& txc, const TActorContext& ctx) {
        const TValidatedWriteTx::TPtr& writeTx = tx->WriteTx();

        const ui64 tableId = writeTx->TableId().PathId.LocalPathId;
        const TTableId fullTableId(self->GetPathOwnerId(), tableId);
        const ui64 localTableId = self->GetLocalTableId(fullTableId);
        if (localTableId == 0) {
            tx->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, TStringBuilder() << "Unknown table id " << tableId);
            return;
        }
        const ui64 shadowTableId = self->GetShadowTableId(fullTableId);

        const TUserTable& TableInfo_ = *self->GetUserTables().at(tableId);
        Y_ABORT_UNLESS(TableInfo_.LocalTid == localTableId);
        Y_ABORT_UNLESS(TableInfo_.ShadowTid == shadowTableId);

        const ui32 writeTableId = localTableId;
        auto [readVersion, writeVersion] = self->GetReadWriteVersions(tx);

        TDataShardUserDb userDb(*self, txc.DB, readVersion);
        TDataShardChangeGroupProvider groupProvider(*self, txc.DB);

        TVector<TRawTypeValue> key;
        TVector<NTable::TUpdateOp> value;

        const TSerializedCellMatrix& matrix = writeTx->Matrix();
        TConstArrayRef<TCell> cells = matrix.GetCells();
        const ui32 rowCount = matrix.GetRowCount();
        const ui16 colCount = matrix.GetColCount();

        for (ui32 rowIdx = 0; rowIdx < rowCount; ++rowIdx)
        {
            key.clear();
            ui64 keyBytes = 0;
            for (ui16 keyColIdx = 0; keyColIdx < TableInfo_.KeyColumnIds.size(); ++keyColIdx) {
                const auto& cellType = TableInfo_.KeyColumnTypes[keyColIdx];
                const TCell& cell = cells[rowIdx * colCount + keyColIdx];
                if (cellType.GetTypeId() == NScheme::NTypeIds::Uint8 && !cell.IsNull() && cell.AsValue<ui8>() > 127) {
                    tx->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, "Keys with Uint8 column values >127 are currently prohibited");
                    return;
                }

                keyBytes += cell.Size();
                key.emplace_back(TRawTypeValue(cell.AsRef(), cellType));
            }

            if (keyBytes > NLimits::MaxWriteKeySize) {
                tx->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, TStringBuilder() << "Row key size of " << keyBytes << " bytes is larger than the allowed threshold " << NLimits::MaxWriteKeySize);
                return;
            }

            value.clear();
            for (ui16 valueColIdx = TableInfo_.KeyColumnIds.size(); valueColIdx < colCount; ++valueColIdx) {
                ui32 columnTag = writeTx->RecordOperation().GetColumnIds(valueColIdx);
                const TCell& cell = cells[rowIdx * colCount + valueColIdx];
                if (cell.Size() > NLimits::MaxWriteValueSize) {
                    tx->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST, TStringBuilder() << "Row cell size of " << cell.Size() << " bytes is larger than the allowed threshold " << NLimits::MaxWriteValueSize);
                    return;
                }

                auto* col = TableInfo_.Columns.FindPtr(valueColIdx + 1);
                Y_ABORT_UNLESS(col);

                value.emplace_back(NTable::TUpdateOp(columnTag, NTable::ECellOp::Set, TRawTypeValue(cell.AsRef(), col->Type)));
            }

            txc.DB.Update(writeTableId, NTable::ERowOp::Upsert, key, value, writeVersion);
            self->GetConflictsCache().GetTableCache(writeTableId).RemoveUncommittedWrites(writeTx->KeyCells(), txc.DB);
        }
        //TODO: Counters
        // self->IncCounter(COUNTER_UPLOAD_ROWS, rowCount);
        // self->IncCounter(COUNTER_UPLOAD_ROWS_BYTES, matrix.GetBuffer().size());

        TableInfo_.Stats.UpdateTime = TAppData::TimeProvider->Now();

        tx->SetWriteResult(NEvents::TDataEvents::TEvWriteResult::BuildCommited(writeTx->TabletId(), tx->GetTxId()));

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "tx " << tx->GetTxId() << " at " << self->TabletID() << " write operation is executed");
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        TWriteOperation* tx = dynamic_cast<TWriteOperation*>(op.Get());
        Y_ABORT_UNLESS(tx != nullptr);

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "tx " << op->GetTxId() << " at " << tx->WriteTx()->TabletId() << " is executing write operation");

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
            tx->SetWriteResult(NEvents::TDataEvents::TEvWriteResult::BuildPrepared(tx->WriteTx()->TabletId(), op->GetTxId(), {0, 0, {}}));
            return EExecutionStatus::DelayCompleteNoMoreRestarts;
        }

        TDataShardLocksDb locksDb(DataShard, txc);
        TSetupSysLocks guardLocks(op, DataShard, &locksDb);

        try {
            DoExecute(&DataShard, tx, txc, ctx);
        } catch (const TNeedGlobalTxId&) {
            Y_VERIFY_S(op->GetGlobalTxId() == 0,
                "Unexpected TNeedGlobalTxId exception for direct operation with TxId# " << op->GetGlobalTxId());
            Y_VERIFY_S(op->IsImmediate(),
                "Unexpected TNeedGlobalTxId exception for a non-immediate operation with TxId# " << op->GetTxId());

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

        op->ChangeRecords() = std::move(tx->WriteTx()->GetCollectedChanges());

        DataShard.SysLocksTable().ApplyLocks();
        DataShard.SubscribeNewLocks(ctx);
        Pipeline.AddCommittingOp(op);

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Pipeline.RemoveCommittingOp(op);
        DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));
        DataShard.EmitHeartbeats(ctx);

        TWriteOperation* tx = dynamic_cast<TWriteOperation*>(op.Get());
        Y_ABORT_UNLESS(tx != nullptr);

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "tx " << op->GetTxId() << " at " << tx->WriteTx()->TabletId() << " complete write operation");

        //TODO: Counters
        // if (WriteResult->Record.status() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED || WriteResult->Record.status() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED) {
        //     self->IncCounter(COUNTER_WRITE_SUCCESS);
        // } else {
        //     self->IncCounter(COUNTER_WRITE_ERROR);
        // }

        ctx.Send(tx->GetEv()->Sender, tx->WriteResult().release(), 0, tx->GetEv()->Cookie);
    }

};  // TWriteUnit

THolder<TExecutionUnit> CreateWriteUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TWriteUnit(self, pipeline));
}

} // NDataShard
} // NKikimr
