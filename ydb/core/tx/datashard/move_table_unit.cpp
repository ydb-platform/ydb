#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TMoveTableUnit : public TExecutionUnit {
    TVector<NMiniKQL::IChangeCollector::TChange> ChangeRecords;

public:
    TMoveTableUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::MoveTable, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    void MoveChangeRecords(NIceDb::TNiceDb& db, const NKikimrTxDataShard::TMoveTable& move, TVector<NMiniKQL::IChangeCollector::TChange>& changeRecords) {
        const THashMap<TPathId, TPathId> remap = DataShard.GetRemapIndexes(move);

        for (auto& record: changeRecords) {
            if (remap.contains(record.PathId())) { // here could be the records for already deleted indexes, so skip them
                record.SetPathId(remap.at(record.PathId()));
                DataShard.MoveChangeRecord(db, record.Order(), record.PathId());
            }
        }
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_VERIFY(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        if (tx->GetSchemeTxType() != TSchemaOperation::ETypeMoveTable) {
            return EExecutionStatus::Executed;
        }

        const auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasMoveTable()) {
            return EExecutionStatus::Executed;
        }

        NIceDb::TNiceDb db(txc.DB);

        ChangeRecords.clear();
        if (!DataShard.LoadChangeRecords(db, ChangeRecords)) {
            return EExecutionStatus::Restart;
        }

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "TMoveTableUnit Execute"
            << ": schemeTx# " << schemeTx.DebugString()
            << ": changeRecords size# " << ChangeRecords.size()
            << ", at tablet# " << DataShard.TabletID());

        DataShard.SuspendChangeSender(ctx);

        const auto& params = schemeTx.GetMoveTable();
        DataShard.MoveUserTable(op, params, ctx, txc);
        MoveChangeRecords(db, params, ChangeRecords);

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr, const TActorContext& ctx) override {
        DataShard.CreateChangeSender(ctx);
        DataShard.MaybeActivateChangeSender(ctx);
        DataShard.EnqueueChangeRecords(std::move(ChangeRecords));
    }
};

THolder<TExecutionUnit> CreateMoveTableUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TMoveTableUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
