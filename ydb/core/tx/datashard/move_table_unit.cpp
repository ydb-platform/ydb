#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TMoveTableUnit : public TExecutionUnit {
public:
    TMoveTableUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::DropIndexNotice, false, dataShard, pipeline)
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
        TVector<NMiniKQL::IChangeCollector::TChange> changeRecords;
        if (!DataShard.LoadChangeRecords(db, changeRecords)) {
            return EExecutionStatus::Restart;
        }

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "TMoveTableUnit Execute"
            << ": schemeTx# " << schemeTx.DebugString()
            << ": changeRecords size# " << changeRecords.size()
            << ", at tablet# " << DataShard.TabletID());

        const auto& params = schemeTx.GetMoveTable();

        DataShard.KillChangeSender(ctx);

        DataShard.MoveUserTable(op, params, ctx, txc);

        DataShard.CreateChangeSender(ctx);
        MoveChangeRecords(db, params, changeRecords);
        DataShard.EnqueueChangeRecords(std::move(changeRecords));
        DataShard.MaybeActivateChangeSender(ctx);

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        return EExecutionStatus::ExecutedNoMoreRestarts;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
        // nothing
    }
};

THolder<TExecutionUnit> CreateMoveTableUnit(
    TDataShard& dataShard,
    TPipeline& pipeline)
{
    return THolder(new TMoveTableUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
