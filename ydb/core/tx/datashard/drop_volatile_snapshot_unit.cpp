#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TDropVolatileSnapshotUnit : public TExecutionUnit {
public:
    TDropVolatileSnapshotUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::DropVolatileSnapshot, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext&) override {
        Y_ABORT_UNLESS(op->IsSnapshotTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        const auto& record = tx->GetSnapshotTx();
        if (!record.HasDropVolatileSnapshot()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = record.GetDropVolatileSnapshot();

        const ui64 ownerId = params.GetOwnerId();
        const ui64 pathId = params.GetPathId();

        const TSnapshotKey key(ownerId, pathId, params.GetStep(), params.GetTxId());

        bool removed = DataShard.GetSnapshotManager().RemoveSnapshot(txc.DB, key);

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        if (removed) {
            return EExecutionStatus::ExecutedNoMoreRestarts;
        } else {
            return EExecutionStatus::Executed;
        }
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
        // nothing
    }
};

THolder<TExecutionUnit> CreateDropVolatileSnapshotUnit(
        TDataShard& dataShard,
        TPipeline& pipeline)
{
    return THolder(new TDropVolatileSnapshotUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
