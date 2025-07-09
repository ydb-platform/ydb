#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TDropPersistentSnapshotUnit : public TExecutionUnit {
public:
    TDropPersistentSnapshotUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::DropPersistentSnapshot, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext&) override {
        Y_ENSURE(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasDropPersistentSnapshot()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetDropPersistentSnapshot();

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

THolder<TExecutionUnit> CreateDropPersistentSnapshotUnit(
        TDataShard& dataShard,
        TPipeline& pipeline)
{
    return THolder(new TDropPersistentSnapshotUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
