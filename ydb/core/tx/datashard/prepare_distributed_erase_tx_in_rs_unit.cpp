#include "datashard_active_transaction.h"
#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TPrepareDistributedEraseTxInRSUnit : public TExecutionUnit {
public:
    TPrepareDistributedEraseTxInRSUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::PrepareDistributedEraseTxInRS, false, self, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext&, const TActorContext&) override {
        Y_ENSURE(op->IsDistributedEraseTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        const auto& eraseTx = tx->GetDistributedEraseTx();
        if (!eraseTx->HasDependencies()) {
            return EExecutionStatus::Executed;
        }

        for (const auto& dependency : eraseTx->GetDependencies()) {
            op->InReadSets().emplace(std::make_pair(dependency.GetShardId(), DataShard.TabletID()), TVector<TRSData>());
        }

        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
    }
};

THolder<TExecutionUnit> CreatePrepareDistributedEraseTxInRSUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TPrepareDistributedEraseTxInRSUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
