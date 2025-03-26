#include "datashard_failpoints.h"
#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "probes.h"

LWTRACE_USING(DATASHARD_PROVIDER)

namespace NKikimr {
namespace NDataShard {

class TFinalizeDataTxPlanUnit: public TExecutionUnit {
public:
    TFinalizeDataTxPlanUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::FinalizeDataTxPlan, false, dataShard, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        Y_UNUSED(ctx);

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());
        Y_ENSURE(tx->IsDataTx(), "unexpected non-data tx");

        if (auto& dataTx = tx->GetDataTx()) {
            // Restore transaction type flags
            if (dataTx->IsKqpDataTx() && !tx->IsKqpDataTransaction())
                tx->SetKqpDataTransactionFlag();
            Y_ENSURE(!dataTx->IsKqpScanTx(), "unexpected kqp scan tx");
        }

        tx->FinalizeDataTxPlan();

        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Y_UNUSED(op);
        Y_UNUSED(ctx);
    }
};

THolder<TExecutionUnit> CreateFinalizeDataTxPlanUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TFinalizeDataTxPlanUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
