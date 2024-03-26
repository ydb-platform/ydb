#include "datashard_impl.h"
#include "datashard_kqp.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TPrepareKqpDataTxInRSUnit : public TExecutionUnit {
public:
    TPrepareKqpDataTxInRSUnit(TDataShard &dataShard, TPipeline &pipeline);
    ~TPrepareKqpDataTxInRSUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext &txc, const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op, const TActorContext &ctx) override;
};

TPrepareKqpDataTxInRSUnit::TPrepareKqpDataTxInRSUnit(TDataShard &dataShard,
    TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::PrepareKqpDataTxInRS, true, dataShard, pipeline) {}

TPrepareKqpDataTxInRSUnit::~TPrepareKqpDataTxInRSUnit() {}

bool TPrepareKqpDataTxInRSUnit::IsReadyToExecute(TOperation::TPtr) const {
    return true;
}

EExecutionStatus TPrepareKqpDataTxInRSUnit::Execute(TOperation::TPtr op, TTransactionContext &txc,
    const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    if (tx->IsTxDataReleased()) {
        switch (Pipeline.RestoreDataTx(tx, txc, ctx)) {
            case ERestoreDataStatus::Ok:
                break;
            case ERestoreDataStatus::Restart:
                return EExecutionStatus::Restart;
            case ERestoreDataStatus::Error:
                Y_ABORT("Failed to restore tx data: %s", tx->GetDataTx()->GetErrors().c_str());
        }
    }

    if (tx->GetDataTx()->CheckCancelled(DataShard.TabletID())) {
        tx->ReleaseTxData(txc, ctx);
        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::CANCELLED)
            ->AddError(NKikimrTxDataShard::TError::EXECUTION_CANCELLED, "Tx was cancelled");

        DataShard.IncCounter(op->IsImmediate() ? COUNTER_IMMEDIATE_TX_CANCELLED : COUNTER_PLANNED_TX_CANCELLED);

        return EExecutionStatus::Executed;
    }

    try {
        KqpPrepareInReadsets(op->InReadSets(), tx->GetDataTx()->GetKqpLocks(),
            &tx->GetDataTx()->GetKqpTasksRunner(), DataShard.TabletID());
    } catch (const yexception& e) {
        LOG_CRIT_S(ctx, NKikimrServices::TX_DATASHARD, "Exception while preparing in-readsets for KQP transaction "
            << *op << " at " << DataShard.TabletID() << ": " << CurrentExceptionMessage());
        Y_FAIL_S("Unexpected exception in KQP in-readsets prepare: " << CurrentExceptionMessage());
    }

    return EExecutionStatus::Executed;
}

void TPrepareKqpDataTxInRSUnit::Complete(TOperation::TPtr, const TActorContext &) {}

THolder<TExecutionUnit> CreatePrepareKqpDataTxInRSUnit(TDataShard &dataShard, TPipeline &pipeline) {
    return THolder(new TPrepareKqpDataTxInRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
