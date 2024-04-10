#include "datashard_impl.h"
#include "datashard_kqp.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "datashard_write_operation.h"

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TPrepareWriteTxInRSUnit : public TExecutionUnit {
public:
    TPrepareWriteTxInRSUnit(TDataShard &dataShard, TPipeline &pipeline);
    ~TPrepareWriteTxInRSUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext &txc, const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op, const TActorContext &ctx) override;
};

TPrepareWriteTxInRSUnit::TPrepareWriteTxInRSUnit(TDataShard &dataShard,
    TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::PrepareWriteTxInRS, true, dataShard, pipeline) {}

TPrepareWriteTxInRSUnit::~TPrepareWriteTxInRSUnit() {}

bool TPrepareWriteTxInRSUnit::IsReadyToExecute(TOperation::TPtr) const {
    return true;
}

EExecutionStatus TPrepareWriteTxInRSUnit::Execute(TOperation::TPtr op, TTransactionContext &txc,
    const TActorContext &ctx)
{
    TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

    const TValidatedWriteTx::TPtr& writeTx = writeOp->GetWriteTx();

    if (writeOp->IsTxDataReleased()) {
        switch (Pipeline.RestoreWriteTx(writeOp, txc)) {
            case ERestoreDataStatus::Ok:
                break;
            case ERestoreDataStatus::Restart:
                return EExecutionStatus::Restart;
            case ERestoreDataStatus::Error:
                Y_ABORT("Failed to restore writeOp data: %s", writeTx->GetErrStr().c_str());
        }
    }

    if (writeTx->CheckCancelled()) {
        writeOp->ReleaseTxData(txc);
        writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED, "Tx was cancelled");
        DataShard.IncCounter(COUNTER_WRITE_CANCELLED);
        return EExecutionStatus::Executed;
    }

    try {
        KqpPrepareInReadsets(op->InReadSets(), writeTx->GetKqpLocks() ? writeTx->GetKqpLocks().value() : NKikimrDataEvents::TKqpLocks{}, nullptr, DataShard.TabletID());
    } catch (const yexception& e) {
        LOG_CRIT_S(ctx, NKikimrServices::TX_DATASHARD, "Exception while preparing in-readsets for KQP transaction "
            << *op << " at " << DataShard.TabletID() << ": " << CurrentExceptionMessage());
        Y_FAIL_S("Unexpected exception in KQP in-readsets prepare: " << CurrentExceptionMessage());
    }

    return EExecutionStatus::Executed;
}

void TPrepareWriteTxInRSUnit::Complete(TOperation::TPtr, const TActorContext &) {}

THolder<TExecutionUnit> CreatePrepareWriteTxInRSUnit(TDataShard &dataShard, TPipeline &pipeline) {
    return THolder(new TPrepareWriteTxInRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
