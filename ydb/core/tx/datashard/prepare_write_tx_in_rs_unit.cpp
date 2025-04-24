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
    const TActorContext &)
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
                Y_ENSURE(false, "Failed to restore writeOp data: " << writeTx->GetErrStr());
        }
    }

    if (writeTx->CheckCancelled()) {
        writeOp->ReleaseTxData(txc);
        writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED, "Tx was cancelled");
        DataShard.IncCounter(COUNTER_WRITE_CANCELLED);
        return EExecutionStatus::Executed;
    }

    KqpPrepareInReadsets(op->InReadSets(), writeTx->GetKqpLocks() ? writeTx->GetKqpLocks().value() : NKikimrDataEvents::TKqpLocks{}, nullptr, DataShard.TabletID());

    return EExecutionStatus::Executed;
}

void TPrepareWriteTxInRSUnit::Complete(TOperation::TPtr, const TActorContext &) {}

THolder<TExecutionUnit> CreatePrepareWriteTxInRSUnit(TDataShard &dataShard, TPipeline &pipeline) {
    return THolder(new TPrepareWriteTxInRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
