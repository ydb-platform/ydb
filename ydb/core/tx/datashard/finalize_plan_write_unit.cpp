#include "datashard_impl.h"
#include "datashard_pipeline.h"

#include "ydb/core/tx/datashard/datashard_write_operation.h"

namespace NKikimr {
namespace NDataShard {

class TFinalizeWriteTxPlanUnit: public TExecutionUnit {
public:
    TFinalizeWriteTxPlanUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::FinalizeWriteTxPlan, false, dataShard, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(txc);
        Y_UNUSED(ctx);

        TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

        writeOp->FinalizeWriteTxPlan();

        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Y_UNUSED(op);
        Y_UNUSED(ctx);
    }
};

THolder<TExecutionUnit> CreateFinalizeWriteTxPlanUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TFinalizeWriteTxPlanUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
