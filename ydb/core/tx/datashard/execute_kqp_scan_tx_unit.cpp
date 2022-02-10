#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

using namespace NKqp;
using namespace NMiniKQL;

class TExecuteKqpScanTxUnit : public TExecutionUnit {
public:
    TExecuteKqpScanTxUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::ExecuteKqpScanTx, false, dataShard, pipeline) {
    }

    ~TExecuteKqpScanTxUnit() override {
    }

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && WillRejectDataTx(op)) {
            return true;
        }

        if (DataShard.IsStopping()) {
            // Avoid doing any new work when datashard is stopping
            return false;
        }

        return !op->HasRuntimeConflicts();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext&, const TActorContext& ctx) override {
        if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && CheckRejectDataTx(op, ctx)) {
            return EExecutionStatus::Executed;
        }

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)
            ->AddError(NKikimrTxDataShard::TError::BAD_TX_KIND, "Unexpected KqpScanTx");
        op->Abort();

        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "Unexpected KqpScanTx");

        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
    }
};

THolder<TExecutionUnit> CreateExecuteKqpScanTxUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return MakeHolder<TExecuteKqpScanTxUnit>(dataShard, pipeline);
}

} // namespace NDataShard
} // namespace NKikimr
