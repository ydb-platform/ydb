#include "datashard_read_operation.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr::NDataShard {

class TReadUnit : public TExecutionUnit {
public:
    TReadUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::ExecuteRead, true, self, pipeline)
    {
    }

    ~TReadUnit() = default;

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        return !op->HasRuntimeConflicts();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        IReadOperation* readOperation = dynamic_cast<IReadOperation*>(op.Get());
        Y_VERIFY(readOperation);

        if (!readOperation->Execute(txc, ctx)) {
            return EExecutionStatus::Restart;
        }

        // TODO: check if we can send result right here to decrease latency

        // note that op has set locks itself, no ApplyLocks() required
        DataShard.SubscribeNewLocks(ctx);

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        IReadOperation* readOperation = dynamic_cast<IReadOperation*>(op.Get());
        Y_VERIFY(readOperation);

        readOperation->Complete(ctx);
    }
};

THolder<TExecutionUnit> CreateReadUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TReadUnit(self, pipeline));
}

} // NKikimr::NDataShard
