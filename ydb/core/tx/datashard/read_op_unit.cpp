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
        return !op->HasRuntimeConflicts() && op->GetDependencies().empty();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        IReadOperation* readOperation = dynamic_cast<IReadOperation*>(op.Get());
        Y_ABORT_UNLESS(readOperation);

        auto status = readOperation->Execute(txc, ctx);
        if (status == EExecutionStatus::Restart || status == EExecutionStatus::Continue)
            return status;

        // note that op has set locks itself, no ApplyLocks() required
        DataShard.SubscribeNewLocks(ctx);

        return status;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        IReadOperation* readOperation = dynamic_cast<IReadOperation*>(op.Get());
        Y_ABORT_UNLESS(readOperation);

        readOperation->Complete(ctx);
    }
};

THolder<TExecutionUnit> CreateReadUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TReadUnit(self, pipeline));
}

} // NKikimr::NDataShard
