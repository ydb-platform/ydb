#include "datashard_read_operation.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr::NDataShard {

class TCheckReadUnit : public TExecutionUnit {
public:
    TCheckReadUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CheckRead, true, self, pipeline)
    {
    }

    ~TCheckReadUnit() = default;

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        return !op->HasRuntimeConflicts();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        IReadOperation* readOperation = dynamic_cast<IReadOperation*>(op.Get());
        Y_ENSURE(readOperation != nullptr);

        readOperation->CheckRequestAndInit(txc, ctx);
        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
        // CheckRequestAndInit either already failed request and replied to user
        // or prepared operation for further execution
    }
};

std::unique_ptr<TExecutionUnit> CreateCheckReadUnit(TDataShard& self, TPipeline& pipeline) {
    return std::unique_ptr<TCheckReadUnit>(new TCheckReadUnit(self, pipeline));
}

} // NKikimr::NDataShard
