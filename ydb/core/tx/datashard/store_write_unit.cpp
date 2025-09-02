#include "const.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

#include "datashard_write_operation.h"

namespace NKikimr {
namespace NDataShard {

class TStoreWriteUnit : public TExecutionUnit {
public:
    TStoreWriteUnit(TDataShard &dataShard,
                     TPipeline &pipeline);
    ~TStoreWriteUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TStoreWriteUnit::TStoreWriteUnit(TDataShard &dataShard,
                                   TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::StoreWrite, false, dataShard, pipeline)
{
}

TStoreWriteUnit::~TStoreWriteUnit()
{
}

bool TStoreWriteUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TStoreWriteUnit::Execute(TOperation::TPtr op,
                                           TTransactionContext &txc,
                                           const TActorContext &ctx)
{
    Y_ENSURE(!op->IsAborted() && !op->IsInterrupted());

    TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);
    auto writeTx = writeOp->GetWriteTx();
    Y_ENSURE(writeTx);

    bool cached = Pipeline.SaveForPropose(writeTx);
    if (cached) {
        Pipeline.RegisterDistributedWrites(op, txc.DB);
    }

    Pipeline.ProposeTx(op, writeOp->GetTxBody(), txc, ctx);

    if (!op->HasVolatilePrepareFlag()) {
        writeOp->ClearTxBody();
    }

    writeOp->ClearWriteTx();

    return EExecutionStatus::DelayCompleteNoMoreRestarts;
}

void TStoreWriteUnit::Complete(TOperation::TPtr op,
                                const TActorContext &ctx)
{
    Pipeline.ProposeComplete(op, ctx);
}

THolder<TExecutionUnit> CreateStoreWriteUnit(TDataShard &dataShard,
                                              TPipeline &pipeline)
{
    return MakeHolder<TStoreWriteUnit>(dataShard, pipeline);
}

} // namespace NDataShard
} // namespace NKikimr
