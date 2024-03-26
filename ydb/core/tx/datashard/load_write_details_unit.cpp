#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "datashard_write_operation.h"

namespace NKikimr {
namespace NDataShard {

class TLoadWriteDetailsUnit : public TExecutionUnit {
public:
    TLoadWriteDetailsUnit(TDataShard &dataShard,
                       TPipeline &pipeline);
    ~TLoadWriteDetailsUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TLoadWriteDetailsUnit::TLoadWriteDetailsUnit(TDataShard &dataShard,
                                       TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::LoadTxDetails, true, dataShard, pipeline)
{
}

TLoadWriteDetailsUnit::~TLoadWriteDetailsUnit()
{
}

bool TLoadWriteDetailsUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TLoadWriteDetailsUnit::Execute(TOperation::TPtr op,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

    if (!Pipeline.LoadWriteDetails(txc, ctx, writeOp))
        return EExecutionStatus::Restart;

    return EExecutionStatus::Executed;
}

void TLoadWriteDetailsUnit::Complete(TOperation::TPtr,
                                  const TActorContext &)
{
}

THolder<TExecutionUnit> CreateLoadWriteDetailsUnit(TDataShard &dataShard,
                                                TPipeline &pipeline)
{
    return MakeHolder<TLoadWriteDetailsUnit>(dataShard, pipeline);
}

} // namespace NDataShard
} // namespace NKikimr
