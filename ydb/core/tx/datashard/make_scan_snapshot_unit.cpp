#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TMakeScanSnapshotUnit : public TExecutionUnit {
public:
    TMakeScanSnapshotUnit(TDataShard &dataShard,
                          TPipeline &pipeline);
    ~TMakeScanSnapshotUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TMakeScanSnapshotUnit::TMakeScanSnapshotUnit(TDataShard &dataShard,
                                             TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::MakeScanSnapshot, false, dataShard, pipeline)
{
}

TMakeScanSnapshotUnit::~TMakeScanSnapshotUnit()
{
}

bool TMakeScanSnapshotUnit::IsReadyToExecute(TOperation::TPtr op) const
{
    // Pass aborted operations
    if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && WillRejectDataTx(op)) {
        return true;
    }

    return op->HasUsingSnapshotFlag() || !op->HasRuntimeConflicts();
}

EExecutionStatus TMakeScanSnapshotUnit::Execute(TOperation::TPtr op,
                                                TTransactionContext &,
                                                const TActorContext &ctx)
{
    // Pass aborted operations
    if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && CheckRejectDataTx(op, ctx)) {
        return EExecutionStatus::Executed;
    }

    // Already set for ReadTable from persistent snapshots
    return EExecutionStatus::Executed;
}

void TMakeScanSnapshotUnit::Complete(TOperation::TPtr,
                                     const TActorContext &)
{
}

THolder<TExecutionUnit> CreateMakeScanSnapshotUnit(TDataShard &dataShard,
                                                   TPipeline &pipeline)
{
    return THolder(new TMakeScanSnapshotUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
