#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "datashard_write_operation.h"

#include <ydb/core/base/appdata.h>

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

    if (writeOp->FindUnavailablePreparedReadSnapshot(DataShard, txc.DB)) {
        // Skip generating a commit vote and waiting for other participants.
        // ExecuteWrite persists the abort vote in the same transaction that
        // completes this operation, so restart cannot replay it as a write.
        op->RewriteExecutionPlan(TVector<EExecutionUnitKind>{
            EExecutionUnitKind::BuildAndWaitDependencies,
            EExecutionUnitKind::ExecuteWrite,
            EExecutionUnitKind::CompleteWrite,
            EExecutionUnitKind::CompletedOperations,
        });
    } else if (!op->HasVolatilePrepareFlag()) {
        TVector<EExecutionUnitKind> plan{EExecutionUnitKind::BuildAndWaitDependencies};

        const bool voteAfterWrite = writeOp->GetReadSnapshot() &&
            writeOp->GetWriteTx()->GetLockMode() == TDataShardUserDb::ELockMode::OptimisticSnapshotIsolation &&
            writeOp->CanAbortPreparedWrite();
        if (!writeOp->IsOutRSStored() &&
            (voteAfterWrite || AppData()->FeatureFlags.GetEnableDataShardWriteAlwaysVolatile()))
        {
            // Snapshot isolation can detect row conflicts only while executing
            // the writes. Keep the changes tentative while waiting for peers
            // and send our vote after local execution establishes the decision.
            plan.push_back(EExecutionUnitKind::PrepareWriteTxInRS);
            plan.push_back(EExecutionUnitKind::LoadInRS);
        } else {
            plan.push_back(EExecutionUnitKind::BuildWriteOutRS);
            plan.push_back(EExecutionUnitKind::StoreAndSendWriteOutRS);
            plan.push_back(EExecutionUnitKind::PrepareWriteTxInRS);
            plan.push_back(EExecutionUnitKind::LoadAndWaitInRS);
        }

        plan.push_back(EExecutionUnitKind::BlockFailPoint);
        plan.push_back(EExecutionUnitKind::ExecuteWrite);
        plan.push_back(EExecutionUnitKind::CompleteWrite);
        plan.push_back(EExecutionUnitKind::CompletedOperations);
        op->RewriteExecutionPlan(plan);
    }

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
