#include "const.h"
#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TStoreSchemeTxUnit : public TExecutionUnit {
public:
    TStoreSchemeTxUnit(TDataShard &dataShard,
                       TPipeline &pipeline);
    ~TStoreSchemeTxUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TStoreSchemeTxUnit::TStoreSchemeTxUnit(TDataShard &dataShard,
                                       TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::StoreSchemeTx, false, dataShard, pipeline)
{
}

TStoreSchemeTxUnit::~TStoreSchemeTxUnit()
{
}

bool TStoreSchemeTxUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TStoreSchemeTxUnit::Execute(TOperation::TPtr op,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    Y_ABORT_UNLESS(op->IsSchemeTx());
    Y_ABORT_UNLESS(!op->IsAborted() && !op->IsInterrupted());

    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());
    ui64 ssTabletId = tx->GetSchemeShardId();

    if (DataShard.GetCurrentSchemeShardId() == INVALID_TABLET_ID) {
        DataShard.PersistCurrentSchemeShardId(ssTabletId, txc);
    } else {
        Y_ABORT_UNLESS(DataShard.GetCurrentSchemeShardId() == ssTabletId,
                 "Got scheme transaction from unknown SchemeShard %" PRIu64, ssTabletId);
    }

    if (ui64 subDomainPathId = tx->GetSubDomainPathId()) {
        DataShard.PersistSubDomainPathId(ssTabletId, subDomainPathId, txc);
        DataShard.StopFindSubDomainPathId();
        DataShard.StartWatchingSubDomainPathId();
    } else {
        DataShard.StartFindSubDomainPathId();
    }

    if (DataShard.GetPathOwnerId() == INVALID_TABLET_ID) {
        if (tx->GetSchemeTx().HasCreateTable() && tx->GetSchemeTx().GetCreateTable().HasPathId()) { // message from new SS
            ui64 ownerPathId = tx->GetSchemeTx().GetCreateTable().GetPathId().GetOwnerId();
            DataShard.PersistOwnerPathId(ownerPathId, txc);
        } else { // message from old SS
            DataShard.PersistOwnerPathId(ssTabletId, txc);
        }
    }

    if (!DataShard.GetProcessingParams()) {
        DataShard.PersistProcessingParams(tx->GetProcessingParams(), txc);
    }

    TSchemaOperation schemeOp(op->GetTxId(), tx->GetSchemeTxType(), op->GetTarget(),
                             tx->GetSchemeShardId(), op->GetMinStep(), op->GetMaxStep(),
                             0, op->IsReadOnly(), false, TString(), 0, 0);
    Pipeline.ProposeSchemeTx(schemeOp, txc);

    Pipeline.ProposeTx(op, tx->GetTxBody(), txc, ctx);
    tx->ClearTxBody();
    // TODO: make cache for scheme tx similar to data tx.
    tx->ClearSchemeTx();

    return EExecutionStatus::DelayCompleteNoMoreRestarts;
}

void TStoreSchemeTxUnit::Complete(TOperation::TPtr op,
                                  const TActorContext &ctx)
{
    Pipeline.ProposeComplete(op, ctx);
}

THolder<TExecutionUnit> CreateStoreSchemeTxUnit(TDataShard &dataShard,
                                                TPipeline &pipeline)
{
    return THolder(new TStoreSchemeTxUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
