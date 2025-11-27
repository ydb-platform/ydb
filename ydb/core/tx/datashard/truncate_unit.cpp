#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TTruncateUnit : public TExecutionUnit {
public:
    TTruncateUnit(TDataShard&, TPipeline&);
    ~TTruncateUnit() override;

    bool IsReadyToExecute(TOperation::TPtr) const override;
    EExecutionStatus Execute(TOperation::TPtr, TTransactionContext&, const TActorContext&) override;
    void Complete(TOperation::TPtr, const TActorContext&) override;
};

TTruncateUnit::TTruncateUnit(TDataShard& dataShard, TPipeline& pipeline)
    : TExecutionUnit(EExecutionUnitKind::Truncate, false, dataShard, pipeline)
{
}

TTruncateUnit::~TTruncateUnit() {
}

bool TTruncateUnit::IsReadyToExecute(TOperation::TPtr) const {
    // TODO: flown4qqqq
    return true;
}

EExecutionStatus TTruncateUnit::Execute(
    TOperation::TPtr op, TTransactionContext& txc, const TActorContext& actorCtx
) {
    TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    auto& schemeTx = tx->GetSchemeTx();

    if (!schemeTx.HasTruncateTable()) {
        return EExecutionStatus::Executed;
    }

    const auto& truncate = schemeTx.GetTruncateTable();
    const auto& pathId = truncate.GetPathId();
    Y_ENSURE(DataShard.GetPathOwnerId() == pathId.GetOwnerId());
    auto tableId = pathId.GetLocalId();
    Y_ENSURE(DataShard.GetUserTables().contains(tableId));

    const auto& userTable = DataShard.GetUserTables().at(tableId);
    auto localTid = userTable->LocalTid;

    LOG_DEBUG_S(actorCtx, NKikimrServices::TX_DATASHARD,
               "TTruncateUnit::Execute - About to TRUNCATE TABLE at " << DataShard.TabletID()
               << " tableId# " << tableId << " localTid# " << localTid << " TxId = " << op->GetTxId());

    txc.DB.Truncate(localTid);
    txc.DB.NoMoreReadsForTx();

    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
    op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());
    
    LOG_DEBUG_S(actorCtx, NKikimrServices::TX_DATASHARD,
               "TTruncateUnit::Execute - Finished successfully. TableId = " << tableId
               << " TxId = " << op->GetTxId() << " - Operation COMPLETED");

    return EExecutionStatus::DelayCompleteNoMoreRestarts;
}

void TTruncateUnit::Complete(TOperation::TPtr,
                                    const TActorContext &)
{
}

THolder<TExecutionUnit> CreateTruncateUnit(TDataShard &dataShard, TPipeline &pipeline) {
    return THolder(new TTruncateUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
