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
    : TExecutionUnit(EExecutionUnitKind::Truncate, true, dataShard, pipeline)
{
}

TTruncateUnit::~TTruncateUnit() {
}

bool TTruncateUnit::IsReadyToExecute(TOperation::TPtr) const {
    return true;
}

EExecutionStatus TTruncateUnit::Execute(
    TOperation::TPtr op, TTransactionContext& txc, const TActorContext&
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

    txc.DB.Truncate(tableId);

    return EExecutionStatus::Executed;
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
