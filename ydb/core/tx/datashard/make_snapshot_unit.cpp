#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TMakeSnapshotUnit : public TExecutionUnit {
public:
    TMakeSnapshotUnit(TDataShard &dataShard,
                      TPipeline &pipeline);
    ~TMakeSnapshotUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TMakeSnapshotUnit::TMakeSnapshotUnit(TDataShard &dataShard,
                                     TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::MakeSnapshot, false, dataShard, pipeline)
{
}

TMakeSnapshotUnit::~TMakeSnapshotUnit()
{
}

bool TMakeSnapshotUnit::IsReadyToExecute(TOperation::TPtr op) const
{
    if (!op->IsWaitingForSnapshot())
        return true;

    return !op->InputSnapshots().empty();
}

EExecutionStatus TMakeSnapshotUnit::Execute(TOperation::TPtr op,
                                            TTransactionContext &txc,
                                            const TActorContext &)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    auto &schemeTx = tx->GetSchemeTx();
    if (!schemeTx.HasSendSnapshot() && !schemeTx.HasCreateIncrementalBackupSrc())
        return EExecutionStatus::Executed;

    if (!op->IsWaitingForSnapshot()) {
        auto& snapshot =
            schemeTx.HasSendSnapshot() ?
            schemeTx.GetSendSnapshot() :
            schemeTx.GetCreateIncrementalBackupSrc().GetSendSnapshot();
        ui64 tableId = snapshot.GetTableId_Deprecated();
        if (snapshot.HasTableId()) {
            Y_ENSURE(DataShard.GetPathOwnerId() == snapshot.GetTableId().GetOwnerId());
            tableId = snapshot.GetTableId().GetTableId();
        }
        Y_ENSURE(DataShard.GetUserTables().contains(tableId));
        ui32 localTableId = DataShard.GetUserTables().at(tableId)->LocalTid;
        const auto& openTxs = txc.DB.GetOpenTxs(localTableId);
        TIntrusivePtr<TTableSnapshotContext> snapContext
            = new TTxTableSnapshotContext(op->GetStep(), op->GetTxId(), {localTableId}, !openTxs.empty());
        txc.Env.MakeSnapshot(snapContext);

        op->SetWaitingForSnapshotFlag();
        return EExecutionStatus::Continue;
    }

    Y_ENSURE(!op->InputSnapshots().empty());
    op->ResetWaitingForSnapshotFlag();

    return EExecutionStatus::Executed;
}

void TMakeSnapshotUnit::Complete(TOperation::TPtr,
                                 const TActorContext &)
{
}

THolder<TExecutionUnit> CreateMakeSnapshotUnit(TDataShard &dataShard,
                                               TPipeline &pipeline)
{
    return THolder(new TMakeSnapshotUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
