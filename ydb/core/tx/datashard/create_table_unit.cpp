#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TCreateTableUnit : public TExecutionUnit {
public:
    TCreateTableUnit(TDataShard &dataShard,
                     TPipeline &pipeline);
    ~TCreateTableUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
    TVector<THolder<TEvChangeExchange::TEvAddSender>> AddSenders;
};

TCreateTableUnit::TCreateTableUnit(TDataShard &dataShard,
                                   TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::CreateTable, false, dataShard, pipeline)
{
}

TCreateTableUnit::~TCreateTableUnit()
{
}

bool TCreateTableUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TCreateTableUnit::Execute(TOperation::TPtr op,
                                           TTransactionContext &txc,
                                           const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    const auto &schemeTx = tx->GetSchemeTx();
    if (!schemeTx.HasCreateTable())
        return EExecutionStatus::Executed;

    const auto &createTableTx = schemeTx.GetCreateTable();

    TPathId tableId(DataShard.GetPathOwnerId(), createTableTx.GetId_Deprecated());
    if (createTableTx.HasPathId()) {
        Y_ABORT_UNLESS(DataShard.GetPathOwnerId() == createTableTx.GetPathId().GetOwnerId());
        tableId.LocalPathId = createTableTx.GetPathId().GetLocalId();
    }

    if (DataShard.HasUserTable(tableId)) {
        // Table already created, assume we restarted after a successful commit
        return EExecutionStatus::Executed;
    }

    const ui64 schemaVersion = createTableTx.HasTableSchemaVersion() ? createTableTx.GetTableSchemaVersion() : 0u;

    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
               "Trying to CREATE TABLE at " << DataShard.TabletID()
               << " tableId# " << tableId
               << " schema version# " << schemaVersion);

    TUserTable::TPtr info = DataShard.CreateUserTable(txc, schemeTx.GetCreateTable());
    DataShard.AddUserTable(tableId, info);

    info->ForEachAsyncIndex([&](const auto& indexPathId, const auto&) {
        AddSenders.emplace_back(new TEvChangeExchange::TEvAddSender(
            tableId, TEvChangeExchange::ESenderType::AsyncIndex, indexPathId
        ));
    });

    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
    op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

    if (DataShard.GetState() == TShardState::WaitScheme) {
        txc.DB.NoMoreReadsForTx();
        DataShard.SetPersistState(TShardState::Ready, txc);
        DataShard.CheckMvccStateChangeCanStart(ctx); // Recheck
        DataShard.SendRegistrationRequestTimeCast(ctx);
    }

    return EExecutionStatus::DelayCompleteNoMoreRestarts;
}

void TCreateTableUnit::Complete(TOperation::TPtr, const TActorContext &ctx)
{
    for (auto& ev : AddSenders) {
        ctx.Send(DataShard.GetChangeSender(), ev.Release());
    }

    DataShard.MaybeActivateChangeSender(ctx);
}

THolder<TExecutionUnit> CreateCreateTableUnit(TDataShard &dataShard,
                                              TPipeline &pipeline)
{
    return THolder(new TCreateTableUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
