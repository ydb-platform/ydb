#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TDropTableUnit : public TExecutionUnit {
public:
    TDropTableUnit(TDataShard &dataShard,
                   TPipeline &pipeline);
    ~TDropTableUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
    TVector<THolder<TEvChangeExchange::TEvRemoveSender>> RemoveSenders;
};

TDropTableUnit::TDropTableUnit(TDataShard &dataShard,
                               TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::DropTable, false, dataShard, pipeline)
{
}

TDropTableUnit::~TDropTableUnit()
{
}

bool TDropTableUnit::IsReadyToExecute(TOperation::TPtr op) const
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    auto &schemeTx = tx->GetSchemeTx();
    if (!schemeTx.HasDropTable())
        return true;

    if (op->GetSpecialDependencies().empty()) {
        // We must wait for all immediate ops to complete first
        // This is probably not necessary, because we add all dependencies
        // when transaction is first added to pipeline, but it's better
        // to wait again if some transaction manages to sneak thru.
        for (auto &pr : Pipeline.GetImmediateOps()) {
            op->AddSpecialDependency(pr.second);
        }
    }

    // We shouldn't have any normal dependencies
    Y_ABORT_UNLESS(op->GetDependencies().empty());

    return op->GetSpecialDependencies().empty();
}

EExecutionStatus TDropTableUnit::Execute(TOperation::TPtr op,
                                         TTransactionContext &txc,
                                         const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    auto &schemeTx = tx->GetSchemeTx();
    if (!schemeTx.HasDropTable())
        return EExecutionStatus::Executed;

    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
               "Trying to DROP TABLE at " << DataShard.TabletID());

    ui64 tableId = schemeTx.GetDropTable().GetId_Deprecated();
    if (schemeTx.GetDropTable().HasPathId()) {
        Y_ABORT_UNLESS(DataShard.GetPathOwnerId() == schemeTx.GetDropTable().GetPathId().GetOwnerId());
        tableId = schemeTx.GetDropTable().GetPathId().GetLocalId();
    }

    auto it = DataShard.GetUserTables().find(tableId);
    Y_ABORT_UNLESS(it != DataShard.GetUserTables().end());
    {
        it->second->ForEachAsyncIndex([&](const auto& indexPathId, const auto&) {
            RemoveSenders.emplace_back(new TEvChangeExchange::TEvRemoveSender(indexPathId));
        });
        for (const auto& [streamPathId, _] : it->second->CdcStreams) {
            RemoveSenders.emplace_back(new TEvChangeExchange::TEvRemoveSender(streamPathId));
        }
    }

    DataShard.DropUserTable(txc, tableId);

    // FIXME: transactions need to specify ownerId
    TVector<TSnapshotKey> snapshotsToRemove;
    TSnapshotTableKey snapshotsScope(DataShard.GetPathOwnerId(), tableId);
    for (const auto& kv : DataShard.GetSnapshotManager().GetSnapshots(snapshotsScope)) {
        snapshotsToRemove.push_back(kv.first);
    }

    for (const auto& key : snapshotsToRemove) {
        NIceDb::TNiceDb db(txc.DB);
        DataShard.GetSnapshotManager().PersistRemoveSnapshot(db, key);
    }

    txc.DB.NoMoreReadsForTx();
    DataShard.SetPersistState(TShardState::PreOffline, txc);
    DataShard.NotifyAllOverloadSubscribers();

    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
    op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

    return EExecutionStatus::DelayCompleteNoMoreRestarts;
}

void TDropTableUnit::Complete(TOperation::TPtr,
                              const TActorContext &ctx)
{
    for (auto& ev : RemoveSenders) {
        ctx.Send(DataShard.GetChangeSender(), ev.Release());
    }
}

THolder<TExecutionUnit> CreateDropTableUnit(TDataShard &dataShard,
                                            TPipeline &pipeline)
{
    return THolder(new TDropTableUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
