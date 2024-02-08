#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TPrepareDataTxInRSUnit : public TExecutionUnit {
public:
    TPrepareDataTxInRSUnit(TDataShard &dataShard,
                           TPipeline &pipeline);
    ~TPrepareDataTxInRSUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TPrepareDataTxInRSUnit::TPrepareDataTxInRSUnit(TDataShard &dataShard,
                                               TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::PrepareDataTxInRS, true, dataShard, pipeline)
{
}

TPrepareDataTxInRSUnit::~TPrepareDataTxInRSUnit()
{
}

bool TPrepareDataTxInRSUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TPrepareDataTxInRSUnit::Execute(TOperation::TPtr op,
                                                 TTransactionContext &txc,
                                                 const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    if (tx->IsTxDataReleased()) {
        switch (Pipeline.RestoreDataTx(tx, txc, ctx)) {
            case ERestoreDataStatus::Ok:
                break;
            case ERestoreDataStatus::Restart:
                return EExecutionStatus::Restart;
            case ERestoreDataStatus::Error:
                Y_ABORT("Failed to restore tx data: %s", tx->GetDataTx()->GetErrors().c_str());
        }
    }

    IEngineFlat *engine = tx->GetDataTx()->GetEngine();
    Y_VERIFY_S(engine, "missing engine for " << *op << " at " << DataShard.TabletID());

    // TODO: cancel tx in special execution unit.
    if (tx->GetDataTx()->CheckCancelled(DataShard.TabletID()))
        engine->Cancel();

    try {
        auto &inReadSets = op->InReadSets();
        auto result = engine->PrepareIncomingReadsets();
        Y_VERIFY_S(result == IEngineFlat::EResult::Ok,
                   "Cannot prepare input RS for " << *op << " at "
                   << DataShard.TabletID() << ": " << engine->GetErrors());

        ui32 rsCount = engine->GetExpectedIncomingReadsetsCount();
        for (ui32 i = 0; i < rsCount; ++i) {
            ui64 shard = engine->GetExpectedIncomingReadsetOriginShard(i);
            inReadSets.insert(std::make_pair(std::make_pair(shard, DataShard.TabletID()),
                                             TVector<TRSData>()));
        }
    } catch (const TMemoryLimitExceededException &) {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Operation " << *op << " at " << DataShard.TabletID()
                    << " exceeded memory limit " << txc.GetMemoryLimit()
                    << " and requests " << txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR
                    << " more for the next try");

        engine->ReleaseUnusedMemory();
        txc.RequestMemory(txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR);

        tx->ReleaseTxData(txc, ctx);

        return EExecutionStatus::Restart;
    } catch (const TNotReadyTabletException&) {
        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Tablet " << DataShard.TabletID() << " is not ready for " << *op
                    << " execution");

        return EExecutionStatus::Restart;
    }

    return EExecutionStatus::Executed;
}

void TPrepareDataTxInRSUnit::Complete(TOperation::TPtr,
                                      const TActorContext &)
{
}

THolder<TExecutionUnit> CreatePrepareDataTxInRSUnit(TDataShard &dataShard,
                                                    TPipeline &pipeline)
{
    return THolder(new TPrepareDataTxInRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
