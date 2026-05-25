#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

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
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    if (tx->IsTxDataReleased()) {
        switch (Pipeline.RestoreDataTx(tx, txc, ctx, tx->GetUserCtx())) {
            case ERestoreDataStatus::Ok:
                break;
            case ERestoreDataStatus::Restart:
                return EExecutionStatus::Restart;
            case ERestoreDataStatus::Error:
                Y_ENSURE(false, "Failed to restore tx data: " << tx->GetDataTx()->GetErrors());
        }
    }

    IEngineFlat *engine = tx->GetDataTx()->GetEngine();
    Y_ENSURE(engine, "missing engine for " << *op << " at " << DataShard.TabletID());

    // TODO: cancel tx in special execution unit.
    if (tx->GetDataTx()->CheckCancelled(DataShard.TabletID()))
        engine->Cancel();

    try {
        auto &inReadSets = op->InReadSets();
        auto result = engine->PrepareIncomingReadsets();
        Y_ENSURE(result == IEngineFlat::EResult::Ok,
                   "Cannot prepare input RS for " << *op << " at "
                   << DataShard.TabletID() << ": " << engine->GetErrors());

        ui32 rsCount = engine->GetExpectedIncomingReadsetsCount();
        for (ui32 i = 0; i < rsCount; ++i) {
            ui64 shard = engine->GetExpectedIncomingReadsetOriginShard(i);
            inReadSets.insert(std::make_pair(std::make_pair(shard, DataShard.TabletID()),
                                             TVector<TRSData>()));
        }
    } catch (const TMemoryLimitExceededException &) {
        YDB_LOG_CTX_TRACE(ctx, "Operation at exceeded memory limit and requests more for the next try",
            {"#_*op", *op},
            {"TabletID", DataShard.TabletID()},
            {"GetMemoryLimit", txc.GetMemoryLimit()},
            {"#_txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR", txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR});

        engine->ReleaseUnusedMemory();
        txc.RequestMemory(txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR);

        tx->ReleaseTxData(txc, ctx);

        return EExecutionStatus::Restart;
    } catch (const TNotReadyTabletException&) {
        YDB_LOG_CTX_TRACE(ctx, "Tablet is not ready for execution",
            {"TabletID", DataShard.TabletID()},
            {"#_*op", *op});

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
