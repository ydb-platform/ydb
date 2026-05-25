#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TBuildDataTxOutRSUnit : public TExecutionUnit {
public:
    TBuildDataTxOutRSUnit(TDataShard &dataShard,
                          TPipeline &pipeline);
    ~TBuildDataTxOutRSUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TBuildDataTxOutRSUnit::TBuildDataTxOutRSUnit(TDataShard &dataShard,
                                             TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::BuildDataTxOutRS, true, dataShard, pipeline)
{
}

TBuildDataTxOutRSUnit::~TBuildDataTxOutRSUnit()
{
}

bool TBuildDataTxOutRSUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TBuildDataTxOutRSUnit::Execute(TOperation::TPtr op,
                                                TTransactionContext &txc,
                                                const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    DataShard.ReleaseCache(*tx);

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

    TDataShardLocksDb locksDb(DataShard, txc);
    TSetupSysLocks guardLocks(op, DataShard, &locksDb);

    tx->GetDataTx()->SetMvccVersion(DataShard.GetMvccVersion(tx));
    IEngineFlat *engine = tx->GetDataTx()->GetEngine();
    try {
        auto &outReadSets = op->OutReadSets();

        if (tx->GetDataTx()->CheckCancelled(DataShard.TabletID()))
            engine->Cancel();
        else
            engine->SetMemoryLimit(txc.GetMemoryLimit() - tx->GetDataTx()->GetTxSize());

        op->OutReadSets().clear();

        auto result = engine->PrepareOutgoingReadsets();
        Y_ENSURE(result == IEngineFlat::EResult::Ok,
                   "Engine errors at " << DataShard.TabletID() << " for " << *op
                   << ": " << engine->GetErrors());

        outReadSets.clear();
        for (ui32 i = 0, e = engine->GetOutgoingReadsetsCount(); i < e; ++i) {
            auto rs = engine->GetOutgoingReadset(i);
            outReadSets[std::make_pair(rs.OriginShardId, rs.TargetShardId)] = rs.Body;
        }

        engine->AfterOutgoingReadsetsExtracted();
    } catch (const TMemoryLimitExceededException &) {
        YDB_LOG_CTX_TRACE(ctx, "Operation at exceeded memory limit and requests more for the next try",
            {"#_*op", *op},
            {"TabletID", DataShard.TabletID()},
            {"GetMemoryLimit", txc.GetMemoryLimit()},
            {"#_txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR", txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR});

        txc.NotEnoughMemory();
        DataShard.IncCounter(DataShard.NotEnoughMemoryCounter(txc.GetNotEnoughMemoryCount()));

        engine->ReleaseUnusedMemory();
        txc.RequestMemory(txc.GetMemoryLimit() * MEMORY_REQUEST_FACTOR);

        tx->ReleaseTxData(txc, ctx);

        return EExecutionStatus::Restart;
    } catch (const TNotReadyTabletException&) {
        YDB_LOG_CTX_DEBUG(ctx, "Tablet is not ready for execution",
            {"TabletID", DataShard.TabletID()},
            {"#_*op", *op});

        DataShard.IncCounter(COUNTER_TX_TABLET_NOT_READY);

        tx->ReleaseTxData(txc, ctx);

        return EExecutionStatus::Restart;
    }

    return EExecutionStatus::Executed;
}

void TBuildDataTxOutRSUnit::Complete(TOperation::TPtr,
                                     const TActorContext &)
{
}

THolder<TExecutionUnit> CreateBuildDataTxOutRSUnit(TDataShard &dataShard,
                                                   TPipeline &pipeline)
{
    return THolder(new TBuildDataTxOutRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
