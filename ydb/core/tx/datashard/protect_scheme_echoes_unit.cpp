#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TProtectSchemeEchoesUnit : public TExecutionUnit {
public:
    TProtectSchemeEchoesUnit(TDataShard &dataShard, TPipeline &pipeline)
        : TExecutionUnit(EExecutionUnitKind::ProtectSchemeEchoes, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &) override
    {
        Y_ENSURE(op->IsSchemeTx());

        TActiveTransaction *activeTx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(activeTx, "cannot cast operation of kind " << op->GetKind());
        const NKikimrTxDataShard::TFlatSchemeTransaction &tx = activeTx->GetSchemeTx();

        TSchemeOpSeqNo seqNo(tx.GetSeqNo());
        TSchemeOpSeqNo lastSeqNo = DataShard.GetLastSchemeOpSeqNo();

        // Don't accept new proposals with the same seqNo past this point
        // This way we are protected from any propose retries (echoes) that
        // are stuck in queues for dozens of minutes, unexpectedly arriving
        // after this transaction has already been completed, acknowledged
        // and forgotten.
        if (seqNo >= lastSeqNo) {
            DataShard.UpdateLastSchemeOpSeqNo(++seqNo, txc);
            return EExecutionStatus::ExecutedNoMoreRestarts;
        }

        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
        // nothing
    }
};

THolder<TExecutionUnit> CreateProtectSchemeEchoesUnit(TDataShard &dataShard, TPipeline &pipeline) {
    return THolder(new TProtectSchemeEchoesUnit(dataShard, pipeline));
}

} // namespace NlatterDataShard
} // namespace NKikimr
