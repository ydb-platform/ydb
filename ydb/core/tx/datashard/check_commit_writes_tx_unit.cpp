#include "datashard_active_transaction.h"
#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TCheckCommitWritesTxUnit : public TExecutionUnit {
public:
    TCheckCommitWritesTxUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CheckCommitWritesTx, false, self, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext&, const TActorContext& ctx) override {
        Y_ENSURE(op->IsCommitWritesTx());
        Y_ENSURE(!op->IsAborted());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        if (CheckRejectDataTx(op, ctx)) {
            op->Abort(EExecutionUnitKind::FinishPropose);
            return EExecutionStatus::Executed;
        }

        const auto& commitTx = tx->GetCommitWritesTx()->GetBody();

        auto buildUnsuccessfulResult = [&](
                const TString& reason,
                NKikimrTxDataShard::TEvProposeTransactionResult::EStatus status = NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST,
                NKikimrTxDataShard::TError::EKind kind = NKikimrTxDataShard::TError::BAD_ARGUMENT)
        {
            BuildResult(op, status)->AddError(kind, reason);
            op->Abort(EExecutionUnitKind::FinishPropose);
            return EExecutionStatus::Executed;
        };

        const auto& tableId = commitTx.GetTableId();
        if (tableId.GetOwnerId() != DataShard.GetPathOwnerId() ||
            !DataShard.GetUserTables().contains(tableId.GetTableId()))
        {
            return buildUnsuccessfulResult(
                TStringBuilder()
                    << "CommitWrites refers to table "
                    << tableId.GetOwnerId() << ":" << tableId.GetTableId()
                    << " that doesn't exist at shard "
                    << DataShard.TabletID(),
                NKikimrTxDataShard::TEvProposeTransactionResult::ERROR,
                NKikimrTxDataShard::TError::SCHEME_ERROR);
        }

        const auto& tableInfo = *DataShard.GetUserTables().at(tableId.GetTableId());

        if (tableId.GetSchemaVersion() && tableInfo.GetTableSchemaVersion() != tableId.GetSchemaVersion()) {
            return buildUnsuccessfulResult(
                TStringBuilder()
                    << "SchemaVersion mismatch for table "
                    << tableId.GetOwnerId() << ":" << tableId.GetTableId()
                    << " requested " << tableId.GetSchemaVersion()
                    << " expected " << tableInfo.GetTableSchemaVersion()
                    << " at shard " << DataShard.TabletID(),
                NKikimrTxDataShard::TEvProposeTransactionResult::ERROR,
                NKikimrTxDataShard::TError::SCHEME_ERROR);
        }

        if (!commitTx.HasWriteTxId() || commitTx.GetWriteTxId() == 0) {
            return buildUnsuccessfulResult("CommitWrites transaction must have a valid WriteTxId");
        }

        if (!Pipeline.AssignPlanInterval(op)) {
            const TString err = TStringBuilder()
                << "Can't propose tx " << op->GetTxId()
                << " at blocked shard " << DataShard.TabletID();

            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, err);
            return buildUnsuccessfulResult(
                err,
                NKikimrTxDataShard::TEvProposeTransactionResult::ERROR,
                NKikimrTxDataShard::TError::SHARD_IS_BLOCKED);
        }

        BuildResult(op)->SetPrepared(op->GetMinStep(), op->GetMaxStep(), op->GetReceivedAt());

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
            "Prepared " << op->GetKind()
            << " transaction txId " << op->GetTxId()
            << " at shard " << DataShard.TabletID());
        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
    }
};

THolder<TExecutionUnit> CreateCheckCommitWritesTxUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TCheckCommitWritesTxUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
