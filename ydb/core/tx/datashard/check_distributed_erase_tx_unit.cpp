#include "datashard_active_transaction.h"
#include "datashard_direct_erase.h"
#include "datashard_distributed_erase.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

#include <util/generic/bitmap.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NDataShard {

class TCheckDistributedEraseTxUnit : public TExecutionUnit {
public:
    TCheckDistributedEraseTxUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CheckDistributedEraseTx, false, self, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext&, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(op->IsDistributedEraseTx());
        Y_ABORT_UNLESS(!op->IsAborted());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        if (CheckRejectDataTx(op, ctx)) {
            op->Abort(EExecutionUnitKind::FinishPropose);
            return EExecutionStatus::Executed;
        }

        const auto& eraseTx = tx->GetDistributedEraseTx();
        const auto& request = eraseTx->GetRequest();

        auto buildUnsuccessfulResult = [&](
                const TString& reason,
                NKikimrTxDataShard::TEvProposeTransactionResult::EStatus status = NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST,
                NKikimrTxDataShard::TError::EKind kind = NKikimrTxDataShard::TError::BAD_ARGUMENT) {

            BuildResult(op, status)->AddError(kind, reason);
            op->Abort(EExecutionUnitKind::FinishPropose);
            return EExecutionStatus::Executed;
        };

        NKikimrTxDataShard::TEvEraseRowsResponse::EStatus status;
        TString error;
        if (!TDirectTxErase::CheckRequest(&DataShard, request, status, error)) {
            return buildUnsuccessfulResult(error);
        }

        const size_t count = (size_t)eraseTx->HasDependents() + (size_t)eraseTx->HasDependencies();
        if (count != 1) {
            return buildUnsuccessfulResult("Distributed erase tx can only have dependents or dependencies, not both");
        }

        for (const auto& dependency : eraseTx->GetDependencies()) {
            auto presentRows = DeserializeBitMap<TDynBitMap>(dependency.GetPresentRows());
            if (request.KeyColumnsSize() == presentRows.Count()) {
                continue;
            }

            return buildUnsuccessfulResult(TStringBuilder() << "Present rows count mismatch"
                << ": got " << presentRows.Count()
                << ", expected " << request.KeyColumnsSize()
            );
        }

        // checked at CheckedExecute stage
        Y_ABORT_UNLESS(DataShard.GetUserTables().contains(request.GetTableId()));
        const TUserTable& tableInfo = *DataShard.GetUserTables().at(request.GetTableId());

        for (const auto columnId : eraseTx->GetIndexColumnIds()) {
            if (tableInfo.Columns.contains(columnId)) {
                continue;
            }

            return buildUnsuccessfulResult(TStringBuilder() << "Unknown index column id " << columnId);
        }

        for (const auto& serializedCells : eraseTx->GetIndexColumns()) {
            TSerializedCellVec indexCells;
            if (!TSerializedCellVec::TryParse(serializedCells, indexCells)) {
                return buildUnsuccessfulResult("Cannot parse index column value");
            }

            if (indexCells.GetCells().size() != static_cast<ui32>(eraseTx->GetIndexColumnIds().size())) {
                return buildUnsuccessfulResult(TStringBuilder() << "Cell count doesn't match row scheme"
                    << ": got " << indexCells.GetCells().size()
                    << ", expected " << eraseTx->GetIndexColumnIds().size());
            }
        }

        if (!Pipeline.AssignPlanInterval(op)) {
            const TString err = TStringBuilder() << "Can't propose"
                << " tx " << op->GetTxId()
                << " at blocked shard " << DataShard.TabletID();

            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, err);
            return buildUnsuccessfulResult(
                err,
                NKikimrTxDataShard::TEvProposeTransactionResult::ERROR,
                NKikimrTxDataShard::TError::SHARD_IS_BLOCKED
            );
        }

        BuildResult(op)->SetPrepared(op->GetMinStep(), op->GetMaxStep(), op->GetReceivedAt());

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Prepared"
            << " " << op->GetKind()
            << " transaction txId " << op->GetTxId()
            << " at tablet " << DataShard.TabletID());
        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
    }
};

THolder<TExecutionUnit> CreateCheckDistributedEraseTxUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TCheckDistributedEraseTxUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
