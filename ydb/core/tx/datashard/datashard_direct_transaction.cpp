#include "datashard_direct_transaction.h"
#include "datashard_direct_erase.h"
#include "datashard_direct_upload.h"

namespace NKikimr {
namespace NDataShard {

TDirectTransaction::TDirectTransaction(TInstant receivedAt, ui64 tieBreakerIndex, TEvDataShard::TEvUploadRowsRequest::TPtr& ev)
    : TOperation(TBasicOpInfo(EOperationKind::DirectTx, Flags, 0, receivedAt, tieBreakerIndex))
    , Impl(new TDirectTxUpload(ev))
{
}

TDirectTransaction::TDirectTransaction(TInstant receivedAt, ui64 tieBreakerIndex, TEvDataShard::TEvEraseRowsRequest::TPtr& ev)
    : TOperation(TBasicOpInfo(EOperationKind::DirectTx, Flags, 0, receivedAt, tieBreakerIndex))
    , Impl(new TDirectTxErase(ev))
{
}

void TDirectTransaction::BuildExecutionPlan(bool loaded)
{
    Y_ENSURE(GetExecutionPlan().empty());
    Y_ENSURE(!loaded);

    TVector<EExecutionUnitKind> plan;
    plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
    plan.push_back(EExecutionUnitKind::DirectOp);
    plan.push_back(EExecutionUnitKind::CompletedOperations);

    RewriteExecutionPlan(plan);
}

bool TDirectTransaction::Execute(TDataShard* self, TTransactionContext& txc) {
    auto mvccVersion = self->GetMvccVersion(this);

    // NOTE: may throw TNeedGlobalTxId exception, which is handled in direct tx unit
    absl::flat_hash_set<ui64> volatileReadDependencies;
    if (!Impl->Execute(self, txc, mvccVersion, GetGlobalTxId(), volatileReadDependencies)) {
        if (!volatileReadDependencies.empty()) {
            for (ui64 txId : volatileReadDependencies) {
                AddVolatileDependency(txId);
                bool ok = self->GetVolatileTxManager().AttachBlockedOperation(txId, GetTxId());
                Y_ENSURE(ok, "Unexpected failure to attach " << *static_cast<TOperation*>(this) << " to volatile tx " << txId);
            }
        }
        return false;
    }

    // Note: we always wait for completion, so we can ignore the result
    self->PromoteImmediatePostExecuteEdges(mvccVersion, TDataShard::EPromotePostExecuteEdges::ReadWrite, txc);

    return true;
}

void TDirectTransaction::SendResult(TDataShard* self, const TActorContext& ctx) {
    auto result = Impl->GetResult(self);
    if (CachedMvccVersion) {
        self->SendImmediateWriteResult(*CachedMvccVersion, result.Target, result.Event.Release(), result.Cookie);
    } else {
        ctx.Send(result.Target, result.Event.Release(), 0, result.Cookie);
    }
}

TVector<IDataShardChangeCollector::TChange> TDirectTransaction::GetCollectedChanges() const {
    return Impl->GetCollectedChanges();
}

} // NDataShard
} // NKikimr
