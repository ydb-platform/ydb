#include "datashard_direct_transaction.h"
#include "datashard_direct_erase.h"
#include "datashard_direct_upload.h"

namespace NKikimr {
namespace NDataShard {

TDirectTransaction::TDirectTransaction(ui64 txId, TInstant receivedAt, ui64 tieBreakerIndex, TEvDataShard::TEvUploadRowsRequest::TPtr& ev)
    : TOperation(TBasicOpInfo(txId, EOperationKind::DirectTx, Flags, 0, receivedAt, tieBreakerIndex))
    , Impl(new TDirectTxUpload(ev))
{
}

TDirectTransaction::TDirectTransaction(ui64 txId, TInstant receivedAt, ui64 tieBreakerIndex, TEvDataShard::TEvEraseRowsRequest::TPtr& ev)
    : TOperation(TBasicOpInfo(txId, EOperationKind::DirectTx, Flags, 0, receivedAt, tieBreakerIndex))
    , Impl(new TDirectTxErase(ev))
{
}

void TDirectTransaction::BuildExecutionPlan(bool loaded)
{
    Y_VERIFY(GetExecutionPlan().empty());
    Y_VERIFY(!loaded);

    TVector<EExecutionUnitKind> plan;
    plan.push_back(EExecutionUnitKind::BuildAndWaitDependencies);
    plan.push_back(EExecutionUnitKind::DirectOp);
    plan.push_back(EExecutionUnitKind::CompletedOperations);

    RewriteExecutionPlan(plan);
}

bool TDirectTransaction::Execute(TDataShard* self, TTransactionContext& txc) {
    auto [readVersion, writeVersion] = self->GetReadWriteVersions(this);
    if (!Impl->Execute(self, txc, readVersion, writeVersion))
        return false;

    if (self->IsMvccEnabled()) {
        // Note: we always wait for completion, so we can ignore the result
        self->PromoteImmediatePostExecuteEdges(writeVersion, TDataShard::EPromotePostExecuteEdges::ReadWrite, txc);
    }

    return true;
}

void TDirectTransaction::SendResult(TDataShard* self, const TActorContext& ctx) {
    auto result = Impl->GetResult(self);
    if (MvccReadWriteVersion) {
        self->SendImmediateWriteResult(*MvccReadWriteVersion, result.Target, result.Event.Release(), result.Cookie);
    } else {
        ctx.Send(result.Target, result.Event.Release(), 0, result.Cookie);
    }
}

TVector<IDataShardChangeCollector::TChange> TDirectTransaction::GetCollectedChanges() const {
    return Impl->GetCollectedChanges();
}

} // NDataShard
} // NKikimr
