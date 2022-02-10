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
    auto [readVersion, writeVersion] = self->GetReadWriteVersions();
    if (!Impl->Execute(self, txc, readVersion, writeVersion))
        return false;

    self->PromoteCompleteEdge(writeVersion.Step, txc);
    return true;
}

void TDirectTransaction::SendResult(TDataShard* self, const TActorContext& ctx) { 
    Impl->SendResult(self, ctx);
}

TVector<NMiniKQL::IChangeCollector::TChange> TDirectTransaction::GetCollectedChanges() const {
    return Impl->GetCollectedChanges();
}

} // NDataShard 
} // NKikimr
