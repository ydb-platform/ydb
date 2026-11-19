#include "datashard_txs.h"
#include "datashard_pipeline.h"

namespace NKikimr::NDataShard {

TDataShard::TTxS3UploadRows::TTxS3UploadRows(
    TDataShard* ds, TEvDataShard::TEvS3UploadRowsRequest::TPtr& ev, TActorId sender)
    : TBase(ds)
    , TCommonUploadOps(ev, false, false)
    , Sender(sender)
{
    ++Self->DatabaseFreezeLocalWrites;
}

bool TDataShard::TTxS3UploadRows::Execute(TTransactionContext& txc, const TActorContext&) {
    const auto* request = GetRequest();
    if (!Self->IsActiveS3Import(request->TxId, request->Record.GetTableId(), Sender)) {
        Cancelled = true;
        return true;
    }
    auto op = Self->Pipeline.FindOp(request->TxId);
    // Write every batch at the restore transaction's planned version
    const auto mvccVersion = Self->GetMvccVersion(op.Get());

    // NOTE: will not throw TNeedGlobalTxId since we set breakLocks to false
    if (!TCommonUploadOps::Execute(Self, txc, mvccVersion,
            /* globalTxId */ 0, /* volatile read dependencies */ nullptr))
    {
        return false;
    }

    auto* result = GetResult();
    if (result->Record.GetStatus() == NKikimrTxDataShard::TError::OK) {
        NIceDb::TNiceDb db(txc.DB);
        result->Info = Self->S3Downloads.Store(db, GetRequest()->TxId, GetRequest()->Info);
    }

    // Note: we always wait for completion, so we can ignore the result
    Self->PromoteImmediatePostExecuteEdges(mvccVersion, TDataShard::EPromotePostExecuteEdges::ReadWrite, txc);
    MvccVersion = mvccVersion;

    return true;
}

void TDataShard::TTxS3UploadRows::Complete(const TActorContext&) {
    --Self->DatabaseFreezeLocalWrites;
    if (Cancelled) {
        Self->Send(Sender, new TEvDataShard::TEvS3UploadRowsResponse(
                               Self->TabletID(), NKikimrTxDataShard::TError::EXECUTION_CANCELLED));
        return;
    }
    TActorId target;
    THolder<IEventBase> event;
    ui64 cookie;
    TCommonUploadOps::GetResult(Self, target, event, cookie);

    Self->SendImmediateWriteResult(MvccVersion, target, event.Release(), cookie);
}

}
