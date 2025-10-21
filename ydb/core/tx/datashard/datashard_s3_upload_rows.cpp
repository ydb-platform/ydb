#include "datashard_txs.h"

namespace NKikimr::NDataShard {

TDataShard::TTxS3UploadRows::TTxS3UploadRows(TDataShard* ds, TEvDataShard::TEvS3UploadRowsRequest::TPtr& ev)
    : TBase(ds)
    , TCommonUploadOps(ev, false, false)
{
}

bool TDataShard::TTxS3UploadRows::Execute(TTransactionContext& txc, const TActorContext&) {
    auto mvccVersion = Self->GetMvccVersion();
    
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
    TActorId target;
    THolder<IEventBase> event;
    ui64 cookie;
    TCommonUploadOps::GetResult(Self, target, event, cookie);

    Self->SendImmediateWriteResult(MvccVersion, target, event.Release(), cookie);
}

}
