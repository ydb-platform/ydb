#include "datashard_txs.h"

namespace NKikimr {
namespace NDataShard {

TDataShard::TTxUnsafeUploadRows::TTxUnsafeUploadRows(TDataShard* ds, TEvDataShard::TEvUnsafeUploadRowsRequest::TPtr& ev)
    : TBase(ds)
    , TCommonUploadOps(ev, false, false)
{
}

bool TDataShard::TTxUnsafeUploadRows::Execute(TTransactionContext& txc, const TActorContext&) {
    auto [readVersion, writeVersion] = Self->GetReadWriteVersions(); 
    if (!TCommonUploadOps::Execute(Self, txc, readVersion, writeVersion))
        return false; 
 
    Self->PromoteCompleteEdge(writeVersion.Step, txc);
    return true; 
}

void TDataShard::TTxUnsafeUploadRows::Complete(const TActorContext& ctx) {
    TCommonUploadOps::SendResult(Self, ctx);
}

} // NDataShard
} // NKikimr
