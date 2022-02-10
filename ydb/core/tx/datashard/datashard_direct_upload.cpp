#include "datashard_direct_upload.h"
 
namespace NKikimr { 
namespace NDataShard {
 
TDirectTxUpload::TDirectTxUpload(TEvDataShard::TEvUploadRowsRequest::TPtr& ev)
    : TCommonUploadOps(ev, true, true)
{
}

bool TDirectTxUpload::Execute(TDataShard* self, TTransactionContext& txc, const TRowVersion& readVersion, const TRowVersion& writeVersion) {
    return TCommonUploadOps::Execute(self, txc, readVersion, writeVersion);
} 
 
void TDirectTxUpload::SendResult(TDataShard* self, const TActorContext& ctx) {
    TCommonUploadOps::SendResult(self, ctx);
} 
 
TVector<NMiniKQL::IChangeCollector::TChange> TDirectTxUpload::GetCollectedChanges() const {
    return TCommonUploadOps::GetCollectedChanges();
}

} // NDataShard
} // NKikimr
