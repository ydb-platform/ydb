#include "datashard_direct_upload.h"

namespace NKikimr {
namespace NDataShard {

TDirectTxUpload::TDirectTxUpload(TEvDataShard::TEvUploadRowsRequest::TPtr& ev)
    : TCommonUploadOps(ev, true, true)
{
}

bool TDirectTxUpload::Execute(TDataShard* self, TTransactionContext& txc,
        const TRowVersion& readVersion, const TRowVersion& writeVersion,
        ui64 globalTxId)
{
    return TCommonUploadOps::Execute(self, txc, readVersion, writeVersion, globalTxId);
}

TDirectTxResult TDirectTxUpload::GetResult(TDataShard* self) {
    TDirectTxResult res;
    TCommonUploadOps::GetResult(self, res.Target, res.Event, res.Cookie);
    return res;
}

TVector<IDataShardChangeCollector::TChange> TDirectTxUpload::GetCollectedChanges() const {
    return TCommonUploadOps::GetCollectedChanges();
}

} // NDataShard
} // NKikimr
