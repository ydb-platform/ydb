#pragma once

#include "datashard_common_upload.h"
#include "datashard_direct_transaction.h"

namespace NKikimr {
namespace NDataShard {

class TDirectTxUpload : public IDirectTx,
                        public TCommonUploadOps<
                            TEvDataShard::TEvUploadRowsRequest,
                            TEvDataShard::TEvUploadRowsResponse> {
public:
    explicit TDirectTxUpload(TEvDataShard::TEvUploadRowsRequest::TPtr& ev);

    bool Execute(TDataShard* self, TTransactionContext& txc, const TRowVersion& readVersion, const TRowVersion& writeVersion) override;
    void SendResult(TDataShard* self, const TActorContext& ctx) override;
    TVector<NMiniKQL::IChangeCollector::TChange> GetCollectedChanges() const override;
};

} // NDataShard
} // NKikimr
