#pragma once

#include "datashard_common_upload.h"
#include "datashard_direct_transaction.h"

namespace NKikimr {
namespace NDataShard {

class TDirectTxUpload : public IDirectTx
                      , public TCommonUploadOps<
                            NEvDataShard::TEvUploadRowsRequest,
                            NEvDataShard::TEvUploadRowsResponse>
{
public:
    explicit TDirectTxUpload(NEvDataShard::TEvUploadRowsRequest::TPtr& ev);

    bool Execute(TDataShard* self, TTransactionContext& txc,
        const TRowVersion& readVersion, const TRowVersion& writeVersion,
        ui64 globalTxId, absl::flat_hash_set<ui64>& volatileReadDependencies) override;
    TDirectTxResult GetResult(TDataShard* self) override;
    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const override;
};

} // NDataShard
} // NKikimr
