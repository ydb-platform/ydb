#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_sharing/common/transactions/tx_extension.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/session/destination.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/events/transfer.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

namespace NKikimr::NOlap::NDataSharing {

class TTxDataFromSource: public TExtendedTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = TExtendedTransactionBase<NColumnShard::TColumnShard>;
    std::shared_ptr<TDestinationSession> Session;
    THashMap<ui64, NEvents::TPathIdData> PortionsByPathId;
    THashMap<TString, THashSet<NBlobCache::TUnifiedBlobId>> SharedBlobIds;
    const TTabletId SourceTabletId;
protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;
public:
    TTxDataFromSource(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& session, const THashMap<ui64, NEvents::TPathIdData>& portionsByPathId, const TTabletId sourceTabletId);

    TTxType GetTxType() const override { return NColumnShard::TXTYPE_DATA_SHARING_DATA_FROM_SOURCE; }
};


}
