#pragma once
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/tablet/ext_tx_base.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/events/transfer.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/session/destination.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/schema_version.h>

namespace NKikimr::NOlap::NDataSharing {

class TTxDataFromSource: public NColumnShard::TExtendedTransactionBase {
private:
    using TBase = NColumnShard::TExtendedTransactionBase;
    std::shared_ptr<TDestinationSession> Session;
    THashMap<NColumnShard::TInternalPathId, NEvents::TPathIdData> PortionsByPathId;
    THashMap<TString, THashSet<NBlobCache::TUnifiedBlobId>> SharedBlobIds;
    std::vector<NOlap::TSchemaPresetVersionInfo> SchemeHistory;
    const TTabletId SourceTabletId;
protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;
public:
    TTxDataFromSource(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& session, THashMap<NColumnShard::TInternalPathId, NEvents::TPathIdData>&& portionsByPathId, std::vector<NOlap::TSchemaPresetVersionInfo>&& schemas, const TTabletId sourceTabletId);

    TTxType GetTxType() const override { return NColumnShard::TXTYPE_DATA_SHARING_DATA_FROM_SOURCE; }
};


}
