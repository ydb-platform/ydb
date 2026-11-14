#include "stages.h"

#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>

namespace NKikimr::NOlap::NEngineLoading {

bool TEngineShardingInfoReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    return Self->MutableVersionedIndex().LoadShardingInfo(db);
}

bool TEngineShardingInfoReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return NColumnShard::Schema::Precharge<NColumnShard::Schema::ShardingInfo>(db, txc.DB.GetScheme());
}

bool TEngineUncommittedPortionsReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    std::vector<std::pair<std::shared_ptr<TGranuleMeta>, TPortionInfo::TPtr>> portions;
    for (const auto& address : Addresses) {
        const auto granule = Self->GetGranulePtrVerified(address.GetPathId());
        if (!db.LoadPortionVerified(
                address, [&](std::unique_ptr<TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
                    const TIndexInfo& indexInfo = portion->GetSchema(Self->GetVersionedIndex())->GetIndexInfo();
                    AFL_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, *DsGroupSelector));
                    portions.emplace_back(granule, portion->Build());
                })) {
            return false;
        }
    }
    for (auto&& [granule, portion] : portions) {
        granule->UpsertPortionOnLoad(portion);
    }
    return true;
}

bool TEngineUncommittedPortionsReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    bool ready = true;
    for (const auto& address : Addresses) {
        ready &=
            db.Table<NColumnShard::Schema::IndexPortions>().Key(address.GetPathId().GetRawValue(), address.GetPortionId()).Select().IsReady();
    }
    return ready;
}

bool TEngineLoadingFinish::DoExecute(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) {
    Self->FinishLoading();
    return true;
}

bool TEngineCountersReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    return Self->LoadCounters(db);
}

bool TEngineCountersReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return NColumnShard::Schema::Precharge<NColumnShard::Schema::Value>(db, txc.DB.GetScheme());
}

}   // namespace NKikimr::NOlap::NEngineLoading
