#include "stages.h"

#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>

namespace NKikimr::NOlap::NEngineLoading {

bool TEngineShardingInfoReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    return Self->VersionedIndex.LoadShardingInfo(db);
}

bool TEngineShardingInfoReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return NColumnShard::Schema::Precharge<NColumnShard::Schema::ShardingInfo>(db, txc.DB.GetScheme());
}

bool TEnginePortionsReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    const TVersionedIndex& index = Self->VersionedIndex;
    return db.LoadPortions([&](TPortionInfoConstructor&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
        const TIndexInfo& indexInfo = portion.GetSchema(index)->GetIndexInfo();
        AFL_VERIFY(portion.MutableMeta().LoadMetadata(metaProto, indexInfo, db.GetDsGroupSelectorVerified()));
        AFL_VERIFY(Context->MutableConstructors().AddConstructorVerified(std::move(portion)));
    });
    return true;
}

bool TEnginePortionsReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return NColumnShard::Schema::Precharge<NColumnShard::Schema::IndexPortions>(db, txc.DB.GetScheme());
}

bool TEngineColumnsReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    TPortionInfo::TSchemaCursor schema(Self->VersionedIndex);
    return db.LoadColumns([&](const TColumnChunkLoadContextV1& loadContext) {
        auto* constructor = Context->MutableConstructors().GetConstructorVerified(loadContext.GetPathId(), loadContext.GetPortionId());
        constructor->LoadRecord(loadContext);
    });
}

bool TEngineColumnsReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return NColumnShard::Schema::Precharge<NColumnShard::Schema::IndexColumns>(db, txc.DB.GetScheme());
}

bool TEngineIndexesReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    return db.LoadIndexes([&](const ui64 pathId, const ui64 portionId, const TIndexChunkLoadContext& loadContext) {
        auto* constructor = Context->MutableConstructors().GetConstructorVerified(pathId, portionId);
        constructor->LoadIndex(loadContext);
    });
}

bool TEngineIndexesReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return NColumnShard::Schema::Precharge<NColumnShard::Schema::IndexIndexes>(db, txc.DB.GetScheme());
}

bool TEngineLoadingFinish::DoExecute(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) {
    Self->FinishLoading(Context);
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
