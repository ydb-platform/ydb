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
