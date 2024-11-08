#include "granule.h"
#include "stages.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NLoading {

bool TGranulePortionsReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    const TVersionedIndex& index = *VersionedIndex;
    Context->MutableConstructors().ClearPortions();
    return db.LoadPortions(Self->GetPathId(), [&](TPortionInfoConstructor&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
        const TIndexInfo& indexInfo = portion.GetSchema(index)->GetIndexInfo();
        AFL_VERIFY(portion.MutableMeta().LoadMetadata(metaProto, indexInfo, db.GetDsGroupSelectorVerified()));
        AFL_VERIFY(Context->MutableConstructors().AddConstructorVerified(std::move(portion)));
    });
    return true;
}

bool TGranulePortionsReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return db.Table<NColumnShard::Schema::IndexPortions>().Prefix(Self->GetPathId()).Select().IsReady();
}

bool TGranuleColumnsReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    TPortionInfo::TSchemaCursor schema(*VersionedIndex);
    Context->MutableConstructors().ClearColumns();
    return db.LoadColumns(Self->GetPathId(), [&](const TColumnChunkLoadContextV1& loadContext) {
        auto* constructor = Context->MutableConstructors().GetConstructorVerified(loadContext.GetPortionId());
        constructor->LoadRecord(loadContext);
    });
}

bool TGranuleColumnsReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return db.Table<NColumnShard::Schema::IndexColumnsV1>().Prefix(Self->GetPathId()).Select().IsReady();
}

bool TGranuleIndexesReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    Context->MutableConstructors().ClearIndexes();
    return db.LoadIndexes(Self->GetPathId(), [&](const ui64 /*pathId*/, const ui64 portionId, const TIndexChunkLoadContext& loadContext) {
        auto* constructor = Context->MutableConstructors().GetConstructorVerified(portionId);
        constructor->LoadIndex(loadContext);
    });
}

bool TGranuleIndexesReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return db.Table<NColumnShard::Schema::IndexIndexes>().Prefix(Self->GetPathId()).Select().IsReady();
}

bool TGranuleFinishLoading::DoExecute(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) {
    Self->FinishLoading(Context);
    return true;
}

}   // namespace NKikimr::NOlap::NLoading
