#include "granule.h"
#include "stages.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NLoading {

bool TGranuleOnlyPortionsReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    std::vector<TPortionInfo::TPtr> portions;
    if (!db.LoadPortions(Self->GetPathId(), [&](TPortionInfoConstructor&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
        const TIndexInfo& indexInfo = portion.GetSchema(*VersionedIndex)->GetIndexInfo();
        AFL_VERIFY(portion.MutableMeta().LoadMetadata(metaProto, indexInfo, *DsGroupSelector));
        portions.emplace_back(portion.BuildPortionPtr());
    })) {
        return false;
    }
    for (auto&& i : portions) {
        Self->UpsertPortionOnLoad(i);
    }
    return true;
}

bool TGranuleOnlyPortionsReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return db.Table<NColumnShard::Schema::IndexPortions>().Prefix(Self->GetPathId()).Select().IsReady();
}

bool TGranulePortionsReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    const TVersionedIndex& index = *VersionedIndex;
    Context->MutableConstructors().ClearPortions();
    return db.LoadPortions(Self->GetPathId(), [&](TPortionInfoConstructor&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
        const TIndexInfo& indexInfo = portion.GetSchema(index)->GetIndexInfo();
        AFL_VERIFY(portion.MutableMeta().LoadMetadata(metaProto, indexInfo, db.GetDsGroupSelectorVerified()));
        AFL_VERIFY(Context->MutableConstructors().AddConstructorVerified(std::move(portion)));
    });
}

bool TGranulePortionsReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return db.Table<NColumnShard::Schema::IndexPortions>().Prefix(Self->GetPathId()).Select().IsReady();
}

bool TGranuleColumnsReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    TPortionInfo::TSchemaCursor schema(*VersionedIndex);
    Context->MutableConstructors().ClearColumns();
    return db.LoadColumns(Self->GetPathId(), [&](TColumnChunkLoadContextV1&& loadContext) {
        auto* constructor = Context->MutableConstructors().GetConstructorVerified(loadContext.GetPortionId());
        constructor->LoadRecord(std::move(loadContext));
    });
}

bool TGranuleColumnsReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return db.Table<NColumnShard::Schema::IndexColumnsV1>().Prefix(Self->GetPathId()).Select().IsReady();
}

bool TGranuleIndexesReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    Context->MutableConstructors().ClearIndexes();
    return db.LoadIndexes(Self->GetPathId(), [&](const ui64 /*pathId*/, const ui64 portionId, TIndexChunkLoadContext&& loadContext) {
        auto* constructor = Context->MutableConstructors().GetConstructorVerified(portionId);
        constructor->LoadIndex(std::move(loadContext));
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
