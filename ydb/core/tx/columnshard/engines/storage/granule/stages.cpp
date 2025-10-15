#include "granule.h"
#include "stages.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NLoading {

bool TGranuleOnlyPortionsReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    std::vector<TPortionInfo::TPtr> portions;
    if (!db.LoadPortions(Self->GetPathId(),
            [&](std::unique_ptr<TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
                const TIndexInfo& indexInfo = portion->GetSchema(*VersionedIndex)->GetIndexInfo();
                AFL_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, *DsGroupSelector));
                portions.emplace_back(portion->Build());
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
    return db.Table<NColumnShard::Schema::IndexPortions>().Prefix(Self->GetPathId().GetRawValue()).Select().IsReady();
}

bool TGranuleColumnsReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    return db.LoadColumns(Self->GetPathId(), [&](TColumnChunkLoadContextV2&& loadContext) {
        Context->Add(std::move(loadContext));
    });
}

bool TGranuleColumnsReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return db.Table<NColumnShard::Schema::IndexColumnsV2>().Prefix(Self->GetPathId().GetRawValue()).Select().IsReady();
}

bool TGranuleIndexesReader::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    TDbWrapper db(txc.DB, &*DsGroupSelector);
    return db.LoadIndexes(
        Self->GetPathId(), [&](const TInternalPathId /*pathId*/, const ui64 /*portionId*/, TIndexChunkLoadContext&& loadContext) {
            Context->Add(std::move(loadContext));
        });
}

bool TGranuleIndexesReader::DoPrecharge(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NIceDb::TNiceDb db(txc.DB);
    return db.Table<NColumnShard::Schema::IndexIndexes>().Prefix(Self->GetPathId().GetRawValue()).Select().IsReady();
}

bool TGranuleFinishAccessorsLoading::DoExecute(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) {
    THashMap<ui64, TPortionDataAccessors> constructors = Context->ExtractConstructors();
    AFL_VERIFY(Self->GetPortions().size() == constructors.size());
    for (auto&& i : Self->GetPortions()) {
        auto it = constructors.find(i.first);
        AFL_VERIFY(it != constructors.end());
        auto accessor =
            TPortionAccessorConstructor::BuildForLoading(i.second, it->second.DetachBuildInfo(), std::move(it->second.MutableIndexes()));
        Self->GetDataAccessorsManager()->AddPortion(accessor);
    }
    return true;
}

bool TGranuleFinishCommonLoading::DoExecute(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) {
    AFL_VERIFY(!Started);
    Started = true;
    Self->OnAfterPortionsLoad();
    return true;
}

}   // namespace NKikimr::NOlap::NLoading
