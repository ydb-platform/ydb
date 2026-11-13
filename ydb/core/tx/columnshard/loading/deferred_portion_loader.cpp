#include "deferred_portion_loader.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/index_portions_by_last_key.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NColumnShard {

bool TDeferredPortionLoader::DecideOnStart() {
    Enabled = HasAppData() && AppDataVerified().ColumnShardConfig.GetDeferredPortionLoadingSettings().GetEnabled();
    for (const auto& [_, granule] : TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetTables()) {
        // in_mem reads the column chunks at the start, so it needs all the portions loaded right away
        Enabled &= !granule->LoadsChunksAtStart();
    }
    return Enabled;
}

bool TDeferredPortionLoader::Start() {
    AFL_VERIFY(Enabled);
    AFL_VERIFY(TablesToLoad.empty());
    for (const auto& [pathId, granule] : TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetTables()) {
        if (!granule->GetLoadedKeyRanges().CoversAll()) {
            TablesToLoad.emplace_back(pathId);
        }
    }
    return !TablesToLoad.empty();
}

bool TDeferredPortionLoader::TryStartBatch() {
    if (BatchInFlight || TablesToLoad.empty()) {
        return false;
    }
    // a test holds the loading here; the periodic wakeup resumes it
    if (!NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::PortionLoading)) {
        return false;
    }
    BatchInFlight = true;
    return true;
}

bool TDeferredPortionLoader::ReadBatch(
    NTabletFlatExecutor::TTransactionContext& txc, const NOlap::IBlobGroupSelector& dsGroupSelector, TBatch& batch) const {
    AFL_VERIFY(BatchInFlight);
    const auto& engine = TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
    batch.PathId = TablesToLoad.front();
    batch.Portions.clear();
    const auto below = NextBatchBelow.find(batch.PathId);
    std::vector<ui64> portionIds;
    const ui32 rowsLimit = std::max<ui32>(1, AppDataVerified().ColumnShardConfig.GetDeferredPortionLoadingSettings().GetPortionsPerBatch());
    if (!NOlap::TIndexPortionsByLastKey::ReadBatch(txc.DB, batch.PathId,
            below == NextBatchBelow.end() ? std::nullopt : std::optional<TOwnedCellVec>(below->second), rowsLimit, portionIds, batch.LastKey)) {
        return false;
    }
    NIceDb::TNiceDb db(txc.DB);
    // every row is touched before the restart, so that one restart requests all missing pages
    bool ready = true;
    for (const ui64 portionId : portionIds) {
        ready &= db.Table<Schema::IndexPortions>().Key(batch.PathId.GetRawValue(), portionId).Select().IsReady();
    }
    if (!ready) {
        return false;
    }
    NOlap::TDbWrapper wrapper(txc.DB, &dsGroupSelector);
    const NOlap::TVersionedIndex& versionedIndex = engine.GetVersionedIndex();
    for (const ui64 portionId : portionIds) {
        AFL_VERIFY(wrapper.LoadPortionVerified(NOlap::TPortionAddress(batch.PathId, portionId),
            [&](std::unique_ptr<NOlap::TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
                const NOlap::TIndexInfo& indexInfo = portion->GetSchema(versionedIndex)->GetIndexInfo();
                AFL_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, dsGroupSelector));
                batch.Portions.emplace_back(portion->Build());
            }));
    }
    return true;
}

bool TDeferredPortionLoader::OnBatchLoaded(TBatch&& batch) {
    AFL_VERIFY(BatchInFlight);
    AFL_VERIFY(!TablesToLoad.empty() && TablesToLoad.front() == batch.PathId)("path_id", batch.PathId);
    BatchInFlight = false;
    ++BatchesLoaded;
    TablesToLoad.pop_front();
    auto& engine = TablesManager.MutablePrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
    if (!batch.LastKey) {
        engine.AddPortionsLoadedInBatch(batch.PathId, batch.Portions, NOlap::TLoadedKeyRanges::BuildEverything());
        NextBatchBelow.erase(batch.PathId);
    } else {
        // the batch ends with every portion whose last key is LastKey
        AFL_VERIFY(!batch.Portions.empty());
        engine.AddPortionsLoadedInBatch(
            batch.PathId, batch.Portions, NOlap::TLoadedKeyRanges::BuildFrom(batch.Portions.back()->GetMeta().IndexKeyEnd()));
        NextBatchBelow.insert_or_assign(batch.PathId, std::move(*batch.LastKey));
        TablesToLoad.emplace_back(batch.PathId);
    }
    return TablesToLoad.empty();
}

}   // namespace NKikimr::NColumnShard
