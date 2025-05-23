#include "detect_missing_chunks.h"
#include "normalizer.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/counters/portion_index.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap {

TConclusion<std::vector<INormalizerTask::TPtr>> TDetectMissingChunks::DoInit(
    const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexColumnsV1>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexColumnsV2>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    TTablesManager tablesManager(controller.GetStoragesManager(), std::make_shared<NDataAccessorControl::TLocalManager>(nullptr),
        std::make_shared<TSchemaObjectsCache>(), std::make_shared<TPortionIndexStats>(), 0);
    if (!tablesManager.InitFromDB(db)) {
        ACFL_TRACE("normalizer", "TChunksV0MetaNormalizer")("error", "can't initialize tables manager");
        return TConclusionStatus::Fail("Can't load index");
    }

    THashSet<TPortionKey> portionsWithChunksV1;
    {
        auto rowset = db.Table<Schema::IndexColumnsV1>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            portionsWithChunksV1.insert(
                TPortionKey(rowset.GetValue<Schema::IndexColumnsV1::PathId>(), rowset.GetValue<Schema::IndexColumnsV1::PortionId>()));

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    THashSet<ui64> portionsWithChunksV2;
    {
        auto rowset = db.Table<Schema::IndexColumnsV2>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            portionsWithChunksV2.insert(
                TPortionKey(rowset.GetValue<Schema::IndexColumnsV2::PathId>(), rowset.GetValue<Schema::IndexColumnsV2::PortionId>()));

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    THashMap<ui64, TPortionAccessorConstructor> constructors;
    TDbWrapper wrapper(db.GetDatabase(), nullptr);
    if (!wrapper.LoadPortions(
            {}, [&](std::unique_ptr<TPortionInfoConstructor>&& portion, const NKikimrTxColumnShard::TIndexPortionMeta& metaProto) {
                const TIndexInfo& indexInfo =
                    portion->GetSchema(tablesManager.GetPrimaryIndexAsVerified<TColumnEngineForLogs>().GetVersionedIndex())->GetIndexInfo();
                AFL_VERIFY(portion->MutableMeta().LoadMetadata(metaProto, indexInfo, *DsGroupSelector));
                const ui64 portionId = portion->GetPortionIdVerified();
                AFL_VERIFY(constructors.emplace(portionId, TPortionAccessorConstructor(std::move(portion))).second);
            })) {
        return TConclusionStatus::Fail("repeated read db");
    }

    AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "missing_chunks_info")("portion_v1", portionsWithChunksV1.size())(
        "portion_v2", portionsWithChunksV2.size())("portions", constructors.size());

    for (auto& [id, portion] : constructors) {
        if (!portionsWithChunksV2.contains(TPortionKey(
                portion.GetPortionConstructor().GetPathId().GetRawValue(), portion.GetPortionConstructor().GetPortionIdVerified()))) {
            auto data = portion.MutablePortionConstructor().Build();
            AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("aboba", "missing_chunks")("portion", data->DebugString(true));
        }
    }

    return std::vector<INormalizerTask::TPtr>();
}

}   // namespace NKikimr::NOlap
