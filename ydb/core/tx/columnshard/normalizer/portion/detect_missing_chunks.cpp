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
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexColumnsV1>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexColumnsV2>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
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

    AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "missing_chunks_info")("portion_v1", portionsWithChunksV1.size())(
        "portion_v2", portionsWithChunksV2.size());

    {
        auto rowset = db.Table<Schema::IndexPortions>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        if (rowset.EndOfSet()) {
            AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "empty_portions_index");
        }

        while (!rowset.EndOfSet()) {
            TPortionKey portion(rowset.GetValue<Schema::IndexPortions::PathId>(), rowset.GetValue<Schema::IndexPortions::PortionId>());
            TPortionLoadContext info(rowset);

            if (!portionsWithChunksV1.contains(portion)) {
                AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "missing_portion_v1")("path_id", info.GetPathId())(
                    "portion_id", info.GetPortionId())("meta", info.GetMetaProto().DebugString());
            }

            if (!portionsWithChunksV2.contains(portion)) {
                AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "missing_portion_v2")("path_id", info.GetPathId())(
                    "portion_id", info.GetPortionId())("meta", info.GetMetaProto().DebugString());
            }

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    return std::vector<INormalizerTask::TPtr>();
}

}   // namespace NKikimr::NOlap
