#include "ro_controller.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/gc.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/indexation.h>
#include <ydb/core/tx/columnshard/engines/changes/ttl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NYDBTest::NColumnShard {

bool TReadOnlyController::DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (batch) {
        FilteredRecordsCount.Add(batch->num_rows());
    }
    return true;
}

bool TReadOnlyController::DoOnWriteIndexComplete(const NOlap::TColumnEngineChanges& change, const ::NKikimr::NColumnShard::TColumnShard& /*shard*/) {
    if (change.TypeString() == NOlap::TTTLColumnEngineChanges::StaticTypeName()) {
        TTLFinishedCounter.Inc();
    }
    if (change.TypeString() == NOlap::TInsertColumnEngineChanges::StaticTypeName()) {
        InsertFinishedCounter.Inc();
    }
    if (change.TypeString() == NOlap::TCompactColumnEngineChanges::StaticTypeName()) {
        CompactionFinishedCounter.Inc();
        AFL_VERIFY(CompactionsLimit.Dec() >= 0);
    }
    return true;
}

bool TReadOnlyController::DoOnWriteIndexStart(const ui64 tabletId, NOlap::TColumnEngineChanges& change) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", change.TypeString())("tablet_id", tabletId);
    if (change.TypeString() == NOlap::TTTLColumnEngineChanges::StaticTypeName()) {
        TTLStartedCounter.Inc();
    }
    if (change.TypeString() == NOlap::TInsertColumnEngineChanges::StaticTypeName()) {
        InsertStartedCounter.Inc();
    }
    if (change.TypeString() == NOlap::TCompactColumnEngineChanges::StaticTypeName()) {
        CompactionStartedCounter.Inc();
    }
    return true;
}

}
