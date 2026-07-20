#include "ro_controller.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/gc.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_portions.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_tables.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/ttl.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NYDBTest::NColumnShard {

bool TReadOnlyController::DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (batch) {
        FilteredRecordsCount.Add(batch->num_rows());
    }
    return true;
}

void TReadOnlyController::DoOnScanFinished(const ui32 statusFinish) {
    using EStatus = NKikimr::NColumnShard::TScanCounters::EStatusFinish;
    if (statusFinish == (ui32)EStatus::Success) {
        ScanFinishedSuccess.Inc();
    } else if (statusFinish == (ui32)EStatus::ExternalAbort) {
        ScanFinishedExternalAbort.Inc();
    }
}

bool TReadOnlyController::DoOnWriteIndexComplete(
    const NOlap::TColumnEngineChanges& change, const ::NKikimr::NColumnShard::TColumnShard& /*shard*/) {
    if (change.TypeString() == NOlap::TCleanupPortionsColumnEngineChanges::StaticTypeName()) {
        CleaningFinishedCounter.Inc();
    }
    if (change.TypeString() == NOlap::TCleanupTablesColumnEngineChanges::StaticTypeName()) {
        CleaningFinishedCounter.Inc();
    }
    if (change.TypeString() == NOlap::TTTLColumnEngineChanges::StaticTypeName()) {
        TTLFinishedCounter.Inc();
    }
    if (change.TypeString() == NOlap::TCompactColumnEngineChanges::StaticTypeName()) {
        CompactionFinishedCounter.Inc();
        AFL_VERIFY(CompactionsLimit.Dec() >= 0);
    }
    return true;
}

bool TReadOnlyController::DoOnWriteIndexStart(const ui64 tabletId, NOlap::TColumnEngineChanges& change) {
    YDB_LOG_NOTICE("",
        {"event", change.TypeString()},
        {"tabletId", tabletId});
    if (change.TypeString() == NOlap::TCleanupPortionsColumnEngineChanges::StaticTypeName()) {
        CleaningStartedCounter.Inc();
    }
    if (change.TypeString() == NOlap::TCleanupTablesColumnEngineChanges::StaticTypeName()) {
        CleaningStartedCounter.Inc();
    }
    if (change.TypeString() == NOlap::TTTLColumnEngineChanges::StaticTypeName()) {
        TTLStartedCounter.Inc();
    }
    if (change.TypeString() == NOlap::TCompactColumnEngineChanges::StaticTypeName()) {
        CompactionStartedCounter.Inc();
    }
    return true;
}

}   // namespace NKikimr::NYDBTest::NColumnShard
