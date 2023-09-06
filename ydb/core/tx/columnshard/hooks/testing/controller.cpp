#include "controller.h"
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NYDBTest::NColumnShard {

bool TController::DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (batch) {
        FilteredRecordsCount.Add(batch->num_rows());
    }
    return true;
}

bool TController::DoOnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& changes) {
    if (auto compaction = dynamic_pointer_cast<NOlap::TCompactColumnEngineChanges>(changes)) {
        if (compaction->IsSplit()) {
            SplitCompactions.Inc();
        } else {
            InternalCompactions.Inc();
        }
    }
    return true;
}

}
