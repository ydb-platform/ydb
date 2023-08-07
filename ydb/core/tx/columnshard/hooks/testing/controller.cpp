#include "controller.h"
#include <ydb/core/tx/columnshard/engines/reader/order_control/pk_with_limit.h>
#include <ydb/core/tx/columnshard/engines/reader/order_control/default.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/in_granule_compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/split_compaction.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NYDBTest::NColumnShard {

bool TController::DoOnSortingPolicy(std::shared_ptr<NOlap::NIndexedReader::IOrderPolicy> policy) {
    if (dynamic_cast<const NOlap::NIndexedReader::TPKSortingWithLimit*>(policy.get())) {
        SortingWithLimit.Inc();
    } else if (dynamic_cast<const NOlap::NIndexedReader::TAnySorting*>(policy.get())) {
        AnySorting.Inc();
    } else {
        Y_VERIFY(false);
    }
    return true;
}

bool TController::HasPKSortingOnly() const {
    return SortingWithLimit.Val() && !AnySorting.Val();
}

bool TController::DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (batch) {
        FilteredRecordsCount.Add(batch->num_rows());
    }
    return true;
}

bool TController::DoOnStartCompaction(const std::shared_ptr<NOlap::TColumnEngineChanges>& changes) {
    if (dynamic_pointer_cast<NOlap::TInGranuleCompactColumnEngineChanges>(changes)) {
        InternalCompactions.Inc();
    }
    if (dynamic_pointer_cast<NOlap::TSplitCompactColumnEngineChanges>(changes)) {
        SplitCompactions.Inc();
    }
    return true;
}

}
