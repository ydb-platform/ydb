#pragma once
#include "batch_slice.h"
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

class TRBSplitLimiter {
private:
    std::deque<TBatchSerializedSlice> Slices;
    const NColumnShard::TIndexationCounters Counters;
    std::shared_ptr<arrow::RecordBatch> Batch;
public:
    TRBSplitLimiter(const TGranuleMeta* granuleMeta, const NColumnShard::TIndexationCounters& counters,
        ISchemaDetailInfo::TPtr schemaInfo, const std::shared_ptr<arrow::RecordBatch> batch);

    bool Next(std::vector<std::vector<TOrderedColumnChunk>>& portionBlobs, std::shared_ptr<arrow::RecordBatch>& batch);
};

}
