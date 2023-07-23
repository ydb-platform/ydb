#pragma once
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

class TSplitLimiter {
private:
    static const inline double ReduceCorrectionKff = 0.9;
    static const inline double IncreaseCorrectionKff = 1.1;
    static const inline ui64 ExpectedBlobSize = 6 * 1024 * 1024;
    static const inline ui64 MinBlobSize = 1 * 1024 * 1024;

    const NColumnShard::TIndexationCounters Counters;
    ui32 BaseStepRecordsCount = 0;
    ui32 CurrentStepRecordsCount = 0;
    std::shared_ptr<arrow::RecordBatch> Batch;
    std::vector<TColumnSummary> SortedColumnIds;
    ui32 Position = 0;
    ISnapshotSchema::TPtr Schema;
public:
    TSplitLimiter(const TGranuleMeta* granuleMeta, const NColumnShard::TIndexationCounters& counters,
        ISnapshotSchema::TPtr schema, const std::shared_ptr<arrow::RecordBatch> batch);

    bool Next(std::vector<TString>& portionBlobs, std::shared_ptr<arrow::RecordBatch>& batch, const TSaverContext& saverContext);
};

}
