#pragma once
#include "conveyor_task.h"
#include "granule.h"
#include "processing_context.h"
#include "order_control/abstract.h"
#include <util/generic/hash.h>

namespace NKikimr::NOlap {
class TIndexedReadData;
}

namespace NKikimr::NOlap::NIndexedReader {

class TGranulesFillingContext {
private:
    YDB_READONLY_DEF(std::vector<std::string>, PKColumnNames);
    TReadMetadata::TConstPtr ReadMetadata;
    bool StartedFlag = false;
    const bool InternalReading = false;
    TProcessingController Processing;
    TResultController Result;
    TIndexedReadData& Owner;
    std::set<ui32> EarlyFilterColumns;
    std::set<ui32> PostFilterColumns;
    std::set<ui32> FilterStageColumns;
    std::set<ui32> UsedColumns;
    IOrderPolicy::TPtr SortingPolicy;
    NColumnShard::TScanCounters Counters;
    bool PredictEmptyAfterFilter(const TPortionInfo& portionInfo) const;

    static constexpr ui32 GranulesCountProcessingLimit = 16;
    static constexpr ui64 ExpectedBytesForGranule = 200 * 1024 * 1024;
    static constexpr i64 ProcessingBytesLimit = GranulesCountProcessingLimit * ExpectedBytesForGranule;
    bool CheckBufferAvailable() const;
public:
    bool TryStartProcessGranule(const ui64 granuleId, const TBlobRange& range);
    TGranulesFillingContext(TReadMetadata::TConstPtr readMetadata, TIndexedReadData & owner, const bool internalReading);

    void OnBlobReady(const ui64 /*granuleId*/, const TBlobRange& /*range*/) noexcept {
    }

    TReadMetadata::TConstPtr GetReadMetadata() const noexcept {
        return ReadMetadata;
    }

    const std::set<ui32>& GetEarlyFilterColumns() const noexcept {
        return EarlyFilterColumns;
    }

    const std::set<ui32>& GetPostFilterColumns() const noexcept {
        return PostFilterColumns;
    }

    IOrderPolicy::TPtr GetSortingPolicy() const noexcept {
        return SortingPolicy;
    }

    NColumnShard::TScanCounters GetCounters() const noexcept {
        return Counters;
    }

    NColumnShard::TDataTasksProcessorContainer GetTasksProcessor() const;

    void DrainNotIndexedBatches(THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>* batches);
    NIndexedReader::TBatch* GetBatchInfo(const TBatchAddress& address);

    void AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch);
    void OnBatchReady(const NIndexedReader::TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch);

    TGranule::TPtr GetGranuleVerified(const ui64 granuleId) {
        return Processing.GetGranuleVerified(granuleId);
    }

    bool IsFinished() const {
        return Processing.IsFinished() && !Result.GetCount();
    }

    void OnNewBatch(TBatch& batch) {
        Y_VERIFY(!StartedFlag);
        if (!InternalReading && PredictEmptyAfterFilter(batch.GetPortionInfo())) {
            batch.ResetNoFilter(FilterStageColumns);
        } else {
            batch.ResetNoFilter(UsedColumns);
        }
    }

    std::vector<TGranule::TPtr> DetachReadyInOrder();

    void Abort() {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "abort");
        Processing.Abort();
        Result.Clear();
        Y_VERIFY(IsFinished());
    }

    TGranule::TPtr UpsertGranule(const ui64 granuleId) {
        Y_VERIFY(!StartedFlag);
        auto g = Processing.GetGranule(granuleId);
        if (!g) {
            return Processing.InsertGranule(std::make_shared<TGranule>(granuleId, *this));
        } else {
            return g;
        }
    }

    void OnGranuleReady(const ui64 granuleId);

    void Wakeup(TGranule& granule) {
        SortingPolicy->Wakeup(granule, *this);
    }

    void PrepareForStart() {
        Y_VERIFY(!StartedFlag);
        StartedFlag = true;
        SortingPolicy->Fill(*this);
    }
};

}
