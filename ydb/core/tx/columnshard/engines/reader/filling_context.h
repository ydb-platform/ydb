#pragma once
#include "conveyor_task.h"
#include "granule.h"
#include "order_controller.h"
#include <util/generic/hash.h>

namespace NKikimr::NOlap {
class TIndexedReadData;
}

namespace NKikimr::NOlap::NIndexedReader {

class TGranulesFillingContext {
private:
    bool AbortedFlag = false;
    TReadMetadata::TConstPtr ReadMetadata;
    const bool InternalReading = false;
    TIndexedReadData& Owner;
    THashMap<ui64, NIndexedReader::TGranule*> GranulesToOut;
    std::set<ui64> ReadyGranulesAccumulator;
    THashMap<ui64, NIndexedReader::TGranule> Granules;
    std::set<ui32> EarlyFilterColumns;
    std::set<ui32> PostFilterColumns;
    std::set<ui32> FilterStageColumns;
    std::set<ui32> UsedColumns;
    IOrderPolicy::TPtr SortingPolicy;
    NColumnShard::TScanCounters Counters;
    std::vector<NIndexedReader::TBatch*> Batches;

    bool PredictEmptyAfterFilter(const TPortionInfo& portionInfo) const;

public:
    TGranulesFillingContext(TReadMetadata::TConstPtr readMetadata, TIndexedReadData& owner, const bool internalReading, const ui32 batchesCount);

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
    TBatch& GetBatchInfo(const ui32 batchNo);

    void AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch);
    void OnBatchReady(const NIndexedReader::TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch);

    NIndexedReader::TGranule& GetGranuleVerified(const ui64 granuleId) {
        auto it = Granules.find(granuleId);
        Y_VERIFY(it != Granules.end());
        return it->second;
    }

    bool IsInProgress() const { return Granules.size() > ReadyGranulesAccumulator.size(); }

    void OnNewBatch(TBatch& batch) {
        if (!InternalReading && PredictEmptyAfterFilter(batch.GetPortionInfo())) {
            batch.ResetNoFilter(FilterStageColumns);
        } else {
            batch.ResetNoFilter(UsedColumns);
        }
        Batches[batch.GetBatchNo()] = &batch;
    }

    std::vector<TGranule*> DetachReadyInOrder() {
        Y_VERIFY(SortingPolicy);
        return SortingPolicy->DetachReadyGranules(GranulesToOut);
    }

    void Abort() {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "abort");
        for (auto&& i : Granules) {
            ReadyGranulesAccumulator.emplace(i.first);
        }
        AbortedFlag = true;
        Y_VERIFY(ReadyGranulesAccumulator.size() == Granules.size());
        Y_VERIFY(!IsInProgress());
    }

    TGranule& UpsertGranule(const ui64 granuleId) {
        auto itGranule = Granules.find(granuleId);
        if (itGranule == Granules.end()) {
            itGranule = Granules.emplace(granuleId, NIndexedReader::TGranule(granuleId, *this)).first;
        }
        return itGranule->second;
    }

    void OnGranuleReady(TGranule& granule) {
        Y_VERIFY(GranulesToOut.emplace(granule.GetGranuleId(), &granule).second);
        Y_VERIFY(ReadyGranulesAccumulator.emplace(granule.GetGranuleId()).second || AbortedFlag);
    }

    void Wakeup(TGranule& granule) {
        SortingPolicy->Wakeup(granule, *this);
    }

    void PrepareForStart() {
        SortingPolicy->Fill(*this);
    }
};

}
