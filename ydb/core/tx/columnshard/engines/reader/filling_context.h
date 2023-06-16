#pragma once
#include "conveyor_task.h"
#include "granule.h"
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
    const bool InternalReading = false;
    bool NotIndexedBatchesInitialized = false;
    TIndexedReadData& Owner;
    THashMap<ui64, NIndexedReader::TGranule::TPtr> GranulesToOut;
    std::set<ui64> ReadyGranulesAccumulator;
    THashMap<ui64, NIndexedReader::TGranule::TPtr> GranulesWaiting;
    std::set<ui32> EarlyFilterColumns;
    std::set<ui32> PostFilterColumns;
    std::set<ui32> FilterStageColumns;
    std::set<ui32> UsedColumns;
    IOrderPolicy::TPtr SortingPolicy;
    NColumnShard::TScanCounters Counters;
    std::set<ui64> GranulesInProcessing;
    i64 BlobsSizeInProcessing = 0;
    bool PredictEmptyAfterFilter(const TPortionInfo& portionInfo) const;

    static constexpr ui32 GranulesCountProcessingLimit = 16;
    static constexpr ui64 ExpectedBytesForGranule = 200 * 1024 * 1024;
    static constexpr i64 ProcessingBytesLimit = GranulesCountProcessingLimit * ExpectedBytesForGranule;
public:
    TGranulesFillingContext(TReadMetadata::TConstPtr readMetadata, TIndexedReadData& owner, const bool internalReading);

    bool CanProcessMore() const;
    
    void OnBlobReady(const ui64 granuleId, const TBlobRange& range) noexcept {
        GranulesInProcessing.emplace(granuleId);
        BlobsSizeInProcessing += range.Size;
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
        auto it = GranulesWaiting.find(granuleId);
        Y_VERIFY(it != GranulesWaiting.end());
        return it->second;
    }

    bool IsInProgress() const { return GranulesWaiting.size(); }

    void OnNewBatch(TBatch& batch) {
        if (!InternalReading && PredictEmptyAfterFilter(batch.GetPortionInfo())) {
            batch.ResetNoFilter(FilterStageColumns);
        } else {
            batch.ResetNoFilter(UsedColumns);
        }
    }

    std::vector<TGranule::TPtr> DetachReadyInOrder() {
        Y_VERIFY(SortingPolicy);
        return SortingPolicy->DetachReadyGranules(GranulesToOut);
    }

    void Abort() {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "abort");
        GranulesWaiting.clear();
        Y_VERIFY(!IsInProgress());
    }

    TGranule::TPtr UpsertGranule(const ui64 granuleId) {
        auto itGranule = GranulesWaiting.find(granuleId);
        if (itGranule == GranulesWaiting.end()) {
            itGranule = GranulesWaiting.emplace(granuleId, std::make_shared<TGranule>(granuleId, *this)).first;
        }
        return itGranule->second;
    }

    void OnGranuleReady(const ui64 granuleId) {
        auto granule = GetGranuleVerified(granuleId);
        Y_VERIFY(GranulesToOut.emplace(granule->GetGranuleId(), granule).second);
        Y_VERIFY(ReadyGranulesAccumulator.emplace(granule->GetGranuleId()).second);
        Y_VERIFY(GranulesWaiting.erase(granuleId));
        GranulesInProcessing.erase(granule->GetGranuleId());
        BlobsSizeInProcessing -= granule->GetBlobsDataSize();
        Y_VERIFY(BlobsSizeInProcessing >= 0);
    }

    void Wakeup(TGranule& granule) {
        SortingPolicy->Wakeup(granule, *this);
    }

    void PrepareForStart() {
        SortingPolicy->Fill(*this);
    }
};

}
