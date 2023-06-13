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
    bool AbortedFlag = false;
    TReadMetadata::TConstPtr ReadMetadata;
    const bool InternalReading = false;
    TIndexedReadData& Owner;
    THashMap<ui64, NIndexedReader::TGranule*> GranulesToOut;
    std::set<ui64> ReadyGranulesAccumulator;
    std::deque<NIndexedReader::TGranule> GranulesStorage;
    THashMap<ui64, NIndexedReader::TGranule*> GranulesUpserted;
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
    TBatch& GetBatchInfo(const TBatchAddress& address);

    void AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch);
    void OnBatchReady(const NIndexedReader::TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch);

    NIndexedReader::TGranule& GetGranuleVerified(const ui64 granuleId) {
        auto it = GranulesUpserted.find(granuleId);
        Y_VERIFY(it != GranulesUpserted.end());
        return *it->second;
    }

    bool IsInProgress() const { return GranulesStorage.size() > ReadyGranulesAccumulator.size(); }

    void OnNewBatch(TBatch& batch) {
        if (!InternalReading && PredictEmptyAfterFilter(batch.GetPortionInfo())) {
            batch.ResetNoFilter(FilterStageColumns);
        } else {
            batch.ResetNoFilter(UsedColumns);
        }
    }

    std::vector<TGranule*> DetachReadyInOrder() {
        Y_VERIFY(SortingPolicy);
        return SortingPolicy->DetachReadyGranules(GranulesToOut);
    }

    void Abort() {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "abort");
        for (auto&& i : GranulesStorage) {
            ReadyGranulesAccumulator.emplace(i.GetGranuleId());
        }
        AbortedFlag = true;
        Y_VERIFY(ReadyGranulesAccumulator.size() == GranulesStorage.size());
        Y_VERIFY(!IsInProgress());
    }

    TGranule& UpsertGranule(const ui64 granuleId) {
        auto itGranule = GranulesUpserted.find(granuleId);
        if (itGranule == GranulesUpserted.end()) {
            GranulesStorage.emplace_back(NIndexedReader::TGranule(granuleId, GranulesStorage.size(), *this));
            itGranule = GranulesUpserted.emplace(granuleId, &GranulesStorage.back()).first;
        }
        return *itGranule->second;
    }

    void OnGranuleReady(TGranule& granule) {
        Y_VERIFY(GranulesToOut.emplace(granule.GetGranuleId(), &granule).second);
        Y_VERIFY(ReadyGranulesAccumulator.emplace(granule.GetGranuleId()).second || AbortedFlag);
        Y_VERIFY(GranulesInProcessing.erase(granule.GetGranuleId()));
        BlobsSizeInProcessing -= granule.GetBlobsDataSize();
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
