#pragma once
#include "batch.h"
#include "read_metadata.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NIndexedReader {

class TGranulesFillingContext;

class TGranule {
private:
    ui64 GranuleId = 0;

    bool NotIndexedBatchReadyFlag = false;
    std::shared_ptr<arrow::RecordBatch> NotIndexedBatch;
    std::shared_ptr<NArrow::TColumnFilter> NotIndexedBatchFutureFilter;

    std::vector<std::shared_ptr<arrow::RecordBatch>> NonSortableBatches;
    std::vector<std::shared_ptr<arrow::RecordBatch>> SortableBatches;
    bool DuplicationsAvailableFlag = false;
    bool ReadyFlag = false;
    std::deque<TBatch> Batches;
    std::set<ui32> WaitBatches;
    std::set<ui32> GranuleBatchNumbers;
    TGranulesFillingContext* Owner = nullptr;
    THashSet<const void*> BatchesToDedup;

    void CheckReady();
public:
    TGranule(const ui64 granuleId, TGranulesFillingContext& owner)
        : GranuleId(granuleId)
        , Owner(&owner) {
    }

    ui64 GetGranuleId() const noexcept {
        return GranuleId;
    }

    const THashSet<const void*>& GetBatchesToDedup() const noexcept {
        return BatchesToDedup;
    }

    const std::shared_ptr<arrow::RecordBatch>& GetNotIndexedBatch() const noexcept {
        return NotIndexedBatch;
    }

    const std::shared_ptr<NArrow::TColumnFilter>& GetNotIndexedBatchFutureFilter() const noexcept {
        return NotIndexedBatchFutureFilter;
    }

    bool IsNotIndexedBatchReady() const noexcept {
        return NotIndexedBatchReadyFlag;
    }

    bool IsDuplicationsAvailable() const noexcept {
        return DuplicationsAvailableFlag;
    }

    void SetDuplicationsAvailable(bool val) noexcept {
        DuplicationsAvailableFlag = val;
    }

    bool IsReady() const noexcept {
        return ReadyFlag;
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> GetReadyBatches() const {
        std::vector<std::shared_ptr<arrow::RecordBatch>> result;
        result.reserve(SortableBatches.size() + NonSortableBatches.size() + 1);
        if (NotIndexedBatch) {
            result.emplace_back(NotIndexedBatch);
        }
        result.insert(result.end(), NonSortableBatches.begin(), NonSortableBatches.end());
        result.insert(result.end(), SortableBatches.begin(), SortableBatches.end());
        return result;
    }

    void AddNotIndexedBatch(std::shared_ptr<arrow::RecordBatch> batch);

    const TGranulesFillingContext& GetOwner() const {
        return *Owner;
    }
    std::deque<TBatch*> SortBatchesByPK(const bool reverse, TReadMetadata::TConstPtr readMetadata);

    const std::set<ui32>& GetEarlyFilterColumns() const;
    void OnBatchReady(const TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch);
    bool OnFilterReady(TBatch& batchInfo);
    TBatch& AddBatch(const ui32 batchNo, const TPortionInfo& portionInfo);
    void AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch) const;

};

}
