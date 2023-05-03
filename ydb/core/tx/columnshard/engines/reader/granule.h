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
    YDB_READONLY(ui64, GranuleId, 0);
    std::vector<std::shared_ptr<arrow::RecordBatch>> NonSortableBatches;
    std::vector<std::shared_ptr<arrow::RecordBatch>> SortableBatches;
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, NotIndexedBatch);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, NotIndexedBatchFutureFilter);
    YDB_FLAG_ACCESSOR(DuplicationsAvailable, false);
    YDB_READONLY_FLAG(Ready, false);
    std::deque<TBatch> Batches;
    std::set<ui32> WaitBatches;
    std::set<ui32> GranuleBatchNumbers;
    TGranulesFillingContext* Owner = nullptr;
    YDB_READONLY_DEF(THashSet<const void*>, BatchesToDedup);
public:
    TGranule(const ui64 granuleId, TGranulesFillingContext& owner)
        : GranuleId(granuleId)
        , Owner(&owner) {
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
