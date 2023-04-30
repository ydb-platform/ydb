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
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::RecordBatch>>, ReadyBatches);
    YDB_FLAG_ACCESSOR(DuplicationsAvailable, false);
    YDB_READONLY_FLAG(Ready, false);
    std::deque<TBatch> Batches;
    std::set<ui32> WaitBatches;
    std::set<ui32> GranuleBatchNumbers;
    TGranulesFillingContext* Owner = nullptr;
public:
    TGranule(const ui64 granuleId, TGranulesFillingContext& owner)
        : GranuleId(granuleId)
        , Owner(&owner) {
    }

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
