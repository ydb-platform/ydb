#include "filling_context.h"
#include "filling_context.h"
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>

namespace NKikimr::NOlap::NIndexedReader {

TGranulesFillingContext::TGranulesFillingContext(TReadMetadata::TConstPtr readMetadata, TIndexedReadData& owner, const bool internalReading, const ui32 batchesCount)
    : ReadMetadata(readMetadata)
    , InternalReading(internalReading)
    , Owner(owner)
    , Counters(owner.GetCounters())
{
    Batches.resize(batchesCount, nullptr);
    SortingPolicy = InternalReading ? std::make_shared<TNonSorting>(ReadMetadata) : ReadMetadata->BuildSortingPolicy();

    UsedColumns = ReadMetadata->GetUsedColumnIds();
    EarlyFilterColumns = ReadMetadata->GetEarlyFilterColumnIds();
    FilterStageColumns = SortingPolicy->GetFilterStageColumns();

    PostFilterColumns = ReadMetadata->GetUsedColumnIds();
    for (auto&& i : FilterStageColumns) {
        PostFilterColumns.erase(i);
    }
}

bool TGranulesFillingContext::PredictEmptyAfterFilter(const TPortionInfo& portionInfo) const {
    if (!portionInfo.AllowEarlyFilter()) {
        return false;
    }
    if (EarlyFilterColumns.empty()) {
        return false;
    }
    if (TIndexInfo::IsSpecialColumns(EarlyFilterColumns)) {
        return false;
    }
    return true;
}

void TGranulesFillingContext::AddBlobForFetch(const TBlobRange& range, NIndexedReader::TBatch& batch) {
    return Owner.AddBlobForFetch(range, batch);
}

void TGranulesFillingContext::OnBatchReady(const NIndexedReader::TBatch& batchInfo, std::shared_ptr<arrow::RecordBatch> batch) {
    return Owner.OnBatchReady(batchInfo, batch);
}

NKikimr::NOlap::NIndexedReader::TBatch& TGranulesFillingContext::GetBatchInfo(const ui32 batchNo) {
    Y_VERIFY(batchNo < Batches.size());
    auto ptr = Batches[batchNo];
    Y_VERIFY(ptr);
    return *ptr;
}

NKikimr::NColumnShard::TDataTasksProcessorContainer TGranulesFillingContext::GetTasksProcessor() const {
    return Owner.GetTasksProcessor();
}

void TGranulesFillingContext::AddNotIndexedBatches(THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>& batches) {
    std::shared_ptr<arrow::RecordBatch> externalBatch;
    for (auto it = batches.begin(); it != batches.end(); ++it) {
        if (!it->first) {
            externalBatch = it->second;
            continue;
        }
        auto itGranule = Granules.find(it->first);
        Y_VERIFY(itGranule != Granules.end());
        itGranule->second.AddNotIndexedBatch(it->second);
    }
    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> resultLocal;
    if (externalBatch) {
        resultLocal.emplace(0, externalBatch);
    }
    std::swap(batches, resultLocal);
}

}
