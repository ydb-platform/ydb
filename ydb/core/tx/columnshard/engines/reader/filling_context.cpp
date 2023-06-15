#include "filling_context.h"
#include "filling_context.h"
#include "order_control/not_sorted.h"
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>

namespace NKikimr::NOlap::NIndexedReader {

TGranulesFillingContext::TGranulesFillingContext(TReadMetadata::TConstPtr readMetadata, TIndexedReadData& owner, const bool internalReading)
    : ReadMetadata(readMetadata)
    , InternalReading(internalReading)
    , Owner(owner)
    , Counters(owner.GetCounters())
{
    SortingPolicy = InternalReading ? std::make_shared<TNonSorting>(ReadMetadata) : ReadMetadata->BuildSortingPolicy();

    UsedColumns = ReadMetadata->GetUsedColumnIds();
    EarlyFilterColumns = ReadMetadata->GetEarlyFilterColumnIds();
    FilterStageColumns = SortingPolicy->GetFilterStageColumns();
    PKColumnNames = ReadMetadata->GetReplaceKey()->field_names();

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

NIndexedReader::TBatch* TGranulesFillingContext::GetBatchInfo(const TBatchAddress& address) {
    auto it = GranulesWaiting.find(address.GetGranuleId());
    if (it == GranulesWaiting.end()) {
        return nullptr;
    } else {
        return &it->second->GetBatchInfo(address.GetBatchGranuleIdx());
    }
}

NKikimr::NColumnShard::TDataTasksProcessorContainer TGranulesFillingContext::GetTasksProcessor() const {
    return Owner.GetTasksProcessor();
}

void TGranulesFillingContext::DrainNotIndexedBatches(THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>* batches) {
    for (auto&& [_, gPtr] : GranulesWaiting) {
        if (!batches) {
            gPtr->AddNotIndexedBatch(nullptr);
        } else {
            auto it = batches->find(gPtr->GetGranuleId());
            if (it == batches->end()) {
                gPtr->AddNotIndexedBatch(nullptr);
            } else {
                gPtr->AddNotIndexedBatch(it->second);
            }
            batches->erase(it);
        }
    }
}

bool TGranulesFillingContext::CanProcessMore() const {
    return GranulesInProcessing.size() <= GranulesCountProcessingLimit ||
        BlobsSizeInProcessing <= ProcessingBytesLimit
        ;
}

}
