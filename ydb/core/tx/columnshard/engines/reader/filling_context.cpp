#include "filling_context.h"
#include "order_control/not_sorted.h"
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>
#include <ydb/core/tx/columnshard/resources/memory.h>
#include <util/string/join.h>

namespace NKikimr::NOlap::NIndexedReader {

TGranulesFillingContext::TGranulesFillingContext(TReadMetadata::TConstPtr readMetadata, TIndexedReadData& owner, const bool internalReading)
    : ReadMetadata(readMetadata)
    , InternalReading(internalReading)
    , Processing(owner.GetMemoryAccessor(), owner.GetCounters())
    , Result(owner.GetCounters())
    , GranulesLiveContext(std::make_shared<TGranulesLiveControl>())
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
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "TGranulesFillingContext")("used", UsedColumns.size())("early", EarlyFilterColumns.size())
        ("filter_stage", FilterStageColumns.size())("PKColumnNames", JoinSeq(",", PKColumnNames))("post_filter", PostFilterColumns.size());
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
    return Processing.GetBatchInfo(address);
}

NIndexedReader::TBatch& TGranulesFillingContext::GetBatchInfoVerified(const TBatchAddress& address) {
    return Processing.GetBatchInfoVerified(address);
}

NKikimr::NColumnShard::TDataTasksProcessorContainer TGranulesFillingContext::GetTasksProcessor() const {
    return Owner.GetTasksProcessor();
}

void TGranulesFillingContext::DrainNotIndexedBatches(THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>* batches) {
    Processing.DrainNotIndexedBatches(batches);
}

bool TGranulesFillingContext::TryStartProcessGranule(const ui64 granuleId, const TBlobRange& range, const bool hasReadyResults) {
    Y_VERIFY_DEBUG(!Result.IsReady(granuleId));
    if (InternalReading || Processing.IsInProgress(granuleId)
        || (!GranulesLiveContext->GetCount() && !hasReadyResults)
        || GetMemoryAccessor()->GetLimiter().HasBufferOrSubscribe(GetMemoryAccessor())
        )
    {
        Processing.StartBlobProcessing(granuleId, range);
        return true;
    } else {
        return false;
    }
}

bool TGranulesFillingContext::ForceStartProcessGranule(const ui64 granuleId, const TBlobRange& range) {
    Y_VERIFY_DEBUG(!Result.IsReady(granuleId));
    Processing.StartBlobProcessing(granuleId, range);
    return true;
}

void TGranulesFillingContext::OnGranuleReady(const ui64 granuleId) {
    Result.AddResult(Processing.ExtractReadyVerified(granuleId));
}

std::vector<NKikimr::NOlap::NIndexedReader::TGranule::TPtr> TGranulesFillingContext::DetachReadyInOrder() {
    Y_VERIFY(SortingPolicy);
    return SortingPolicy->DetachReadyGranules(Result);
}

const std::shared_ptr<NKikimr::NOlap::TActorBasedMemoryAccesor>& TGranulesFillingContext::GetMemoryAccessor() const {
    return Owner.GetMemoryAccessor();
}

}
