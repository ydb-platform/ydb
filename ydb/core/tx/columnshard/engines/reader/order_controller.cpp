#include "order_controller.h"
#include "filling_context.h"

namespace NKikimr::NOlap::NIndexedReader {

void TAnySorting::DoFill(TGranulesFillingContext& context) {
    auto granulesOrder = ReadMetadata->SelectInfo->GranulesOrder(ReadMetadata->IsDescSorted());
    for (ui64 granule : granulesOrder) {
        TGranule& g = context.GetGranuleVerified(granule);
        GranulesOutOrder.emplace_back(&g);
    }
}

std::vector<TGranule*> TAnySorting::DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) {
    std::vector<TGranule*> result;
    while (GranulesOutOrder.size()) {
        NIndexedReader::TGranule* granule = GranulesOutOrder.front();
        if (!granule->IsReady()) {
            break;
        }
        result.emplace_back(granule);
        Y_VERIFY(granulesToOut.erase(granule->GetGranuleId()));
        GranulesOutOrder.pop_front();
    }
    return result;
}

std::vector<TGranule*> TNonSorting::DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) {
    std::vector<TGranule*> result;
    result.reserve(granulesToOut.size());
    for (auto&& i : granulesToOut) {
        result.emplace_back(i.second);
    }
    granulesToOut.clear();
    return result;
}

bool TPKSortingWithLimit::DoOnFilterReady(TBatch& /*batchInfo*/, const TGranule& granule, TGranulesFillingContext& context) {
    Y_VERIFY(ReadMetadata->Limit);
    if (!CurrentItemsLimit) {
        return false;
    }
    Y_VERIFY(GranulesOutOrderForPortions.size());
    if (granule.GetGranuleId() != GranulesOutOrderForPortions.front()->GetGranuleId()) {
        return false;
    }
    while (GranulesOutOrderForPortions.size()) {
        auto it = OrderedBatches.find(GranulesOutOrderForPortions.front()->GetGranuleId());
        auto g = GranulesOutOrderForPortions.front();
        Y_VERIFY(it != OrderedBatches.end());
        if (!it->second.GetStarted()) {
            MergeStream.AddIndependentSource(g->GetNotIndexedBatch(), g->GetNotIndexedBatchFutureFilter());
            it->second.SetStarted(true);
        }
        auto& batches = it->second.MutableBatches();
        while (batches.size() && batches.front()->IsFiltered() && CurrentItemsLimit) {
            auto b = batches.front();
            if (b->IsSortableInGranule()) {
                MergeStream.AddPoolSource(0, b->GetFilterBatch());
            } else {
                MergeStream.AddIndependentSource(b->GetFilterBatch(), b->GetFutureFilter());
            }
            OnBatchFilterInitialized(*b, context);
            batches.pop_front();
        }
        if (MergeStream.IsValid()) {
            while ((batches.empty() || MergeStream.HasRecordsInPool(0)) && CurrentItemsLimit && MergeStream.Next()) {
                --CurrentItemsLimit;
            }
        }
        if (!CurrentItemsLimit || batches.empty()) {
            while (batches.size()) {
                auto b = batches.front();
                context.GetCounters().GetSkippedBytes()->Add(b->GetFetchBytes(context.GetPostFilterColumns()));
                b->InitBatch(nullptr);
                batches.pop_front();
            }
            OrderedBatches.erase(it);
            GranulesOutOrderForPortions.pop_front();
        } else {
            break;
        }
    }
    return false;
}

void TPKSortingWithLimit::DoFill(TGranulesFillingContext& context) {
    auto granulesOrder = ReadMetadata->SelectInfo->GranulesOrder(ReadMetadata->IsDescSorted());
    for (ui64 granule : granulesOrder) {
        TGranule& g = context.GetGranuleVerified(granule);
        GranulesOutOrder.emplace_back(&g);
        Y_VERIFY(OrderedBatches.emplace(granule, TGranuleScanInfo(g.SortBatchesByPK(ReadMetadata->IsDescSorted(), ReadMetadata))).second);
    }
    GranulesOutOrderForPortions = GranulesOutOrder;
}

std::vector<TGranule*> TPKSortingWithLimit::DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) {
    std::vector<TGranule*> result;
    while (GranulesOutOrder.size()) {
        NIndexedReader::TGranule* granule = GranulesOutOrder.front();
        if (!granule->IsReady()) {
            break;
        }
        result.emplace_back(granule);
        Y_VERIFY(granulesToOut.erase(granule->GetGranuleId()));
        GranulesOutOrder.pop_front();
    }
    return result;
}

TPKSortingWithLimit::TPKSortingWithLimit(TReadMetadata::TConstPtr readMetadata)
    :TBase(readMetadata)
    , MergeStream(readMetadata->IndexInfo.GetReplaceKey(), readMetadata->IsDescSorted())
{
    CurrentItemsLimit = ReadMetadata->Limit;
}

void IOrderPolicy::OnBatchFilterInitialized(TBatch& batch, TGranulesFillingContext& context) {
    Y_VERIFY(!!batch.GetFilter());
    if (!batch.GetFilteredRecordsCount()) {
        context.GetCounters().GetEmptyFilterCount()->Add(1);
        context.GetCounters().GetEmptyFilterFetchedBytes()->Add(batch.GetFetchedBytes());
        context.GetCounters().GetSkippedBytes()->Add(batch.GetFetchBytes(context.GetPostFilterColumns()));
        batch.InitBatch(nullptr);
    } else {
        context.GetCounters().GetFilteredRowsCount()->Add(batch.GetFilterBatch()->num_rows());
        if (batch.AskedColumnsAlready(context.GetPostFilterColumns())) {
            context.GetCounters().GetFilterOnlyCount()->Add(1);
            context.GetCounters().GetFilterOnlyFetchedBytes()->Add(batch.GetFetchedBytes());
            context.GetCounters().GetFilterOnlyUsefulBytes()->Add(batch.GetUsefulFetchedBytes());
            context.GetCounters().GetSkippedBytes()->Add(batch.GetFetchBytes(context.GetPostFilterColumns()));

            batch.InitBatch(batch.GetFilterBatch());
        } else {
            context.GetCounters().GetTwoPhasesFilterFetchedBytes()->Add(batch.GetFetchedBytes());
            context.GetCounters().GetTwoPhasesFilterUsefulBytes()->Add(batch.GetUsefulFetchedBytes());

            batch.ResetWithFilter(context.GetPostFilterColumns());
            if (batch.IsFetchingReady()) {
                auto processor = context.GetTasksProcessor();
                if (auto assembleBatchTask = batch.AssembleTask(processor.GetObject(), context.GetReadMetadata())) {
                    processor.Add(context, assembleBatchTask);
                }
            }

            context.GetCounters().GetTwoPhasesCount()->Add(1);
            context.GetCounters().GetTwoPhasesPostFilterFetchedBytes()->Add(batch.GetWaitingBytes());
            context.GetCounters().GetTwoPhasesPostFilterUsefulBytes()->Add(batch.GetUsefulWaitingBytes());
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "additional_data")
                ("filtered_count", batch.GetFilterBatch()->num_rows())
                ("blobs_count", batch.GetWaitingBlobs().size())
                ("columns_count", batch.GetCurrentColumnIds()->size())
                ("fetch_size", batch.GetWaitingBytes())
                ;
        }
    }
}

}
