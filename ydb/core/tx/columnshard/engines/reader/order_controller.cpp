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

bool TPKSortingWithLimit::DoWakeup(const TGranule& granule, TGranulesFillingContext& context) {
    Y_VERIFY(ReadMetadata->Limit);
    if (!CurrentItemsLimit) {
        return false;
    }
    Y_VERIFY(GranulesOutOrderForPortions.size());
    if (GranulesOutOrderForPortions.front().GetGranule()->GetGranuleId() != granule.GetGranuleId()) {
        return false;
    }
    while (GranulesOutOrderForPortions.size()) {
        auto& g = GranulesOutOrderForPortions.front();
        // granule have to wait NotIndexedBatch initialization, at first (StartableFlag initialization).
        // other batches will be delivered in OrderedBatches[granuleId] order
        if (!g.GetGranule()->IsNotIndexedBatchReady()) {
            break;
        }
        if (g.Start()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "granule_started")("granule_id", g.GetGranule()->GetGranuleId())("count", GranulesOutOrderForPortions.size());
            MergeStream.AddIndependentSource(g.GetGranule()->GetNotIndexedBatch(), g.GetGranule()->GetNotIndexedBatchFutureFilter());
        }
        auto& batches = g.GetBatches();
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
        while ((batches.empty() || MergeStream.HasRecordsInPool(0)) && CurrentItemsLimit && MergeStream.DrainCurrent()) {
            --CurrentItemsLimit;
        }
        if (!CurrentItemsLimit || batches.empty()) {
            while (batches.size()) {
                auto b = batches.front();
                context.GetCounters().SkippedBytes->Add(b->GetFetchBytes(context.GetPostFilterColumns()));
                b->InitBatch(nullptr);
                batches.pop_front();
            }
            GranulesOutOrderForPortions.pop_front();
        } else {
            break;
        }
    }
    return true;
}

bool TPKSortingWithLimit::DoOnFilterReady(TBatch& /*batchInfo*/, const TGranule& granule, TGranulesFillingContext& context) {
    return Wakeup(granule, context);
}

void TPKSortingWithLimit::DoFill(TGranulesFillingContext& context) {
    auto granulesOrder = ReadMetadata->SelectInfo->GranulesOrder(ReadMetadata->IsDescSorted());
    for (ui64 granule : granulesOrder) {
        TGranule& g = context.GetGranuleVerified(granule);
        GranulesOutOrder.emplace_back(&g);
        GranulesOutOrderForPortions.emplace_back(g.SortBatchesByPK(ReadMetadata->IsDescSorted(), ReadMetadata), &g);
    }
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
        context.GetCounters().EmptyFilterCount->Add(1);
        context.GetCounters().EmptyFilterFetchedBytes->Add(batch.GetFetchedBytes());
        context.GetCounters().SkippedBytes->Add(batch.GetFetchBytes(context.GetPostFilterColumns()));
        batch.InitBatch(nullptr);
    } else {
        context.GetCounters().FilteredRowsCount->Add(batch.GetFilterBatch()->num_rows());
        if (batch.AskedColumnsAlready(context.GetPostFilterColumns())) {
            context.GetCounters().FilterOnlyCount->Add(1);
            context.GetCounters().FilterOnlyFetchedBytes->Add(batch.GetFetchedBytes());
            context.GetCounters().FilterOnlyUsefulBytes->Add(batch.GetUsefulFetchedBytes());
            context.GetCounters().SkippedBytes->Add(batch.GetFetchBytes(context.GetPostFilterColumns()));

            batch.InitBatch(batch.GetFilterBatch());
        } else {
            context.GetCounters().TwoPhasesFilterFetchedBytes->Add(batch.GetFetchedBytes());
            context.GetCounters().TwoPhasesFilterUsefulBytes->Add(batch.GetUsefulFetchedBytes());

            batch.ResetWithFilter(context.GetPostFilterColumns());
            if (batch.IsFetchingReady()) {
                auto processor = context.GetTasksProcessor();
                if (auto assembleBatchTask = batch.AssembleTask(processor.GetObject(), context.GetReadMetadata())) {
                    processor.Add(context, assembleBatchTask);
                }
            }

            context.GetCounters().TwoPhasesCount->Add(1);
            context.GetCounters().TwoPhasesPostFilterFetchedBytes->Add(batch.GetWaitingBytes());
            context.GetCounters().TwoPhasesPostFilterUsefulBytes->Add(batch.GetUsefulWaitingBytes());
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
