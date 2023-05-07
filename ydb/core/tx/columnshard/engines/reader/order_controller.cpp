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
            MergeStream.AddPoolSource({}, g.GetGranule()->GetNotIndexedBatch(), g.GetGranule()->GetNotIndexedBatchFutureFilter());
        }
        auto& batches = g.GetBatches();
        while (batches.size() && batches.front()->GetFetchedInfo().IsFiltered() && CurrentItemsLimit) {
            auto b = batches.front();
            MergeStream.AddPoolSource(b->GetMergePoolId(), b->GetFetchedInfo().GetFilterBatch(), b->GetFetchedInfo().GetNotAppliedEarlyFilter());
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
    , MergeStream(readMetadata->GetIndexInfo(readMetadata->GetSnapshot()).GetReplaceKey(), readMetadata->IsDescSorted())
{
    CurrentItemsLimit = ReadMetadata->Limit;
}

void IOrderPolicy::OnBatchFilterInitialized(TBatch& batchOriginal, TGranulesFillingContext& context) {
    auto& batch = batchOriginal.GetFetchedInfo();
    Y_VERIFY(!!batch.GetFilter());
    if (!batch.GetFilteredRecordsCount()) {
        context.GetCounters().EmptyFilterCount->Add(1);
        context.GetCounters().EmptyFilterFetchedBytes->Add(batchOriginal.GetFetchedBytes());
        context.GetCounters().SkippedBytes->Add(batchOriginal.GetFetchBytes(context.GetPostFilterColumns()));
        batchOriginal.InitBatch(nullptr);
    } else {
        context.GetCounters().FilteredRowsCount->Add(batch.GetFilterBatch()->num_rows());
        if (batchOriginal.AskedColumnsAlready(context.GetPostFilterColumns())) {
            context.GetCounters().FilterOnlyCount->Add(1);
            context.GetCounters().FilterOnlyFetchedBytes->Add(batchOriginal.GetFetchedBytes());
            context.GetCounters().FilterOnlyUsefulBytes->Add(batchOriginal.GetUsefulFetchedBytes());
            context.GetCounters().SkippedBytes->Add(batchOriginal.GetFetchBytes(context.GetPostFilterColumns()));

            batchOriginal.InitBatch(batch.GetFilterBatch());
        } else {
            context.GetCounters().TwoPhasesFilterFetchedBytes->Add(batchOriginal.GetFetchedBytes());
            context.GetCounters().TwoPhasesFilterUsefulBytes->Add(batchOriginal.GetUsefulFetchedBytes());

            batchOriginal.ResetWithFilter(context.GetPostFilterColumns());
            if (batchOriginal.IsFetchingReady()) {
                auto processor = context.GetTasksProcessor();
                if (auto assembleBatchTask = batchOriginal.AssembleTask(processor.GetObject(), context.GetReadMetadata())) {
                    processor.Add(context, assembleBatchTask);
                }
            }

            context.GetCounters().TwoPhasesCount->Add(1);
            context.GetCounters().TwoPhasesPostFilterFetchedBytes->Add(batchOriginal.GetWaitingBytes());
            context.GetCounters().TwoPhasesPostFilterUsefulBytes->Add(batchOriginal.GetUsefulWaitingBytes());
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "additional_data")
                ("filtered_count", batch.GetFilterBatch()->num_rows())
                ("blobs_count", batchOriginal.GetWaitingBlobs().size())
                ("columns_count", batchOriginal.GetCurrentColumnIds()->size())
                ("fetch_size", batchOriginal.GetWaitingBytes())
                ;
        }
    }
}

}
