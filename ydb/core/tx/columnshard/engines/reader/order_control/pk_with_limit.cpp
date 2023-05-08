#include "pk_with_limit.h"
#include <ydb/core/tx/columnshard/engines/reader/filling_context.h>

namespace NKikimr::NOlap::NIndexedReader {

bool TPKSortingWithLimit::DoWakeup(const TGranule& granule, TGranulesFillingContext& context) {
    Y_VERIFY(ReadMetadata->Limit);
    if (!CurrentItemsLimit) {
        return false;
    }
    Y_VERIFY(GranulesOutOrderForPortions.size());
    if (GranulesOutOrderForPortions.front().GetGranule()->GetGranuleId() != granule.GetGranuleId()) {
        return false;
    }
    while (GranulesOutOrderForPortions.size() && CurrentItemsLimit) {
        auto& g = GranulesOutOrderForPortions.front();
        // granule have to wait NotIndexedBatch initialization, at first (NotIndexedBatchReadyFlag initialization).
        // other batches will be delivered in OrderedBatches[granuleId] order (not sortable at first in according to granule.SortBatchesByPK)
        if (!g.GetGranule()->IsNotIndexedBatchReady()) {
            break;
        }
        if (g.Start()) {
            ++CountProcessedGranules;
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "granule_started")("granule_id", g.GetGranule()->GetGranuleId())("count", GranulesOutOrderForPortions.size());
            MergeStream.AddPoolSource({}, g.GetGranule()->GetNotIndexedBatch(), g.GetGranule()->GetNotIndexedBatchFutureFilter());
        }
        auto& batches = g.GetOrderedBatches();
        while (batches.size() && batches.front()->GetFetchedInfo().IsFiltered() && CurrentItemsLimit) {
            TBatch* b = batches.front();
            std::optional<ui32> batchPoolId;
            if (b->IsSortableInGranule()) {
                ++CountSorted;
                batchPoolId = 0;
            } else {
                ++CountNotSorted;
            }
            MergeStream.AddPoolSource(batchPoolId, b->GetFetchedInfo().GetFilterBatch(), b->GetFetchedInfo().GetNotAppliedEarlyFilter());
            OnBatchFilterInitialized(*b, context);
            batches.pop_front();
            while ((batches.empty() || MergeStream.HasRecordsInPool(0)) && CurrentItemsLimit && MergeStream.DrainCurrent()) {
                if (!--CurrentItemsLimit) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "stop_on_limit")
                        ("limit", ReadMetadata->Limit)("sorted_count", CountSorted)("unsorted_count", CountNotSorted)("granules_count", CountProcessedGranules);
                }
            }
        }
        if (batches.empty()) {
            GranulesOutOrderForPortions.pop_front();
        } else {
            break;
        }
    }
    while (GranulesOutOrderForPortions.size() && !CurrentItemsLimit) {
        auto& g = GranulesOutOrderForPortions.front();
        g.GetGranule()->AddNotIndexedBatch(nullptr);
        auto& batches = g.GetOrderedBatches();
        while (batches.size()) {
            auto b = batches.front();
            context.GetCounters().SkippedBytes->Add(b->GetFetchBytes(context.GetPostFilterColumns()));
            b->InitBatch(nullptr);
            batches.pop_front();
        }
        GranulesOutOrderForPortions.pop_front();
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

}
