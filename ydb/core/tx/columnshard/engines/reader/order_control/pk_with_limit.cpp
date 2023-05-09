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
            TGranule::TBatchForMerge& b = batches.front();
            if (!b.GetPoolId()) {
                ++CountNotSortedPortions;
            } else {
                ++CountBatchesByPools[*b.GetPoolId()];
            }
            ++CountProcessedBatches;
            MergeStream.AddPoolSource(b.GetPoolId(), b->GetFetchedInfo().GetFilterBatch(), b->GetFetchedInfo().GetNotAppliedEarlyFilter());
            OnBatchFilterInitialized(*b, context);
            batches.pop_front();
            if (batches.size()) {
                auto nextBatchControlPoint = batches.front()->GetFirstPK(ReadMetadata->IsDescSorted(), ReadMetadata->GetIndexInfo());
                if (!nextBatchControlPoint) {
                    continue;
                }
                MergeStream.PutControlPoint(nextBatchControlPoint);
            }
            while (CurrentItemsLimit && MergeStream.DrainCurrent()) {
                --CurrentItemsLimit;
            }
            if (MergeStream.ControlPointEnriched()) {
                MergeStream.RemoveControlPoint();
            } else if (batches.size()) {
                Y_VERIFY(!CurrentItemsLimit);
            }
        }
        if (batches.empty()) {
            Y_VERIFY(MergeStream.IsEmpty() || !CurrentItemsLimit);
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
            ++CountSkippedBatches;
            b->InitBatch(nullptr);
            batches.pop_front();
        }
        ++CountSkippedGranules;
        GranulesOutOrderForPortions.pop_front();
    }
    if (GranulesOutOrderForPortions.empty()) {
        if (IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN)) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "stop_on_limit")
                ("limit", ReadMetadata->Limit)("limit_reached", !CurrentItemsLimit)
                ("processed_batches", CountProcessedBatches)("processed_granules", CountProcessedGranules)
                ("skipped_batches", CountSkippedBatches)("skipped_granules", CountSkippedGranules)
                ("pools_count", CountBatchesByPools.size())("bad_pool_size", CountNotSortedPortions)
                ;
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

}
