#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/reader/filling_context.h>

namespace NKikimr::NOlap::NIndexedReader {

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
