#include "result.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>

namespace NKikimr::NOlap::NReader::NSimple {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

bool TSyncPointResult::IsSourcePrepared(const std::shared_ptr<NCommon::IDataSource>& source) const {
    if (!Next) {
        return source->IsSyncSection() && source->HasStageResult() &&
               (source->GetStageResult().HasResultChunk() || source->GetStageResult().IsEmpty());
    } else if (source->IsSyncSection()) {
        AFL_VERIFY(source->HasStageData() || (source->HasStageResult() && source->GetStageResult().IsEmpty()));
        return true;
    } else {
        return false;
    }
}

ISyncPoint::ESourceAction TSyncPointResult::OnSourceReady(const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& reader) {
    const ui32 resultChunkRowsCount = (source->HasStageResult() && !source->GetStageResult().IsEmpty())
        ? source->GetStageResult().GetResultChunkRowsCount()
        : 0;
    LWTRACK(ResultSyncPoint, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
            source->GetTxId(), source->GetDeprecatedPortionId(), GetPointName(), source->GetFilteredRowsCount(), resultChunkRowsCount,
            source->GetReservedMemory(), source->GetSourcesAheadQueueWaitDuration(), source->GetSourcesAhead(), DebugString());
    if (Next) {
        if (source->HasStageResult() && source->GetStageResult().IsEmpty()) {
            return ESourceAction::Finish;
        }
        if (source->HasStageData() && !source->GetStageData().GetTable().HasSomeUsefulInfo()) {
            return ESourceAction::Finish;
        }
        return ESourceAction::ProvideNext;
    } else {
        if (source->GetStageResult().IsEmpty()) {
            return ESourceAction::Finish;
        }
        auto resultChunk = source->MutableStageResult().ExtractResultChunk();
        const bool isFinished = source->GetStageResult().IsFinished();
        if (resultChunk && resultChunk->HasData()) {
            // In streaming mode every page (including the last one) is tracked for
            // backpressure.  We always set partialSourceAddress so that
            // OnSentDataFromInterval is called for every page, which in turn calls
            // OnPageSent() to keep PagesInFlightCount balanced.
            // For the last page Continue() will be a no-op because the source has
            // already been popped from SourcesSequentially by the time the ack arrives.
            const bool isStreamingMode = source->GetAs<IDataSource>()->IsStreamingMode();
            std::optional<TPartialSourceAddress> partialSourceAddress;
            if (isStreamingMode) {
                // Always set address in streaming mode so OnSentDataFromInterval fires
                // for every page (needed to keep PagesInFlightCount balanced).
                partialSourceAddress = TPartialSourceAddress(source->GetSourceIdx(), GetPointIndex());
                Collection->OnPageCreated();
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "page_created")
                    ("source_idx", source->GetSourceIdx())("pages_in_flight", Collection->GetPagesInFlightCount());
            } else if (!isFinished) {
                partialSourceAddress = TPartialSourceAddress(source->GetSourceIdx(), GetPointIndex());
            }
            
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "has_result")("source_idx", source->GetSourceIdx())
                ("table", resultChunk->GetTable()->num_rows())("is_finished", isFinished)("streaming", isStreamingMode);
            auto cursor = Collection->BuildCursor(source, resultChunk->GetStartIndex() + resultChunk->GetRecordsCount(),
                Context->GetCommonContext()->GetReadMetadata()->GetTabletId());
            reader.OnIntervalResult(
                std::make_unique<TPartialReadResult>(source->GetResourceGuards(), source->MutableAs<IDataSource>()->GetGroupGuard(),
                resultChunk->ExtractTable(), std::move(cursor), Context->GetCommonContext(), partialSourceAddress, source->GetDeprecatedPortionId()));

            // In streaming mode, pre-fetch the next page immediately (while the current
            // page is being sent to the client) if we are still below the limit.
            // This allows up to MaxPagesInFlight pages to be in-flight simultaneously.
            // When the limit is reached we skip the pre-fetch here; ISyncPoint::Continue
            // (triggered by OnSentDataFromInterval after the client acknowledges the page)
            // will call ContinueCursor once a slot is freed by OnPageSent.
            // NOTE: both OnSourceReady and Continue run in the actor thread, so there
            // is no race condition between the pre-fetch and the Continue call.
            if (!isFinished && isStreamingMode) {
                if (Collection->GetPagesInFlightCount() < Collection->GetMaxPagesInFlight()) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "prefetch_next_page")
                        ("source_idx", source->GetSourceIdx())
                        ("pages_in_flight", Collection->GetPagesInFlightCount())
                        ("max_pages", Collection->GetMaxPagesInFlight());
                    source->MutableAs<IDataSource>()->ContinueCursor(source);
                } else {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "backpressure_limit_reached")
                        ("source_idx", source->GetSourceIdx())
                        ("pages_in_flight", Collection->GetPagesInFlightCount())
                        ("max_pages", Collection->GetMaxPagesInFlight());
                    // Do not pre-fetch: Continue() will be called when a page is sent.
                }
            }
        } else if (!isFinished) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "continue_source")
                ("source_idx", source->GetSourceIdx())("is_finished", isFinished);
            source->MutableAs<IDataSource>()->ContinueCursor(source);
        }
        if (!isFinished) {
            return ESourceAction::Wait;
        }
        source->MutableAs<IDataSource>()->ClearResult();
        return ESourceAction::ProvideNext;
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
