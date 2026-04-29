#include "result.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

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
            // Two independent address fields are attached to the result:
            //
            //   notFinishedInterval (continuation marker)
            //     Set iff Continue() must be invoked once the client has acked
            //     this result, i.e. the source still has more chunks/pages.
            //     - Non-streaming: set when !isFinished (more chunks in StageResult).
            //     - Streaming:    set when HasMorePages() is true; the final
            //                     streaming page leaves it empty.
            //
            //   streamingPageAck (backpressure marker)
            //     Set iff this page was tracked via OnPageCreated() and the
            //     matching OnPageSent() must be invoked when the client acks.
            //     Only meaningful in streaming mode; set for every streaming
            //     page including the last one.
            //
            // The two fields are independent:
            //   - early streaming page  -> both set
            //   - last  streaming page  -> only streamingPageAck
            //   - non-final non-stream  -> only notFinishedInterval
            //   - final non-stream chunk-> neither
            const bool isStreamingMode = source->GetAs<IDataSource>()->IsStreamingMode();
            const bool hasMorePages = source->GetAs<IDataSource>()->HasMorePages();
            std::optional<TPartialSourceAddress> notFinishedInterval;
            std::optional<TPartialSourceAddress> streamingPageAck;
            if (isStreamingMode) {
                streamingPageAck = TPartialSourceAddress(source->GetSourceIdx(), GetPointIndex());
                if (hasMorePages) {
                    notFinishedInterval = TPartialSourceAddress(source->GetSourceIdx(), GetPointIndex());
                }
                Collection->OnPageCreated();
                // Track resource guard counts per streaming page for diagnostics.
                // A monotonically growing count indicates guards are leaking across pages.
                NYDBTest::TControllers::GetColumnShardController()->OnStreamingPageResult(
                    source->GetResourceGuards().size(), source->GetResourceGuardsMemory());
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "page_created")
                    ("source_idx", source->GetSourceIdx())("pages_in_flight", Collection->GetPagesInFlightCount());
            } else if (!isFinished) {
                notFinishedInterval = TPartialSourceAddress(source->GetSourceIdx(), GetPointIndex());
            }

            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "has_result")("source_idx", source->GetSourceIdx())
                ("table", resultChunk->GetTable()->num_rows())("is_finished", isFinished)("streaming", isStreamingMode)
                ("resource_guards_count", source->GetResourceGuards().size())
                ("resource_guards_memory", source->GetResourceGuardsMemory());
            // resultChunk->GetStartIndex() is the absolute page.GetIndexStart() within
            // the portion (set by TBuildResultStep from the page stored in StageResult).
            // Adding GetRecordsCount() gives the absolute end position, which is what
            // the scan cursor needs to track for resumed scans.
            auto cursor = Collection->BuildCursor(source, resultChunk->GetStartIndex() + resultChunk->GetRecordsCount(),
                Context->GetCommonContext()->GetReadMetadata()->GetTabletId());
            reader.OnIntervalResult(
                std::make_unique<TPartialReadResult>(source->GetResourceGuards(), source->MutableAs<IDataSource>()->GetGroupGuard(),
                resultChunk->ExtractTable(), std::move(cursor), Context->GetCommonContext(), notFinishedInterval, streamingPageAck,
                source->GetDeprecatedPortionId()));
            // In streaming mode, prefetch the next page while the current one is being
            // sent to the client, up to MaxPagesInFlight pages in flight. If the limit
            // is reached, Continue() will resume fetching once a page is acked via
            // OnPageSent(). Note: we use HasMorePages() instead of !isFinished because
            // streaming builds one page per StageResult, so isFinished is true after
            // every built page even when more pages remain in the portion.
            if (isStreamingMode && hasMorePages) {
                if (Collection->GetPagesInFlightCount() < Collection->GetMaxPagesInFlight()) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "prefetch_next_page")
                        ("source_idx", source->GetSourceIdx())
                        ("pages_in_flight", Collection->GetPagesInFlightCount())
                        ("max_pages", Collection->GetMaxPagesInFlight())
                        ("page_index", source->GetAs<IDataSource>()->GetCurrentEarlyPageIndex())
                        ("total_pages", source->GetAs<IDataSource>()->GetEarlyPages().size())
                        ("reverse", source->GetAs<IDataSource>()->GetContext()->GetReadMetadata()->IsDescSorted());
                    auto* simpleSource = source->MutableAs<IDataSource>();
                    // Set prefetch flag BEFORE starting ContinueCursor to prevent race:
                    // If ContinueCursor completes synchronously and re-enters OnSourceReady,
                    // Continue() must not call ContinueCursor again (which would skip a page).
                    simpleSource->SetPrefetchTriggered(true);
                    simpleSource->ContinueCursor(source);
                } else {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "backpressure_limit_reached")
                        ("source_idx", source->GetSourceIdx())
                        ("pages_in_flight", Collection->GetPagesInFlightCount())
                        ("max_pages", Collection->GetMaxPagesInFlight())
                        ("page_index", source->GetAs<IDataSource>()->GetCurrentEarlyPageIndex())
                        ("total_pages", source->GetAs<IDataSource>()->GetEarlyPages().size())
                        ("reverse", source->GetAs<IDataSource>()->GetContext()->GetReadMetadata()->IsDescSorted());
                    // Do not pre-fetch: Continue() will be called when a page is sent.
                    source->MutableAs<IDataSource>()->SetPrefetchTriggered(false);
                }
            } else if (!isFinished) {
                // Non-streaming multi-page result: continue within StageResult.
                source->MutableAs<IDataSource>()->ContinueCursor(source);
                source->MutableAs<IDataSource>()->SetPrefetchTriggered(false);
            } else {
                // No more work: reset the prefetch flag.
                source->MutableAs<IDataSource>()->SetPrefetchTriggered(false);
            }
            // Wait while streaming has more pages, or while non-streaming is unfinished.
            if (!isFinished || (isStreamingMode && hasMorePages)) {
                return ESourceAction::Wait;
            }
        } else if (!isFinished) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "continue_source")
                ("source_idx", source->GetSourceIdx())("is_finished", isFinished);
            source->MutableAs<IDataSource>()->ContinueCursor(source);
            return ESourceAction::Wait;
        } else {
            // Streaming mode special case: isFinished is true after every built
            // page (StageResult holds exactly one page in streaming mode), so when
            // a page yields no data after filtering (resultChunk has !HasData()),
            // execution falls into this `else` branch instead of the !isFinished
            // branch above. We must still advance to the next page via
            // ContinueCursor() when HasMorePages() is true; otherwise the scan
            // would prematurely skip remaining pages (see the BUG_streaming_premature_finish
            // diagnostic below).
            const bool isStreamingMode = source->GetAs<IDataSource>()->IsStreamingMode();
            const bool hasMorePages = source->GetAs<IDataSource>()->HasMorePages();
            if (isStreamingMode && hasMorePages) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "streaming_empty_page_continue")
                    ("source_idx", source->GetSourceIdx())
                    ("is_finished", isFinished)
                    ("page_index", source->GetAs<IDataSource>()->GetCurrentEarlyPageIndex())
                    ("total_pages", source->GetAs<IDataSource>()->GetEarlyPages().size())
                    ("reverse", source->GetAs<IDataSource>()->GetContext()->GetReadMetadata()->IsDescSorted());
                source->MutableAs<IDataSource>()->ContinueCursor(source);
                return ESourceAction::Wait;
            }
        }

        if (source->GetAs<IDataSource>()->IsStreamingMode() && source->GetAs<IDataSource>()->HasMorePages()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "BUG_streaming_premature_finish")
                ("source_idx", source->GetSourceIdx())
                ("page_index", source->GetAs<IDataSource>()->GetCurrentEarlyPageIndex())
                ("total_pages", source->GetAs<IDataSource>()->GetEarlyPages().size())
                ("reason", "empty page after filtering in streaming mode falls through to ProvideNext, skipping remaining pages");
        }
        source->MutableAs<IDataSource>()->ClearResult();
        return ESourceAction::ProvideNext;
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
