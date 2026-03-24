#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>

#include <library/cpp/lwtrace/all.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap::NReader::NSimple {

void ISyncPoint::OnSourcePrepared(std::shared_ptr<NCommon::IDataSource>&& sourceInput, TPlainReadData& reader) {
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build()("sync_point", GetPointName())("aborted", AbortFlag)(
        "tablet_id", Context->GetCommonContext()->GetReadMetadata()->GetTabletId())("prepared_source_idx", sourceInput->GetSourceIdx());
    if (AbortFlag) {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, sourceInput->AddEvent("a" + GetShortPointName()));
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "sync_point_aborted");
        return;
    } else {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, sourceInput->AddEvent("f" + GetShortPointName()));
    }
    AFL_DEBUG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event_log", sourceInput->GetEventsReport())("count", SourcesSequentially.size())(
        "source_idx", sourceInput->GetSourceIdx());
    AFL_VERIFY(sourceInput->IsSyncSection())("source_idx", sourceInput->GetSourceIdx());
    InitSourceTracingMetrics(sourceInput);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "OnSourcePrepared")("source_idx", sourceInput->GetSourceIdx())(
        "prepared", IsSourcePrepared(sourceInput));
    AFL_VERIFY(SourcesSequentially.size());
    AFL_VERIFY(sourceInput->GetSourceIdx() != SourcesSequentially.front()->GetSourceIdx() || IsSourcePrepared(SourcesSequentially.front()));
    while (SourcesSequentially.size() && IsSourcePrepared(SourcesSequentially.front())) {
        auto source = SourcesSequentially.front();
        switch (OnSourceReady(source, reader)) {
            case ESourceAction::Finish: {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "finish_source")("source_idx", source->GetSourceIdx());
                if (Collection) {
                    Collection->OnSourceFinished(source);
                }
                if (Next) {
                    Next->OnSourceFinished();
                }

                SourcesSequentially.pop_front();
                break;
            }
            case ESourceAction::ProvideNext: {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "provide_source")("source_idx", source->GetSourceIdx());
                if (Next) {
                    source->ResetSourceFinishedFlag();
                    Next->AddSource(std::move(source));
                } else if (Collection) {
                    Collection->OnSourceFinished(source);
                }
                SourcesSequentially.pop_front();
                break;
            }
            case ESourceAction::Wait: {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "wait_source")("source_idx", source->GetSourceIdx());
                return;
            }
        }
    }
}

TString ISyncPoint::DebugString() const {
    TStringBuilder sb;
    sb << "{" << PointName << ";IDX=" << PointIndex << ";FIN=" << IsFinished() << ";";
    const TString details = DoDebugString();
    if (!!details) {
        sb << "DETAILS:" << details << ";";
    }
    if (SourcesSequentially.size()) {
        sb << "SRCS:[";
        ui32 idx = 0;
        for (auto&& i : SourcesSequentially) {
            sb << "{" << i->GetSourceIdx() << "," << i->GetSequentialMemoryGroupIdx() << "}" << ",";
            if (++idx == 10) {
                break;
            }
        }
        if (SourcesSequentially.size() > 10) {
            sb << "... (" << SourcesSequentially.size() - idx << " more)";
        }
        sb << "];";
    }
    sb << "}";
    return sb;
}

void ISyncPoint::Continue(const TPartialSourceAddress& continueAddress, TPlainReadData& /*reader*/) {
    AFL_VERIFY(PointIndex == continueAddress.GetSyncPointIndex());
    // In streaming mode with pre-fetch, a source can have multiple pages in-flight
    // simultaneously.  When the source emits its last page it is popped from
    // SourcesSequentially (ESourceAction::ProvideNext / Finish), but the already-emitted
    // pages are still being consumed by the client.  When those pages are acked,
    // Continue() is called with the old source's address.  At that point the source is
    // no longer at the front (or the queue is empty), so there is nothing to continue –
    // the fetch was already triggered by the pre-fetch in OnSourceReady.
    // We simply skip the ContinueCursor call in that case.
    if (SourcesSequentially.empty() || SourcesSequentially.front()->GetSourceIdx() != continueAddress.GetSourceIdx()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "continue_source_already_finished")
            ("continue_source_idx", continueAddress.GetSourceIdx())
            ("queue_empty", SourcesSequentially.empty())
            ("front_source_idx", SourcesSequentially.empty() ? Max<ui32>() : SourcesSequentially.front()->GetSourceIdx());
        return;
    }
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build()("sync_point", GetPointName())("event", "continue_source");
    auto* source = SourcesSequentially.front()->MutableAs<IDataSource>();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("source_idx", SourcesSequentially.front()->GetSourceIdx())
        ("has_cursor", source->HasCursor())("prefetch_triggered", source->IsPrefetchTriggered())("streaming", source->IsStreamingMode());
    // HasCursor() is false when ContinueCursor was already called in OnSourceReady as a
    // pre-fetch (streaming mode, pages-in-flight count was below the limit).  In that
    // case the next page fetch is already in progress and we must not start it again.
    // HasCursor() is true when the pre-fetch was suppressed (limit reached) or in
    // non-streaming mode – both cases require us to trigger the fetch now.
    if (source->HasCursor()) {
        // In streaming mode, if pre-fetching was already triggered, we should not call
        // ContinueCursor again to avoid double-advancing the page index.
        // The pre-fetch will handle advancing to the next page.
        if (source->IsStreamingMode() && source->IsPrefetchTriggered()) {
            // Pre-fetch was already triggered, do not call ContinueCursor again
            // but reset the flag for the next iteration
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_continue_cursor_prefetch_already_triggered")
                ("source_idx", source->GetSourceIdx())
                ("page_index", source->GetCurrentEarlyPageIndex())
                ("total_pages", source->GetEarlyPages().size())
                ("reverse", source->GetContext()->GetReadMetadata()->IsDescSorted());
            source->SetPrefetchTriggered(false);
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "call_continue_cursor")
                ("source_idx", source->GetSourceIdx())
                ("page_index", source->GetCurrentEarlyPageIndex())
                ("total_pages", source->GetEarlyPages().size())
                ("reverse", source->GetContext()->GetReadMetadata()->IsDescSorted())
                ("has_more_pages", source->HasMorePages());
            source->ContinueCursor(SourcesSequentially.front());
        }
    }
}

void ISyncPoint::AddSource(std::shared_ptr<NCommon::IDataSource>&& source) {
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build()("sync_point", GetPointName())("event", "add_source")(
        "tablet_id", Context->GetCommonContext()->GetReadMetadata()->GetTabletId());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("source_idx", source->GetSourceIdx());
    AFL_VERIFY(!AbortFlag);
    source->MutableAs<IDataSource>()->SetPurposeSyncPointIndex(GetPointIndex());
    AFL_VERIFY(!!source);
    if (!LastSourceIdx) {
        LastSourceIdx = source->GetSourceIdx();
    } else {
        AFL_VERIFY(*LastSourceIdx < source->GetSourceIdx())("idx_last", *LastSourceIdx)("idx_new", source->GetSourceIdx());
    }
    LastSourceIdx = source->GetSourceIdx();
    if (auto genSource = OnAddSource(source)) {
        genSource->MutableAs<IDataSource>()->StartProcessing(genSource);
    }
}

void ISyncPoint::InitSourceTracingMetrics(const std::shared_ptr<NCommon::IDataSource>& source) const {
    if (!NLWTrace::HasShuttles(source->GetDataSourceOrbit())) {
        return;
    }
    source->SetSourcesAheadQueueEnterTime(TMonotonic::Now());
    ui32 sourcesAhead = 0;
    for (const auto& s : SourcesSequentially) {
        if (s->GetSourceIdx() == source->GetSourceIdx()) {
            break;
        }
        ++sourcesAhead;
    }
    source->SetSourcesAhead(sourcesAhead);
}

void ISyncPoint::OnSourceFinished() {
    if (Next) {
        Next->OnSourceFinished();
    }
    if (auto genSource = DoOnSourceFinishedOnPreviouse()) {
        genSource->MutableAs<IDataSource>()->StartProcessing(genSource);
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
