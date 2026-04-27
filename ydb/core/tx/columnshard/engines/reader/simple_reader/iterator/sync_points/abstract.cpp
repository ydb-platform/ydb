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
    // This early-return is valid in both modes.
    // Streaming: old in-flight page acks may arrive after the source is popped.
    // Non-streaming: async work may finish and pop the source before the previous
    // page ack arrives.
    if (SourcesSequentially.empty() || SourcesSequentially.front()->GetSourceIdx() != continueAddress.GetSourceIdx()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "continue_source_already_finished")
            ("continue_source_idx", continueAddress.GetSourceIdx())
            ("streaming_page", continueAddress.IsStreamingPage())
            ("queue_empty", SourcesSequentially.empty())
            ("front_source_idx", SourcesSequentially.empty() ? Max<ui32>() : SourcesSequentially.front()->GetSourceIdx());
        return;
    }
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build()("sync_point", GetPointName())("event", "continue_source");
    auto* source = SourcesSequentially.front()->MutableAs<IDataSource>();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("source_idx", SourcesSequentially.front()->GetSourceIdx())
        ("has_cursor", source->HasCursor())("streaming", source->IsStreamingMode());
    // HasCursor() is the single source of truth for whether we still need to advance.
    //   HasCursor()==false: ContinueCursor() has already been invoked (e.g. by the
    //     streaming prefetch path in TSyncPointResult::OnSourceReady), which extracts
    //     ScriptCursor.  Nothing to do here.
    //   HasCursor()==true:  prefetch was skipped (backpressure limit reached) or
    //     streaming is disabled, so advance the cursor now in response to the ack.
    // Note: PrefetchTriggered is intentionally not consulted here. The flag exists
    // to prevent a re-entrant double-advance inside OnSourceReady when ContinueCursor
    // completes synchronously; by the time this ack-driven Continue() runs, the
    // synchronous-completion window has already closed and HasCursor() alone gives
    // the correct answer.
    if (source->HasCursor()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "call_continue_cursor")
            ("source_idx", source->GetSourceIdx())
            ("page_index", source->GetCurrentEarlyPageIndex())
            ("total_pages", source->GetEarlyPages().size())
            ("reverse", source->GetContext()->GetReadMetadata()->IsDescSorted())
            ("has_more_pages", source->HasMorePages());
        source->ContinueCursor(SourcesSequentially.front());
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
