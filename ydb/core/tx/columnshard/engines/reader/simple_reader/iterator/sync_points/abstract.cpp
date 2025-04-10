#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>

namespace NKikimr::NOlap::NReader::NSimple {

void ISyncPoint::OnSourcePrepared(const std::shared_ptr<IDataSource>& sourceInput, TPlainReadData& reader) {
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build()("sync_point", GetPointName())("aborted", AbortFlag);
    if (AbortFlag) {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, sourceInput->AddEvent("a" + GetShortPointName()));
        AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event", "sync_point_aborted")("source_id", sourceInput->GetSourceId());
        AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event_log_abort", sourceInput->GetEventsReport())(
            "count", SourcesSequentially.size())("source_id", sourceInput->GetSourceId());
        return;
    }
    AFL_VERIFY(sourceInput->IsSyncSection());
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, sourceInput->AddEvent("f" + GetShortPointName()));
    AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event_log", sourceInput->GetEventsReport())("count", SourcesSequentially.size())(
        "source_id", sourceInput->GetSourceId());
    AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event", "OnSourcePrepared")("syn_point", GetPointName())(
        "source_id", sourceInput->GetSourceId());
    if (SourcesSequentially.size()) {
        AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event_log", sourceInput->GetEventsReport())("count", SourcesSequentially.size())(
            "source_id", sourceInput->GetSourceId())("first_source_id", SourcesSequentially.front()->GetSourceId());
    }
    while (SourcesSequentially.size() && IsSourcePrepared(SourcesSequentially.front())) {
        auto source = SourcesSequentially.front();
        switch (OnSourceReady(source, reader)) {
            case ESourceAction::Finish: {
                AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event", "finish_source")("source_id", source->GetSourceId());
                reader.GetScanner().MutableSourcesCollection().OnSourceFinished(source);
                SourcesSequentially.pop_front();
                break;
            }
            case ESourceAction::ProvideNext: {
                AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event", "provide_source")("source_id", source->GetSourceId());
                if (Next) {
                    source->ResetSourceFinishedFlag();
                    Next->AddSource(source);
                } else {
                    reader.GetScanner().MutableSourcesCollection().OnSourceFinished(source);
                }
                SourcesSequentially.pop_front();
                break;
            }
            case ESourceAction::Wait: {
                AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event", "ready_source_false")("source_id", source->GetSourceId());
                return;
            }
        }
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
