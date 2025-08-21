#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NReader::NSimple {

void ISyncPoint::OnSourcePrepared(std::shared_ptr<NCommon::IDataSource>&& sourceInput, TPlainReadData& reader) {
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build()("sync_point", GetPointName())("aborted", AbortFlag)(
        "tablet_id", Context->GetCommonContext()->GetReadMetadata()->GetTabletId())("prepared_source_id", sourceInput->GetSourceId());
    if (AbortFlag) {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, sourceInput->AddEvent("a" + GetShortPointName()));
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "sync_point_aborted");
        return;
    } else {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, sourceInput->AddEvent("f" + GetShortPointName()));
    }
    AFL_DEBUG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event_log", sourceInput->GetEventsReport())("count", SourcesSequentially.size())(
        "source_id", sourceInput->GetSourceId());
    AFL_VERIFY(sourceInput->IsSyncSection())("source_id", sourceInput->GetSourceId());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "OnSourcePrepared")("source_id", sourceInput->GetSourceId())(
        "prepared", IsSourcePrepared(sourceInput));
    AFL_VERIFY(SourcesSequentially.size());
    AFL_VERIFY(sourceInput->GetSourceId() != SourcesSequentially.front()->GetSourceId() || IsSourcePrepared(SourcesSequentially.front()));
    while (SourcesSequentially.size() && IsSourcePrepared(SourcesSequentially.front())) {
        auto source = SourcesSequentially.front();
        switch (OnSourceReady(source, reader)) {
            case ESourceAction::Finish: {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "finish_source")("source_id", source->GetSourceId());
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
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "provide_source")("source_id", source->GetSourceId());
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
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "wait_source")("source_id", source->GetSourceId());
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
            sb << "{" << i->GetSourceId() << "," << i->GetSequentialMemoryGroupIdx() << "}" << ",";
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
    AFL_VERIFY(SourcesSequentially.size() && SourcesSequentially.front()->GetSourceId() == continueAddress.GetSourceId())("first_source_id",
                                                                                           SourcesSequentially.front()->GetSourceId())(
                                                                                           "continue_source_id", continueAddress.GetSourceId());
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build()("sync_point", GetPointName())("event", "continue_source");
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("source_id", SourcesSequentially.front()->GetSourceId());
    SourcesSequentially.front()->MutableAs<IDataSource>()->ContinueCursor(SourcesSequentially.front());
}

void ISyncPoint::AddSource(std::shared_ptr<NCommon::IDataSource>&& source) {
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build()("sync_point", GetPointName())("event", "add_source")(
        "tablet_id", Context->GetCommonContext()->GetReadMetadata()->GetTabletId());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("source_id", source->GetSourceId());
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

void ISyncPoint::OnSourceFinished() {
    if (Next) {
        Next->OnSourceFinished();
    }
    if (auto genSource = DoOnSourceFinishedOnPreviouse()) {
        genSource->MutableAs<IDataSource>()->StartProcessing(genSource);
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
