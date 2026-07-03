#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/plain_read_data.h>

#include <library/cpp/lwtrace/all.h>
#include <util/string/builder.h>
#include <ydb/library/actors/struct_log/log_stack.h>

namespace NKikimr::NOlap::NReader::NTrivial {

void ISyncPoint::OnSourcePrepared(std::shared_ptr<NCommon::IDataSource>&& sourceInput, TPlainReadData& reader) {
    YDB_LOG_CREATE_CONTEXT(
        {"syncPoint", GetPointName()},
        {"aborted", AbortFlag},
        {"tabletId", Context->GetCommonContext()->GetReadMetadata()->GetTabletId()},
        {"preparedSourceIdx", sourceInput->GetSourceIdx()});
    if (AbortFlag) {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, sourceInput->AddEvent("a" + GetShortPointName()));
        YDB_LOG_WARN_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
            {"event", "sync_point_aborted"});
        return;
    } else {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, sourceInput->AddEvent("f" + GetShortPointName()));
    }
    YDB_LOG_DEBUG_COMP(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, "",
        {"eventLog", sourceInput->GetEventsReport()},
        {"count", SourcesSequentially.size()},
        {"sourceIdx", sourceInput->GetSourceIdx()});
    AFL_VERIFY(sourceInput->IsSyncSection())("source_idx", sourceInput->GetSourceIdx());
    InitSourceTracingMetrics(sourceInput);
    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
        {"event", "OnSourcePrepared"},
        {"sourceIdx", sourceInput->GetSourceIdx()},
        {"prepared", IsSourcePrepared(sourceInput)});
    AFL_VERIFY(SourcesSequentially.size());
    AFL_VERIFY(sourceInput->GetSourceIdx() != SourcesSequentially.front()->GetSourceIdx() || IsSourcePrepared(SourcesSequentially.front()));
    while (SourcesSequentially.size() && IsSourcePrepared(SourcesSequentially.front())) {
        auto source = SourcesSequentially.front();
        switch (OnSourceReady(source, reader)) {
            case ESourceAction::Finish: {
                YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                    {"event", "finish_source"},
                    {"sourceIdx", source->GetSourceIdx()});
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
                YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                    {"event", "provide_source"},
                    {"sourceIdx", source->GetSourceIdx()});
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
                YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                    {"event", "wait_source"},
                    {"sourceIdx", source->GetSourceIdx()});
                ReleaseInFlightForPreparedEmptySources();
                return;
            }
        }
    }
    ReleaseInFlightForPreparedEmptySources();
}

void ISyncPoint::ReleaseInFlightForPreparedEmptySources() {
    if (!Collection) {
        return;
    }
    for (auto& source : SourcesSequentially) {
        if (!source->IsInFlightReleased() && IsSourcePrepared(source) && source->HasStageResult() && source->GetStageResult().IsEmpty()) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                {"event", "early_release_inflight_empty_source"},
                {"sourceIdx", source->GetSourceIdx()});
            Collection->ReleaseInFlight(source);
            Context->GetCommonContext()->GetCounters().OnEarlyInFlightRelease();
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
    AFL_VERIFY(SourcesSequentially.size() && SourcesSequentially.front()->GetSourceIdx() == continueAddress.GetSourceIdx())("first_source_idx", SourcesSequentially.front()->GetSourceIdx())(
                                                   "continue_source_idx", continueAddress.GetSourceIdx());
    YDB_LOG_CREATE_CONTEXT(
        {"syncPoint", GetPointName()},
        {"event", "continue_source"});
    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
        {"sourceIdx", SourcesSequentially.front()->GetSourceIdx()});
    SourcesSequentially.front()->MutableAs<IDataSource>()->ContinueCursor(SourcesSequentially.front());
}

void ISyncPoint::AddSource(std::shared_ptr<NCommon::IDataSource>&& source) {
    YDB_LOG_CREATE_CONTEXT(
        {"syncPoint", GetPointName()},
        {"event", "add_source"},
        {"tabletId", Context->GetCommonContext()->GetReadMetadata()->GetTabletId()});
    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
        {"sourceIdx", source->GetSourceIdx()});
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

}   // namespace NKikimr::NOlap::NReader::NTrivial
