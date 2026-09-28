#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>

#include <ydb/library/actors/struct_log/log_stack.h>

#include <library/cpp/lwtrace/all.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap::NReader::NSimple {

void ISyncPoint::OnSourcePrepared(std::unique_ptr<NCommon::TDataSourceLease> lease, TPlainReadData& reader) {
    auto& source = lease->GetSource();
    YDB_LOG_CREATE_CONTEXT(
        {"syncPoint", GetPointName()},
        {"aborted", AbortFlag},
        {"tabletId", Context->GetCommonContext()->GetReadMetadata()->GetTabletId()},
        {"preparedSourceIdx", source.GetSourceIdx()});
    if (AbortFlag) {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("a" + GetShortPointName()));
        YDB_LOG_WARN_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
            {"event", "sync_point_aborted"});
        return;
    } else {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("f" + GetShortPointName()));
    }
    YDB_LOG_DEBUG_COMP(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, "",
        {"eventLog", source.GetEventsReport()},
        {"count", SourcesSequentially.size()},
        {"sourceIdx", source.GetSourceIdx()});
    AFL_VERIFY(source.IsSyncSection())("source_idx", source.GetSourceIdx());
    InitSourceTracingMetrics(source);
    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
        {"event", "OnSourcePrepared"},
        {"sourceIdx", source.GetSourceIdx()},
        {"prepared", IsSourcePrepared(source)});
    AFL_VERIFY(SourcesSequentially.size());
    const auto itEntry = FindIf(SourcesSequentially, [&](const TSourceEntry& entry) {
        return entry.SourceIdx == source.GetSourceIdx();
    });
    AFL_VERIFY(itEntry != SourcesSequentially.end())("source_idx", source.GetSourceIdx());
    AFL_VERIFY(!itEntry->Lease);
    itEntry->Lease = std::move(lease);
    AFL_VERIFY(itEntry != SourcesSequentially.begin() || IsPrepared(*itEntry));
    bool drain = true;
    while (drain && SourcesSequentially.size() && IsPrepared(SourcesSequentially.front())) {
        auto& front = SourcesSequentially.front();
        auto& frontSource = front.Lease->GetSource();
        switch (OnSourceReady(*front.Lease, reader)) {
            case ESourceAction::Finish: {
                YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                    {"event", "finish_source"},
                    {"sourceIdx", frontSource.GetSourceIdx()});
                if (Collection) {
                    Collection->OnSourceFinished(frontSource);
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
                    {"sourceIdx", frontSource.GetSourceIdx()});
                if (Next) {
                    frontSource.ResetSourceFinishedFlag();
                    Next->AddSource(std::move(front.Lease));
                } else if (Collection) {
                    Collection->OnSourceFinished(frontSource);
                }
                SourcesSequentially.pop_front();
                break;
            }
            case ESourceAction::Continue: {
                YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                    {"event", "continue_source"},
                    {"sourceIdx", frontSource.GetSourceIdx()});
                IDataSource::ContinueCursor(std::move(front.Lease));
                drain = false;
                break;
            }
            case ESourceAction::Wait: {
                YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                    {"event", "wait_source"},
                    {"sourceIdx", frontSource.GetSourceIdx()});
                drain = false;
                break;
            }
        }
    }
    ReleaseInFlightForPreparedEmptySources();
}

void ISyncPoint::ReleaseInFlightForPreparedEmptySources() {
    if (!Collection) {
        return;
    }
    for (auto& entry : SourcesSequentially) {
        if (!entry.Lease) {
            continue;
        }
        auto& source = entry.Lease->GetSource();
        if (!source.IsInFlightReleased() && IsSourcePrepared(source) && source.HasStageResult() && source.GetStageResult().IsEmpty()) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
                {"event", "early_release_inflight_empty_source"},
                {"sourceIdx", source.GetSourceIdx()});
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
            sb << "{" << i.SourceIdx << "," << i.MemoryGroupIdx << "}" << (i.Lease ? "+" : "-") << ",";
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
    AFL_VERIFY(SourcesSequentially.size() && SourcesSequentially.front().SourceIdx == continueAddress.GetSourceIdx())(
                                                                                      "first_source_idx", SourcesSequentially.front().SourceIdx)(
                                                                                      "continue_source_idx", continueAddress.GetSourceIdx());
    YDB_LOG_CREATE_CONTEXT(
        {"syncPoint", GetPointName()},
        {"event", "continue_source"});
    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
        {"sourceIdx", SourcesSequentially.front().SourceIdx});
    auto& front = SourcesSequentially.front();
    AFL_VERIFY(front.Lease);
    IDataSource::ContinueCursor(std::move(front.Lease));
}

void ISyncPoint::AddSource(std::unique_ptr<NCommon::TDataSourceLease> lease) {
    AFL_VERIFY(lease);
    auto& source = lease->GetSource();
    YDB_LOG_CREATE_CONTEXT(
        {"syncPoint", GetPointName()},
        {"event", "add_source"},
        {"tabletId", Context->GetCommonContext()->GetReadMetadata()->GetTabletId()});
    YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
        {"sourceIdx", source.GetSourceIdx()});
    AFL_VERIFY(!AbortFlag);
    source.MutableAs<IDataSource>()->SetPurposeSyncPointIndex(GetPointIndex());
    // Sources arrive in increasing SourceIdx order, which is what keeps the result stream ordered.
    // Conflicting sources are scanned first and produce no rows, so they carry no place in that order.
    if (!source.IsConflicting()) {
        AFL_VERIFY(!LastSourceIdx || *LastSourceIdx < source.GetSourceIdx())("idx_last", LastSourceIdx)("idx_new", source.GetSourceIdx());
        LastSourceIdx = source.GetSourceIdx();
    }
    if (auto toProcess = OnAddSource(std::move(lease))) {
        IDataSource::StartProcessing(std::move(toProcess));
    }
}

void ISyncPoint::InitSourceTracingMetrics(NCommon::IDataSource& source) const {
    if (!NLWTrace::HasShuttles(source.GetDataSourceOrbit())) {
        return;
    }
    source.SetSourcesAheadQueueEnterTime(TMonotonic::Now());
    ui32 sourcesAhead = 0;
    for (const auto& i : SourcesSequentially) {
        if (i.SourceIdx == source.GetSourceIdx()) {
            break;
        }
        ++sourcesAhead;
    }
    source.SetSourcesAhead(sourcesAhead);
}

void ISyncPoint::OnSourceFinished() {
    if (Next) {
        Next->OnSourceFinished();
    }
    if (auto toProcess = DoOnSourceFinishedOnPreviouse()) {
        IDataSource::StartProcessing(std::move(toProcess));
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
