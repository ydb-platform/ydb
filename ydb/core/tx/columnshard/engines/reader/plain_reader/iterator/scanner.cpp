#include "plain_read_data.h"
#include "scanner.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NPlain {

void TScanHead::OnIntervalResult(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& allocationGuard,
    std::optional<NArrow::TShardedRecordBatch>&& newBatch, const std::shared_ptr<arrow::RecordBatch>& lastPK,
    std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger, const ui32 intervalIdx, TPlainReadData& reader) {
    if (Context->GetReadMetadata()->HasLimit() && (!newBatch || newBatch->GetRecordsCount() == 0) && InFlightLimit < MaxInFlight) {
        InFlightLimit = std::min<ui32>(MaxInFlight, InFlightLimit * 4);
    }
    auto itInterval = FetchingIntervals.find(intervalIdx);
    AFL_VERIFY(itInterval != FetchingIntervals.end());
    itInterval->second->SetMerger(std::move(merger));
    AFL_VERIFY(Context->GetCommonContext()->GetReadMetadata()->IsSorted());
    YDB_LOG_DEBUG("",
        {"event", "interval_result_received"},
        {"intervalIdx", intervalIdx},
        {"intervalId", itInterval->second->GetIntervalId()});
    if (newBatch && newBatch->GetRecordsCount()) {
        std::optional<TPartialSourceAddress> callbackIdxSubscriver;
        std::shared_ptr<NGroupedMemoryManager::TGroupGuard> gGuard;
        if (itInterval->second->HasMerger()) {
            callbackIdxSubscriver = TPartialSourceAddress(intervalIdx, 0);
        } else {
            gGuard = itInterval->second->GetGroupGuard();
        }
        std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>> guards = { std::move(allocationGuard) };
        AFL_VERIFY(ReadyIntervals.emplace(intervalIdx, std::make_unique<TPartialReadResult>(std::move(guards), std::move(gGuard), std::move(*newBatch),
            std::make_shared<TPlainScanCursor>(std::make_shared<NArrow::TSimpleRow>(lastPK, 0)), Context->GetCommonContext(), callbackIdxSubscriver)).second);
    } else {
        AFL_VERIFY(ReadyIntervals.emplace(intervalIdx, nullptr).second);
    }
    Y_ABORT_UNLESS(FetchingIntervals.size());
    while (FetchingIntervals.size()) {
        const auto interval = FetchingIntervals.begin()->second;
        const ui32 intervalIdx = interval->GetIntervalIdx();
        auto it = ReadyIntervals.find(intervalIdx);
        if (it == ReadyIntervals.end()) {
            YDB_LOG_DEBUG("",
                {"event", "interval_result_absent"},
                {"intervalIdx", intervalIdx},
                {"merger", interval->HasMerger()},
                {"intervalId", interval->GetIntervalId()});
            break;
        } else {
            YDB_LOG_DEBUG("",
                {"event", "interval_result"},
                {"intervalIdx", intervalIdx},
                {"count", it->second ? it->second->GetRecordsCount() : 0},
                {"merger", interval->HasMerger()},
                {"intervalId", interval->GetIntervalId()});
        }
        auto result = std::move(it->second);
        ReadyIntervals.erase(it);
        bool hasResult = !!result;
        if (result) {
            reader.OnIntervalResult(std::move(result));
        }
        if (!interval->HasMerger()) {
            FetchingIntervals.erase(FetchingIntervals.begin());
        } else if (hasResult) {
            break;
        } else {
            interval->OnPartSendingComplete();
        }
    }
    if (FetchingIntervals.empty()) {
        AFL_VERIFY(ReadyIntervals.empty());
        YDB_LOG_DEBUG("",
            {"event", "intervals_finished"});
    } else {
        YDB_LOG_DEBUG("",
            {"event", "wait_interval"},
            {"remained", FetchingIntervals.size()},
            {"intervalIdx", FetchingIntervals.begin()->first});
    }
}

TConclusionStatus TScanHead::Start() {
    TScanContext context;
    for (auto itPoint = BorderPoints.begin(); itPoint != BorderPoints.end(); ++itPoint) {
        auto& point = itPoint->second;
        context.OnStartPoint(point);
        if (context.GetIsSpecialPoint()) {
            for (auto&& i : context.GetCurrentSources()) {
                MutableNotStartedSource(i.first).IncIntervalsCount();
            }
        }
        const bool isExclusive = context.GetCurrentSources().size() == 1;
        for (auto&& i : context.GetCurrentSources()) {
            auto& source = MutableNotStartedSource(i.first);
            source.SetExclusiveIntervalOnly((isExclusive && source.GetExclusiveIntervalOnly() && !context.GetIsSpecialPoint()));
        }

        for (auto&& i : point.GetFinishSources()) {
            auto& source = MutableNotStartedSource(i->GetSourceIdx());
            if (!source.NeedAccessorsFetching()) {
                source.SetSourceInMemory(true);
            }
            source.InitFetchingPlan(Context->GetColumnsFetchingPlan(source, true));
        }
        context.OnFinishPoint(point);
        if (context.GetCurrentSources().size()) {
            auto itPointNext = itPoint;
            Y_ABORT_UNLESS(++itPointNext != BorderPoints.end());
            context.OnNextPointInfo(itPointNext->second);
            for (auto&& i : context.GetCurrentSources()) {
                MutableNotStartedSource(i.first).IncIntervalsCount();
            }
        }
    }
    return TConclusionStatus::Success();
}

TScanHead::TScanHead(std::unique_ptr<NCommon::ISourcesConstructor>&& sources, const std::shared_ptr<TSpecialReadContext>& context)
    : Context(context)
{
    if (HasAppData()) {
        if (AppDataVerified().ColumnShardConfig.HasMaxInFlightIntervalsOnRequest()) {
            MaxInFlight = AppDataVerified().ColumnShardConfig.GetMaxInFlightIntervalsOnRequest();
        }
    }

    if (Context->GetReadMetadata()->HasLimit()) {
        InFlightLimit = 1;
    } else {
        InFlightLimit = MaxInFlight;
    }
    while (!sources->IsFinished()) {
        auto lease = sources->TryExtractNext(context, InFlightLimit);
        AFL_VERIFY(lease);
        const auto source = lease->ShareReadOnlyAs<IDataSource>();
        BorderPoints[source->GetStart()].AddStart(source);
        BorderPoints[source->GetFinish()].AddFinish(source);
        AFL_VERIFY(NotStartedSources.emplace(source->GetSourceIdx(), std::move(lease)).second);
    }
}

TConclusion<bool> TScanHead::BuildNextInterval() {
    while (BorderPoints.size() && !Context->IsAborted()) {
        if (BorderPoints.begin()->second.GetStartSources().size()) {
            if (FetchingIntervals.size() >= InFlightLimit) {
                YDB_LOG_TRACE("",
                    {"event", "skip_next_interval"},
                    {"reason", "too many intervals in flight"},
                    {"count", FetchingIntervals.size()},
                    {"limit", InFlightLimit});
                return false;
            }
        }
        auto firstBorderPointInfo = std::move(BorderPoints.begin()->second);
        CurrentState.OnStartPoint(firstBorderPointInfo);

        if (CurrentState.GetIsSpecialPoint()) {
            const ui32 intervalIdx = SegmentIdxCounter++;
            auto interval = std::make_shared<TFetchingInterval>(BorderPoints.begin()->first, BorderPoints.begin()->first, intervalIdx,
                CurrentState.GetCurrentSources(), Context, true, true, false);
            FetchingIntervals.emplace(intervalIdx, interval);
            IntervalStats.emplace_back(CurrentState.GetCurrentSources().size(), true);
            YDB_LOG_DEBUG("",
                {"event", "new_interval"},
                {"intervalIdx", intervalIdx},
                {"interval", interval->DebugJson()});
            StartIntervalSources(*interval);
        }

        CurrentState.OnFinishPoint(firstBorderPointInfo);

        CurrentStart = BorderPoints.begin()->first;
        BorderPoints.erase(BorderPoints.begin());
        if (CurrentState.GetCurrentSources().size()) {
            Y_ABORT_UNLESS(BorderPoints.size());
            CurrentState.OnNextPointInfo(BorderPoints.begin()->second);
            const ui32 intervalIdx = SegmentIdxCounter++;
            auto interval =
                std::make_shared<TFetchingInterval>(*CurrentStart, BorderPoints.begin()->first, intervalIdx, CurrentState.GetCurrentSources(),
                    Context, CurrentState.GetIncludeFinish(), CurrentState.GetIncludeStart(), CurrentState.GetIsExclusiveInterval());
            FetchingIntervals.emplace(intervalIdx, interval);
            IntervalStats.emplace_back(CurrentState.GetCurrentSources().size(), false);
            YDB_LOG_DEBUG("",
                {"event", "new_interval"},
                {"intervalIdx", intervalIdx},
                {"interval", interval->DebugJson()});
            StartIntervalSources(*interval);
            return true;
        } else {
            IntervalStats.emplace_back(CurrentState.GetCurrentSources().size(), false);
        }
    }
    return false;
}

const TReadContext& TScanHead::GetContext() const {
    return *Context->GetCommonContext();
}

bool TScanHead::IsReverse() const {
    return GetContext().GetReadMetadata()->IsDescSorted();
}

void TScanHead::StartIntervalSources(TFetchingInterval& interval) {
    for (auto&& [sourceIdx, source] : interval.GetSources()) {
        if (source->IsDataReady()) {
            continue;
        }
        WaitingIntervals[sourceIdx].emplace_back(interval.GetIntervalIdx());
        auto it = NotStartedSources.find(sourceIdx);
        if (it != NotStartedSources.end()) {
            IDataSource::StartProcessing(std::move(it->second), interval.GetIntervalId());
            NotStartedSources.erase(it);
        }
    }
}

void TScanHead::OnSourceReady(std::unique_ptr<NCommon::TDataSourceLease> lease) {
    const ui32 sourceIdx = lease->GetSource().GetSourceIdx();
    lease.reset();
    std::vector<ui32> intervalIdxs;
    if (auto it = WaitingIntervals.find(sourceIdx); it != WaitingIntervals.end()) {
        intervalIdxs = std::move(it->second);
        WaitingIntervals.erase(it);
    }
    YDB_LOG_DEBUG("",
        {"event", "source_ready"},
        {"intervalsCount", intervalIdxs.size()},
        {"sourceIdx", sourceIdx});
    for (const ui32 intervalIdx : intervalIdxs) {
        auto it = FetchingIntervals.find(intervalIdx);
        AFL_VERIFY(it != FetchingIntervals.end())("interval_idx", intervalIdx);
        it->second->OnSourceFetchStageReady(sourceIdx);
    }
}

void TScanHead::Abort() {
    AFL_VERIFY(Context->IsAborted());
    WaitingIntervals.clear();
    NotStartedSources.clear();
    FetchingIntervals.clear();
    BorderPoints.clear();
    Y_ABORT_UNLESS(IsFinished());
}

void TScanHead::OnSentDataFromInterval(const TPartialSourceAddress& address) const {
    if (Context->IsAborted()) {
        return;
    }
    auto it = FetchingIntervals.find(address.GetSourceIdx());
    AFL_VERIFY(it != FetchingIntervals.end())("interval_idx", address.GetSourceIdx())("count", FetchingIntervals.size());
    it->second->OnPartSendingComplete();
}

}   // namespace NKikimr::NOlap::NReader::NPlain
