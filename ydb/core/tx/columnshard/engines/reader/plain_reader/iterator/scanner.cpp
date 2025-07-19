#include "plain_read_data.h"
#include "scanner.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>

#include <ydb/library/actors/core/log.h>

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
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result_received")("interval_idx", intervalIdx)(
        "intervalId", itInterval->second->GetIntervalId());
    if (newBatch && newBatch->GetRecordsCount()) {
        std::optional<TPartialSourceAddress> callbackIdxSubscriver;
        std::shared_ptr<NGroupedMemoryManager::TGroupGuard> gGuard;
        if (itInterval->second->HasMerger()) {
            callbackIdxSubscriver = TPartialSourceAddress(itInterval->second->GetIntervalId(), intervalIdx, 0);
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
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result_absent")("interval_idx", intervalIdx)(
                "merger", interval->HasMerger())("interval_id", interval->GetIntervalId());
            break;
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result")("interval_idx", intervalIdx)("count",
                it->second ? it->second->GetRecordsCount() : 0)("merger", interval->HasMerger())("interval_id", interval->GetIntervalId());
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
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "intervals_finished");
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "wait_interval")("remained", FetchingIntervals.size())(
            "interval_idx", FetchingIntervals.begin()->first);
    }
}

TConclusionStatus TScanHead::Start() {
    TScanContext context;
    for (auto itPoint = BorderPoints.begin(); itPoint != BorderPoints.end(); ++itPoint) {
        auto& point = itPoint->second;
        context.OnStartPoint(point);
        if (context.GetIsSpecialPoint()) {
            for (auto&& i : context.GetCurrentSources()) {
                i.second->IncIntervalsCount();
            }
        }
        const bool isExclusive = context.GetCurrentSources().size() == 1;
        for (auto&& i : context.GetCurrentSources()) {
            i.second->SetExclusiveIntervalOnly((isExclusive && i.second->GetExclusiveIntervalOnly() && !context.GetIsSpecialPoint()));
        }

        for (auto&& i : point.GetFinishSources()) {
            if (!i->NeedAccessorsFetching()) {
                i->SetSourceInMemory(true);
            }
            i->InitFetchingPlan(Context->GetColumnsFetchingPlan(i));
        }
        context.OnFinishPoint(point);
        if (context.GetCurrentSources().size()) {
            auto itPointNext = itPoint;
            Y_ABORT_UNLESS(++itPointNext != BorderPoints.end());
            context.OnNextPointInfo(itPointNext->second);
            for (auto&& i : context.GetCurrentSources()) {
                i.second->IncIntervalsCount();
            }
        }
    }
    return TConclusionStatus::Success();
}

TScanHead::TScanHead(std::unique_ptr<NCommon::ISourcesConstructor>&& sources, const std::shared_ptr<TSpecialReadContext>& context)
    : Context(context) {
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
        auto source = std::static_pointer_cast<IDataSource>(sources->ExtractNext(context));
        BorderPoints[source->GetStart()].AddStart(source);
        BorderPoints[source->GetFinish()].AddFinish(source);
    }
}

TConclusion<bool> TScanHead::BuildNextInterval() {
    while (BorderPoints.size() && !Context->IsAborted()) {
        if (BorderPoints.begin()->second.GetStartSources().size()) {
            if (FetchingIntervals.size() >= InFlightLimit) {
                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_next_interval")("reason", "too many intervals in flight")(
                    "count", FetchingIntervals.size())("limit", InFlightLimit);
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
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "new_interval")("interval_idx", intervalIdx)(
                "interval", interval->DebugJson());
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
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "new_interval")("interval_idx", intervalIdx)(
                "interval", interval->DebugJson());
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

void TScanHead::Abort() {
    AFL_VERIFY(Context->IsAborted());
    THashSet<ui32> sourceIds;
    for (auto&& i : FetchingIntervals) {
        for (auto&& s : i.second->GetSources()) {
            sourceIds.emplace(s.first);
        }
        i.second->Abort();
    }
    for (auto&& i : BorderPoints) {
        for (auto&& s : i.second.GetStartSources()) {
            if (sourceIds.emplace(s->GetSourceIdx()).second) {
                s->Abort();
            }
        }
        for (auto&& s : i.second.GetFinishSources()) {
            if (sourceIds.emplace(s->GetSourceIdx()).second) {
                s->Abort();
            }
        }
    }
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
