#include "scanner.h"
#include "plain_read_data.h"
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/read_metadata.h>

namespace NKikimr::NOlap::NPlainReader {

void TScanHead::OnIntervalResult(const std::shared_ptr<arrow::RecordBatch>& newBatch, const ui32 intervalIdx, TPlainReadData& reader) {
    if (Context->GetReadMetadata()->Limit && (!newBatch || newBatch->num_rows() == 0) && InFlightLimit < 1000) {
        if (++ZeroCount == std::max<ui64>(16, InFlightLimit)) {
            InFlightLimit *= 2;
            ZeroCount = 0;
        }
    } else {
        ZeroCount = 0;
    }
    auto itInterval = FetchingIntervals.find(intervalIdx);
    AFL_VERIFY(itInterval != FetchingIntervals.end());
    if (!Context->GetCommonContext()->GetReadMetadata()->IsSorted()) {
        reader.OnIntervalResult(newBatch, itInterval->second->GetResourcesGuard());
        AFL_VERIFY(FetchingIntervals.erase(intervalIdx));
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result")("interval_idx", intervalIdx)("count", newBatch ? newBatch->num_rows() : 0);
    } else {
        AFL_VERIFY(ReadyIntervals.emplace(intervalIdx, newBatch).second);
        Y_ABORT_UNLESS(FetchingIntervals.size());
        while (FetchingIntervals.size()) {
            const auto interval = FetchingIntervals.begin()->second;
            const ui32 intervalIdx = interval->GetIntervalIdx();
            auto it = ReadyIntervals.find(intervalIdx);
            if (it == ReadyIntervals.end()) {
                break;
            }
            const std::shared_ptr<arrow::RecordBatch>& batch = it->second;
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result")("interval_idx", intervalIdx)("count", batch ? batch->num_rows() : 0);
            FetchingIntervals.erase(FetchingIntervals.begin());
            reader.OnIntervalResult(batch, interval->GetResourcesGuard());
            ReadyIntervals.erase(it);
        }
        if (FetchingIntervals.empty()) {
            AFL_VERIFY(ReadyIntervals.empty());
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "intervals_finished");
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "wait_interval")("remained", FetchingIntervals.size())("interval_idx", FetchingIntervals.begin()->first);
        }
    }
}

TScanHead::TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, const std::shared_ptr<TSpecialReadContext>& context)
    : Context(context)
{
    InFlightLimit = Context->GetReadMetadata()->Limit ? 1 : Max<ui32>();
    while (sources.size()) {
        auto source = sources.front();
        BorderPoints[source->GetStart()].AddStart(source);
        BorderPoints[source->GetFinish()].AddFinish(source);
        sources.pop_front();
    }

    THashMap<ui32, std::shared_ptr<IDataSource>> currentSources;
    for (auto&& i : BorderPoints) {
        for (auto&& s : i.second.GetStartSources()) {
            AFL_VERIFY(currentSources.emplace(s->GetSourceIdx(), s).second);
        }
        for (auto&& [_, source] : currentSources) {
            source->IncIntervalsCount();
        }
        for (auto&& s : i.second.GetFinishSources()) {
            AFL_VERIFY(currentSources.erase(s->GetSourceIdx()));
        }
    }
}

bool TScanHead::BuildNextInterval() {
    while (BorderPoints.size() && (FetchingIntervals.size() < InFlightLimit || BorderPoints.begin()->second.GetStartSources().empty())) {
        auto firstBorderPointInfo = std::move(BorderPoints.begin()->second);
        bool includeStart = firstBorderPointInfo.GetStartSources().size();

        for (auto&& i : firstBorderPointInfo.GetStartSources()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("add_source", i->GetSourceIdx());
            AFL_VERIFY(CurrentSegments.emplace(i->GetSourceIdx(), i).second)("idx", i->GetSourceIdx());
        }

        if (firstBorderPointInfo.GetStartSources().size() && firstBorderPointInfo.GetFinishSources().size()) {
            includeStart = false;
            const ui32 intervalIdx = SegmentIdxCounter++;
            auto it = FetchingIntervals.emplace(intervalIdx, std::make_shared<TFetchingInterval>(
                BorderPoints.begin()->first, BorderPoints.begin()->first, intervalIdx, CurrentSegments,
                Context, true, true)).first;
            IntervalStats.emplace_back(CurrentSegments.size(), true);
            NResourceBroker::NSubscribe::ITask::StartResourceSubscription(Context->GetCommonContext()->GetResourceSubscribeActorId(), it->second);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "new_interval")("interval_idx", intervalIdx)("interval", it->second->DebugJson());
        }

        for (auto&& i : firstBorderPointInfo.GetFinishSources()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("remove_source", i->GetSourceIdx());
            AFL_VERIFY(CurrentSegments.erase(i->GetSourceIdx()))("idx", i->GetSourceIdx());
        }

        CurrentStart = BorderPoints.begin()->first;
        BorderPoints.erase(BorderPoints.begin());
        if (CurrentSegments.size()) {
            Y_ABORT_UNLESS(BorderPoints.size());
            const bool includeFinish = BorderPoints.begin()->second.GetStartSources().empty();
            const ui32 intervalIdx = SegmentIdxCounter++;
            auto it = FetchingIntervals.emplace(intervalIdx, std::make_shared<TFetchingInterval>(
                *CurrentStart, BorderPoints.begin()->first, intervalIdx, CurrentSegments,
                Context, includeFinish, includeStart)).first;
            IntervalStats.emplace_back(CurrentSegments.size(), false);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "new_interval")("interval_idx", intervalIdx)("interval", it->second->DebugJson());
            NResourceBroker::NSubscribe::ITask::StartResourceSubscription(Context->GetCommonContext()->GetResourceSubscribeActorId(), it->second);
            return true;
        } else {
            IntervalStats.emplace_back(CurrentSegments.size(), false);
        }

    }
    return false;
}

const NKikimr::NOlap::TReadContext& TScanHead::GetContext() const {
    return *Context->GetCommonContext();
}

bool TScanHead::IsReverse() const {
    return GetContext().GetReadMetadata()->IsDescSorted();
}

NKikimr::NOlap::NPlainReader::TFetchingPlan TScanHead::GetColumnsFetchingPlan(const bool exclusiveSource) const {
    return Context->GetColumnsFetchingPlan(exclusiveSource);
}

}
