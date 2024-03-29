#include "scanner.h"
#include "plain_read_data.h"
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

namespace NKikimr::NOlap::NReader::NPlain {

void TScanHead::OnIntervalResult(const std::optional<NArrow::TShardedRecordBatch>& newBatch, const std::shared_ptr<arrow::RecordBatch>& lastPK, const ui32 intervalIdx, TPlainReadData& reader) {
    if (Context->GetReadMetadata()->Limit && (!newBatch || newBatch->GetRecordsCount() == 0) && InFlightLimit < 1000) {
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
        if (newBatch && newBatch->GetRecordsCount()) {
            reader.OnIntervalResult(std::make_shared<TPartialReadResult>(itInterval->second->GetResourcesGuard(), *newBatch, lastPK));
        }
        AFL_VERIFY(FetchingIntervals.erase(intervalIdx));
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result")("interval_idx", intervalIdx)("count", newBatch ? newBatch->GetRecordsCount() : 0);
    } else {
        if (newBatch && newBatch->GetRecordsCount()) {
            AFL_VERIFY(ReadyIntervals.emplace(intervalIdx, std::make_shared<TPartialReadResult>(itInterval->second->GetResourcesGuard(), *newBatch, lastPK)).second);
        } else {
            AFL_VERIFY(ReadyIntervals.emplace(intervalIdx, nullptr).second);
        }
        Y_ABORT_UNLESS(FetchingIntervals.size());
        while (FetchingIntervals.size()) {
            const auto interval = FetchingIntervals.begin()->second;
            const ui32 intervalIdx = interval->GetIntervalIdx();
            auto it = ReadyIntervals.find(intervalIdx);
            if (it == ReadyIntervals.end()) {
                break;
            }
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result")("interval_idx", intervalIdx)("count", it->second ? it->second->GetRecordsCount() : 0);
            FetchingIntervals.erase(FetchingIntervals.begin());
            if (it->second) {
                reader.OnIntervalResult(it->second);
            }
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

TConclusion<bool> TScanHead::BuildNextInterval() {
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
            auto interval = std::make_shared<TFetchingInterval>(
                BorderPoints.begin()->first, BorderPoints.begin()->first, intervalIdx, CurrentSegments,
                Context, true, true, false);
            FetchingIntervals.emplace(intervalIdx, interval);
            IntervalStats.emplace_back(CurrentSegments.size(), true);
            NResourceBroker::NSubscribe::ITask::StartResourceSubscription(Context->GetCommonContext()->GetResourceSubscribeActorId(), interval);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "new_interval")("interval_idx", intervalIdx)("interval", interval->DebugJson());
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
            const bool isExclusiveInterval = (CurrentSegments.size() == 1) && includeStart && includeFinish;
            auto interval = std::make_shared<TFetchingInterval>(*CurrentStart, BorderPoints.begin()->first, intervalIdx, CurrentSegments, Context, includeFinish, includeStart, isExclusiveInterval);
            FetchingIntervals.emplace(intervalIdx, interval);
            IntervalStats.emplace_back(CurrentSegments.size(), false);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "new_interval")("interval_idx", intervalIdx)("interval", interval->DebugJson());
            NResourceBroker::NSubscribe::ITask::StartResourceSubscription(Context->GetCommonContext()->GetResourceSubscribeActorId(), interval);
            return true;
        } else {
            IntervalStats.emplace_back(CurrentSegments.size(), false);
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
    for (auto&& i : FetchingIntervals) {
        i.second->Abort();
    }
    FetchingIntervals.clear();
    BorderPoints.clear();
    Y_ABORT_UNLESS(IsFinished());
}

}
