#include "scanner.h"
#include "plain_read_data.h"
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

namespace NKikimr::NOlap::NReader::NPlain {

void TScanHead::OnIntervalResult(const std::optional<NArrow::TShardedRecordBatch>& newBatch, const std::shared_ptr<arrow::RecordBatch>& lastPK,
    std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger, const ui32 intervalIdx, TPlainReadData& reader) {
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
    itInterval->second->SetMerger(std::move(merger));
    AFL_VERIFY(Context->GetCommonContext()->GetReadMetadata()->IsSorted());
    if (newBatch && newBatch->GetRecordsCount()) {
        const std::optional<ui32> callbackIdxSubscriver = itInterval->second->HasMerger() ? std::optional<ui32>(intervalIdx) : std::nullopt;
        AFL_VERIFY(ReadyIntervals.emplace(intervalIdx, std::make_shared<TPartialReadResult>(itInterval->second->GetResourcesGuard(), *newBatch, lastPK, callbackIdxSubscriver)).second);
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
        auto result = it->second;
        ReadyIntervals.erase(it);
        if (result) {
            reader.OnIntervalResult(result);
        }
        if (!interval->HasMerger()) {
            FetchingIntervals.erase(FetchingIntervals.begin());
        } else if (result) {
            break;
        } else {
            interval->OnPartSendingComplete();
        }
    }
    if (FetchingIntervals.empty()) {
        AFL_VERIFY(ReadyIntervals.empty());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "intervals_finished");
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "wait_interval")("remained", FetchingIntervals.size())("interval_idx", FetchingIntervals.begin()->first);
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

class TSourcesStorageForMemoryOptimization {
private:
    class TSourceInfo {
    private:
        YDB_READONLY_DEF(std::shared_ptr<IDataSource>, Source);
        YDB_READONLY_DEF(std::shared_ptr<IFetchingStep>, FetchingInfo);
    public:
        TSourceInfo(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& fetchingInfo)
            : Source(source)
            , FetchingInfo(fetchingInfo)
        {

        }
    };

    std::map<ui64, THashMap<ui32, TSourceInfo>> Sources;
    YDB_READONLY(ui64, MemorySum, 0);
    YDB_READONLY_DEF(std::set<ui64>, PathIds);
public:
    void UpdateSource(const ui64 oldMemoryInfo, const ui32 sourceIdx) {
        auto it = Sources.find(oldMemoryInfo);
        AFL_VERIFY(it != Sources.end());
        auto itSource = it->second.find(sourceIdx);
        AFL_VERIFY(itSource != it->second.end());
        auto sourceInfo = itSource->second;
        it->second.erase(itSource);
        if (it->second.empty()) {
            Sources.erase(it);
        }
        AFL_VERIFY(MemorySum >= oldMemoryInfo);
        MemorySum -= oldMemoryInfo;
        AddSource(sourceInfo.GetSource(), sourceInfo.GetFetchingInfo());
    }

    void AddSource(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFetchingStep>& fetching) {
        const ui64 sourceMemory = fetching->PredictRawBytes(source);
        MemorySum += sourceMemory;
        AFL_VERIFY(Sources[sourceMemory].emplace(source->GetSourceIdx(), TSourceInfo(source, fetching)).second);
        PathIds.emplace(source->GetPathId());
    }

    bool Optimize(const ui64 memoryLimit) {
        bool modified = true;
        while (MemorySum > memoryLimit && modified) {
            modified = false;
            for (auto it = Sources.rbegin(); it != Sources.rend(); ++it) {
                for (auto&& [sourceIdx, sourceInfo] : it->second) {
                    if (!sourceInfo.GetFetchingInfo()->InitSourceSeqColumnIds(sourceInfo.GetSource())) {
                        continue;
                    }
                    modified = true;
                    UpdateSource(it->first, sourceIdx);
                    break;
                }
                if (modified) {
                    break;
                }
            }
        }
        return MemorySum < memoryLimit;
    }
};

TConclusionStatus TScanHead::DetectSourcesFeatureInContextIntervalScan(const std::map<ui32, std::shared_ptr<IDataSource>>& intervalSources, const bool isExclusiveInterval) const {
    TSourcesStorageForMemoryOptimization optimizer;
    for (auto&& i : intervalSources) {
        if (!isExclusiveInterval) {
            i.second->SetExclusiveIntervalOnly(false);
        }
        auto fetchingPlan = Context->GetColumnsFetchingPlan(i.second);
        optimizer.AddSource(i.second, fetchingPlan);
    }
    const ui64 startMemory = optimizer.GetMemorySum();
    if (!optimizer.Optimize(Context->ReduceMemoryIntervalLimit) && Context->RejectMemoryIntervalLimit < optimizer.GetMemorySum()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "next_internal_broken")
            ("reason", "a lot of memory need")("start", startMemory)
            ("reduce_limit", Context->ReduceMemoryIntervalLimit)
            ("reject_limit", Context->RejectMemoryIntervalLimit)
            ("need", optimizer.GetMemorySum())
            ("path_ids", JoinSeq(",", optimizer.GetPathIds()));
        return TConclusionStatus::Fail("We need a lot of memory in time for interval scanner: " +
            ::ToString(optimizer.GetMemorySum()) + " path_ids: " + JoinSeq(",", optimizer.GetPathIds()) + ". We need wait compaction processing. Sorry.");
    } else if (optimizer.GetMemorySum() < startMemory) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "memory_reduce_active")
            ("reason", "need reduce memory")("start", startMemory)
            ("reduce_limit", Context->ReduceMemoryIntervalLimit)
            ("reject_limit", Context->RejectMemoryIntervalLimit)
            ("need", optimizer.GetMemorySum())
            ("path_ids", JoinSeq(",", optimizer.GetPathIds()));
    }
    return TConclusionStatus::Success();
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
            auto detectorResult = DetectSourcesFeatureInContextIntervalScan(CurrentSegments, false);
            if (!detectorResult) {
                Abort();
                return detectorResult;
            }
            auto interval = std::make_shared<TFetchingInterval>(
                BorderPoints.begin()->first, BorderPoints.begin()->first, intervalIdx, CurrentSegments,
                Context, true, true, false);
            FetchingIntervals.emplace(intervalIdx, interval);
            IntervalStats.emplace_back(CurrentSegments.size(), true);
            NResourceBroker::NSubscribe::ITask::StartResourceSubscription(Context->GetCommonContext()->GetResourceSubscribeActorId(), interval);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "new_interval")("interval_idx", intervalIdx)("interval", interval->DebugJson());
        }

        for (auto&& i : firstBorderPointInfo.GetFinishSources()) {
            i->InitFetchingPlan(Context->GetColumnsFetchingPlan(i), i);
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
            auto detectorResult = DetectSourcesFeatureInContextIntervalScan(CurrentSegments, isExclusiveInterval);
            if (!detectorResult) {
                Abort();
                return detectorResult;
            }
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
    AbortFlag = true;
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

}
