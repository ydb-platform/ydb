#include "scanner.h"
#include "plain_read_data.h"
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NReader::NPlain {

void TScanHead::OnIntervalResult(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& allocationGuard, const std::optional<NArrow::TShardedRecordBatch>& newBatch,
    const std::shared_ptr<arrow::RecordBatch>& lastPK, std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger, const ui32 intervalIdx,
    TPlainReadData& reader) {
    if (Context->GetReadMetadata()->Limit && (!newBatch || newBatch->GetRecordsCount() == 0) && InFlightLimit < 1000) {
        if (++ZeroCount == std::max<ui64>(16, InFlightLimit)) {
            InFlightLimit = std::min<ui32>(MaxInFlight, InFlightLimit * 2);
            ZeroCount = 0;
        }
    } else {
        ZeroCount = 0;
    }
    auto itInterval = FetchingIntervals.find(intervalIdx);
    AFL_VERIFY(itInterval != FetchingIntervals.end());
    itInterval->second->SetMerger(std::move(merger));
    AFL_VERIFY(Context->GetCommonContext()->GetReadMetadata()->IsSorted());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result_received")("interval_idx", intervalIdx)(
        "intervalId", itInterval->second->GetIntervalId());
    if (newBatch && newBatch->GetRecordsCount()) {
        const std::optional<ui32> callbackIdxSubscriver = itInterval->second->HasMerger() ? std::optional<ui32>(intervalIdx) : std::nullopt;
        AFL_VERIFY(ReadyIntervals.emplace(intervalIdx, std::make_shared<TPartialReadResult>(std::move(allocationGuard), *newBatch, lastPK, callbackIdxSubscriver)).second);
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
                "merger", interval->HasMerger())("intervalId", interval->GetIntervalId());
            break;
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "interval_result")("interval_idx", intervalIdx)("count",
                it->second ? it->second->GetRecordsCount() : 0)("merger", interval->HasMerger())("intervalId", interval->GetIntervalId());
        }
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
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "wait_interval")("remained", FetchingIntervals.size())(
            "interval_idx", FetchingIntervals.begin()->first);
    }
}

TConclusionStatus TScanHead::Start() {
    const bool guaranteeExclusivePK = Context->GetCommonContext()->GetReadMetadata()->HasGuaranteeExclusivePK();
    TScanContext context;
    for (auto itPoint = BorderPoints.begin(); itPoint != BorderPoints.end(); ++itPoint) {
        auto& point = itPoint->second;
        context.OnStartPoint(point);
        if (context.GetIsSpecialPoint()) {
            auto detectorResult = DetectSourcesFeatureInContextIntervalScan(context.GetCurrentSources(), guaranteeExclusivePK);
            for (auto&& i : context.GetCurrentSources()) {
                i.second->IncIntervalsCount();
            }
            if (!detectorResult) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "scanner_initializer_aborted")("reason", detectorResult.GetErrorMessage());
                Abort();
                return detectorResult;
            }
        }
        for (auto&& i : point.GetFinishSources()) {
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
            auto detectorResult = DetectSourcesFeatureInContextIntervalScan(context.GetCurrentSources(), guaranteeExclusivePK || context.GetIsExclusiveInterval());
            if (!detectorResult) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "scanner_initializer_aborted")("reason", detectorResult.GetErrorMessage());
                Abort();
                return detectorResult;
            }
        }
    }
    return TConclusionStatus::Success();
}

TScanHead::TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, const std::shared_ptr<TSpecialReadContext>& context)
    : Context(context)
{
    
    if (HasAppData()) {
        if (AppDataVerified().ColumnShardConfig.HasMaxInFlightMemoryOnRequest()) {
            MaxInFlightMemory = AppDataVerified().ColumnShardConfig.GetMaxInFlightMemoryOnRequest();
        }

        if (AppDataVerified().ColumnShardConfig.HasMaxInFlightIntervalsOnRequest()) {
            MaxInFlight = AppDataVerified().ColumnShardConfig.GetMaxInFlightIntervalsOnRequest();
        }
    }

    if (Context->GetReadMetadata()->Limit) {
        InFlightLimit = 1;
    } else {
        InFlightLimit = MaxInFlight;
    }
    while (sources.size()) {
        auto source = sources.front();
        BorderPoints[source->GetStart()].AddStart(source);
        BorderPoints[source->GetFinish()].AddFinish(source);
        sources.pop_front();
    }
}

class TSourcesStorageForMemoryOptimization {
private:
    class TSourceInfo {
    private:
        YDB_READONLY_DEF(std::shared_ptr<IDataSource>, Source);
        YDB_READONLY_DEF(std::shared_ptr<TFetchingScript>, FetchingInfo);
    public:
        TSourceInfo(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<TFetchingScript>& fetchingInfo)
            : Source(source)
            , FetchingInfo(fetchingInfo)
        {

        }

        NJson::TJsonValue DebugJson() const {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("source", Source->DebugJsonForMemory());
//            result.InsertValue("fetching", Fetching->DebugJsonForMemory());
            return result;
        }
    };

    std::map<ui64, THashMap<ui32, TSourceInfo>> Sources;
    YDB_READONLY(ui64, MemorySum, 0);
    YDB_READONLY_DEF(std::set<ui64>, PathIds);
public:
    TString DebugString() const {
        NJson::TJsonValue resultJson;
        auto& memorySourcesArr = resultJson.InsertValue("sources_by_memory", NJson::JSON_ARRAY);
        resultJson.InsertValue("sources_by_memory_count", Sources.size());
        for (auto it = Sources.rbegin(); it != Sources.rend(); ++it) {
            auto& sourceMap = memorySourcesArr.AppendValue(NJson::JSON_MAP);
            sourceMap.InsertValue("memory", it->first);
            auto& sourcesArr = sourceMap.InsertValue("sources", NJson::JSON_ARRAY);
            for (auto&& s : it->second) {
                sourcesArr.AppendValue(s.second.DebugJson());
            }
        }
        return resultJson.GetStringRobust();
    }

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

    void AddSource(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<TFetchingScript>& fetching) {
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

TConclusionStatus TScanHead::DetectSourcesFeatureInContextIntervalScan(const THashMap<ui32, std::shared_ptr<IDataSource>>& intervalSources, const bool isExclusiveInterval) const {
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
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "next_internal_broken")("reason", "a lot of memory need")("start", startMemory)(
            "reduce_limit", Context->ReduceMemoryIntervalLimit)("reject_limit", Context->RejectMemoryIntervalLimit)(
            "need", optimizer.GetMemorySum())("path_ids", JoinSeq(",", optimizer.GetPathIds()))(
            "details", IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_DEBUG, NKikimrServices::TX_COLUMNSHARD_SCAN) ? optimizer.DebugString() : "NEED_DEBUG_LEVEL");
        Context->GetCommonContext()->GetCounters().OnOptimizedIntervalMemoryFailed(optimizer.GetMemorySum());
        return TConclusionStatus::Fail("We need a lot of memory in time for interval scanner: " +
            ::ToString(optimizer.GetMemorySum()) + " path_ids: " + JoinSeq(",", optimizer.GetPathIds()) + ". We need wait compaction processing. Sorry.");
    } else if (optimizer.GetMemorySum() < startMemory) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "memory_reduce_active")("reason", "need reduce memory")("start", startMemory)(
            "reduce_limit", Context->ReduceMemoryIntervalLimit)("reject_limit", Context->RejectMemoryIntervalLimit)(
            "need", optimizer.GetMemorySum())("path_ids", JoinSeq(",", optimizer.GetPathIds()));
        Context->GetCommonContext()->GetCounters().OnOptimizedIntervalMemoryReduced(startMemory - optimizer.GetMemorySum());
    }
    Context->GetCommonContext()->GetCounters().OnOptimizedIntervalMemoryRequired(optimizer.GetMemorySum());
    return TConclusionStatus::Success();
}

TConclusion<bool> TScanHead::BuildNextInterval() {
    if (Context->IsAborted()) {
        return false;
    }
    while (BorderPoints.size()) {
        if (BorderPoints.begin()->second.GetStartSources().size()) {
            if (FetchingIntervals.size() >= InFlightLimit) {
                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_next_interval")("reason", "too many intervals in flight")(
                    "count", FetchingIntervals.size())("limit", InFlightLimit);
                return false;
            }
            if (Context->GetRequestedMemoryBytes() >= MaxInFlightMemory) {
                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_next_interval")("reason", "a lot of memory in usage")(
                    "volume", Context->GetCommonContext()->GetCounters().GetRequestedMemoryBytes())("limit", MaxInFlightMemory);
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

}
