#pragma once
#include "source.h"
#include "interval.h"
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NPlain {

class TPlainReadData;

class TDataSourceEndpoint {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<IDataSource>>, StartSources);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<IDataSource>>, FinishSources);
public:
    void AddStart(std::shared_ptr<IDataSource> source) {
        StartSources.emplace_back(source);
    }
    void AddFinish(std::shared_ptr<IDataSource> source) {
        FinishSources.emplace_back(source);
    }
};

class TIntervalStat {
private:
    YDB_READONLY(ui32, SourcesCount, 0);
    YDB_READONLY(bool, IsPoint, false);
public:
    TIntervalStat(const ui32 sourcesCount, const bool isPoint)
        : SourcesCount(sourcesCount)
        , IsPoint(isPoint)
    {

    }
};

class TScanContext {
private:
    using TCurrentSources = THashMap<ui32, std::shared_ptr<IDataSource>>;
    YDB_READONLY(bool, IncludeStart, false);
    YDB_READONLY(bool, IncludeFinish, false);
    YDB_READONLY_DEF(TCurrentSources, CurrentSources);
    YDB_READONLY(bool, IsSpecialPoint, false);
    YDB_READONLY(bool, IsExclusiveInterval, false);
public:
    void OnStartPoint(const TDataSourceEndpoint& point) {
        IsSpecialPoint = point.GetStartSources().size() && point.GetFinishSources().size();
        IncludeStart = point.GetStartSources().size() && !IsSpecialPoint;
        for (auto&& i : point.GetStartSources()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("add_source", i->GetSourceIdx());
            AFL_VERIFY(CurrentSources.emplace(i->GetSourceIdx(), i).second)("idx", i->GetSourceIdx());
        }
    }

    void OnFinishPoint(const TDataSourceEndpoint& point) {
        for (auto&& i : point.GetFinishSources()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("remove_source", i->GetSourceIdx());
            AFL_VERIFY(CurrentSources.erase(i->GetSourceIdx()))("idx", i->GetSourceIdx());
        }
    }

    void OnNextPointInfo(const TDataSourceEndpoint& nextPoint) {
        IncludeFinish = nextPoint.GetStartSources().empty();
        IsExclusiveInterval = (CurrentSources.size() == 1) && IncludeStart && IncludeFinish;
    }
};

class TScanHead {
private:
    std::shared_ptr<TSpecialReadContext> Context;
    bool SourcesInitialized = false;
    TScanContext CurrentState;
    std::map<NArrow::NMerger::TSortableBatchPosition, TDataSourceEndpoint> BorderPoints;
    std::optional<NArrow::NMerger::TSortableBatchPosition> CurrentStart;
    std::map<ui32, std::shared_ptr<TFetchingInterval>> FetchingIntervals;
    THashMap<ui32, std::shared_ptr<TPartialReadResult>> ReadyIntervals;
    ui32 SegmentIdxCounter = 0;
    std::vector<TIntervalStat> IntervalStats;
    ui64 InFlightLimit = 1;
    ui64 MaxInFlight = 256;
    ui64 MaxInFlightMemory = TGlobalLimits::ScanMemoryLimit;
    ui64 ZeroCount = 0;
    bool AbortFlag = false;
    void DrainSources();
    [[nodiscard]] TConclusionStatus DetectSourcesFeatureInContextIntervalScan(const THashMap<ui32, std::shared_ptr<IDataSource>>& intervalSources, const bool isExclusiveInterval) const;
public:
    void OnSentDataFromInterval(const ui32 intervalIdx) const {
        if (AbortFlag) {
            return;
        }
        auto it = FetchingIntervals.find(intervalIdx);
        AFL_VERIFY(it != FetchingIntervals.end())("interval_idx", intervalIdx)("count", FetchingIntervals.size());
        it->second->OnPartSendingComplete();
    }

    bool IsReverse() const;
    void Abort();

    bool IsFinished() const {
        return BorderPoints.empty() && FetchingIntervals.empty();
    }

    const TReadContext& GetContext() const;

    TString DebugString() const {
        TStringBuilder sb;
        for (auto&& i : IntervalStats) {
            sb << (i.GetIsPoint() ? "^" : "") << i.GetSourcesCount() << ";";
        }
        return sb;
    }

    void OnIntervalResult(const std::optional<NArrow::TShardedRecordBatch>& batch, const std::shared_ptr<arrow::RecordBatch>& lastPK, 
        std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger, const ui32 intervalIdx, TPlainReadData& reader);

    TConclusionStatus Start();

    TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, const std::shared_ptr<TSpecialReadContext>& context);

    [[nodiscard]] TConclusion<bool> BuildNextInterval();

};

}
