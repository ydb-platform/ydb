#pragma once
#include "source.h"
#include "interval.h"
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>

namespace NKikimr::NOlap::NPlainReader {

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

class TScanHead {
private:
    std::shared_ptr<TSpecialReadContext> Context;
    std::map<NIndexedReader::TSortableBatchPosition, TDataSourceEndpoint> BorderPoints;
    std::map<ui32, std::shared_ptr<IDataSource>> CurrentSegments;
    std::optional<NIndexedReader::TSortableBatchPosition> CurrentStart;
    std::map<ui32, std::shared_ptr<TFetchingInterval>> FetchingIntervals;
    THashMap<ui32, std::shared_ptr<arrow::RecordBatch>> ReadyIntervals;
    ui32 SegmentIdxCounter = 0;
    std::vector<TIntervalStat> IntervalStats;
    void DrainSources();
    ui64 InFlightLimit = 1;
    ui64 ZeroCount = 0;
public:

    TFetchingPlan GetColumnsFetchingPlan(const bool exclusiveSource) const;

    bool IsReverse() const;

    void Abort() {
        for (auto&& i : FetchingIntervals) {
            i.second->Abort();
        }
        FetchingIntervals.clear();
        BorderPoints.clear();
        Y_ABORT_UNLESS(IsFinished());
    }

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

    void OnIntervalResult(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 intervalIdx, TPlainReadData& reader);

    TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, const std::shared_ptr<TSpecialReadContext>& context);

    bool BuildNextInterval();

};

}
