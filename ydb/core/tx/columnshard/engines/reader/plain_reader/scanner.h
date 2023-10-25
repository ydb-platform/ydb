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
    TPlainReadData& Reader;
    std::deque<std::shared_ptr<IDataSource>> Sources;
    std::vector<std::shared_ptr<arrow::Field>> ResultFields;
    std::shared_ptr<arrow::Schema> ResultSchema;
    YDB_READONLY_DEF(std::vector<TString>, ResultFieldNames);
    THashMap<ui32, std::shared_ptr<IDataSource>> SourceByIdx;
    std::map<NIndexedReader::TSortableBatchPosition, TDataSourceEndpoint> BorderPoints;
    std::map<ui32, std::shared_ptr<IDataSource>> CurrentSegments;
    std::optional<NIndexedReader::TSortableBatchPosition> CurrentStart;
    std::deque<TFetchingInterval> FetchingIntervals;
    THashMap<ui32, std::shared_ptr<arrow::RecordBatch>> ReadyIntervals;
    ui32 SegmentIdxCounter = 0;
    std::vector<TIntervalStat> IntervalStats;
    void DrainSources();

public:

    TFetchingPlan GetColumnsFetchingPlan(const bool exclusiveSource) const;

    bool IsReverse() const;

    void Abort() {
        for (auto&& i : FetchingIntervals) {
            i.Abort();
        }
        FetchingIntervals.clear();
        BorderPoints.clear();
        Y_ABORT_UNLESS(IsFinished());
    }

    bool IsFinished() const {
        return BorderPoints.empty() && FetchingIntervals.empty();
    }

    void AddSourceByIdx(const std::shared_ptr<IDataSource>& source) {
        Y_ABORT_UNLESS(source);
        SourceByIdx.emplace(source->GetSourceIdx(), source);
    }

    void RemoveSourceByIdx(const std::shared_ptr<IDataSource>& source) {
        Y_ABORT_UNLESS(source);
        SourceByIdx.erase(source->GetSourceIdx());
    }

    TReadContext& GetContext();

    TString DebugString() const {
        TStringBuilder sb;
        for (auto&& i : IntervalStats) {
            sb << (i.GetIsPoint() ? "^" : "") << i.GetSourcesCount() << ";";
        }
        return sb;
    }

    void OnIntervalResult(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 intervalIdx);
    std::shared_ptr<IDataSource> GetSourceVerified(const ui32 idx) const {
        auto it = SourceByIdx.find(idx);
        Y_ABORT_UNLESS(it != SourceByIdx.end());
        return it->second;
    }

    TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, TPlainReadData& reader);

    bool BuildNextInterval();

    std::shared_ptr<NIndexedReader::TMergePartialStream> BuildMerger() const;

};

}
