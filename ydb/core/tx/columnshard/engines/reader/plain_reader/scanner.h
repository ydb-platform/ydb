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

class TScanHead {
private:
    TPlainReadData& Reader;
    std::deque<std::shared_ptr<IDataSource>> Sources;
    std::vector<std::shared_ptr<arrow::Field>> ResultFields;
    THashMap<ui32, std::shared_ptr<IDataSource>> SourceByIdx;
    std::set<NIndexedReader::TSortableBatchPosition> FrontEnds;
    std::map<NIndexedReader::TSortableBatchPosition, TDataSourceEndpoint> BorderPoints;
    std::map<ui32, std::shared_ptr<IDataSource>> CurrentSegments;
    std::optional<NIndexedReader::TSortableBatchPosition> CurrentStart;
    std::deque<TFetchingInterval> FetchingIntervals;
    ui32 SegmentIdxCounter = 0;
    std::shared_ptr<NIndexedReader::TMergePartialStream> Merger;

    void DrainSources();

public:
    void Abort() {
        for (auto&& i : FetchingIntervals) {
            i.Abort();
        }
        FetchingIntervals.clear();
        BorderPoints.clear();
        Y_VERIFY(IsFinished());
    }

    bool IsFinished() const {
        return BorderPoints.empty() && FetchingIntervals.empty();
    }

    void AddSourceByIdx(const std::shared_ptr<IDataSource>& source) {
        Y_VERIFY(source);
        SourceByIdx.emplace(source->GetSourceIdx(), source);
    }

    void RemoveSourceByIdx(const std::shared_ptr<IDataSource>& source) {
        Y_VERIFY(source);
        SourceByIdx.erase(source->GetSourceIdx());
    }

    TReadContext& GetContext();

    void OnIntervalResult(std::shared_ptr<arrow::RecordBatch> batch, const ui32 intervalIdx);
    std::shared_ptr<IDataSource> GetSourceVerified(const ui32 idx) const {
        auto it = SourceByIdx.find(idx);
        Y_VERIFY(it != SourceByIdx.end());
        return it->second;
    }

    TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, TPlainReadData& reader);

    bool BuildNextInterval();

    void DrainResults();

};

}
