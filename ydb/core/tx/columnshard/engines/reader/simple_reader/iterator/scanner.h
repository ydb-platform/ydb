#pragma once
#include "source.h"
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NSimple {

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
    using TCompareKeyForScanSequence = TPortionDataSource::TCompareKeyForScanSequence;

    std::shared_ptr<TSpecialReadContext> Context;
    THashMap<ui64, std::shared_ptr<IDataSource>> FetchingSourcesByIdx;
    std::deque<std::shared_ptr<IDataSource>> SortedSources;
    std::deque<std::shared_ptr<IDataSource>> FetchingSources;
    std::map<TCompareKeyForScanSequence, std::shared_ptr<IDataSource>> FinishedSources;
    std::map<TCompareKeyForScanSequence, std::shared_ptr<IDataSource>> FetchingInFlightSources;
    TPositiveControlInteger IntervalsInFlightCount;
    ui64 FetchedCount = 0;
    ui64 InFlightLimit = 1;
    ui64 MaxInFlight = 256;
    TPositiveControlInteger SourcesInFlightCount;

    ui32 GetInFlightIntervalsCount() const;

public:
    ~TScanHead();

    void ContinueSource(const ui32 sourceIdx) const {
        auto it = FetchingSourcesByIdx.find(sourceIdx);
        AFL_VERIFY(it != FetchingSourcesByIdx.end())("source_idx", sourceIdx)("count", FetchingSourcesByIdx.size());
        it->second->ContinueCursor(it->second);
    }

    bool IsReverse() const;
    void Abort();

    bool IsFinished() const {
        return FetchingSources.empty() && SortedSources.empty();
    }

    const TReadContext& GetContext() const;

    TString DebugString() const {
        TStringBuilder sb;
        sb << "S:";
        for (auto&& i : SortedSources) {
            sb << i->GetSourceId() << ";";
        }
        sb << "F:";
        for (auto&& i : FetchingSources) {
            sb << i->GetSourceId() << ";";
        }
        return sb;
    }

    void OnSourceReady(const std::shared_ptr<IDataSource>& source, std::shared_ptr<arrow::Table>&& table, const ui32 startIndex,
        const ui32 recordsCount, TPlainReadData& reader);

    TConclusionStatus Start();

    TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, const std::shared_ptr<TSpecialReadContext>& context);

    [[nodiscard]] TConclusion<bool> BuildNextInterval();

};

}
