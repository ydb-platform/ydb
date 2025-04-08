#pragma once
#include "collections.h"
#include "source.h"

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TPlainReadData;

class TScanHead {
private:
    std::shared_ptr<TSpecialReadContext> Context;
    THashMap<ui64, std::shared_ptr<IDataSource>> FetchingSourcesByIdx;
    std::deque<std::shared_ptr<IDataSource>> FetchingSources;
    TPositiveControlInteger IntervalsInFlightCount;
    std::unique_ptr<ISourcesCollection> SourcesCollection;

    void StartNextSource(const std::shared_ptr<TPortionDataSource>& source);

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
        return FetchingSources.empty() && SourcesCollection->IsFinished();
    }

    const TReadContext& GetContext() const;

    TString DebugString() const {
        TStringBuilder sb;
        sb << "S:{" << SourcesCollection->DebugString() << "};";
        sb << "F:";
        for (auto&& i : FetchingSources) {
            sb << i->GetSourceId() << ";";
        }
        return sb;
    }

    void OnSourceReady(const std::shared_ptr<IDataSource>& source, std::shared_ptr<arrow::Table>&& table, const ui32 startIndex,
        const ui32 recordsCount, TPlainReadData& reader);

    TConclusionStatus Start();

    TScanHead(std::deque<TSourceConstructor>&& sources, const std::shared_ptr<TSpecialReadContext>& context);

    [[nodiscard]] TConclusion<bool> BuildNextInterval();
};

}   // namespace NKikimr::NOlap::NReader::NSimple
