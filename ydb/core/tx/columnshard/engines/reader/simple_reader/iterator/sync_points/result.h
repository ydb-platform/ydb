#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NReader::NSimple {

class TSyncPointResult: public ISyncPoint {
private:
    using TBase = ISyncPoint;
    virtual std::shared_ptr<TFetchingScript> BuildFetchingPlan(const std::shared_ptr<IDataSource>& source) const override {
    }

    virtual bool OnSourceReady(const std::shared_ptr<IDataSource>& source, TPlainReadData& reader) const override {
        auto resultChunk = source->MutableStageResult().ExtractResultChunk(false);
        const bool isFinished = source->GetStageResult().IsFinished();
        std::optional<ui32> sourceIdxToContinue;
        if (!isFinished) {
            sourceIdxToContinue = source->GetSourceIdx();
        }
        if (resultChunk && resultChunk->HasData()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "has_result")("source_id", source->GetSourceId())(
                "source_idx", source->GetSourceIdx())("table", resultChunk->GetTable()->num_rows());
            auto cursor = SourcesCollection->BuildCursor(source, resultChunk->GetStartIndex() + resultChunk->GetRecordsCount());
            reader.OnIntervalResult(std::make_shared<TPartialReadResult>(source->GetResourceGuards(), source->GetGroupGuard(),
                resultChunk->GetTable(), cursor, Context->GetCommonContext(), sourceIdxToContinue));
        } else if (sourceIdxToContinue) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "continue_source")("source_id", source->GetSourceId())(
                "source_idx", source->GetSourceIdx());
            source->ContinueSource(source);
            return false;
        }
        if (!isFinished) {
            return false;
        }
        AFL_VERIFY(FetchingSourcesByIdx.erase(source->GetSourceIdx()));
        source->ClearResult();
        return true;
    }

public:
    TSyncPointResult(const ui32 limit, const ui32 pointIndex)
        : TBase(pointIndex, "RESULT")
        , Limit(limit) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
