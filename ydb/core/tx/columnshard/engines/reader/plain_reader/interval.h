#pragma once
#include <ydb/core/tx/columnshard/engines/reader/read_filter_merger.h>
#include "source.h"

namespace NKikimr::NOlap::NPlainReader {

class TScanHead;

class TMergingContext {
protected:
    NIndexedReader::TSortableBatchPosition Start;
    NIndexedReader::TSortableBatchPosition Finish;
    const bool IncludeFinish = true;
    const bool IncludeStart = false;
    std::shared_ptr<NIndexedReader::TRecordBatchBuilder> RBBuilder;
    ui32 IntervalIdx = 0;
public:
    TMergingContext(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
        const ui32 intervalIdx, std::shared_ptr<NIndexedReader::TRecordBatchBuilder> builder, const bool includeFinish, const bool includeStart)
        : Start(start)
        , Finish(finish)
        , IncludeFinish(includeFinish)
        , IncludeStart(includeStart)
        , RBBuilder(builder)
        , IntervalIdx(intervalIdx) {

    }

    ui32 GetIntervalIdx() const {
        return IntervalIdx;
    }
};

class TFetchingInterval: public TMergingContext, TNonCopyable {
private:
    using TBase = TMergingContext;
    bool ResultConstructionInProgress = false;
    TScanHead& Scanner;
    std::map<ui32, std::shared_ptr<IDataSource>> Sources;

    bool IsExclusiveSource() const;
    void ConstructResult();

    IDataSource& GetSourceVerified(const ui32 idx) {
        auto it = Sources.find(idx);
        Y_ABORT_UNLESS(it != Sources.end());
        return *it->second;
    }

    bool IsSourcesReady() {
        for (auto&& [_, s] : Sources) {
            if (!s->IsDataReady()) {
                return false;
            }
        }
        return true;
    }

public:
    ~TFetchingInterval();

    void Abort() {
        for (auto&& i : Sources) {
            i.second->Abort();
        }
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("start", Start.DebugJson());
        result.InsertValue("idx", IntervalIdx);
        result.InsertValue("finish", Finish.DebugJson());
        auto& jsonSources = result.InsertValue("sources", NJson::JSON_ARRAY);
        for (auto&& [_, i] : Sources) {
            jsonSources.AppendValue(i->DebugJson());
        }
        result.InsertValue("include_finish", IncludeFinish);
        return result;
    }

    void OnSourceFetchStageReady(const ui32 sourceIdx);
    void OnSourceFilterStageReady(const ui32 sourceIdx);

    void StartMerge(std::shared_ptr<NIndexedReader::TMergePartialStream> merger);

    TFetchingInterval(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
        const ui32 intervalIdx, const std::map<ui32, std::shared_ptr<IDataSource>>& sources, TScanHead& scanner,
        std::shared_ptr<NIndexedReader::TRecordBatchBuilder> builder, const bool includeFinish, const bool includeStart);
};

}
