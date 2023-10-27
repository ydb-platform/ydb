#pragma once
#include <ydb/core/tx/columnshard/engines/reader/read_filter_merger.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include "source.h"

namespace NKikimr::NOlap::NPlainReader {

class TScanHead;

class TMergingContext {
protected:
    NIndexedReader::TSortableBatchPosition Start;
    NIndexedReader::TSortableBatchPosition Finish;
    const bool IncludeFinish = true;
    const bool IncludeStart = false;
    ui32 IntervalIdx = 0;
public:
    bool IsExclusiveInterval(const ui32 sourcesCount) const {
        return IncludeFinish && IncludeStart && sourcesCount == 1;
    }

    TMergingContext(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
        const ui32 intervalIdx, const bool includeFinish, const bool includeStart)
        : Start(start)
        , Finish(finish)
        , IncludeFinish(includeFinish)
        , IncludeStart(includeStart)
        , IntervalIdx(intervalIdx) {

    }

    ui32 GetIntervalIdx() const {
        return IntervalIdx;
    }
};

class TFetchingInterval: public TMergingContext, public TNonCopyable, public NResourceBroker::NSubscribe::ITask {
private:
    using TTaskBase = NResourceBroker::NSubscribe::ITask;
    using TBase = TMergingContext;
    bool ResultConstructionInProgress = false;
    std::shared_ptr<TSpecialReadContext> Context;
    NColumnShard::TCounterGuard TaskGuard;
    std::map<ui32, std::shared_ptr<IDataSource>> Sources;

    bool IsExclusiveInterval() const;
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
    std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;
protected:
    virtual void DoOnAllocationSuccess(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) override;

public:
    ~TFetchingInterval();

    const std::map<ui32, std::shared_ptr<IDataSource>>& GetSources() const {
        return Sources;
    }

    const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& GetResourcesGuard() const {
        return ResourcesGuard;
    }

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

    void OnInitResourcesGuard(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard);
    void OnSourceFetchStageReady(const ui32 sourceIdx);
    void OnSourceFilterStageReady(const ui32 sourceIdx);

    TFetchingInterval(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
        const ui32 intervalIdx, const std::map<ui32, std::shared_ptr<IDataSource>>& sources, const std::shared_ptr<TSpecialReadContext>& context,
        const bool includeFinish, const bool includeStart);
};

}
