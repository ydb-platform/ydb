#pragma once
#include <ydb/core/tx/columnshard/engines/reader/read_filter_merger.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include "source.h"

namespace NKikimr::NOlap::NPlainReader {

class TScanHead;

class TMergingContext {
protected:
    YDB_READONLY_DEF(NIndexedReader::TSortableBatchPosition, Start);
    YDB_READONLY_DEF(NIndexedReader::TSortableBatchPosition, Finish);
    YDB_READONLY(bool, IncludeFinish, true);
    YDB_READONLY(bool, IncludeStart, false);
    YDB_READONLY(ui32, IntervalIdx, 0);
    bool IsExclusiveIntervalFlag = false;
public:
    TMergingContext(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
        const ui32 intervalIdx, const bool includeFinish, const bool includeStart, const bool isExclusiveInterval)
        : Start(start)
        , Finish(finish)
        , IncludeFinish(includeFinish)
        , IncludeStart(includeStart)
        , IntervalIdx(intervalIdx)
        , IsExclusiveIntervalFlag(isExclusiveInterval)
    {

    }

    bool IsExclusiveInterval() const {
        return IsExclusiveIntervalFlag;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("start", Start.DebugJson());
        result.InsertValue("idx", IntervalIdx);
        result.InsertValue("finish", Finish.DebugJson());
        result.InsertValue("include_finish", IncludeFinish);
        result.InsertValue("exclusive", IsExclusiveIntervalFlag);
        return result;
    }

};

class TFetchingInterval: public TNonCopyable, public NResourceBroker::NSubscribe::ITask {
private:
    using TTaskBase = NResourceBroker::NSubscribe::ITask;
    std::shared_ptr<TMergingContext> MergingContext;
    TAtomic SourcesFinalized = 0;
    std::shared_ptr<TSpecialReadContext> Context;
    NColumnShard::TCounterGuard TaskGuard;
    std::map<ui32, std::shared_ptr<IDataSource>> Sources;
    void ConstructResult();

    std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;
    const ui32 IntervalIdx;
    TAtomicCounter ReadySourcesCount = 0;
    TAtomicCounter ReadyGuards = 0;
    ui32 WaitSourcesCount = 0;
    void OnInitResourcesGuard(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard);
protected:
    virtual void DoOnAllocationSuccess(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) override;

public:
    ui32 GetIntervalIdx() const {
        return IntervalIdx;
    }

    const std::map<ui32, std::shared_ptr<IDataSource>>& GetSources() const {
        return Sources;
    }

    const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& GetResourcesGuard() const {
        return ResourcesGuard;
    }

    void Abort() {
        if (AtomicCas(&SourcesFinalized, 1, 0)) {
            for (auto&& i : Sources) {
                i.second->Abort();
            }
        }
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("merging_context", MergingContext ? MergingContext->DebugJson() : "");
        auto& jsonSources = result.InsertValue("sources", NJson::JSON_ARRAY);
        for (auto&& [_, i] : Sources) {
            jsonSources.AppendValue(i->DebugJson());
        }
        return result;
    }

    void OnSourceFetchStageReady(const ui32 sourceIdx);

    TFetchingInterval(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
        const ui32 intervalIdx, const std::map<ui32, std::shared_ptr<IDataSource>>& sources, const std::shared_ptr<TSpecialReadContext>& context,
        const bool includeFinish, const bool includeStart, const bool isExclusiveInterval);
};

}
