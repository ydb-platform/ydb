#pragma once
#include "source.h"
#include "merge.h"

#include <ydb/core/tx/columnshard/resource_subscriber/task.h>

namespace NKikimr::NOlap::NReader::NPlain {

class TFetchingInterval: public TNonCopyable {
private:
    std::shared_ptr<TMergingContext> MergingContext;
    TAtomic SourcesFinalized = 0;
    TAtomic PartSendingWait = 0;
    std::unique_ptr<NArrow::NMerger::TMergePartialStream> Merger;
    std::shared_ptr<TSpecialReadContext> Context;
    NColumnShard::TCounterGuard TaskGuard;
    THashMap<ui32, std::shared_ptr<IDataSource>> Sources;

    void ConstructResult();

    const ui32 IntervalIdx;
    const std::shared_ptr<NGroupedMemoryManager::TGroupGuard> IntervalGroupGuard;
    TAtomicCounter ReadySourcesCount = 0;
    ui32 WaitSourcesCount = 0;
    NColumnShard::TConcreteScanCounters::TScanIntervalStateGuard IntervalStateGuard;

public:
    std::set<ui64> GetPathIds() const {
        std::set<ui64> result;
        for (auto&& i : Sources) {
            result.emplace(i.second->GetPathId());
        }
        return result;
    }

    ui32 GetIntervalIdx() const {
        return IntervalIdx;
    }

    ui32 GetIntervalId() const {
        AFL_VERIFY(IntervalGroupGuard);
        return IntervalGroupGuard->GetGroupId();
    }

    const THashMap<ui32, std::shared_ptr<IDataSource>>& GetSources() const {
        return Sources;
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

    NJson::TJsonValue DebugJsonForMemory() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        auto& jsonSources = result.InsertValue("sources", NJson::JSON_ARRAY);
        for (auto&& [_, i] : Sources) {
            jsonSources.AppendValue(i->DebugJsonForMemory());
        }
        return result;
    }

    void OnSourceFetchStageReady(const ui32 sourceIdx);
    void OnPartSendingComplete();
    void SetMerger(std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger);
    bool HasMerger() const;
    std::shared_ptr<NGroupedMemoryManager::TGroupGuard> GetGroupGuard() const {
        return IntervalGroupGuard;
    }

    TFetchingInterval(const NArrow::NMerger::TSortableBatchPosition& start, const NArrow::NMerger::TSortableBatchPosition& finish,
        const ui32 intervalIdx, const THashMap<ui32, std::shared_ptr<IDataSource>>& sources, const std::shared_ptr<TSpecialReadContext>& context,
        const bool includeFinish, const bool includeStart, const bool isExclusiveInterval);
    
    ~TFetchingInterval() {
    }
};

}
