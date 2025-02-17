#pragma once
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/library/formats/arrow/replace_key.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>

namespace NKikimr::NOlap::NActualizer {

class TSchemeGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    std::shared_ptr<NColumnShard::TValueAggregationAgent> QueueSizeInternalWrite;
    std::shared_ptr<NColumnShard::TValueAggregationAgent> QueueSizeExternalWrite;

    NMonitoring::TDynamicCounters::TCounterPtr Extracts;
    NMonitoring::TDynamicCounters::TCounterPtr SkipNotOptimized;
    NMonitoring::TDynamicCounters::TCounterPtr SkipNotReadyWrite;
    NMonitoring::TDynamicCounters::TCounterPtr SkipPortionNotActualizable;
    NMonitoring::TDynamicCounters::TCounterPtr EmptyTargetSchema;
    NMonitoring::TDynamicCounters::TCounterPtr RefreshEmpty;
    NMonitoring::TDynamicCounters::TCounterPtr SkipPortionToRemove;
    NMonitoring::TDynamicCounters::TCounterPtr RefreshValue;
    NMonitoring::TDynamicCounters::TCounterPtr AddPortion;
    NMonitoring::TDynamicCounters::TCounterPtr RemovePortion;

public:
    TSchemeGlobalCounters()
        : TBase("SchemeActualizer")
        , Extracts(TBase::GetDeriviative("Extracts/Count"))
        , SkipNotOptimized(TBase::GetDeriviative("SkipNotOptimized/Count"))
        , SkipNotReadyWrite(TBase::GetDeriviative("SkipNotReadyWrite/Count"))
        , SkipPortionNotActualizable(TBase::GetDeriviative("SkipPortionNotActualizable/Count"))
        , EmptyTargetSchema(TBase::GetDeriviative("EmptyTargetSchema/Count"))
        , RefreshEmpty(TBase::GetDeriviative("RefreshEmpty/Count"))
        , SkipPortionToRemove(TBase::GetDeriviative("SkipPortionToRemove/Count"))
        , RefreshValue(TBase::GetDeriviative("RefreshValue/Count")) 
        , AddPortion(TBase::GetDeriviative("AddPortion/Count"))
        , RemovePortion(TBase::GetDeriviative("RemovePortion/Count")) {
        QueueSizeExternalWrite = TBase::GetValueAutoAggregations("Granule/Scheme/Actualization/QueueSize/ExternalWrite");
        QueueSizeInternalWrite = TBase::GetValueAutoAggregations("Granule/Scheme/Actualization/QueueSize/InternalWrite");
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildQueueSizeExternalWrite() {
        return Singleton<TSchemeGlobalCounters>()->QueueSizeExternalWrite->GetClient();
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildQueueSizeInternalWrite() {
        return Singleton<TSchemeGlobalCounters>()->QueueSizeInternalWrite->GetClient();
    }
    static void OnAddPortion() {
        Singleton<TSchemeGlobalCounters>()->AddPortion->Add(1);
    }
    static void OnRemovePortion() {
        Singleton<TSchemeGlobalCounters>()->RemovePortion->Add(1);
    }
    static void OnSkipPortionNotActualizable() {
        Singleton<TSchemeGlobalCounters>()->SkipPortionNotActualizable->Add(1);
    }
    static void OnEmptyTargetSchema() {
        Singleton<TSchemeGlobalCounters>()->EmptyTargetSchema->Add(1);
    }
    static void OnRefreshEmpty() {
        Singleton<TSchemeGlobalCounters>()->RefreshEmpty->Add(1);
    }
    static void OnSkipPortionToRemove() {
        Singleton<TSchemeGlobalCounters>()->SkipPortionToRemove->Add(1);
    }
    static void OnRefreshValue() {
        Singleton<TSchemeGlobalCounters>()->RefreshValue->Add(1);
    }
    static void OnExtract() {
        Singleton<TSchemeGlobalCounters>()->Extracts->Add(1);
    }
    static void OnSkipNotOptimized() {
        Singleton<TSchemeGlobalCounters>()->SkipNotOptimized->Add(1);
    }
    static void OnSkipNotReadyWrite() {
        Singleton<TSchemeGlobalCounters>()->SkipNotReadyWrite->Add(1);
    }
};

class TSchemeCounters {
public:
    const std::shared_ptr<NColumnShard::TValueAggregationClient> QueueSizeInternalWrite;
    const std::shared_ptr<NColumnShard::TValueAggregationClient> QueueSizeExternalWrite;

    TSchemeCounters()
        : QueueSizeInternalWrite(TSchemeGlobalCounters::BuildQueueSizeInternalWrite())
        , QueueSizeExternalWrite(TSchemeGlobalCounters::BuildQueueSizeExternalWrite())
{
    }

};

}
