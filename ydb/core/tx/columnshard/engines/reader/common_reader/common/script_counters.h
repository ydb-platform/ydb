#pragma once
#include <ydb/library/signals/owner.h>

#include <util/generic/hash.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TFetchingStepSignals: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr ExecutionDurationCounter;
    NMonitoring::TDynamicCounters::TCounterPtr TotalDurationCounter;
    NMonitoring::TDynamicCounters::TCounterPtr BytesCounter;
    NMonitoring::TDynamicCounters::TCounterPtr SkipGraphNode;
    NMonitoring::TDynamicCounters::TCounterPtr SkipGraphNodeRecords;
    NMonitoring::TDynamicCounters::TCounterPtr ExecuteGraphNode;
    NMonitoring::TDynamicCounters::TCounterPtr ExecuteGraphNodeRecords;

public:
    TFetchingStepSignals(NColumnShard::TCommonCountersOwner&& owner)
        : TBase(std::move(owner))
        , ExecutionDurationCounter(TBase::GetDeriviative("Duration/Execution/Us"))
        , TotalDurationCounter(TBase::GetDeriviative("Duration/Total/Us"))
        , BytesCounter(TBase::GetDeriviative("Bytes/Count"))
        , SkipGraphNode(TBase::GetDeriviative("Skips/Count"))
        , SkipGraphNodeRecords(TBase::GetDeriviative("Skips/Records/Count"))
        , ExecuteGraphNode(TBase::GetDeriviative("Executions/Count"))
        , ExecuteGraphNodeRecords(TBase::GetDeriviative("Executions/Records/Count")) {
    }

    void OnSkipGraphNode(const ui32 recordsCount) {
        SkipGraphNode->Add(1);
        SkipGraphNodeRecords->Add(recordsCount);
    }

    void OnExecuteGraphNode(const ui32 recordsCount) {
        ExecuteGraphNode->Add(1);
        ExecuteGraphNodeRecords->Add(recordsCount);
    }

    void AddExecutionDuration(const TDuration d) const {
        ExecutionDurationCounter->Add(d.MicroSeconds());
    }

    void AddTotalDuration(const TDuration d) const {
        TotalDurationCounter->Add(d.MicroSeconds());
    }

    void AddBytes(const ui32 v) const {
        BytesCounter->Add(v);
    }
};

class TFetchingStepsSignalsCollection: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    TMutex Mutex;
    THashMap<TString, std::shared_ptr<TFetchingStepSignals>> Collection;
    std::shared_ptr<TFetchingStepSignals> GetSignalsImpl(const TString& name) {
        TGuard<TMutex> g(Mutex);
        auto it = Collection.find(name);
        if (it == Collection.end()) {
            it = Collection.emplace(name, std::make_shared<TFetchingStepSignals>(CreateSubGroup("step_name", name))).first;
        }
        return it->second;
    }

public:
    TFetchingStepsSignalsCollection()
        : TBase("ScanSteps") {
    }

    static std::shared_ptr<TFetchingStepSignals> GetSignals(const TString& name) {
        return Singleton<TFetchingStepsSignalsCollection>()->GetSignalsImpl(name);
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
