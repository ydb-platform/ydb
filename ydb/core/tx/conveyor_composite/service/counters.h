#pragma once
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/conveyor_composite/usage/common.h>

#include <ydb/library/signals/owner.h>

namespace NKikimr::NConveyorComposite {

using TTaskSignals = NConveyor::TTaskSignals;

class TCategorySignals: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    THashMap<TString, std::shared_ptr<TTaskSignals>> TaskClassSignals;
    YDB_READONLY(ESpecialTaskCategory, Category, ESpecialTaskCategory::Insert);

public:
    TCategorySignals(NColumnShard::TCommonCountersOwner& base, const ESpecialTaskCategory cat)
        : TBase(base, "category", ::ToString(cat))
        , Category(cat) {
    }

    std::shared_ptr<TTaskSignals> GetTaskSignals(const TString& taskClassName) {
        auto it = TaskClassSignals.find(taskClassName);
        if (it == TaskClassSignals.end()) {
            it = TaskClassSignals.emplace(taskClassName, std::make_shared<TTaskSignals>(*this, taskClassName)).first;
        }
        return it->second;
    }
};

class TCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    THashMap<ESpecialTaskCategory, std::shared_ptr<TCategorySignals>> CategorySignals;

public:
    const ::NMonitoring::TDynamicCounters::TCounterPtr ProcessesCount;

    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitingQueueSize;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitingQueueSizeLimit;

    const ::NMonitoring::TDynamicCounters::TCounterPtr InProgressSize;

    const ::NMonitoring::TDynamicCounters::TCounterPtr AvailableWorkersCount;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WorkersCountLimit;
    const ::NMonitoring::TDynamicCounters::TCounterPtr AmountCPULimit;

    const ::NMonitoring::TDynamicCounters::TCounterPtr IncomingRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr SolutionsRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr OverlimitRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitWorkerRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr UseWorkerRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr ChangeCPULimitRate;

    const ::NMonitoring::THistogramPtr WaitingHistogram;
    const ::NMonitoring::THistogramPtr PackHistogram;
    const ::NMonitoring::THistogramPtr PackExecuteHistogram;
    const ::NMonitoring::THistogramPtr TaskExecuteHistogram;
    const ::NMonitoring::THistogramPtr SendBackHistogram;
    const ::NMonitoring::THistogramPtr SendFwdHistogram;
    const ::NMonitoring::THistogramPtr ReceiveTaskHistogram;

    const ::NMonitoring::TDynamicCounters::TCounterPtr SendBackDuration;
    const ::NMonitoring::TDynamicCounters::TCounterPtr SendFwdDuration;
    const ::NMonitoring::TDynamicCounters::TCounterPtr ExecuteDuration;

    std::shared_ptr<TCategorySignals> GetCategorySignals(const ESpecialTaskCategory cat) {
        auto it = CategorySignals.find(cat);
        if (it == CategorySignals.end()) {
            it = CategorySignals.emplace(cat, std::make_shared<TCategorySignals>(*this, cat)).first;
        }
        return it->second;
    }

    TCounters(const TString& conveyorName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals);
};

}   // namespace NKikimr::NConveyorComposite
