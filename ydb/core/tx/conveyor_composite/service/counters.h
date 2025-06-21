#pragma once
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/conveyor_composite/usage/common.h>

#include <ydb/library/signals/owner.h>

namespace NKikimr::NConveyorComposite {

using TTaskSignals = NConveyor::TTaskSignals;

class TCategorySignals: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    YDB_READONLY(ESpecialTaskCategory, Category, ESpecialTaskCategory::Insert);

public:
    const ::NMonitoring::TDynamicCounters::TCounterPtr ProcessesCount;

    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitingQueueSize;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitingQueueSizeLimit;

    TCategorySignals(NColumnShard::TCommonCountersOwner& base, const ESpecialTaskCategory cat)
        : TBase(base, "category", ::ToString(cat))
        , Category(cat)
        , ProcessesCount(TBase::GetValue("ProcessesCount"))
        , WaitingQueueSize(TBase::GetValue("WaitingQueueSize"))
        , WaitingQueueSizeLimit(TBase::GetValue("WaitingQueueSizeLimit")) {
    }
};

class TWPCategorySignals: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    THashMap<TString, std::shared_ptr<TTaskSignals>> TaskClassSignals;
    YDB_READONLY(ESpecialTaskCategory, Category, ESpecialTaskCategory::Insert);

public:
    const ::NMonitoring::THistogramPtr WaitingHistogram;
    const ::NMonitoring::TDynamicCounters::TCounterPtr NoTasks;
    const ::NMonitoring::THistogramPtr TaskExecuteHistogram;
    const ::NMonitoring::TDynamicCounters::TCounterPtr ValueWeight;

    const ::NMonitoring::TDynamicCounters::TCounterPtr ExecuteDuration;

    TWPCategorySignals(NColumnShard::TCommonCountersOwner& base, const ESpecialTaskCategory cat);

    std::shared_ptr<TTaskSignals> GetTaskSignals(const TString& taskClassName) {
        auto it = TaskClassSignals.find(taskClassName);
        if (it == TaskClassSignals.end()) {
            it = TaskClassSignals.emplace(taskClassName, std::make_shared<TTaskSignals>(*this, taskClassName)).first;
        }
        return it->second;
    }
};

class TWorkersPoolCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    THashMap<ESpecialTaskCategory, std::shared_ptr<TWPCategorySignals>> CategorySignals;

public:
    const ::NMonitoring::THistogramPtr PackSizeHistogram;
    const ::NMonitoring::THistogramPtr PackExecuteHistogram;
    const ::NMonitoring::THistogramPtr SendBackHistogram;
    const ::NMonitoring::THistogramPtr SendFwdHistogram;

    const ::NMonitoring::TDynamicCounters::TCounterPtr SendBackDuration;
    const ::NMonitoring::TDynamicCounters::TCounterPtr SendFwdDuration;

    const ::NMonitoring::TDynamicCounters::TCounterPtr AvailableWorkersCount;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WorkersCountLimit;
    const ::NMonitoring::TDynamicCounters::TCounterPtr AmountCPULimit;

    const ::NMonitoring::TDynamicCounters::TCounterPtr IncomingRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr SolutionsRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr OverlimitRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr WaitWorkerRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr UseWorkerRate;
    const ::NMonitoring::TDynamicCounters::TCounterPtr ChangeCPULimitRate;

    TWorkersPoolCounters(const TString& poolName, const NColumnShard::TCommonCountersOwner& owner);

    std::shared_ptr<TWPCategorySignals> GetCategorySignals(const ESpecialTaskCategory cat) {
        auto it = CategorySignals.find(cat);
        if (it == CategorySignals.end()) {
            it = CategorySignals.emplace(cat, std::make_shared<TWPCategorySignals>(*this, cat)).first;
        }
        return it->second;
    }
};

class TCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    THashMap<ESpecialTaskCategory, std::shared_ptr<TCategorySignals>> CategorySignals;
    THashMap<TString, std::shared_ptr<TWorkersPoolCounters>> WorkersPoolSignals;

public:
    const ::NMonitoring::THistogramPtr ReceiveTaskHistogram;
    const ::NMonitoring::TDynamicCounters::TCounterPtr ReceiveTaskDuration;

    TCounters(const TString& module, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals)
        : TBase(module, baseSignals)
        , ReceiveTaskHistogram(TBase::GetHistogram("ReceiveTask/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
        , ReceiveTaskDuration(TBase::GetDeriviative("ReceiveTask/Duration/Us"))
    {
    }

    std::shared_ptr<TCategorySignals> GetCategorySignals(const ESpecialTaskCategory cat) {
        auto it = CategorySignals.find(cat);
        if (it == CategorySignals.end()) {
            it = CategorySignals.emplace(cat, std::make_shared<TCategorySignals>(*this, cat)).first;
        }
        return it->second;
    }

    std::shared_ptr<TWorkersPoolCounters> GetWorkersPoolSignals(const TString& poolName) {
        auto it = WorkersPoolSignals.find(poolName);
        if (it == WorkersPoolSignals.end()) {
            it = WorkersPoolSignals.emplace(poolName, std::make_shared<TWorkersPoolCounters>(poolName, *this)).first;
        }
        return it->second;
    }
};

}   // namespace NKikimr::NConveyorComposite
