#pragma once
#include "owner.h"
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

template <class TObject>
class TMonitoringObjectsCounterImpl: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr Counter;
    NMonitoring::TDynamicCounters::TCounterPtr Allocation;
    NMonitoring::TDynamicCounters::TCounterPtr Free;
public:
    TMonitoringObjectsCounterImpl()
        : TBase("ObjectsCounter")
    {
        TBase::DeepSubGroup("type_id", TypeName<TObject>());
        Counter = TCommonCountersOwner::GetValue("ObjectsCount");
        Allocation = TCommonCountersOwner::GetDeriviative("Allocation");
        Free = TCommonCountersOwner::GetDeriviative("Free");
    }

    void Inc() const {
        Counter->Inc();
        Allocation->Add(1);
    }

    void Dec() const {
        Counter->Dec();
        Free->Add(1);
    }
};

template <class TObject, bool UseSignals = true, bool UseLogs = false>
class TMonitoringObjectsCounter {
private:
    static inline TAtomicCounter Counter = 0;
public:
    static inline TAtomicCounter GetCounter() {
        return Counter.Val();
    }

    TMonitoringObjectsCounter() {
        if (UseSignals) {
            Singleton<TMonitoringObjectsCounterImpl<TObject>>()->Inc();
        }
        Counter.Inc();
        if (UseLogs) {
            YDB_LOG_TRACE_COMP(NKikimrServices::OBJECTS_MONITORING, "Create object",
                {"object_type", TypeName<TObject>()},
                {"count", Counter.Val()});
        }
    }
    TMonitoringObjectsCounter(const TMonitoringObjectsCounter&)
        : TMonitoringObjectsCounter()
    {
    }
    TMonitoringObjectsCounter(TMonitoringObjectsCounter&&)
        : TMonitoringObjectsCounter()
    {
    }
    ~TMonitoringObjectsCounter() {
        if (UseSignals) {
            Singleton<TMonitoringObjectsCounterImpl<TObject>>()->Dec();
        }
        Counter.Dec();
        if (UseLogs) {
            YDB_LOG_TRACE_COMP(NKikimrServices::OBJECTS_MONITORING, "Destroy object",
                {"object_type", TypeName<TObject>()},
                {"count", Counter.Val()});
        }
    }
};

}
