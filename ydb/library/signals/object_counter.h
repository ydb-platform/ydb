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
            ACFL_TRACE(NKikimrServices::OBJECTS_MONITORING)("event", "create")("object_type", TypeName<TObject>())("count", Counter.Val());
        }
    }
    ~TMonitoringObjectsCounter() {
        if (UseSignals) {
            Singleton<TMonitoringObjectsCounterImpl<TObject>>()->Dec();
        }
        Counter.Dec();
        if (UseLogs) {
            ACFL_TRACE(NKikimrServices::OBJECTS_MONITORING)("event", "destroy")("object_type", TypeName<TObject>())("count", Counter.Val());
        }
    }
};

}
