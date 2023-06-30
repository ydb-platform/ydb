#pragma once
#include "owner.h"
#include <ydb/core/protos/services.pb.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NColumnShard {

template <class TObject, bool UseSignals, bool UseLogs>
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

template <class TObject, bool UseSignals, bool UseLogs>
class TMonitoringObjectsCounter {
private:
    static inline TAtomicCounter Counter = 0;
public:
    TMonitoringObjectsCounter() {
        if (UseSignals) {
            Singleton<TMonitoringObjectsCounterImpl<TObject, UseSignals, UseLogs>>()->Inc();
        }
        Counter.Inc();
        if (UseLogs) {
            ACFL_TRACE(NKikimrServices::OBJECTS_MONITORING)("event", "create")("object_type", TypeName<TObject>())("count", Counter.Val());
        }
    }
    ~TMonitoringObjectsCounter() {
        if (UseSignals) {
            Singleton<TMonitoringObjectsCounterImpl<TObject, UseSignals, UseLogs>>()->Dec();
        }
        Counter.Dec();
        if (UseLogs) {
            ACFL_TRACE(NKikimrServices::OBJECTS_MONITORING)("event", "destroy")("object_type", TypeName<TObject>())("count", Counter.Val());
        }
    }
};

}
