#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/tracing.h>

namespace NKikimr {
namespace NTracing {

class TTabletTraces {
public:
    using ListType = TList<THolder<ITrace>>;
    using IndexType = THashMap<TTraceID, ListType::iterator>;

    bool AddTrace(ITrace* trace);
    void GetTraces(TVector<TTraceID>& result) const;
    ITrace* GetTrace(TTraceID& traceID);
    ui64 GetSize() const;
    ui64 GetCount() const;

private:
    ListType Traces;
    IndexType Indexes;
    ui64 TotalSize = 0;
};

class TTraceCollection : public ITraceCollection {
public:
    struct TTabletData {
        ui64 TabletID;
        TTabletTraces Traces;
    };

    using MainQueueType = TList<TTabletData>;
    using IndexType = THashMap<ui64, MainQueueType::iterator>;

    TTraceCollection(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

    void AddTrace(ui64 tabletID, ITrace* trace) override;
    // Shows all tablet IDs for which it has data
    void GetTabletIDs(TVector<ui64>& tabletIDs) const override;
    bool HasTabletID(ui64 tabletID) const override;
    // Returns a list of introspection trace ids and creation times for given tabletId
    bool GetTraces(ui64 tabletID, TVector<TTraceID>& result) override;
    ITrace* GetTrace(ui64 tabletID, TTraceID& traceID) override;

private:
    void RemoveElement(MainQueueType::iterator element);
    void CheckSizeLimit();
    void AddTotalSize(ui64 value);
    void SubTotalSize(ui64 value);

    MainQueueType MainQueue;
    IndexType Indexes;
    ui64 TotalSize = 0;

    ::NMonitoring::TDynamicCounters::TCounterPtr ReportedSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReportedCurrentCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReportedTotalCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReportedTabletCount;
    bool Reporting = false;
};

}
}
