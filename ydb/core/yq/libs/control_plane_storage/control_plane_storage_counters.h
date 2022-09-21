#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/public/api/protos/yq.pb.h>

namespace NYq {

class TRequestCounters: public virtual TThrRefBase {
public:
    const TString Name;

    ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
    ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
    ::NMonitoring::TDynamicCounters::TCounterPtr Error;
    ::NMonitoring::TDynamicCounters::TCounterPtr Retry;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseBytes;
    NMonitoring::THistogramPtr LatencyMs;
    ::NMonitoring::TDynamicCounterPtr Issues;

    explicit TRequestCounters(const TString& name);

    void Register(const ::NMonitoring::TDynamicCounterPtr& counters);

private:
    static NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets();
};

class TFinalStatusCounters: public virtual TThrRefBase {
    ::NMonitoring::TDynamicCounters::TCounterPtr Completed;
    ::NMonitoring::TDynamicCounters::TCounterPtr AbortedBySystem;
    ::NMonitoring::TDynamicCounters::TCounterPtr AbortedByUser;
    ::NMonitoring::TDynamicCounters::TCounterPtr Failed;
    ::NMonitoring::TDynamicCounters::TCounterPtr Paused;

public:
    TFinalStatusCounters(const ::NMonitoring::TDynamicCounterPtr& counters);

    void IncByStatus(YandexQuery::QueryMeta::ComputeStatus finalStatus);
};

} // NYq
