#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/public/api/protos/draft/fq.pb.h>

namespace NFq {

class TRequestScopeCounters: public virtual TThrRefBase {
public:
    const TString Name;

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
    ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
    ::NMonitoring::TDynamicCounters::TCounterPtr Error;
    ::NMonitoring::TDynamicCounters::TCounterPtr Retry;

    explicit TRequestScopeCounters(const TString& name);

    void Register(const ::NMonitoring::TDynamicCounterPtr& counters);

    virtual ~TRequestScopeCounters() override;
};

class TRequestCommonCounters: public virtual TThrRefBase {
public:
    const TString Name;

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
    ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
    ::NMonitoring::TDynamicCounters::TCounterPtr Error;
    ::NMonitoring::TDynamicCounters::TCounterPtr ParseProtobufError;
    ::NMonitoring::TDynamicCounters::TCounterPtr Retry;
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr ResponseBytes;
    ::NMonitoring::THistogramPtr LatencyMs;
    ::NMonitoring::TDynamicCounterPtr Issues;

    explicit TRequestCommonCounters(const TString& name);

    void Register(const ::NMonitoring::TDynamicCounterPtr& counters);

    virtual ~TRequestCommonCounters() override;

private:
    static NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets();
};

class TFinalStatusCounters: public virtual TThrRefBase {
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr Completed;
    ::NMonitoring::TDynamicCounters::TCounterPtr AbortedBySystem;
    ::NMonitoring::TDynamicCounters::TCounterPtr AbortedByUser;
    ::NMonitoring::TDynamicCounters::TCounterPtr Failed;
    ::NMonitoring::TDynamicCounters::TCounterPtr Paused;

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr Unavailable;

public:
    TFinalStatusCounters(const ::NMonitoring::TDynamicCounterPtr& counters);

    void IncByStatus(FederatedQuery::QueryMeta::ComputeStatus finalStatus);

    virtual ~TFinalStatusCounters() override;
};

} // NFq
