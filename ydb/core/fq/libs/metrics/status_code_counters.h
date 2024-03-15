#pragma once

#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq {


class TStatusCodeCounters: public virtual TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TStatusCodeCounters>;

public:
    TStatusCodeCounters(const TString& name, const ::NMonitoring::TDynamicCounterPtr& counters);

    // This call isn't thread safe
    void IncByStatusCode(NYql::NDqProto::StatusIds::StatusCode statusCode);

    virtual ~TStatusCodeCounters() override;

private:
    TString Name;
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    TMap<NYql::NDqProto::StatusIds::StatusCode, ::NMonitoring::TDynamicCounters::TCounterPtr> CountersByStatusCode;
};

} // namespace NFq
