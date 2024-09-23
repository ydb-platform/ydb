#pragma once

#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/mutex.h>

namespace NFq {

class TStatusCodeCounters: public virtual TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TStatusCodeCounters>;

public:
    TStatusCodeCounters(const ::NMonitoring::TDynamicCounterPtr& counters);

    // This call isn't thread safe
    void IncByStatusCode(NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& issues);

    virtual ~TStatusCodeCounters() override = default;

private:
    ::NMonitoring::TDynamicCounterPtr Counters;
    TMap<NYql::NDqProto::StatusIds::StatusCode, ::NMonitoring::TDynamicCounters::TCounterPtr> CountersByStatusCode;
};

class TStatusCodeByScopeCounters: public virtual TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TStatusCodeByScopeCounters>;

public:
    TStatusCodeByScopeCounters(const TString& subComponentName, const ::NMonitoring::TDynamicCounterPtr& counters);

    void IncByScopeAndStatusCode(const TString& scope, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& issues);

private:
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounterPtr SubComponentCounters;
    TMap<TString, TStatusCodeCounters::TPtr> StatusCodeCountersByScope;
    TMutex Mutex;
};

TString LabelNameFromStatusCodeAndIssues(NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& issues);

} // namespace NFq
