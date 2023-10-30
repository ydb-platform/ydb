#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/common/cache.h>

namespace NFq {
namespace NPrivate {

struct TRequestScopeCounters : public virtual TThrRefBase {
    const TString Name;

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
    ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
    ::NMonitoring::TDynamicCounters::TCounterPtr Error;
    ::NMonitoring::TDynamicCounters::TCounterPtr Timeout;
    ::NMonitoring::TDynamicCounters::TCounterPtr Retry;

    explicit TRequestScopeCounters(const TString& name)
        : Name(name) { }

    void Register(const ::NMonitoring::TDynamicCounterPtr& counters) {
        Counters = counters;
        ::NMonitoring::TDynamicCounterPtr subgroup =
            counters->GetSubgroup("request_scope", Name);
        InFly   = subgroup->GetCounter("InFly", false);
        Ok      = subgroup->GetCounter("Ok", true);
        Error   = subgroup->GetCounter("Error", true);
        Timeout = subgroup->GetCounter("Timeout", true);
        Timeout = subgroup->GetCounter("Retry", true);
    }

    virtual ~TRequestScopeCounters() override {
        Counters->RemoveSubgroup("request_scope", Name);
    }

private:
    static ::NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets() {
        return ::NMonitoring::ExplicitHistogram(
            {0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
    }
};

struct TRequestCommonCounters : public virtual TThrRefBase {
    const TString Name;

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
    ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
    ::NMonitoring::TDynamicCounters::TCounterPtr Error;
    ::NMonitoring::TDynamicCounters::TCounterPtr Timeout;
    ::NMonitoring::TDynamicCounters::TCounterPtr Retry;
    ::NMonitoring::THistogramPtr LatencyMs;

    explicit TRequestCommonCounters(const TString& name)
        : Name(name) { }

    void Register(const ::NMonitoring::TDynamicCounterPtr& counters) {
        Counters = counters;
        ::NMonitoring::TDynamicCounterPtr subgroup =
            counters->GetSubgroup("request_common", Name);
        InFly     = subgroup->GetCounter("InFly", false);
        Ok        = subgroup->GetCounter("Ok", true);
        Error     = subgroup->GetCounter("Error", true);
        Timeout   = subgroup->GetCounter("Timeout", true);
        Retry     = subgroup->GetCounter("Retry", true);
        LatencyMs = subgroup->GetHistogram("LatencyMs", GetLatencyHistogramBuckets());
    }

    virtual ~TRequestCommonCounters() override {
        Counters->RemoveSubgroup("request_common", Name);
    }

private:
    static ::NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets() {
        return ::NMonitoring::ExplicitHistogram(
            {0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
    }
};

using TRequestScopeCountersPtr  = TIntrusivePtr<TRequestScopeCounters>;
using TRequestCommonCountersPtr = TIntrusivePtr<TRequestCommonCounters>;

struct TRequestCounters {
    TRequestScopeCountersPtr Scope;
    TRequestCommonCountersPtr Common;

    void IncInFly() {
        Scope->InFly->Inc();
        Common->InFly->Inc();
    }

    void DecInFly() {
        Scope->InFly->Dec();
        Common->InFly->Dec();
    }

    void IncOk() {
        Scope->Ok->Inc();
        Common->Ok->Inc();
    }

    void IncError() {
        Scope->Error->Inc();
        Common->Error->Inc();
    }

    void IncTimeout() {
        Scope->Timeout->Inc();
        Common->Timeout->Inc();
    }
};

enum ERequestTypeScope {
    RTS_CREATE_QUERY,
    RTS_LIST_QUERIES,
    RTS_DESCRIBE_QUERY,
    RTS_GET_QUERY_STATUS,
    RTS_MODIFY_QUERY,
    RTS_DELETE_QUERY,
    RTS_CONTROL_QUERY,
    RTS_GET_RESULT_DATA,
    RTS_LIST_JOBS,
    RTS_DESCRIBE_JOB,
    RTS_CREATE_CONNECTION,
    RTS_LIST_CONNECTIONS,
    RTS_DESCRIBE_CONNECTION,
    RTS_MODIFY_CONNECTION,
    RTS_DELETE_CONNECTION,
    RTS_TEST_CONNECTION,
    RTS_CREATE_BINDING,
    RTS_LIST_BINDINGS,
    RTS_DESCRIBE_BINDING,
    RTS_MODIFY_BINDING,
    RTS_DELETE_BINDING,
    RTS_MAX,
};

enum ERequestTypeCommon {
    RTC_RESOLVE_FOLDER,
    RTC_CREATE_QUERY,
    RTC_LIST_QUERIES,
    RTC_DESCRIBE_QUERY,
    RTC_GET_QUERY_STATUS,
    RTC_MODIFY_QUERY,
    RTC_DELETE_QUERY,
    RTC_CONTROL_QUERY,
    RTC_GET_RESULT_DATA,
    RTC_LIST_JOBS,
    RTC_DESCRIBE_JOB,
    RTC_CREATE_CONNECTION,
    RTC_LIST_CONNECTIONS,
    RTC_DESCRIBE_CONNECTION,
    RTC_MODIFY_CONNECTION,
    RTC_DELETE_CONNECTION,
    RTC_TEST_CONNECTION,
    RTC_CREATE_BINDING,
    RTC_LIST_BINDINGS,
    RTC_DESCRIBE_BINDING,
    RTC_MODIFY_BINDING,
    RTC_DELETE_BINDING,
    RTC_RESOLVE_SUBJECT_TYPE,
    RTC_DESCRIBE_CPS_ENTITY,
    RTC_CREATE_YDB_SESSION,
    RTC_CREATE_CONNECTION_IN_YDB,
    RTC_CREATE_BINDING_IN_YDB,
    RTC_MODIFY_CONNECTION_IN_YDB,
    RTC_MODIFY_BINDING_IN_YDB,
    RTC_DELETE_CONNECTION_IN_YDB,
    RTC_DELETE_BINDING_IN_YDB,
    RTC_CREATE_COMPUTE_DATABASE,
    RTC_LIST_CPS_ENTITY,
    RTC_MAX,
};

class TCounters : public virtual TThrRefBase {
    struct TMetricsScope {
        TString CloudId;
        TString Scope;

        TMetricsScope() = default;

        TMetricsScope(const TString& cloudId, const TString& scope)
            : CloudId(cloudId)
            , Scope(scope)
        { }

        bool operator<(const TMetricsScope& right) const {
            return std::tie(CloudId, Scope) < std::tie(right.CloudId, right.Scope);
        }
    };

    using TScopeCounters    = std::array<TRequestScopeCountersPtr, RTS_MAX>;
    using TScopeCountersPtr = std::shared_ptr<TScopeCounters>;

    std::array<TRequestCommonCountersPtr, RTC_MAX> CommonRequests = CreateArray<RTC_MAX, TRequestCommonCountersPtr>({
        {MakeIntrusive<TRequestCommonCounters>("ResolveFolder")},
        {MakeIntrusive<TRequestCommonCounters>("CreateQuery")},
        {MakeIntrusive<TRequestCommonCounters>("ListQueries")},
        {MakeIntrusive<TRequestCommonCounters>("DescribeQuery")},
        {MakeIntrusive<TRequestCommonCounters>("GetQueryStatus")},
        {MakeIntrusive<TRequestCommonCounters>("ModifyQuery")},
        {MakeIntrusive<TRequestCommonCounters>("DeleteQuery")},
        {MakeIntrusive<TRequestCommonCounters>("ControlQuery")},
        {MakeIntrusive<TRequestCommonCounters>("GetResultData")},
        {MakeIntrusive<TRequestCommonCounters>("ListJobs")},
        {MakeIntrusive<TRequestCommonCounters>("DescribeJob")},
        {MakeIntrusive<TRequestCommonCounters>("CreateConnection")},
        {MakeIntrusive<TRequestCommonCounters>("ListConnections")},
        {MakeIntrusive<TRequestCommonCounters>("DescribeConnection")},
        {MakeIntrusive<TRequestCommonCounters>("ModifyConnection")},
        {MakeIntrusive<TRequestCommonCounters>("DeleteConnection")},
        {MakeIntrusive<TRequestCommonCounters>("TestConnection")},
        {MakeIntrusive<TRequestCommonCounters>("CreateBinding")},
        {MakeIntrusive<TRequestCommonCounters>("ListBindings")},
        {MakeIntrusive<TRequestCommonCounters>("DescribeBinding")},
        {MakeIntrusive<TRequestCommonCounters>("ModifyBinding")},
        {MakeIntrusive<TRequestCommonCounters>("DeleteBinding")},
        {MakeIntrusive<TRequestCommonCounters>("ResolveSubjectType")},
        {MakeIntrusive<TRequestCommonCounters>("DescribeCPSEntity")},
        {MakeIntrusive<TRequestCommonCounters>("CreateYDBSession")},
        {MakeIntrusive<TRequestCommonCounters>("CreateConnectionInYDB")},
        {MakeIntrusive<TRequestCommonCounters>("CreateBindingInYDB")},
        {MakeIntrusive<TRequestCommonCounters>("ModifyConnectionInYDB")},
        {MakeIntrusive<TRequestCommonCounters>("ModifyBindingInYDB")},
        {MakeIntrusive<TRequestCommonCounters>("DeleteConnectionInYDB")},
        {MakeIntrusive<TRequestCommonCounters>("DeleteBindingInYDB")},
        {MakeIntrusive<TRequestCommonCounters>("CreateComputeDatabase")},
        {MakeIntrusive<TRequestCommonCounters>("ListCPSEntities")},
    });

    TTtlCache<TMetricsScope, TScopeCountersPtr, TMap> ScopeCounters{TTtlCacheSettings{}.SetTtl(TDuration::Days(1))};
    ::NMonitoring::TDynamicCounterPtr Counters;

public:
    explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters)
    {
        for (auto& request : CommonRequests) {
            request->Register(Counters);
        }
    }

    TRequestCounters GetCounters(const TString& cloudId, const TString& scope, ERequestTypeScope scopeType, ERequestTypeCommon commonType) {
        return {GetScopeCounters(cloudId, scope, scopeType), GetCommonCounters(commonType)};
    }

    TRequestCommonCountersPtr GetCommonCounters(ERequestTypeCommon type) {
        return CommonRequests[type];
    }

    TRequestScopeCountersPtr GetScopeCounters(const TString& cloudId, const TString& scope, ERequestTypeScope type) {
        TMetricsScope key{cloudId, scope};
        TMaybe<TScopeCountersPtr> cacheVal;
        ScopeCounters.Get(key, &cacheVal);
        if (cacheVal) {
            return (**cacheVal)[type];
        }

        auto scopeRequests = std::make_shared<TScopeCounters>(CreateArray<RTS_MAX, TRequestScopeCountersPtr>({
            {MakeIntrusive<TRequestScopeCounters>("CreateQuery")},
            {MakeIntrusive<TRequestScopeCounters>("ListQueries")},
            {MakeIntrusive<TRequestScopeCounters>("DescribeQuery")},
            {MakeIntrusive<TRequestScopeCounters>("GetQueryStatus")},
            {MakeIntrusive<TRequestScopeCounters>("ModifyQuery")},
            {MakeIntrusive<TRequestScopeCounters>("DeleteQuery")},
            {MakeIntrusive<TRequestScopeCounters>("ControlQuery")},
            {MakeIntrusive<TRequestScopeCounters>("GetResultData")},
            {MakeIntrusive<TRequestScopeCounters>("ListJobs")},
            {MakeIntrusive<TRequestScopeCounters>("DescribeJob")},
            {MakeIntrusive<TRequestScopeCounters>("CreateConnection")},
            {MakeIntrusive<TRequestScopeCounters>("ListConnections")},
            {MakeIntrusive<TRequestScopeCounters>("DescribeConnection")},
            {MakeIntrusive<TRequestScopeCounters>("ModifyConnection")},
            {MakeIntrusive<TRequestScopeCounters>("DeleteConnection")},
            {MakeIntrusive<TRequestScopeCounters>("TestConnection")},
            {MakeIntrusive<TRequestScopeCounters>("CreateBinding")},
            {MakeIntrusive<TRequestScopeCounters>("ListBindings")},
            {MakeIntrusive<TRequestScopeCounters>("DescribeBinding")},
            {MakeIntrusive<TRequestScopeCounters>("ModifyBinding")},
            {MakeIntrusive<TRequestScopeCounters>("DeleteBinding")},
        }));

        auto scopeCounters = Counters
                                 ->GetSubgroup("cloud_id", cloudId)
                                 ->GetSubgroup("scope", scope);

        for (auto& request : *scopeRequests) {
            request->Register(scopeCounters);
        }
        cacheVal = scopeRequests;
        ScopeCounters.Put(key, cacheVal);
        return (*scopeRequests)[type];
    }
};

} // namespace NPrivate
} // namespace NFq
