#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>


namespace NKikimr::NKqp::NWorkload {

class TCpuQuotaManager {
    struct TCounters {
        const ::NMonitoring::TDynamicCounterPtr SubComponent;
        struct TCommonMetrics {
            ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
            ::NMonitoring::TDynamicCounters::TCounterPtr Error;
        };

        TCommonMetrics CpuLoadRequest;
        ::NMonitoring::TDynamicCounters::TCounterPtr InstantLoadPercentage;
        ::NMonitoring::TDynamicCounters::TCounterPtr AverageLoadPercentage;
        ::NMonitoring::TDynamicCounters::TCounterPtr QuotedLoadPercentage;

        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& subComponent);

    private:
        void Register();
        void RegisterCommonMetrics(TCommonMetrics& metrics) const;
    };

public:
    struct TCpuQuotaResponse {
        explicit TCpuQuotaResponse(int32_t currentLoad, NYdb::EStatus status = NYdb::EStatus::SUCCESS, NYql::TIssues issues = {});

        const int32_t CurrentLoad;
        const NYdb::EStatus Status;
        const NYql::TIssues Issues;
    };

public:
    TCpuQuotaManager(TDuration monitoringRequestDelay, TDuration averageLoadInterval, TDuration idleTimeout, double defaultQueryLoad, bool strict, ui64 cpuNumber, const ::NMonitoring::TDynamicCounterPtr& subComponent);

    double GetInstantLoad() const;
    double GetAverageLoad() const;
    TDuration GetMonitoringRequestDelay() const;
    TInstant GetMonitoringRequestTime() const;

    void UpdateCpuLoad(double instantLoad, ui64 cpuNumber, bool success);
    bool CheckLoadIsOutdated();

    bool HasCpuQuota(double maxClusterLoad);
    TCpuQuotaResponse RequestCpuQuota(double quota, double maxClusterLoad);
    void AdjustCpuQuota(double quota, TDuration duration, double cpuSecondsConsumed);

private:
    TCounters Counters;

    const TDuration MonitoringRequestDelay;
    const TDuration AverageLoadInterval;
    const TDuration IdleTimeout;
    const double DefaultQueryLoad;
    const bool Strict;
    ui64 CpuNumber = 0;

    TInstant LastCpuLoad;
    TInstant LastUpdateCpuLoad;
    TInstant LastRequestCpuQuota;

    double InstantLoad = 0.0;
    double AverageLoad = 0.0;
    double QuotedLoad = 0.0;
    bool Ready = false;
};

}  // namespace NKikimr::NKqp::NWorkload
