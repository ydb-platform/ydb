#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <deque>


namespace NKikimr::NWorkloadManager {

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
        ::NMonitoring::TDynamicCounters::TCounterPtr PendingQuotaPercentage;

        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& subComponent);

    private:
        void Register();
        void RegisterCommonMetrics(TCommonMetrics& metrics) const;
    };

public:
    struct TSettings {
        TDuration MonitoringRequestDelay = TDuration::Seconds(1);
        TDuration AverageLoadInterval = TDuration::Seconds(10);
        // How long a reservation is held before the query's load is expected to be measured
        TDuration LoadVisibilityDelay = TDuration::Seconds(5);
        TDuration IdleTimeout = TDuration::Seconds(60);
        double DefaultQueryLoad = 0.1;
        bool Strict = true;
        bool EnableLoadReservations = false;
        // Only an initial value, a non zero cpu number from a load measurement wins
        ui64 CpuNumber = 0;
    };

    struct TCpuQuotaResponse {
        explicit TCpuQuotaResponse(int32_t currentLoad, NYdb::EStatus status = NYdb::EStatus::SUCCESS, NYql::TIssues issues = {});

        const int32_t CurrentLoad;
        const NYdb::EStatus Status;
        const NYql::TIssues Issues;
    };

public:
    TCpuQuotaManager(const TSettings& settings, const ::NMonitoring::TDynamicCounterPtr& subComponent);
    virtual ~TCpuQuotaManager() = default;

    void UpdateSettings(const TSettings& settings);

    double GetInstantLoad() const;
    double GetAverageLoad() const;
    double GetQuotedLoad() const;
    TDuration GetMonitoringRequestDelay() const;
    TInstant GetMonitoringRequestTime() const;

    void UpdateCpuLoad(double instantLoad, ui64 cpuNumber, bool success);
    bool CheckLoadIsOutdated();

    bool HasCpuQuota(double maxClusterLoad);
    TCpuQuotaResponse RequestCpuQuota(double quota, double maxClusterLoad);
    void AdjustCpuQuota(double quota, TDuration duration, double cpuSecondsConsumed);

protected:
    virtual TInstant GetNow() const;

private:
    // Quota for a query admitted but not yet visible in the measured load
    struct TPendingQuota {
        TInstant ExpireAt;
        double Quota;
    };

    void PopPendingQuota();
    void ExpirePendingQuota(TInstant now);
    void ReleasePendingQuota(double quota, TDuration duration, TInstant now);
    void UpdateQuotaCounters();

    TCounters Counters;
    TSettings Settings;
    ui64 CpuNumber = 0;

    TInstant LastCpuLoad;
    TInstant LastUpdateCpuLoad;
    TInstant LastRequestCpuQuota;

    double InstantLoad = 0.0;
    double AverageLoad = 0.0;
    double QuotedLoad = 0.0;
    bool Ready = false;

    std::deque<TPendingQuota> PendingQuotas;
    double PendingQuota = 0.0;
};

}  // namespace NKikimr::NWorkloadManager
