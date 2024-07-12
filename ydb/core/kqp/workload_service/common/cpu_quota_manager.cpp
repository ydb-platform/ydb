#include "cpu_quota_manager.h"

#include <util/string/builder.h>


namespace NKikimr::NKqp::NWorkload {

//// TCpuQuotaManager::TCounters

TCpuQuotaManager::TCounters::TCounters(const ::NMonitoring::TDynamicCounterPtr& subComponent)
    : SubComponent(subComponent)
{
    Register();
}

void TCpuQuotaManager::TCounters::Register() {
    RegisterCommonMetrics(CpuLoadRequest);
    InstantLoadPercentage = SubComponent->GetCounter("InstantLoadPercentage", false);
    AverageLoadPercentage = SubComponent->GetCounter("AverageLoadPercentage", false);
    QuotedLoadPercentage = SubComponent->GetCounter("QuotedLoadPercentage", false);
}

void TCpuQuotaManager::TCounters::RegisterCommonMetrics(TCommonMetrics& metrics) const {
    metrics.Ok = SubComponent->GetCounter("Ok", true);
    metrics.Error = SubComponent->GetCounter("Error", true);
}

//// TCpuQuotaManager::TCpuQuotaResponse

TCpuQuotaManager::TCpuQuotaResponse::TCpuQuotaResponse(int32_t currentLoad, NYdb::EStatus status, NYql::TIssues issues)
    : CurrentLoad(currentLoad)
    , Status(status)
    , Issues(std::move(issues))
{}

//// TCpuQuotaManager

TCpuQuotaManager::TCpuQuotaManager(TDuration monitoringRequestDelay, TDuration averageLoadInterval, TDuration idleTimeout, double defaultQueryLoad, bool strict, ui64 cpuNumber, const ::NMonitoring::TDynamicCounterPtr& subComponent)
    : Counters(subComponent)
    , MonitoringRequestDelay(monitoringRequestDelay)
    , AverageLoadInterval(averageLoadInterval)
    , IdleTimeout(idleTimeout)
    , DefaultQueryLoad(defaultQueryLoad)
    , Strict(strict)
    , CpuNumber(cpuNumber)
{}

double TCpuQuotaManager::GetInstantLoad() const {
    return InstantLoad;
}

double TCpuQuotaManager::GetAverageLoad() const {
    return AverageLoad;
}

TDuration TCpuQuotaManager::GetMonitoringRequestDelay() const {
    return GetMonitoringRequestTime() - TInstant::Now();
}

TInstant TCpuQuotaManager::GetMonitoringRequestTime() const {
    TDuration delay = MonitoringRequestDelay;
    if (IdleTimeout && TInstant::Now() - LastRequestCpuQuota > IdleTimeout) {
        delay = AverageLoadInterval / 2;
    }

    return LastUpdateCpuLoad ? LastUpdateCpuLoad + delay : TInstant::Now();
}

void TCpuQuotaManager::UpdateCpuLoad(double instantLoad, ui64 cpuNumber, bool success) {
    auto now = TInstant::Now();
    LastUpdateCpuLoad = now;

    if (!success) {
        Counters.CpuLoadRequest.Error->Inc();
        CheckLoadIsOutdated();
        return;
    }

    auto delta = now - LastCpuLoad;
    LastCpuLoad = now;

    if (cpuNumber) {
        CpuNumber = cpuNumber;
    }

    InstantLoad = instantLoad;
    // exponential moving average
    if (!Ready || delta >= AverageLoadInterval) {
        AverageLoad = InstantLoad;
        QuotedLoad = InstantLoad;
    } else {
        auto ratio = static_cast<double>(delta.GetValue()) / AverageLoadInterval.GetValue();
        AverageLoad = (1 - ratio) * AverageLoad + ratio * InstantLoad;
        QuotedLoad = (1 - ratio) * QuotedLoad + ratio * InstantLoad;
    }
    Ready = true;
    Counters.CpuLoadRequest.Ok->Inc();
    Counters.InstantLoadPercentage->Set(static_cast<ui64>(InstantLoad * 100));
    Counters.AverageLoadPercentage->Set(static_cast<ui64>(AverageLoad * 100));
    Counters.QuotedLoadPercentage->Set(static_cast<ui64>(QuotedLoad * 100));
}

bool TCpuQuotaManager::CheckLoadIsOutdated() {
    if (TInstant::Now() - LastCpuLoad > AverageLoadInterval) {
        Ready = false;
        QuotedLoad = 0.0;
        Counters.QuotedLoadPercentage->Set(0);
    }
    return Ready;
}

bool TCpuQuotaManager::HasCpuQuota(double maxClusterLoad) {
    LastRequestCpuQuota = TInstant::Now();
    return maxClusterLoad == 0.0 || ((Ready || !Strict) && QuotedLoad < maxClusterLoad);
}

TCpuQuotaManager::TCpuQuotaResponse TCpuQuotaManager::RequestCpuQuota(double quota, double maxClusterLoad) {
    if (quota < 0.0 || quota > 1.0) {
        return TCpuQuotaResponse(-1, NYdb::EStatus::OVERLOADED, {NYql::TIssue(TStringBuilder() << "Incorrect quota value (exceeds 1.0 or less than 0.0) " << quota)});
    }
    quota = quota ? quota : DefaultQueryLoad;

    CheckLoadIsOutdated();
    if (!HasCpuQuota(maxClusterLoad)) {
        return TCpuQuotaResponse(-1, NYdb::EStatus::OVERLOADED, {NYql::TIssue(TStringBuilder()
            << "Cluster is overloaded, current quoted load " << static_cast<ui64>(QuotedLoad * 100)
            << "%, average load " << static_cast<ui64>(AverageLoad * 100) << "%"
        )});
    }

    QuotedLoad += quota;
    Counters.QuotedLoadPercentage->Set(static_cast<ui64>(QuotedLoad * 100));
    return TCpuQuotaResponse(QuotedLoad * 100);
}

void TCpuQuotaManager::AdjustCpuQuota(double quota, TDuration duration, double cpuSecondsConsumed) {
    if (!CpuNumber) {
        return;
    }

    if (duration && duration < AverageLoadInterval / 2 && quota <= 1.0) {
        quota = quota ? quota : DefaultQueryLoad;
        auto load = (cpuSecondsConsumed * 1000.0 / duration.MilliSeconds()) / CpuNumber;
        if (quota > load) {
            auto adjustment = (quota - load) / 2;
            if (QuotedLoad > adjustment) {
                QuotedLoad -= adjustment;
            } else {
                QuotedLoad = 0.0;
            }
            Counters.QuotedLoadPercentage->Set(static_cast<ui64>(QuotedLoad * 100));
        }
    }
}

}  // namespace NKikimr::NKqp::NWorkload
