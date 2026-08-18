#include "cpu_quota_manager.h"

#include <util/string/builder.h>


namespace NKikimr::NWorkloadManager {

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
    PendingQuotaPercentage = SubComponent->GetCounter("PendingQuotaPercentage", false);
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

TCpuQuotaManager::TCpuQuotaManager(const TSettings& settings, const ::NMonitoring::TDynamicCounterPtr& subComponent)
    : Counters(subComponent)
{
    UpdateSettings(settings);
}

void TCpuQuotaManager::UpdateSettings(const TSettings& settings) {
    const TSettings previous = Settings;
    Settings = settings;

    // AverageLoadInterval is an EMA divisor, DefaultQueryLoad is charged per query
    if (!Settings.AverageLoadInterval) {
        Settings.AverageLoadInterval = previous.AverageLoadInterval;
    }
    if (Settings.DefaultQueryLoad <= 0.0) {
        Settings.DefaultQueryLoad = previous.DefaultQueryLoad;
    }

    // While reservations were in charge the legacy counter kept drifting up unused, so it has
    // to be reseeded, otherwise switching back would stall admission until the drift decays
    if (previous.EnableLoadReservations && !Settings.EnableLoadReservations) {
        QuotedLoad = InstantLoad;
    }
}

TInstant TCpuQuotaManager::GetNow() const {
    return TInstant::Now();
}

double TCpuQuotaManager::GetInstantLoad() const {
    return InstantLoad;
}

double TCpuQuotaManager::GetAverageLoad() const {
    return AverageLoad;
}

double TCpuQuotaManager::GetQuotedLoad() const {
    return Settings.EnableLoadReservations ? InstantLoad + PendingQuota : QuotedLoad;
}

void TCpuQuotaManager::PopPendingQuota() {
    PendingQuota -= PendingQuotas.front().Quota;
    PendingQuotas.pop_front();
    if (PendingQuotas.empty()) {
        PendingQuota = 0.0;
    }
}

void TCpuQuotaManager::ExpirePendingQuota(TInstant now) {
    while (!PendingQuotas.empty() && PendingQuotas.front().ExpireAt <= now) {
        PopPendingQuota();
    }
}

void TCpuQuotaManager::UpdateQuotaCounters() {
    Counters.QuotedLoadPercentage->Set(static_cast<ui64>(GetQuotedLoad() * 100));
    Counters.PendingQuotaPercentage->Set(static_cast<ui64>(PendingQuota * 100));
}

TDuration TCpuQuotaManager::GetMonitoringRequestDelay() const {
    return GetMonitoringRequestTime() - GetNow();
}

TInstant TCpuQuotaManager::GetMonitoringRequestTime() const {
    const auto now = GetNow();

    TDuration delay = Settings.MonitoringRequestDelay;
    if (Settings.IdleTimeout && now - LastRequestCpuQuota > Settings.IdleTimeout) {
        delay = Settings.AverageLoadInterval / 2;
    }

    return LastUpdateCpuLoad ? LastUpdateCpuLoad + delay : now;
}

void TCpuQuotaManager::UpdateCpuLoad(double instantLoad, ui64 cpuNumber, bool success) {
    auto now = GetNow();
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
    ExpirePendingQuota(now);

    // exponential moving average
    if (!Ready || delta >= Settings.AverageLoadInterval) {
        AverageLoad = InstantLoad;
        QuotedLoad = InstantLoad;
    } else {
        auto ratio = static_cast<double>(delta.GetValue()) / Settings.AverageLoadInterval.GetValue();
        AverageLoad = (1 - ratio) * AverageLoad + ratio * InstantLoad;
        QuotedLoad = (1 - ratio) * QuotedLoad + ratio * InstantLoad;
    }
    Ready = true;
    Counters.CpuLoadRequest.Ok->Inc();
    Counters.InstantLoadPercentage->Set(static_cast<ui64>(InstantLoad * 100));
    Counters.AverageLoadPercentage->Set(static_cast<ui64>(AverageLoad * 100));
    UpdateQuotaCounters();
}

bool TCpuQuotaManager::CheckLoadIsOutdated() {
    if (GetNow() - LastCpuLoad > Settings.AverageLoadInterval) {
        Ready = false;
        QuotedLoad = 0.0;
        UpdateQuotaCounters();
    }
    return Ready;
}

bool TCpuQuotaManager::HasCpuQuota(double maxClusterLoad) {
    const auto now = GetNow();
    LastRequestCpuQuota = now;
    ExpirePendingQuota(now);
    return maxClusterLoad == 0.0 || ((Ready || !Settings.Strict) && GetQuotedLoad() < maxClusterLoad);
}

TCpuQuotaManager::TCpuQuotaResponse TCpuQuotaManager::RequestCpuQuota(double quota, double maxClusterLoad) {
    if (quota < 0.0 || quota > 1.0) {
        return TCpuQuotaResponse(-1, NYdb::EStatus::OVERLOADED, {NYql::TIssue(TStringBuilder() << "Incorrect quota value (exceeds 1.0 or less than 0.0) " << quota)});
    }
    quota = quota ? quota : Settings.DefaultQueryLoad;

    CheckLoadIsOutdated();
    if (!HasCpuQuota(maxClusterLoad)) {
        return TCpuQuotaResponse(-1, NYdb::EStatus::OVERLOADED, {NYql::TIssue(TStringBuilder()
            << "Cluster is overloaded, current quoted load " << static_cast<ui64>(GetQuotedLoad() * 100)
            << "%, average load " << static_cast<ui64>(AverageLoad * 100) << "%"
        )});
    }

    QuotedLoad += quota;
    PendingQuotas.push_back({GetNow() + Settings.LoadVisibilityDelay, quota});
    PendingQuota += quota;
    UpdateQuotaCounters();
    return TCpuQuotaResponse(GetQuotedLoad() * 100);
}

void TCpuQuotaManager::AdjustCpuQuota(double quota, TDuration duration, double cpuSecondsConsumed) {
    ExpirePendingQuota(GetNow());

    // The query is over, so its whole reservation is returned. Reservations are all worth the
    // same, so releasing the soonest expiring one is releasing this query's own.
    if (duration < Settings.LoadVisibilityDelay && !PendingQuotas.empty()) {
        PopPendingQuota();
    }

    if (CpuNumber && duration && duration < Settings.AverageLoadInterval / 2 && quota <= 1.0) {
        const double queryQuota = quota ? quota : Settings.DefaultQueryLoad;
        const double load = (cpuSecondsConsumed * 1000.0 / duration.MilliSeconds()) / CpuNumber;
        if (queryQuota > load) {
            const double adjustment = (queryQuota - load) / 2;
            QuotedLoad = (QuotedLoad > adjustment) ? QuotedLoad - adjustment : 0.0;
        }
    }

    UpdateQuotaCounters();
}

}  // namespace NKikimr::NWorkloadManager
