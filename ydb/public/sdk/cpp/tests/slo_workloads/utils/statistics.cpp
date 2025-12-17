#include "statistics.h"

#include "metrics.h"
#include "utils.h"


namespace {
    // Calculated percentiles for a period of time
    struct TPercentile {
        TDuration P50;
        TDuration P90;
        TDuration P95;
        TDuration P99;
        TDuration P99_9;
        TDuration P100;
    };

    void CalculatePercentiles(TPercentile& p, std::vector<TDuration>& delays) {
        size_t count = delays.size();
        if (count) {
            std::sort(delays.begin(), delays.end());
            p.P50 = delays[(count - 1) * 50 / 100];
            p.P90 = delays[(count - 1) * 90 / 100];
            p.P95 = delays[(count - 1) * 95 / 100];
            p.P99 = delays[(count - 1) * 99 / 100];
            p.P99_9 = delays[(count - 1) * 999 / 1000];
            p.P100 = delays[count - 1];
        }
    }
}

TStat::TStat(const std::optional<std::string>& metricsPushUrl, const std::string& operationType)
    : StartTime(TInstant::Now())
    , MetricsPusher(metricsPushUrl ? CreateOtelMetricsPusher(*metricsPushUrl, operationType) : CreateNoopMetricsPusher())
{
    MetricsPushQueue.Start(20);
}

void TStat::Start() {
    StartTime = TInstant::Now();
}

void TStat::Finish() {
    FinishTime = TInstant::Now();
}

std::shared_ptr<TStatUnit> TStat::StartRequest() {
    std::lock_guard lock(Mutex);

    ++Infly;
    return std::make_shared<TStatUnit>(TInstant::Now());
}

void TStat::FinishRequest(const std::shared_ptr<TStatUnit>& unit, const TFinalStatus& status) {
    std::lock_guard lock(Mutex);

    unit->End = TInstant::Now();

    auto delay = unit->End - unit->Start;

    --Infly;

    if (status) {
        ++Statuses[status->GetStatus()];
    } else {
        ++ApplicationTimeout;
    }

    if (status && status->GetStatus() == NYdb::EStatus::SUCCESS) {
        OkDelays.push_back(delay);
    }

    ScheduleMetricsPush([this, delay, status, unit]() {
        MetricsPusher->PushRequestData({
            .Delay = delay,
            .Status = status->GetStatus(),
            .RetryAttempts = unit->RetryAttempts
        });
    });
}

void TStat::ReportMaxInfly() {
    std::lock_guard lock(Mutex);

    ++CountMaxInfly;
}

void TStat::ReportStats(std::uint64_t sessions, std::uint64_t readPromises, std::uint64_t executorPromises) {
    std::lock_guard lock(Mutex);

    ActiveSessions = sessions;
    ReadPromises = readPromises;
    ExecutorPromises = executorPromises;
}

void TStat::PrintStatistics(TStringBuilder& out) {
    std::lock_guard lock(Mutex);

    std::uint64_t total = CountMaxInfly;
    for (const auto& [status, counter] : Statuses) {
        total += counter;
    }
    total += ApplicationTimeout;

    TDuration timePassed;
    if (FinishTime < StartTime) {
        // If we ask for current progress
        timePassed = TInstant::Now() - StartTime;
    } else {
        timePassed = FinishTime - StartTime;
    }

    std::uint64_t rps = total * 1000000 / timePassed.MicroSeconds();
    out << total << " requests total" << Endl
        << Statuses[NYdb::EStatus::SUCCESS] << " succeeded";
    if (total) {
        out << " (" << Statuses[NYdb::EStatus::SUCCESS] * 100 / total << "%)";
    }
    for (const auto&[status, counter] : Statuses) {
        out << Endl << counter << " replies with status " << YdbStatusToString(status) << Endl;
    }
    out << Endl << CountMaxInfly << " failed due to max infly" << Endl
        << ApplicationTimeout << " application timeouts" << Endl
        << "Time passed: " << timePassed.ToString() << Endl
        << "Real rps: " << rps << Endl;

    if (OkDelays.size()) {
        TPercentile p;
        CalculatePercentiles(p, OkDelays);

        out << "Global latency percentiles:" << Endl
            << "P50: " << p.P50 << "\tP90: " << p.P90 << "\tP95: " << p.P95 << "\tP99: " << p.P99
            << "\tP99.9: " << p.P99_9 << "\tP100: " << p.P100 << Endl;
    } else {
        out << "Can't calculate latency percentiles: No data (zero requests measured)" << Endl;
    }
}

TInstant TStat::GetStartTime() const {
    return StartTime;
}

void TStat::ScheduleMetricsPush(std::function<void()> func) {
    if (!MetricsPushQueue.AddFunc(func)) {
        Cerr << TInstant::Now().ToRfc822StringLocal() << ": Failed to push metrics" << Endl;
    }
}
