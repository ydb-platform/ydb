#pragma once

#include "metrics.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/thread/pool.h>

#include <mutex>

inline std::string GetMillisecondsStr(const TDuration& d) {
    return TStringBuilder() << d.MilliSeconds() << '.' << Sprintf("%03" PRIu64, d.MicroSeconds() % 1000);
}

inline double GetMillisecondsDouble(const TDuration& d) {
    return static_cast<double>(d.MicroSeconds()) / 1000;
}

using TFinalStatus = std::optional<NYdb::TStatus>;
using TAsyncFinalStatus = NThreading::TFuture<TFinalStatus>;

// Request unit
struct TStatUnit {
    TStatUnit(TInstant start)
        : Start(start)
        , RetryAttempts(0)
        , IsFirstAttempt(true)
    {}

    void IncRetryAttempts() {
        if (IsFirstAttempt) {
            IsFirstAttempt = false;
        } else {
            ++RetryAttempts;
        }
    }

    TInstant Start;
    TInstant End;

    std::uint64_t RetryAttempts;
    bool IsFirstAttempt;
};

class TStat {
public:
    explicit TStat(const std::optional<std::string>& metricsPushUrl, const std::string& operationType);

    void Start();
    void Finish();

    std::shared_ptr<TStatUnit> StartRequest();
    void FinishRequest(const std::shared_ptr<TStatUnit>& unit, const TFinalStatus& status);

    void ReportMaxInfly();
    void ReportStats(std::uint64_t sessions, std::uint64_t readPromises, std::uint64_t executorPromises);

    void PrintStatistics(TStringBuilder& out);

    TInstant GetStartTime() const;

private:
    void ScheduleMetricsPush(std::function<void()> func);

    std::recursive_mutex Mutex;
    TThreadPool MetricsPushQueue;

    TInstant StartTime;
    TInstant FinishTime;

    // program lifetime
    std::uint64_t Infly = 0;
    std::uint64_t ActiveSessions = 0;

    std::map<NYdb::EStatus, std::uint64_t> Statuses;
    std::uint64_t CountMaxInfly = 0;
    std::uint64_t ApplicationTimeout = 0;
    std::vector<TDuration> OkDelays;

    // Debug use only:
    std::uint64_t ReadPromises = 0;
    std::uint64_t ExecutorPromises = 0;

    std::unique_ptr<IMetricsPusher> MetricsPusher;
};
