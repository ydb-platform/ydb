#pragma once

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

struct TInnerStatus {
    enum EInnerStatus {
        StatusUnknown,
        StatusReceived,
        StatusApplicationTimeout,
        StatusNotFinished
    };

    TInnerStatus(EInnerStatus innerStatus = StatusUnknown, NYdb::EStatus ydbStatus = NYdb::EStatus::STATUS_UNDEFINED)
        : InnerStatus(innerStatus)
        , YdbStatus(ydbStatus)
    {}

    EInnerStatus InnerStatus;
    NYdb::EStatus YdbStatus;
};

class TPeriodData;

// Primitive latency unit
struct TStatUnit {
    TStatUnit(const std::shared_ptr<TPeriodData>& periodData, TInstant startTime);
    void Report(const TInnerStatus& status);
    TDuration Delay() const;

    std::shared_ptr<TPeriodData> PeriodData;
    TInstant Start;
    TInstant End;
};

struct TCounters {
    std::uint64_t Infly = 0;
    std::uint64_t ActiveSessions = 0;

    // Debug use only:
    std::uint64_t ReadPromises = 0;
    std::uint64_t ExecutorPromises = 0;
};

struct TReplies {
    std::map<NYdb::EStatus, std::uint64_t> Statuses;
    std::uint64_t CountMaxInfly = 0;
    std::uint64_t CountHighLatency = 0;
    std::uint64_t ApplicationTimeout = 0;
    std::uint64_t NotFinished = 0;
};

// Calculated percentiles for a period of time
struct TPercentile {
    TDuration P50;
    TDuration P90;
    TDuration P95;
    TDuration P99;
    TDuration P99_9;
    TDuration P100;
};

// Accumulated statistics for a period of time
struct TPeriodStat {
    std::uint64_t Seconds = 0;
    TCounters Counters;
    TReplies Replies;
    TPercentile Oks;
    TPercentile NotOks;
};

class TStat;

// Full latency data for a period of time
class TPeriodData : public std::enable_shared_from_this<TPeriodData> {
public:
    TPeriodData(
        TStat* stats,
        bool RetryMode,
        const TDuration maxDelay,
        std::uint64_t currentSecond,
        std::uint64_t currInfly,
        std::uint64_t currSessions,
        std::uint64_t currPromises,
        std::uint64_t currExecutorPromises
    );
    ~TPeriodData();
    TStatUnit CreateStatUnit(TInstant startTime);
    void AddStat(TDuration delay, const TInnerStatus& status);
    std::uint64_t GetCurrentSecond() const;
    void ReportMaxInfly();
    void ReportInfly(std::uint64_t infly);
    void ReportActiveSessions(std::uint64_t sessions);

    // Debug use only:
    void ReportReadPromises(std::uint64_t promises);
    void ReportExecutorPromises(std::uint64_t promises);

private:
    TStat* Stats;
    bool RetryMode;
    std::uint64_t CurrentSecond;
    std::vector<TDuration> OkDelays;
    std::vector<TDuration> NotOkDelays;
    TCounters Counters;
    TReplies Replies;
    TDuration MaxDelay;
};

class IMetricsPusher {
public:
    virtual ~IMetricsPusher() = default;
    virtual void PushData(const TPeriodStat& p) = 0;
};

class TStat {
    friend class TPeriodData;
public:
    TStat(
        TDuration maxDelay,
        const std::string& resultFileName,
        bool pushMetrics,
        bool retryMode
    );
    ~TStat();
    void Reset();
    void Finish();
    void Flush();
    TStatUnit CreateStatUnit();
    void Report(TStatUnit& unit, const TInnerStatus& status);
    void Report(TStatUnit& unit, TInnerStatus::EInnerStatus innerStatus);
    void Report(TStatUnit& unit, const TFinalStatus& status);
    void Report(TStatUnit& unit, NYdb::EStatus status);
    void ReportMaxInfly();
    void ReportStats(std::uint64_t infly, std::uint64_t sessions, std::uint64_t readPromises, std::uint64_t executorPromises);
    void Reserve(size_t size);
    void ReportLatencyData(
        std::uint64_t currSecond,
        TCounters&& counters,
        TReplies&& replies,
        std::vector<TDuration>&& oks,
        std::vector<TDuration>& notOks
    );
    void UpdateSessionStats(const std::unordered_map<std::string, size_t>& sessionStats);

    void PrintStatistics(TStringBuilder& out);
    void SaveResult();

    TInstant GetStartTime() const;

private:
    void OnReport(TStatUnit& unit, const TInnerStatus& status);
    void CheckCurrentSecond(TInstant now);
    void PushMetricsData(const TPeriodStat& p);
    void ResetMetricsPusher(std::uint64_t timestamp);
    void CalculateGlobalPercentile();
    void CalculateFailSeconds();
    std::uint64_t GetTotal();

    std::recursive_mutex Mutex;
    TThreadPool MetricsPushQueue;
    // Current period we collect stats for
    std::shared_ptr<TPeriodData> ActivePeriod;
    std::vector<TPeriodStat> LatencyStats;
    std::vector<std::vector<TDuration>> LatencyData;
    TDuration MaxDelay;
    TInstant StartTime;
    TInstant FinishTime;
    std::unordered_map<std::string, size_t> SessionStats;
    std::uint64_t CurrentSecond = 0;
    // program lifetime
    TCounters Counters;
    TReplies Replies;
    bool PushMetrics;
    bool RetryMode;
    std::string ResultFileName;
    std::unique_ptr<IMetricsPusher> MetricsPusher;

    std::unique_ptr<TPercentile> GlobalPercentile;
    std::unique_ptr<std::uint64_t> FailSeconds;
};

std::string YdbStatusToString(NYdb::EStatus status);
