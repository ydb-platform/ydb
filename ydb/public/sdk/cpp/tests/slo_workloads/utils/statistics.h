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
    TStatUnit(TPeriodData* periodData, TInstant startTime);
    void Report(const TInnerStatus& status);
    TDuration Delay() const;

    std::shared_ptr<TPeriodData> PeriodData;
    TInstant Start;
    TInstant End;
};

struct TCounters {
    ui64 Infly = 0;
    ui64 ActiveSessions = 0;

    // Debug use only:
    ui64 ReadPromises = 0;
    ui64 ExecutorPromises = 0;
};

struct TReplies {
    std::map<NYdb::EStatus, ui64> Statuses;
    ui64 CountMaxInfly = 0;
    ui64 CountHighLatency = 0;
    ui64 ApplicationTimeout = 0;
    ui64 NotFinished = 0;
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
    ui64 Seconds = 0;
    TCounters Counters;
    TReplies Replies;
    TPercentile Oks;
    TPercentile NotOks;
};

class TStat;

// Full latency data for a period of time
class TPeriodData {
public:
    TPeriodData(
        TStat* stats,
        bool RetryMode,
        const TDuration maxDelay,
        ui64 currentSecond,
        ui64 currInfly,
        ui64 currSessions,
        ui64 currPromises,
        ui64 currExecutorPromises
    );
    ~TPeriodData();
    TStatUnit CreateStatUnit(TInstant startTime);
    void AddStat(TDuration delay, const TInnerStatus& status);
    ui64 GetCurrentSecond() const;
    void ReportMaxInfly();
    void ReportInfly(ui64 infly);
    void ReportActiveSessions(ui64 sessions);

    // Debug use only:
    void ReportReadPromises(ui64 promises);
    void ReportExecutorPromises(ui64 promises);

private:
    TStat* Stats;
    bool RetryMode;
    ui64 CurrentSecond;
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
    TStatUnit CreateStatUnit();
    void Report(TStatUnit& unit, const TInnerStatus& status);
    void Report(TStatUnit& unit, TInnerStatus::EInnerStatus innerStatus);
    void Report(TStatUnit& unit, const TFinalStatus& status);
    void Report(TStatUnit& unit, NYdb::EStatus status);
    void ReportMaxInfly();
    void ReportStats(ui64 infly, ui64 sessions, ui64 readPromises, ui64 executorPromises);
    void Reserve(size_t size);
    void ReportLatencyData(
        ui64 currSecond,
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
    void ResetMetricsPusher(ui64 timestamp);
    void CalculateGlobalPercentile();
    void CalculateFailSeconds();
    ui64 GetTotal();

    std::mutex Mutex;
    TThreadPool MetricsPushQueue;
    // Current period we collect stats for
    std::shared_ptr<TPeriodData> ActivePeriod;
    std::vector<TPeriodStat> LatencyStats;
    std::vector<std::vector<TDuration>> LatencyData;
    TDuration MaxDelay;
    TInstant StartTime;
    TInstant FinishTime;
    std::unordered_map<std::string, size_t> SessionStats;
    ui64 CurrentSecond = 0;
    // program lifetime
    TCounters Counters;
    TReplies Replies;
    bool PushMetrics;
    bool RetryMode;
    std::string ResultFileName;
    std::unique_ptr<IMetricsPusher> MetricsPusher;

    std::unique_ptr<TPercentile> GlobalPercentile;
    std::unique_ptr<ui64> FailSeconds;
};

std::string YdbStatusToString(NYdb::EStatus status);
