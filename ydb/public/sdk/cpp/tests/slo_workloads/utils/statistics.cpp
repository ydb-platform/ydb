#include "statistics.h"

#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/json_value.h>

#include <util/stream/file.h>


TStatUnit::TStatUnit(const std::shared_ptr<TPeriodData>& periodData, TInstant startTime)
    : PeriodData(periodData)
    , Start(startTime)
{
}

void TStatUnit::Report(const TInnerStatus& status) {
    PeriodData->AddStat(Delay(), status);
}

TDuration TStatUnit::Delay() const {
    return End - Start;
}

TPeriodData::TPeriodData(
    TStat* stats,
    bool retryMode,
    const TDuration maxDelay,
    std::uint64_t currentSecond,
    std::uint64_t currInfly,
    std::uint64_t currSessions,
    std::uint64_t currPromises,
    std::uint64_t currExecutorPromises
)
    : Stats(stats)
    , RetryMode(retryMode)
    , CurrentSecond(currentSecond)
    , MaxDelay(maxDelay)
{
    Counters.Infly = currInfly;
    Counters.ActiveSessions = currSessions;
    Counters.ReadPromises = currPromises;
    Counters.ExecutorPromises = currExecutorPromises;
}

TPeriodData::~TPeriodData() {
    Stats->ReportLatencyData(CurrentSecond, std::move(Counters), std::move(Replies), std::move(OkDelays), NotOkDelays);
}

TStatUnit TPeriodData::CreateStatUnit(TInstant startTime) {
    return TStatUnit(shared_from_this(), startTime);
}

std::uint64_t TPeriodData::GetCurrentSecond() const {
    return CurrentSecond;
}

void TPeriodData::ReportMaxInfly() {
    ++Replies.CountMaxInfly;
}

void TPeriodData::ReportInfly(std::uint64_t infly) {
    if (infly > Counters.Infly) {
        Counters.Infly = infly;
    }
}

void TPeriodData::ReportActiveSessions(std::uint64_t sessions) {
    if (sessions > Counters.ActiveSessions) {
        Counters.ActiveSessions = sessions;
    }
}

// Debug use only:
void TPeriodData::ReportReadPromises(std::uint64_t promises) {
    if (promises > Counters.ReadPromises) {
        Counters.ReadPromises = promises;
    }
}
void TPeriodData::ReportExecutorPromises(std::uint64_t promises) {
    if (promises > Counters.ExecutorPromises) {
        Counters.ExecutorPromises = promises;
    }
}

void TPeriodData::AddStat(TDuration delay, const TInnerStatus& status) {
    if (status.InnerStatus == TInnerStatus::StatusReceived && status.YdbStatus == NYdb::EStatus::SUCCESS) {
        OkDelays.push_back(delay);
    } else {
        NotOkDelays.push_back(delay);
    }

    switch (status.InnerStatus) {
    case TInnerStatus::StatusReceived:
        if (status.YdbStatus != NYdb::EStatus::SUCCESS || !RetryMode || delay <= MaxDelay) {
            ++Replies.Statuses[status.YdbStatus];
        } else {
            ++Replies.CountHighLatency;
        }
        break;
    case TInnerStatus::StatusApplicationTimeout:
        ++Replies.ApplicationTimeout;
        break;
    case TInnerStatus::StatusNotFinished:
        ++Replies.NotFinished;
        break;
    default:
        Y_ABORT_UNLESS(status.InnerStatus);
        break;
    }
}

namespace {
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

TStat::TStat(
    TDuration maxDelay,
    const std::string& resultFileName,
    bool pushMetrics,
    bool retryMode
)
    : MaxDelay(maxDelay)
    , StartTime(TInstant::Now())
    , PushMetrics(pushMetrics)
    , RetryMode(retryMode)
    , ResultFileName(resultFileName)
{
    MetricsPushQueue.Start(20);
}

TStat::~TStat() {
    Flush();
}

void TStat::Flush() {
    std::uint64_t lastSecMeasured = TInstant::Now().Seconds();
    if (ActivePeriod) {
        lastSecMeasured = ActivePeriod->GetCurrentSecond();
        ActivePeriod.reset();
    }
    if (PushMetrics) {
        ResetMetricsPusher(lastSecMeasured + 1);
        MetricsPushQueue.Stop();
    }
}

void TStat::Reset() {
    if (PushMetrics) {
        ResetMetricsPusher(TInstant::Now().Seconds() - 1);
    }
    StartTime = TInstant::Now();
}

void TStat::Finish() {
    FinishTime = TInstant::Now();
}

TStatUnit TStat::CreateStatUnit() {
    std::lock_guard lock(Mutex);

    TInstant Now = TInstant::Now();
    CheckCurrentSecond(Now);
    return ActivePeriod->CreateStatUnit(Now);
}

void TStat::Report(TStatUnit& unit, const TInnerStatus& status) {
    unit.End = TInstant::Now();
    OnReport(unit, status);
}

void TStat::Report(TStatUnit& unit, TInnerStatus::EInnerStatus innerStatus) {
    Report(unit, TInnerStatus(innerStatus));
}

void TStat::Report(TStatUnit& unit, const TFinalStatus& status) {
    Report(
        unit,
        status
        ? TInnerStatus(TInnerStatus::StatusReceived, status->GetStatus())
        : TInnerStatus(TInnerStatus::StatusApplicationTimeout)
    );
}

void TStat::Report(TStatUnit& unit, NYdb::EStatus status) {
    Report(unit, TInnerStatus(TInnerStatus::StatusReceived, status));
}

void TStat::ReportMaxInfly() {
    std::lock_guard lock(Mutex);

    ++Replies.CountMaxInfly;
    TInstant Now = TInstant::Now();
    CheckCurrentSecond(Now);
    ActivePeriod->ReportMaxInfly();
}

void TStat::ReportStats(std::uint64_t infly, std::uint64_t sessions, std::uint64_t readPromises, std::uint64_t executorPromises) {
    std::lock_guard lock(Mutex);

    Counters.Infly = infly;
    Counters.ActiveSessions = sessions;
    Counters.ReadPromises = readPromises;
    Counters.ExecutorPromises = executorPromises;
    TInstant Now = TInstant::Now();
    CheckCurrentSecond(Now);
    ActivePeriod->ReportInfly(infly);
    ActivePeriod->ReportActiveSessions(sessions);
    ActivePeriod->ReportReadPromises(readPromises);
    ActivePeriod->ReportExecutorPromises(executorPromises);
}

void TStat::Reserve(size_t size) {
    std::lock_guard lock(Mutex);

    LatencyStats.reserve(size);
    LatencyData.reserve(size);
}

void TStat::ReportLatencyData(
    std::uint64_t currSecond,
    TCounters&& counters,
    TReplies&& replies,
    std::vector<TDuration>&& oks,
    std::vector<TDuration>& notOks
) {
    std::lock_guard lock(Mutex);

    LatencyStats.emplace_back();
    TPeriodStat& p = LatencyStats.back();
    p.Seconds = currSecond;
    std::swap(p.Counters, counters);
    std::swap(p.Replies, replies);
    CalculatePercentiles(p.Oks, oks);
    CalculatePercentiles(p.NotOks, notOks);
    LatencyData.emplace_back(std::move(oks));
    PushMetricsData(p);
}

void TStat::UpdateSessionStats(
    const std::unordered_map<std::string, size_t>& sessionStats
) {
    std::lock_guard lock(Mutex);

    SessionStats = sessionStats;
}

void TStat::PrintStatistics(TStringBuilder& out) {
    std::lock_guard lock(Mutex);

    TInstant now = TInstant::Now();
    CheckCurrentSecond(now);
    std::uint64_t total = GetTotal();

    TDuration timePassed;
    if (FinishTime < StartTime) {
        // If we ask for current progress
        timePassed = now - StartTime;
    } else {
        timePassed = FinishTime - StartTime;
    }

    std::uint64_t rps = total * 1000000 / timePassed.MicroSeconds();
    out << total << " requests total" << Endl
        << Replies.Statuses[NYdb::EStatus::SUCCESS] << " succeeded";
    if (total) {
        out << " (" << Replies.Statuses[NYdb::EStatus::SUCCESS] * 100 / total << "%)";
    }
    for (const auto&[status, counter] : Replies.Statuses) {
        out << Endl << counter << " replies with status " << YdbStatusToString(status) << Endl;
    }
    out << Endl << Replies.CountMaxInfly << " failed due to max infly" << Endl
        << Replies.CountHighLatency << " OK results exceeded latency limit of " << MaxDelay << Endl
        << Replies.ApplicationTimeout << " application timeouts" << Endl
        << Replies.NotFinished << " requests not finished within program lifetime" << Endl
        << "Time passed: " << timePassed.ToString() << Endl
        << "Real rps: " << rps << Endl;

    if (LatencyData.size()) {
        CalculateGlobalPercentile();
        TPercentile& p = *GlobalPercentile;
        out << "Global latency percentiles (" << LatencyData.size() << " seconds measured):" << Endl
            << "P50: " << p.P50 << "\tP90: " << p.P90 << "\tP95: " << p.P95 << "\tP99: " << p.P99
            << "\tP99.9: " << p.P99_9 << "\tP100: " << p.P100 << Endl;
        CalculateFailSeconds();
        out << *FailSeconds << " seconds where p99 reached max delay of " << MaxDelay << Endl;
    } else {
        out << "Can't calculate latency percentiles: No data (zero requests measured)" << Endl;
    }
}

void TStat::SaveResult() {
    std::lock_guard lock(Mutex);

    if (LatencyData.size()) {
        CalculateGlobalPercentile();
        CalculateFailSeconds();
        NJson::TJsonValue root;
        root["Oks"] = Replies.Statuses[NYdb::EStatus::SUCCESS];
        root["Total"] = GetTotal();
        root["P99"] = GlobalPercentile->P99.MilliSeconds();
        root["FailSeconds"] = *FailSeconds;
        root["StartTime"] = StartTime.Seconds();
        root["FinishTime"] = FinishTime.Seconds();
        NJson::TJsonValue& items = root["SessionCountsAtFinish"];
        items.SetType(NJson::JSON_ARRAY);
        for (const auto& s : SessionStats) {
            items.AppendValue(s.second);
        }
        TFileOutput resultFile(ResultFileName);
        NJsonWriter::TBuf buf;
        buf.WriteJsonValue(&root);
        resultFile << buf.Str();
        Cout << "Result saved to file " << ResultFileName << Endl;
    }
}

void TStat::CalculateGlobalPercentile() {
    if (GlobalPercentile) {
        return;
    }
    std::vector<TDuration> fullData;
    size_t totalSize = 0;
    for (auto& periodData : LatencyData) {
        totalSize += periodData.size();
    }
    fullData.reserve(totalSize);
    for (auto& periodData : LatencyData) {
        fullData.insert(fullData.end(), periodData.begin(), periodData.end());
    }
    GlobalPercentile = std::make_unique<TPercentile>();
    CalculatePercentiles(*GlobalPercentile, fullData);
}

namespace {
    bool IsGoodInterval(const TPeriodStat& stat, const TDuration& maxDelay) {
        for (const auto& [status, counter] : stat.Replies.Statuses) {
            if (status != NYdb::EStatus::SUCCESS && counter) {
                return false;
            }
        }
        return stat.Oks.P99 <= maxDelay && !stat.Replies.CountMaxInfly && !stat.Replies.ApplicationTimeout;
    }
}

void TStat::CalculateFailSeconds() {
    if (FailSeconds) {
        return;
    }
    std::sort(LatencyStats.begin(), LatencyStats.end(), [&](const TPeriodStat& a, const TPeriodStat& b) {
        return a.Seconds < b.Seconds;
    });
    FailSeconds = std::make_unique<size_t>(0);
    size_t& failSeconds = *FailSeconds;
    std::uint64_t lastSecChecked = LatencyStats[0].Seconds - 1;
    for (auto& stat : LatencyStats) {
        failSeconds += stat.Seconds - lastSecChecked - 1;
        lastSecChecked = stat.Seconds;
        if (!IsGoodInterval(stat, MaxDelay)) {
            ++failSeconds;
        }
    }
}

std::uint64_t TStat::GetTotal() {
    std::uint64_t total = Replies.CountMaxInfly + Replies.CountHighLatency + Replies.NotFinished;
    if (!RetryMode) {
        for (const auto& [status, counter] : Replies.Statuses) {
            total += counter;
        }
        total += Replies.ApplicationTimeout;
    } else {
        total += Replies.Statuses[NYdb::EStatus::SUCCESS];
    }
    return total;
}

TInstant TStat::GetStartTime() const {
    return StartTime;
}

void TStat::OnReport(TStatUnit& unit, const TInnerStatus& status) {
    std::lock_guard lock(Mutex);

    TDuration delay = unit.Delay();
    switch (status.InnerStatus) {
    case TInnerStatus::StatusReceived:
        if (status.YdbStatus != NYdb::EStatus::SUCCESS || !RetryMode || delay <= MaxDelay) {
            ++Replies.Statuses[status.YdbStatus];
        } else {
            ++Replies.CountHighLatency;
        }
        break;
    case TInnerStatus::StatusApplicationTimeout:
        ++Replies.ApplicationTimeout;
        break;
    case TInnerStatus::StatusNotFinished:
        ++Replies.NotFinished;
        break;
    default:
        Y_ABORT_UNLESS(status.InnerStatus);
        break;
    }
    // Saving delay stats
    unit.Report(status);
}

void TStat::CheckCurrentSecond(TInstant now) {
    std::uint64_t currSecond = now.Seconds();
    if (currSecond > CurrentSecond) {
        CurrentSecond = currSecond;
        ActivePeriod = std::make_shared<TPeriodData>(
            this,
            RetryMode,
            MaxDelay,
            currSecond,
            Counters.Infly,
            Counters.ActiveSessions,
            Counters.ReadPromises,
            Counters.ExecutorPromises
        );
    }
}

void TStat::PushMetricsData(const TPeriodStat& p) {
    if (!PushMetrics) {
        return;
    }
    auto threadFunc = [this, p]() {
        MetricsPusher->PushData(p);
    };
    if (!MetricsPushQueue.AddFunc(threadFunc)) {
        Cerr << TInstant::Now().ToRfc822StringLocal() << ": Failed to push data to solomon" << Endl;
    }
}

void TStat::ResetMetricsPusher(std::uint64_t timestamp) {
    while (timestamp >= TInstant::Now().Seconds()) {
        Sleep(TDuration::Seconds(1));
    }
    TPeriodStat pStat;
    pStat.Seconds = timestamp;
    PushMetricsData(pStat);
}

std::string YdbStatusToString(NYdb::EStatus status) {
    switch (status) {
        case NYdb::EStatus::SUCCESS:
            return "SUCCESS";
        case NYdb::EStatus::BAD_REQUEST:
            return "BAD_REQUEST";
        case NYdb::EStatus::UNAUTHORIZED:
            return "UNAUTHORIZED";
        case NYdb::EStatus::INTERNAL_ERROR:
            return "INTERNAL_ERROR";
        case NYdb::EStatus::ABORTED:
            return "ABORTED";
        case NYdb::EStatus::UNAVAILABLE:
            return "UNAVAILABLE";
        case NYdb::EStatus::OVERLOADED:
            return "OVERLOADED";
        case NYdb::EStatus::SCHEME_ERROR:
            return "SCHEME_ERROR";
        case NYdb::EStatus::GENERIC_ERROR:
            return "GENERIC_ERROR";
        case NYdb::EStatus::TIMEOUT:
            return "TIMEOUT";
        case NYdb::EStatus::BAD_SESSION:
            return "BAD_SESSION";
        case NYdb::EStatus::PRECONDITION_FAILED:
            return "PRECONDITION_FAILED";
        case NYdb::EStatus::ALREADY_EXISTS:
            return "ALREADY_EXISTS";
        case NYdb::EStatus::NOT_FOUND:
            return "NOT_FOUND";
        case NYdb::EStatus::SESSION_EXPIRED:
            return "SESSION_EXPIRED";
        case NYdb::EStatus::CANCELLED:
            return "CANCELLED";
        case NYdb::EStatus::UNDETERMINED:
            return "UNDETERMINED";
        case NYdb::EStatus::UNSUPPORTED:
            return "UNSUPPORTED";
        case NYdb::EStatus::SESSION_BUSY:
            return "SESSION_BUSY";
        case NYdb::EStatus::EXTERNAL_ERROR:
            return "EXTERNAL_ERROR";
        case NYdb::EStatus::STATUS_UNDEFINED:
            return "STATUS_UNDEFINED";
        case NYdb::EStatus::TRANSPORT_UNAVAILABLE:
            return "TRANSPORT_UNAVAILABLE";
        case NYdb::EStatus::CLIENT_RESOURCE_EXHAUSTED:
            return "CLIENT_RESOURCE_EXHAUSTED";
        case NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED:
            return "CLIENT_DEADLINE_EXCEEDED";
        case NYdb::EStatus::CLIENT_INTERNAL_ERROR:
            return "CLIENT_INTERNAL_ERROR";
        case NYdb::EStatus::CLIENT_CANCELLED:
            return "CLIENT_CANCELLED";
        case NYdb::EStatus::CLIENT_UNAUTHENTICATED:
            return "CLIENT_UNAUTHENTICATED";
        case NYdb::EStatus::CLIENT_CALL_UNIMPLEMENTED:
            return "CLIENT_CALL_UNIMPLEMENTED";
        case NYdb::EStatus::CLIENT_OUT_OF_RANGE:
            return "CLIENT_OUT_OF_RANGE";
        case NYdb::EStatus::CLIENT_DISCOVERY_FAILED:
            return "CLIENT_DISCOVERY_FAILED";
        case NYdb::EStatus::CLIENT_LIMITS_REACHED:
            return "CLIENT_LIMITS_REACHED";
    }
}
