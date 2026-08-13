#include "thread_stats.h"

#include <ydb/library/actors/util/datetime.h>

#ifdef _linux_
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <limits>
#endif

#include <util/generic/string.h>
#include <util/digest/numeric.h>
#include <util/string/builder.h>

#include <utility>

namespace NActors {

namespace {

#ifdef _linux_
ssize_t ReadFile(int fd, char* buffer, size_t size, off_t offset = 0) {
    ssize_t result;
    do {
        result = pread(fd, buffer, size, offset);
    } while (result < 0 && errno == EINTR);
    return result;
}

bool ParseNumber(const char*& begin, const char* end, ui64& value) {
    while (begin != end && (*begin == ' ' || *begin == '\t')) {
        ++begin;
    }
    const auto result = std::from_chars(begin, end, value);
    if (result.ec != std::errc()) {
        return false;
    }
    begin = result.ptr;
    return true;
}

bool ParseNumberField(const char*& begin, const char* end, ui64& value) {
    return ParseNumber(begin, end, value)
        && begin != end
        && (*begin == ' ' || *begin == '\t' || *begin == '\n');
}

bool ParseSchedStat(const char* begin, const char* end, TThreadSchedulerStats& stats) {
    return ParseNumberField(begin, end, stats.RuntimeNs)
        && ParseNumberField(begin, end, stats.WaitNs);
}

bool SkipField(const char*& begin, const char* end) {
    while (begin != end && (*begin == ' ' || *begin == '\t')) {
        ++begin;
    }
    if (begin == end) {
        return false;
    }
    while (begin != end && *begin != ' ' && *begin != '\t' && *begin != '\n') {
        ++begin;
    }
    return true;
}

bool ParseCpuTime(const char* begin, const char* end, ui64& cpuTimeUs) {
    const char* closeParen = end;
    while (closeParen != begin && closeParen[-1] != ')') {
        --closeParen;
    }
    if (closeParen == begin) {
        return false;
    }

    begin = closeParen;

    // Skip state (field 3) and fields 4 through 13. The next two fields are
    // utime and stime, both expressed in clock ticks.
    for (size_t field = 3; field <= 13; ++field) {
        if (!SkipField(begin, end)) {
            return false;
        }
    }

    ui64 userTicks = 0;
    ui64 systemTicks = 0;
    if (!ParseNumberField(begin, end, userTicks) || !ParseNumberField(begin, end, systemTicks)
            || userTicks > std::numeric_limits<ui64>::max() - systemTicks) {
        return false;
    }

    static const ui64 ticksPerSecond = [] {
        const long value = sysconf(_SC_CLK_TCK);
        return value > 0 ? static_cast<ui64>(value) : 0;
    }();
    if (!ticksPerSecond) {
        return false;
    }

    const ui64 ticks = userTicks + systemTicks;
    cpuTimeUs = ticks / ticksPerSecond * 1'000'000
        + ticks % ticksPerSecond * 1'000'000 / ticksPerSecond;
    return true;
}

bool ReadWholeFile(int fd, TString& buffer) {
    constexpr size_t BufferSize = 4096;
    char chunk[BufferSize];
    off_t offset = 0;
    buffer.clear();

    while (true) {
        const ssize_t size = ReadFile(fd, chunk, sizeof(chunk), offset);
        if (size < 0) {
            return false;
        }
        if (size == 0) {
            return true;
        }

        buffer.append(chunk, static_cast<size_t>(size));
        offset += size;
    }
}

bool ParseNonvoluntaryContextSwitches(const TString& status, ui64& value) {
    constexpr TStringBuf field = "nonvoluntary_ctxt_switches:";
    const TStringBuf content(status);
    size_t position = 0;
    while ((position = content.find(field, position)) != TStringBuf::npos) {
        if (position == 0 || content[position - 1] == '\n') {
            const char* begin = content.data() + position + field.size();
            return ParseNumberField(begin, content.end(), value);
        }
        position += field.size();
    }
    return false;
}
#endif

} // anonymous namespace

TThreadSchedulerStatsReader::TThreadSchedulerStatsReader(ui64 threadId, TDuration updateInterval,
        EThreadCpuTimeReadMode cpuTimeReadMode)
    : ThreadId(threadId)
    , ReadCpuTime(cpuTimeReadMode == EThreadCpuTimeReadMode::Enabled)
    , UpdateIntervalTs(Us2Ts(updateInterval.MicroSeconds()))
{
    if (!UpdateIntervalTs) {
        UpdateIntervalTs = 1;
    }
    NextReadTs = GetCycleCountFast() + IntHash(threadId) % UpdateIntervalTs;
}

TThreadSchedulerStatsReader::TThreadSchedulerStatsReader(
        TThreadSchedulerStatsReader&& other) noexcept
    : ThreadId(other.ThreadId)
    , ReadCpuTime(other.ReadCpuTime)
    , UpdateIntervalTs(other.UpdateIntervalTs)
    , NextReadTs(other.NextReadTs)
    , CachedStats(other.CachedStats)
    , CachedCpuTimeUs(other.CachedCpuTimeUs)
    , HasCachedSchedulerStats(other.HasCachedSchedulerStats)
    , HasCachedCpuTime(other.HasCachedCpuTime)
    , StatusBuffer(std::move(other.StatusBuffer))
    , SchedStatFd(std::exchange(other.SchedStatFd, -1))
    , StatusFd(std::exchange(other.StatusFd, -1))
    , StatFd(std::exchange(other.StatFd, -1))
{}

TThreadSchedulerStatsReader& TThreadSchedulerStatsReader::operator=(
        TThreadSchedulerStatsReader&& other) noexcept {
    if (this != &other) {
        CloseFiles();
        ThreadId = other.ThreadId;
        ReadCpuTime = other.ReadCpuTime;
        UpdateIntervalTs = other.UpdateIntervalTs;
        NextReadTs = other.NextReadTs;
        CachedStats = other.CachedStats;
        CachedCpuTimeUs = other.CachedCpuTimeUs;
        HasCachedSchedulerStats = other.HasCachedSchedulerStats;
        HasCachedCpuTime = other.HasCachedCpuTime;
        StatusBuffer = std::move(other.StatusBuffer);
        SchedStatFd = std::exchange(other.SchedStatFd, -1);
        StatusFd = std::exchange(other.StatusFd, -1);
        StatFd = std::exchange(other.StatFd, -1);
    }
    return *this;
}

TThreadSchedulerStatsReader::~TThreadSchedulerStatsReader() {
    CloseFiles();
}

void TThreadSchedulerStatsReader::CloseFiles() {
#ifdef _linux_
    if (SchedStatFd >= 0) {
        close(SchedStatFd);
        SchedStatFd = -1;
    }
    if (StatusFd >= 0) {
        close(StatusFd);
        StatusFd = -1;
    }
    if (StatFd >= 0) {
        close(StatFd);
        StatFd = -1;
    }
#endif
}

TThreadSchedulerStatsReadResult TThreadSchedulerStatsReader::Read(
        TThreadSchedulerStats& stats) {
    const auto cachedResult = [&] {
        stats = CachedStats;
        return TThreadSchedulerStatsReadResult{
            HasCachedSchedulerStats
                ? EThreadSchedulerStatsReadStatus::Cached
                : EThreadSchedulerStatsReadStatus::Unavailable,
            HasCachedCpuTime
                ? EThreadSchedulerStatsReadStatus::Cached
                : EThreadSchedulerStatsReadStatus::Unavailable,
            CachedCpuTimeUs,
        };
    };

    const ui64 now = GetCycleCountFast();
    if (now < NextReadTs) {
        return cachedResult();
    }
    NextReadTs = now + UpdateIntervalTs;

#ifdef _linux_
    if (SchedStatFd < 0 || StatusFd < 0 || (ReadCpuTime && StatFd < 0)) {
        const TString taskPath = TStringBuilder() << "/proc/self/task/" << ThreadId;
        if (SchedStatFd < 0) {
            SchedStatFd = open((taskPath + "/schedstat").c_str(), O_RDONLY | O_CLOEXEC);
        }
        if (StatusFd < 0) {
            StatusFd = open((taskPath + "/status").c_str(), O_RDONLY | O_CLOEXEC);
        }
        if (ReadCpuTime && StatFd < 0) {
            StatFd = open((taskPath + "/stat").c_str(), O_RDONLY | O_CLOEXEC);
        }
    }

    TThreadSchedulerStatsReadResult result = cachedResult();

    // CPU time comes from stat independently of schedstat/status so that a
    // missing scheduler field cannot suppress the CPU utilization counter.
    if (ReadCpuTime && StatFd >= 0) {
        char statBuffer[1024];
        const ssize_t statSize = ReadFile(StatFd, statBuffer, sizeof(statBuffer));
        ui64 cpuTimeUs = 0;
        if (statSize > 0
                && ParseCpuTime(statBuffer, statBuffer + statSize, cpuTimeUs)) {
            CachedCpuTimeUs = cpuTimeUs;
            HasCachedCpuTime = true;
            result.CpuTimeStatus = EThreadSchedulerStatsReadStatus::Updated;
            result.CpuTimeUs = cpuTimeUs;
        }
    }

    if (SchedStatFd >= 0 && StatusFd >= 0) {
        TThreadSchedulerStats schedulerStats;
        char schedStatBuffer[256];
        const ssize_t schedStatSize = ReadFile(SchedStatFd, schedStatBuffer, sizeof(schedStatBuffer));
        if (schedStatSize > 0
                && ParseSchedStat(schedStatBuffer, schedStatBuffer + schedStatSize, schedulerStats)
                && ReadWholeFile(StatusFd, StatusBuffer)
                && ParseNonvoluntaryContextSwitches(
                    StatusBuffer, schedulerStats.NonvoluntaryContextSwitches)) {
            CachedStats = schedulerStats;
            stats = schedulerStats;
            HasCachedSchedulerStats = true;
            result.SchedulerStatsStatus = EThreadSchedulerStatsReadStatus::Updated;
        }
    }

    return result;
#else
    return cachedResult();
#endif
}

} // namespace NActors
