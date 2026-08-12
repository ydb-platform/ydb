#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NActors {

struct TThreadSchedulerStats {
    ui64 RuntimeNs = 0;
    ui64 WaitNs = 0;
    ui64 NonvoluntaryContextSwitches = 0;
};

enum class EThreadSchedulerStatsReadStatus : ui8 {
    Unavailable,
    Cached,
    Updated,
};

struct TThreadSchedulerStatsReadResult {
    EThreadSchedulerStatsReadStatus SchedulerStatsStatus = EThreadSchedulerStatsReadStatus::Unavailable;
    EThreadSchedulerStatsReadStatus CpuTimeStatus = EThreadSchedulerStatsReadStatus::Unavailable;
    ui64 CpuTimeUs = 0;
};

enum class EThreadCpuTimeReadMode : ui8 {
    Disabled,
    Enabled,
};

class TThreadSchedulerStatsReader {
public:
    TThreadSchedulerStatsReader(ui64 threadId, TDuration updateInterval,
        EThreadCpuTimeReadMode cpuTimeReadMode = EThreadCpuTimeReadMode::Disabled);
    ~TThreadSchedulerStatsReader();

    TThreadSchedulerStatsReader(const TThreadSchedulerStatsReader&) = delete;
    TThreadSchedulerStatsReader& operator=(const TThreadSchedulerStatsReader&) = delete;
    TThreadSchedulerStatsReader(TThreadSchedulerStatsReader&& other) noexcept;
    TThreadSchedulerStatsReader& operator=(TThreadSchedulerStatsReader&& other) noexcept;

    TThreadSchedulerStatsReadResult Read(TThreadSchedulerStats& stats);

private:
    void CloseFiles();

private:
    ui64 ThreadId = 0;
    bool ReadCpuTime = false;
    ui64 UpdateIntervalTs = 0;
    ui64 NextReadTs = 0;
    TThreadSchedulerStats CachedStats;
    ui64 CachedCpuTimeUs = 0;
    bool HasCachedSchedulerStats = false;
    bool HasCachedCpuTime = false;
    TString StatusBuffer;
    int SchedStatFd = -1;
    int StatusFd = -1;
    int StatFd = -1;
};

} // namespace NActors
