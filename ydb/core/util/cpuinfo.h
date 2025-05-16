#pragma once
#include <cstddef>
#include <sys/types.h>
#include <map>
#include <unordered_map>
#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKikimr {

std::size_t RealNumberOfCpus();

class TSystemThreadsMonitor {
public:
    struct TSystemThreadPoolInfo { // returned data per refresh per second
        TString Name;
        ui32 Threads;
        float SystemUsage;
        float UserUsage;
        ui32 MajorPageFaults;
        ui32 MinorPageFaults;
        std::vector<std::pair<char, ui32>> States;
    };

    std::vector<TSystemThreadPoolInfo> GetThreadPools(TInstant now);

private:
    struct TSystemThreadPoolState { // stored state of a thread
        char State;
        ui64 SystemTime;
        ui64 UserTime;
        ui64 MajorPageFaults;
        ui64 MinorPageFaults;
        ui64 DeltaSystemTime;
        ui64 DeltaUserTime;
        ui64 DeltaMajorPageFaults;
        ui64 DeltaMinorPageFaults;
    };

    std::unordered_map<pid_t, TSystemThreadPoolState> SystemThreads;
    std::map<std::string, std::vector<pid_t>> NameToThreadIndex;
    TInstant UpdateTime;

    void UpdateSystemThreads(pid_t pid);
    void UpdateSystemThread(pid_t pid, pid_t tid);
    static std::string GetNormalizedThreadName(const std::string& name);
};

} // namespace NKikimr
