#include "cpuinfo.h"
#include <cstdlib>
#if defined (_linux_)
    #include <dirent.h>
    #include <unistd.h>
#endif
#include <fcntl.h>
#include <fstream>
#include <unordered_set>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/system/info.h>

size_t NKikimr::RealNumberOfCpus() {
    // copy-pasted from util/system/info.cpp
#if defined(_linux_)
    unsigned ret;
    int fd, nread, column;
    char buf[512];
    static const char matchstr[] = "processor\t:";

    fd = open("/proc/cpuinfo", O_RDONLY);

    if (fd == -1) {
        abort();
    }

    column = 0;
    ret = 0;

    while (true) {
        nread = read(fd, buf, sizeof(buf));

        if (nread <= 0) {
            break;
        }

        for (int i = 0; i < nread; ++i) {
            const char ch = buf[i];

            if (ch == '\n') {
                column = 0;
            } else if (column != -1) {
                if (AsciiToLower(ch) == matchstr[column]) {
                    ++column;

                    if (column == sizeof(matchstr) - 1) {
                        column = -1;
                        ++ret;
                    }
                } else {
                    column = -1;
                }
            }
        }
    }

    if (ret == 0) {
        abort();
    }

    close(fd);

    return ret;
#else
    return NSystemInfo::NumberOfCpus();
#endif
}

double TicksPerSec() {
#ifdef _SC_CLK_TCK
    return sysconf(_SC_CLK_TCK);
#else
    return 1;
#endif
}

std::vector<NKikimr::TSystemThreadsMonitor::TSystemThreadPoolInfo> NKikimr::TSystemThreadsMonitor::GetThreadPools(TInstant now) {
    UpdateSystemThreads(getpid());
    std::vector<TSystemThreadPoolInfo> result;
    result.reserve(NameToThreadIndex.size());
    double passedSeconds = (now - UpdateTime).SecondsFloat();
    double ticks = TicksPerSec() * passedSeconds;
    for (const auto& [name, tids] : NameToThreadIndex) {
        TSystemThreadPoolInfo& info = result.emplace_back();
        info.Name = name;
        info.Threads = tids.size();
        ui64 majorPageFaults = 0;
        ui64 minorPageFaults = 0;
        ui64 systemTime = 0;
        ui64 userTime = 0;
        std::array<ui16, 256> states{};
        for (pid_t tid : tids) {
            const TSystemThreadPoolState& state = SystemThreads[tid];
            majorPageFaults += state.DeltaMajorPageFaults;
            minorPageFaults += state.DeltaMinorPageFaults;
            systemTime += state.DeltaSystemTime;
            userTime += state.DeltaUserTime;
            if (state.State >= 'A' && state.State <= 'z') {
                states[size_t(state.State)]++;
            }
        }
        for (char c = 'A'; c <= 'z'; ++c) {
            if (states[size_t(c)] > 0) {
                info.States.emplace_back(c, states[c]);
            }
        }
        info.MajorPageFaults = double(majorPageFaults) / passedSeconds;
        info.MinorPageFaults = double(minorPageFaults) / passedSeconds;
        info.SystemUsage = double(systemTime) / ticks / info.Threads;
        info.UserUsage = double(userTime) / ticks / info.Threads;
    }
    UpdateTime = now;
    return result;
}

void NKikimr::TSystemThreadsMonitor::UpdateSystemThreads(pid_t pid) {
#if defined(_linux_)
    NameToThreadIndex.clear();
    std::string taskDir = "/proc/" + std::to_string(pid) + "/task";
    DIR* dir = opendir(taskDir.c_str());
    if (!dir) {
        SystemThreads.clear();
        return;
    }
    std::unordered_set<pid_t> currentThreads;
    currentThreads.reserve(SystemThreads.size());
    for (const auto& [tid, state] : SystemThreads) {
        currentThreads.insert(tid);
    }
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] == '.') {
            continue;  // skip '.' and '..'
        }
        pid_t tid = atoi(entry->d_name);
        if (tid > 0) {
            UpdateSystemThread(pid, tid);
            currentThreads.erase(tid);
        }
    }
    for (pid_t tid : currentThreads) {
        SystemThreads.erase(tid);
    }
    closedir(dir);
#else
    Y_UNUSED(pid);
#endif
}

void NKikimr::TSystemThreadsMonitor::UpdateSystemThread(pid_t pid, pid_t tid) {
#if defined(_linux_)
    TSystemThreadPoolState& state = SystemThreads[tid];
    std::ifstream file("/proc/" + std::to_string(pid) + "/task/" + std::to_string(tid) + "/stat");
    if (file) {
        std::string line;
        std::getline(file, line);
        size_t name_start = line.find('(');
        size_t name_end = line.find(')', name_start);
        if (name_start == std::string::npos || name_end == std::string::npos) {
            return;
        }
        std::string threadName = line.substr(name_start + 1, name_end - name_start - 1);
        std::istringstream iss(line.substr(name_end + 2));
        std::string ignore;
        ui64 majorPageFaults;
        ui64 minorPageFaults;
        ui64 systemTime;
        ui64 userTime;
        iss >> state.State >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
            >> minorPageFaults >> ignore >> majorPageFaults >> ignore >> userTime >> systemTime;
        state.DeltaMajorPageFaults = majorPageFaults - state.MajorPageFaults;
        state.DeltaMinorPageFaults = minorPageFaults - state.MinorPageFaults;
        state.DeltaSystemTime = systemTime - state.SystemTime;
        state.DeltaUserTime = userTime - state.UserTime;
        state.MajorPageFaults = majorPageFaults;
        state.MinorPageFaults = minorPageFaults;
        state.SystemTime = systemTime;
        state.UserTime = userTime;
        //Cerr << threadName << " " << state.SystemTime << " " << state.UserTime << Endl;
        std::string name = GetNormalizedThreadName(threadName);
        NameToThreadIndex[name].push_back(tid);
    }
#else
    Y_UNUSED(tid);
    Y_UNUSED(pid);
#endif
}

std::string NKikimr::TSystemThreadsMonitor::GetNormalizedThreadName(const std::string& name) {
    std::string result = name;
    while (!result.empty() && (isdigit(result.back()) || result.back() == '-' || result.back() == ' ')) {
        result.resize(result.size() - 1);
    }
    if (result.empty()) {
        return name;
    }
    // some well-known hacks
    if (result == "default-executo") {
        result = "default-executor";
    }
    if (result == "resolver-execut") {
        result = "resolver-executor";
    }
    if (result == "grpc_global_tim") {
        result = "grpc_global_timer";
    }
    if (result == "kikimr.Schedule") {
        result = "kikimr.Scheduler";
    }
    return result;
}
