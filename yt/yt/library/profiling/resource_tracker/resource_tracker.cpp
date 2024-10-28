#include "resource_tracker.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/ypath/token.h>

#include <util/folder/filelist.h>

#include <util/stream/file.h>

#include <util/string/vector.h>

#if defined(_linux_)
#include <unistd.h>
#endif

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

using namespace NYPath;
using namespace NYTree;
using namespace NProfiling;
using namespace NConcurrency;

DEFINE_REFCOUNTED_TYPE(TCpuCgroupTracker)
DEFINE_REFCOUNTED_TYPE(TMemoryCgroupTracker)
DEFINE_REFCOUNTED_TYPE(TResourceTracker)

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("Profiling");
static TProfiler Profiler("/resource_tracker");
static TProfiler MemoryProfiler("/memory/cgroup");

// Please, refer to /proc documentation to know more about available information.
// http://www.kernel.org/doc/Documentation/filesystems/proc.txt
static constexpr auto procPath = "/proc/self/task";

////////////////////////////////////////////////////////////////////////////////

namespace {

i64 GetTicksPerSecond()
{
#if defined(_linux_)
    return sysconf(_SC_CLK_TCK);
#else
    return -1;
#endif
}

#ifdef RESOURCE_TRACKER_ENABLED


#endif

} // namespace

void TCpuCgroupTracker::CollectSensors(ISensorWriter* writer)
{
    try {
        auto cgroups = GetProcessCgroups();
        for (const auto& group : cgroups) {
            for (const auto& controller : group.Controllers) {
                if (controller == "cpu") {
                    auto stat = GetCgroupCpuStat(group.ControllersName, group.Path);

                    if (!FirstCgroupStat_) {
                        FirstCgroupStat_ = stat;
                    }

                    writer->AddCounter(
                        "/cgroup/periods",
                        stat.NrPeriods - FirstCgroupStat_->NrPeriods);

                    writer->AddCounter(
                        "/cgroup/throttled_periods",
                        stat.NrThrottled - FirstCgroupStat_->NrThrottled);

                    writer->AddCounter(
                        "/cgroup/throttled_cpu_percent",
                        (stat.ThrottledTime - FirstCgroupStat_->ThrottledTime) / 10'000'000);

                    writer->AddCounter(
                        "/cgroup/wait_cpu_percent",
                        (stat.WaitTime - FirstCgroupStat_->WaitTime) / 10'000'000);

                    return;
                }
            }
        }
    } catch (const std::exception& ex) {
        if (!CgroupErrorLogged_) {
            YT_LOG_INFO(ex, "Failed to collect cgroup cpu statistics");
            CgroupErrorLogged_ = true;
        }
    }
}

void TMemoryCgroupTracker::CollectSensors(ISensorWriter* writer)
{
    try {
        auto cgroups = GetProcessCgroups();
        for (const auto& group : cgroups) {
            for (const auto& controller : group.Controllers) {
                if (controller == "memory") {
                    auto stat = GetCgroupMemoryStat(group.Path);

                    writer->AddGauge("/memory_limit", stat.HierarchicalMemoryLimit);
                    writer->AddGauge("/cache", stat.Cache);
                    writer->AddGauge("/rss", stat.Rss);
                    writer->AddGauge("/rss_huge", stat.RssHuge);
                    writer->AddGauge("/mapped_file", stat.MappedFile);
                    writer->AddGauge("/dirty", stat.Dirty);
                    writer->AddGauge("/writeback", stat.Writeback);

                    TotalMemoryLimit_.store(stat.HierarchicalMemoryLimit);
                    AnonymousMemoryLimit_.store(SafeGetAnonymousMemoryLimit(
                        group.Path,
                        stat.HierarchicalMemoryLimit));

                    return;
                }
            }
        }
    } catch (const std::exception& ex) {
        if (!CgroupErrorLogged_) {
            YT_LOG_INFO(ex, "Failed to collect cgroup memory statistics");
            CgroupErrorLogged_ = true;
        }
    }
}

i64 TMemoryCgroupTracker::SafeGetAnonymousMemoryLimit(const TString& cgroupPath, i64 totalMemoryLimit)
{
    try {
        auto anonymousLimit = GetCgroupAnonymousMemoryLimit(cgroupPath);
        auto result = anonymousLimit.value_or(totalMemoryLimit);
        result = std::min(result, totalMemoryLimit);
        return result != 0 ? result : totalMemoryLimit;
    } catch (const std::exception& ex) {
        if (!AnonymousLimitErrorLogged_) {
            YT_LOG_INFO(ex, "Failed to collect cgroup anonymous memory limit");
            AnonymousLimitErrorLogged_ = true;
        }
    }

    return totalMemoryLimit;
}

i64 TMemoryCgroupTracker::GetTotalMemoryLimit() const
{
    return TotalMemoryLimit_.load();
}

i64 TMemoryCgroupTracker::GetAnonymousMemoryLimit() const
{
    return AnonymousMemoryLimit_.load();
}

TResourceTracker::TTimings TResourceTracker::TTimings::operator-(const TResourceTracker::TTimings& other) const
{
    return {UserJiffies - other.UserJiffies, SystemJiffies - other.SystemJiffies, CpuWaitNsec - other.CpuWaitNsec};
}

TResourceTracker::TTimings& TResourceTracker::TTimings::operator+=(const TResourceTracker::TTimings& other)
{
    UserJiffies += other.UserJiffies;
    SystemJiffies += other.SystemJiffies;
    CpuWaitNsec += other.CpuWaitNsec;
    return *this;
}

TResourceTracker::TResourceTracker()
    // CPU time is measured in jiffies; we need USER_HZ to convert them
    // to milliseconds and percentages.
    : TicksPerSecond_(GetTicksPerSecond())
    , LastUpdateTime_(TInstant::Now())
    , CpuCgroupTracker_(New<TCpuCgroupTracker>())
    , MemoryCgroupTracker_(New<TMemoryCgroupTracker>())
{
    Profiler.AddFuncGauge("/memory_usage/rss", MakeStrong(this), [] {
        return GetProcessMemoryUsage().Rss;
    });

    Profiler.AddFuncGauge("/utilization/max", MakeStrong(this), [this] {
        return MaxThreadPoolUtilization_.load();
    });

    Profiler.AddProducer("", CpuCgroupTracker_);
    MemoryProfiler.AddProducer("", MemoryCgroupTracker_);

    Profiler
        .WithProducerRemoveSupport()
        .AddProducer("", MakeStrong(this));
}

void TResourceTracker::CollectSensors(ISensorWriter* writer)
{
    i64 timeDeltaUsec = TInstant::Now().MicroSeconds() - LastUpdateTime_.MicroSeconds();
    if (timeDeltaUsec <= 0) {
        return;
    }

    auto tidToInfo = ProcessThreads();
    CollectSensorsAggregatedTimings(writer, TidToInfo_, tidToInfo, timeDeltaUsec);
    TidToInfo_ = tidToInfo;

    LastUpdateTime_ = TInstant::Now();
}

void TResourceTracker::SetVCpuFactor(double vCpuFactor)
{
    VCpuFactor_ = vCpuFactor;
}

bool TResourceTracker::ProcessThread(TString tid, TResourceTracker::TThreadInfo* info)
{
    auto threadStatPath = NFS::CombinePaths(procPath, tid);
    auto statPath = NFS::CombinePaths(threadStatPath, "stat");
    auto statusPath = NFS::CombinePaths(threadStatPath, "status");
    auto schedStatPath = NFS::CombinePaths(threadStatPath, "schedstat");

    try {
        // Parse status.
        {
            TIFStream file(statusPath);
            for (TString line; file.ReadLine(line); ) {
                auto tokens = SplitString(line, "\t");

                if (tokens.size() < 2) {
                    continue;
                }

                if (tokens[0] == "Name:") {
                    info->ThreadName = tokens[1];
                } else if (tokens[0] == "SigBlk:") {
                    // This is a heuristic way to distinguish YT thread from non-YT threads.
                    // It is used primarily for CHYT, which links against CH and Poco that
                    // have their own complicated manner of handling threads. We want to be
                    // able to visually distinguish them from our threads.
                    //
                    // Poco threads always block SIGQUIT, SIGPIPE and SIGTERM; we use the latter
                    // one presence. Feel free to change this heuristic if needed.
                    YT_VERIFY(tokens[1].size() == 16);
                    auto mask = IntFromString<ui64, 16>(tokens[1]);
                    // Note that signals are 1-based, so 14-th bit is SIGTERM (15).
                    bool sigtermBlocked = (mask >> 14) & 1ull;
                    info->IsYtThread = !sigtermBlocked;
                }
            }
        }

        // Parse schedstat.
        {
            TIFStream file(schedStatPath);
            auto tokens = SplitString(file.ReadLine(), " ");
            if (tokens.size() < 3) {
                return false;
            }

            info->Timings.CpuWaitNsec = FromString<i64>(tokens[1]);
        }

        // Parse stat.
        {
            TIFStream file(statPath);
            auto tokens = SplitString(file.ReadLine(), " ");
            if (tokens.size() < 15) {
                return false;
            }

            info->Timings.UserJiffies = FromString<i64>(tokens[13]);
            info->Timings.SystemJiffies = FromString<i64>(tokens[14]);
        }

        info->ProfilingKey = info->ThreadName;

        if (!TidToInfo_.contains(tid)) {
            YT_LOG_TRACE("Thread %v named %v", tid, info->ThreadName);
        }

        // Group threads by thread pool, using YT thread naming convention.
        if (auto index = info->ProfilingKey.rfind(':'); index != TString::npos) {
            bool isDigit = std::all_of(info->ProfilingKey.cbegin() + index + 1, info->ProfilingKey.cend(), [] (char c) {
                return std::isdigit(c);
            });
            if (isDigit) {
                info->ProfilingKey = info->ProfilingKey.substr(0, index);
            }
        }

        if (!info->IsYtThread) {
            info->ProfilingKey += "@";
        }
    } catch (const TIoException&) {
        // Ignore all IO exceptions.
        return false;
    }

    YT_LOG_TRACE("Thread statistics (Tid: %v, ThreadName: %v, IsYtThread: %v, UserJiffies: %v, SystemJiffies: %v, CpuWaitNsec: %v)",
        tid,
        info->ThreadName,
        info->IsYtThread,
        info->Timings.UserJiffies,
        info->Timings.SystemJiffies,
        info->Timings.CpuWaitNsec);

    return true;
}

TResourceTracker::TThreadMap TResourceTracker::ProcessThreads()
{
    TDirsList dirsList;
    try {
        dirsList.Fill(procPath);
    } catch (const TSystemError&) {
        // Ignore all exceptions.
        return {};
    }

    TThreadMap tidToStats;

    for (int index = 0; index < static_cast<int>(dirsList.Size()); ++index) {
        auto tid = TString(dirsList.Next());
        TThreadInfo info;
        if (ProcessThread(tid, &info)) {
            tidToStats[tid] = info;
        } else {
            YT_LOG_TRACE("Failed to prepare thread info for thread (Tid: %v)", tid);
        }
    }

    return tidToStats;
}

void TResourceTracker::CollectSensorsAggregatedTimings(
    ISensorWriter* writer,
    const TResourceTracker::TThreadMap& oldTidToInfo,
    const TResourceTracker::TThreadMap& newTidToInfo,
    i64 timeDeltaUsec)
{
    double totalUserCpuTime = 0.0;
    double totalSystemCpuTime = 0.0;
    double totalCpuWaitTime = 0.0;

    THashMap<TString, TTimings> profilingKeyToAggregatedTimings;
    THashMap<TString, int> profilingKeyToCount;

    // Consider only those threads which did not change their thread names.
    // In each group of such threads with same thread name, export aggregated timings.

    for (const auto& [tid, newInfo] : newTidToInfo) {
        ++profilingKeyToCount[newInfo.ProfilingKey];

        auto it = oldTidToInfo.find(tid);

        if (it == oldTidToInfo.end()) {
            continue;
        }

        const auto& oldInfo = it->second;

        if (oldInfo.ProfilingKey != newInfo.ProfilingKey) {
            continue;
        }

        profilingKeyToAggregatedTimings[newInfo.ProfilingKey] += newInfo.Timings - oldInfo.Timings;
    }

    double vCpuFactor = VCpuFactor_;

    double maxUtilization = 0.0;
    for (const auto& [profilingKey, aggregatedTimings] : profilingKeyToAggregatedTimings) {
        // Multiplier 1e6 / timeDelta is for taking average over time (all values should be "per second").
        // Multiplier 100 for CPU time is for measuring CPU load in percents. It is due to historical reasons.
        double userCpuTime = std::max<double>(0.0, 100. * aggregatedTimings.UserJiffies / TicksPerSecond_ * (1e6 / timeDeltaUsec));
        double systemCpuTime = std::max<double>(0.0, 100. * aggregatedTimings.SystemJiffies / TicksPerSecond_ * (1e6 / timeDeltaUsec));
        double waitTime = std::max<double>(0.0, 100 * aggregatedTimings.CpuWaitNsec / 1e9 * (1e6 / timeDeltaUsec));

        totalUserCpuTime += userCpuTime;
        totalSystemCpuTime += systemCpuTime;
        totalCpuWaitTime += waitTime;

        auto threadCount = profilingKeyToCount[profilingKey];
        double utilization = (userCpuTime + systemCpuTime) / (100 * threadCount);

        double totalCpuTime = userCpuTime + systemCpuTime;

        TWithTagGuard tagGuard(writer, "thread", profilingKey);
        writer->AddGauge("/user_cpu", userCpuTime);
        writer->AddGauge("/system_cpu", systemCpuTime);
        writer->AddGauge("/total_cpu", totalCpuTime);
        writer->AddGauge("/cpu_wait", waitTime);
        writer->AddGauge("/thread_count", threadCount);
        writer->AddGauge("/utilization", utilization);
        if (vCpuFactor != 0.0) {
            writer->AddGauge("/user_vcpu", userCpuTime * vCpuFactor);
            writer->AddGauge("/system_vcpu", systemCpuTime * vCpuFactor);
            writer->AddGauge("/total_vcpu", totalCpuTime * vCpuFactor);
        }

        maxUtilization = std::max(maxUtilization, utilization);

        YT_LOG_TRACE("Thread CPU timings in percent/sec (ProfilingKey: %v, UserCpu: %v, SystemCpu: %v, CpuWait: %v)",
            profilingKey,
            userCpuTime,
            systemCpuTime,
            waitTime);
    }

    LastUserCpu_.store(totalUserCpuTime);
    LastSystemCpu_.store(totalSystemCpuTime);
    LastCpuWait_.store(totalCpuWaitTime);
    MaxThreadPoolUtilization_ = maxUtilization;

    YT_LOG_DEBUG("Total CPU timings in percent/sec (UserCpu: %v, SystemCpu: %v, CpuWait: %v)",
        totalUserCpuTime,
        totalSystemCpuTime,
        totalCpuWaitTime);

    int fileDescriptorCount = GetFileDescriptorCount();
    writer->AddGauge("/open_fds", fileDescriptorCount);
    YT_LOG_DEBUG("Assessed open file descriptors (Count: %v)", fileDescriptorCount);
}

double TResourceTracker::GetUserCpu()
{
    return LastUserCpu_.load();
}

double TResourceTracker::GetSystemCpu()
{
    return LastSystemCpu_.load();
}

double TResourceTracker::GetCpuWait()
{
    return LastCpuWait_.load();
}

i64 TResourceTracker::GetTotalMemoryLimit()
{
    return MemoryCgroupTracker_->GetTotalMemoryLimit();
}

i64 TResourceTracker::GetAnonymousMemoryLimit()
{
    return MemoryCgroupTracker_->GetAnonymousMemoryLimit();
}

TResourceTrackerPtr GetResourceTracker()
{
    return LeakyRefCountedSingleton<TResourceTracker>();
}

void EnableResourceTracker()
{
    GetResourceTracker();
}

void SetVCpuFactor(double vCpuFactor)
{
    GetResourceTracker()->SetVCpuFactor(vCpuFactor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
