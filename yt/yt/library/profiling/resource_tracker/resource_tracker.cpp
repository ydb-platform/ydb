#include "resource_tracker.h"

#include "config.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/library/cgroup/statistics.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/producer.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <library/cpp/yt/misc/leaky_global.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

#include <util/folder/filelist.h>

#include <util/stream/file.h>

#include <util/string/vector.h>

#include <mutex>

#if defined(_linux_)
#include <unistd.h>
#endif

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

using namespace NYPath;
using namespace NProfiling;
using namespace NConcurrency;
using namespace NThreading;
using namespace NCGroups;

////////////////////////////////////////////////////////////////////////////////

namespace {

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, Logger, "Profiling");
YT_DEFINE_LEAKY_GLOBAL(const TProfiler, ResourceTrackerProfiler, "/resource_tracker");
YT_DEFINE_LEAKY_GLOBAL(const TProfiler, MemoryProfiler, "/memory/cgroup");

// Please, refer to /proc documentation to know more about available information.
// http://www.kernel.org/doc/Documentation/filesystems/proc.txt
constexpr auto ProcfsCurrentTaskPath = "/proc/self/task";

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCpuCgroupTracker)
DECLARE_REFCOUNTED_CLASS(TMemoryCgroupTracker)

////////////////////////////////////////////////////////////////////////////////

class TCpuCgroupTracker
    : public NProfiling::ISensorProducer
{
public:
    void CollectSensors(ISensorWriter* writer) override
    {
        try {
            auto cpuStatistics = TSelfCGroupsStatisticsFetcher::Get()->GetCpuThrottlingStatistics();

            if (!FirstCpuStatistics_) {
                FirstCpuStatistics_ = cpuStatistics;
            }

            writer->AddCounter(
                "/cgroup/periods",
                cpuStatistics.NrPeriods - FirstCpuStatistics_->NrPeriods);

            writer->AddCounter(
                "/cgroup/throttled_periods",
                cpuStatistics.NrThrottled - FirstCpuStatistics_->NrThrottled);

            writer->AddCounter(
                "/cgroup/throttled_cpu_percent",
                (cpuStatistics.ThrottledTime - FirstCpuStatistics_->ThrottledTime).NanoSeconds() / 10'000'000);

            writer->AddCounter(
                "/cgroup/wait_cpu_percent",
                (cpuStatistics.WaitTime - FirstCpuStatistics_->WaitTime).NanoSeconds() / 10'000'000);
        } catch (const std::exception& ex) {
            if (!CgroupErrorLogged_) {
                YT_TLOG_INFO("Failed to collect cgroup cpu statistics")
                    .With(ex);
                CgroupErrorLogged_ = true;
            }
        }
    }

private:
    std::optional<TCpuThrottlingStatistics> FirstCpuStatistics_;
    bool CgroupErrorLogged_ = false;
};

DEFINE_REFCOUNTED_TYPE(TCpuCgroupTracker)

////////////////////////////////////////////////////////////////////////////////

class TMemoryCgroupTracker
    : public NProfiling::ISensorProducer
{
public:
    void CollectSensors(ISensorWriter* writer) override
    {
        try {
            const auto* fetcher = TSelfCGroupsStatisticsFetcher::Get();
            auto memoryStatistics = fetcher->GetMemoryStatistics();

            writer->AddGauge("/cache", memoryStatistics.Cache);
            writer->AddGauge("/rss", memoryStatistics.AnonWithSwapCached);
            writer->AddGauge("/rss_huge", memoryStatistics.RssHuge);
            writer->AddGauge("/mapped_file", memoryStatistics.MappedFile);
            writer->AddGauge("/dirty", memoryStatistics.Dirty);
            writer->AddGauge("/writeback", memoryStatistics.Writeback);

            auto memoryLimits = fetcher->GetMemoryLimits();
            if (memoryLimits.MemoryLimit) {
                writer->AddGauge("/memory_limit", *memoryLimits.MemoryLimit);
                TotalMemoryLimit_.store(*memoryLimits.MemoryLimit);
            }

            auto anonymousMemoryLimit = ComputeAnonymousMemoryLimit(
                memoryLimits.AnonymousMemoryLimit,
                memoryLimits.MemoryLimit);
            if (anonymousMemoryLimit) {
                AnonymousMemoryLimit_.store(*anonymousMemoryLimit);
            }
        } catch (const std::exception& ex) {
            if (!CgroupErrorLogged_) {
                YT_TLOG_INFO("Failed to collect cgroup memory statistics")
                    .With(ex);
                CgroupErrorLogged_ = true;
            }
        }
    }

    i64 GetTotalMemoryLimit() const
    {
        return TotalMemoryLimit_.load();
    }

    i64 GetAnonymousMemoryLimit() const
    {
        return AnonymousMemoryLimit_.load();
    }

private:
    bool CgroupErrorLogged_ = false;

    std::atomic<i64> TotalMemoryLimit_ = 0;
    std::atomic<i64> AnonymousMemoryLimit_ = 0;

    static std::optional<i64> ComputeAnonymousMemoryLimit(
        std::optional<i64> anonymousMemoryLimit,
        std::optional<i64> totalMemoryLimit)
    {
        if (!totalMemoryLimit) {
            return anonymousMemoryLimit;
        }
        if (!anonymousMemoryLimit) {
            return totalMemoryLimit;
        }
        i64 result = std::min(*anonymousMemoryLimit, *totalMemoryLimit);
        return result != 0 ? result : totalMemoryLimit;
    }
};

DEFINE_REFCOUNTED_TYPE(TMemoryCgroupTracker)

////////////////////////////////////////////////////////////////////////////////

class TResourceTrackerImpl
    : public NProfiling::ISensorProducer
{
public:
    static TResourceTrackerImpl* Get()
    {
        return LeakyRefCountedSingleton<TResourceTrackerImpl>().Get();
    }

    void Enable()
    {
        std::call_once(EnabledFlag_, [&] {
            ResourceTrackerProfiler().AddFuncGauge("/memory_usage/rss", MakeStrong(this), [] {
                return GetProcessMemoryUsage().Rss;
            });

            ResourceTrackerProfiler().AddFuncGauge("/utilization/max", MakeStrong(this), [this] {
                return MaxThreadPoolUtilization_.load();
            });

            ResourceTrackerProfiler().AddProducer("", CpuCgroupTracker_);
            MemoryProfiler().AddProducer("", MemoryCgroupTracker_);

            ResourceTrackerProfiler()
                .WithProducerRemoveSupport()
                .AddProducer("", MakeStrong(this));
        });
    }

    double GetUserCpu()
    {
        return LastUserCpu_.load();
    }

    double GetSystemCpu()
    {
        return LastSystemCpu_.load();
    }

    double GetCpuWait()
    {
        return LastCpuWait_.load();
    }

    i64 GetTotalMemoryLimit()
    {
        return MemoryCgroupTracker_->GetTotalMemoryLimit();
    }

    i64 GetAnonymousMemoryLimit()
    {
        return MemoryCgroupTracker_->GetAnonymousMemoryLimit();
    }

    void SetCpuToVCpuFactor(double factor)
    {
        VCpuFactor_.store(factor);
    }

    void Configure(const TResourceTrackerConfigPtr& config)
    {
        if (config->CpuToVCpuFactor) {
            SetCpuToVCpuFactor(*config->CpuToVCpuFactor);
        }
        if (config->Enable) {
            Enable();
        }
    }

    TResourceTrackerTagsGuard RegisterPerThreadExtraTags(
        const std::string& threadName,
        const NProfiling::TTagSet& extraTags)
    {
        return PerThreadExtraTags_.Transform([&threadName, &extraTags] (auto& storage) {
            auto [it, inserted] = storage.try_emplace(threadName, extraTags);
            if (!inserted) {
                return TResourceTrackerTagsGuard(std::nullopt);
            }

            return TResourceTrackerTagsGuard(threadName);
        });
    }

    void UnregisterPerThreadExtraTags(const std::string& threadName)
    {
        PerThreadExtraTags_.Transform([&threadName] (auto& storage) {
            storage.erase(threadName);
        });
    }

private:
    const TCpuCgroupTrackerPtr CpuCgroupTracker_ = New<TCpuCgroupTracker>();
    const TMemoryCgroupTrackerPtr MemoryCgroupTracker_ = New<TMemoryCgroupTracker>();

    // CPU time is measured in jiffies; we need USER_HZ to convert them
    // to milliseconds and percentages.
    const i64 TicksPerSecond_ = [] {
#if defined(_linux_)
        return sysconf(_SC_CLK_TCK);
#else
        return -1;
#endif
    }();

    std::once_flag EnabledFlag_;

    std::atomic<double> VCpuFactor_;

    TInstant LastUpdateTime_ = TInstant::Now();

    // Values below are in percent.
    std::atomic<double> LastUserCpu_;
    std::atomic<double> LastSystemCpu_;
    std::atomic<double> LastCpuWait_;

    std::atomic<double> MaxThreadPoolUtilization_;

    TAtomicObject<THashMap<std::string, TTagSet>> PerThreadExtraTags_;

    void CollectSensors(ISensorWriter* writer) override
    {
        i64 timeDeltaUsec = TInstant::Now().MicroSeconds() - LastUpdateTime_.MicroSeconds();
        if (timeDeltaUsec <= 0) {
            return;
        }

        auto tidToInfo = ParseThreadInfos();
        CollectSensorsAggregatedTimings(writer, TidToInfo_, tidToInfo, timeDeltaUsec);
        TidToInfo_ = tidToInfo;

        LastUpdateTime_ = TInstant::Now();
    }

    struct TTimings
    {
        i64 UserJiffies = 0;
        i64 SystemJiffies = 0;
        i64 CpuWaitNsec = 0;

        TTimings operator-(const TTimings& other) const
        {
            return {UserJiffies - other.UserJiffies, SystemJiffies - other.SystemJiffies, CpuWaitNsec - other.CpuWaitNsec};
        }

        TTimings& operator+=(const TTimings& other)
        {
            UserJiffies += other.UserJiffies;
            SystemJiffies += other.SystemJiffies;
            CpuWaitNsec += other.CpuWaitNsec;
            return *this;
        }
    };

    struct TThreadInfo
    {
        std::string ThreadName;
        TTimings Timings;
        bool IsYTThread = true;
        //! This key is IsYtThread ? ThreadName : ThreadName + "@".
        //! It allows to distinguish YT threads from non-YT threads that
        //! inherited parent YT thread name.
        std::string ProfilingKey;
    };

    // Thread id -> stats
    using TThreadInfoMap = THashMap<NThreading::TThreadId, TThreadInfo>;
    TThreadInfoMap TidToInfo_;

    std::optional<TThreadInfo> TryParseThreadInfo(NThreading::TThreadId tid)
    {
        TThreadInfo info;

        auto threadStatPath = NFS::CombinePaths(ProcfsCurrentTaskPath, ToString(tid));
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
                        info.ThreadName = tokens[1];
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
                        info.IsYTThread = !sigtermBlocked;
                    }
                }
            }

            // Parse schedstat.
            {
                TIFStream file(schedStatPath);
                auto tokens = SplitString(file.ReadLine(), " ");
                if (tokens.size() < 3) {
                    return std::nullopt;
                }

                info.Timings.CpuWaitNsec = FromString<i64>(tokens[1]);
            }

            // Parse stat.
            {
                TIFStream file(statPath);
                auto tokens = SplitString(file.ReadLine(), " ");
                if (tokens.size() < 15) {
                    return std::nullopt;
                }

                info.Timings.UserJiffies = FromString<i64>(tokens[13]);
                info.Timings.SystemJiffies = FromString<i64>(tokens[14]);
            }

            info.ProfilingKey = info.ThreadName;

            // Group threads by thread pool, using YT thread naming convention.
            if (auto index = info.ProfilingKey.rfind(':'); index != std::string::npos) {
                bool isDigit = std::all_of(info.ProfilingKey.cbegin() + index + 1, info.ProfilingKey.cend(), [] (char c) {
                    return std::isdigit(c);
                });
                if (isDigit) {
                    info.ProfilingKey = info.ProfilingKey.substr(0, index);
                }
            }

            if (!info.IsYTThread) {
                info.ProfilingKey += "@";
            }
        } catch (const TIoException&) {
            // Ignore all IO exceptions.
            return std::nullopt;
        }

        YT_TLOG_TRACE("Thread statistics")
            .With("Tid", tid)
            .With("ThreadName", info.ThreadName)
            .With("IsYtThread", info.IsYTThread)
            .With("UserJiffies", info.Timings.UserJiffies)
            .With("SystemJiffies", info.Timings.SystemJiffies)
            .With("CpuWaitNsec", info.Timings.CpuWaitNsec);

        return info;
    }

    TThreadInfoMap ParseThreadInfos()
    {
        TDirsList dirsList;
        try {
            dirsList.Fill(ProcfsCurrentTaskPath);
        } catch (const TSystemError&) {
            // Ignore all exceptions.
            return {};
        }

        TThreadInfoMap tidToStats;

        for (int index = 0; index < static_cast<int>(dirsList.Size()); ++index) {
            auto tidStr = TStringBuf(dirsList.Next());
            NThreading::TThreadId tid;
            if (!TryFromString(tidStr, tid)) {
                continue;
            }
            auto info = TryParseThreadInfo(tid);
            if (info) {
                tidToStats[tid] = *info;
            } else {
                YT_TLOG_TRACE("Failed to parse thread info")
                    .With("Tid", tid);
            }
        }

        return tidToStats;
    }

    void CollectSensorsAggregatedTimings(
        ISensorWriter* writer,
        const TThreadInfoMap& oldTidToInfo,
        const TThreadInfoMap& newTidToInfo,
        i64 timeDeltaUsec)
    {
        double totalUserCpuTime = 0.0;
        double totalSystemCpuTime = 0.0;
        double totalCpuWaitTime = 0.0;

        THashMap<std::string, TTimings> profilingKeyToAggregatedTimings;
        THashMap<std::string, int> profilingKeyToCount;

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
            PerThreadExtraTags_.Read([&tagGuard, &profilingKey] (const auto& storage) {
                if (auto it = storage.find(profilingKey); it != storage.end()) {
                    for (const auto& tag : it->second.Tags()) {
                        tagGuard.AddTag(tag);
                    }
                }
            });

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

            YT_TLOG_TRACE("Thread CPU timings in percent/sec")
                .With("ProfilingKey", profilingKey)
                .With("UserCpu", userCpuTime)
                .With("SystemCpu", systemCpuTime)
                .With("CpuWait", waitTime);
        }

        LastUserCpu_.store(totalUserCpuTime);
        LastSystemCpu_.store(totalSystemCpuTime);
        LastCpuWait_.store(totalCpuWaitTime);
        MaxThreadPoolUtilization_.store(maxUtilization);

        YT_TLOG_DEBUG("Total CPU timings in percent/sec")
            .With("UserCpu", totalUserCpuTime)
            .With("SystemCpu", totalSystemCpuTime)
            .With("CpuWait", totalCpuWaitTime);

        int fileDescriptorCount = GetFileDescriptorCount();
        writer->AddGauge("/open_fds", fileDescriptorCount);
        YT_TLOG_DEBUG("Assessed open file descriptors")
            .With("Count", fileDescriptorCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

TResourceTrackerTagsGuard::TResourceTrackerTagsGuard(std::optional<std::string> threadName)
    : ThreadName_(std::move(threadName))
{ }

TResourceTrackerTagsGuard::TResourceTrackerTagsGuard(TResourceTrackerTagsGuard&& other) noexcept
{
    ThreadName_ = std::move(other.ThreadName_);
    other.ThreadName_ = std::nullopt;
}

TResourceTrackerTagsGuard& TResourceTrackerTagsGuard::operator=(TResourceTrackerTagsGuard&& other) noexcept
{
    if (this == &other) {
        return *this;
    }

    if (ThreadName_) {
        Release();
    }

    ThreadName_ = std::move(other.ThreadName_);
    other.ThreadName_ = std::nullopt;
    return *this;
}

TResourceTrackerTagsGuard::~TResourceTrackerTagsGuard()
{
    Release();
}

void TResourceTrackerTagsGuard::Release() noexcept
{
    if (ThreadName_) {
        TResourceTrackerImpl::Get()->UnregisterPerThreadExtraTags(*ThreadName_);
        ThreadName_ = std::nullopt;
    }
}

TResourceTrackerTagsGuard::operator bool() const
{
    return ThreadName_.has_value();
}

const std::optional<std::string>& TResourceTrackerTagsGuard::GetThreadName() const
{
    return ThreadName_;
}

void TResourceTracker::Enable()
{
    TResourceTrackerImpl::Get()->Enable();
}

double TResourceTracker::GetUserCpu()
{
    return TResourceTrackerImpl::Get()->GetUserCpu();
}

double TResourceTracker::GetSystemCpu()
{
    return TResourceTrackerImpl::Get()->GetSystemCpu();
}

double TResourceTracker::GetCpuWait()
{
    return TResourceTrackerImpl::Get()->GetCpuWait();
}

i64 TResourceTracker::GetTotalMemoryLimit()
{
    return TResourceTrackerImpl::Get()->GetTotalMemoryLimit();
}

i64 TResourceTracker::GetAnonymousMemoryLimit()
{
    return TResourceTrackerImpl::Get()->GetAnonymousMemoryLimit();
}

void TResourceTracker::SetCpuToVCpuFactor(double factor)
{
    TResourceTrackerImpl::Get()->SetCpuToVCpuFactor(factor);
}

void TResourceTracker::Configure(const TResourceTrackerConfigPtr& config)
{
    TResourceTrackerImpl::Get()->Configure(config);
}

TResourceTrackerTagsGuard TResourceTracker::RegisterPerThreadExtraTags(
    const std::string& threadName,
    const NProfiling::TTagSet& extraTags)
{
    return TResourceTrackerImpl::Get()->RegisterPerThreadExtraTags(threadName, extraTags);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
