#pragma once

#include <vector>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/misc/proc.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCpuCgroupTracker)

class TCpuCgroupTracker
    : public NProfiling::ISensorProducer
{
public:
    void CollectSensors(ISensorWriter* writer) override;

private:
    std::optional<TCgroupCpuStat> FirstCgroupStat_;
    bool CgroupErrorLogged_ = false;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMemoryCgroupTracker)

class TMemoryCgroupTracker
    : public NProfiling::ISensorProducer
{
public:
    void CollectSensors(ISensorWriter* writer) override;

    i64 GetTotalMemoryLimit() const;
    i64 GetAnonymousMemoryLimit() const;

private:
    bool CgroupErrorLogged_ = false;
    bool AnonymousLimitErrorLogged_ = false;

    std::atomic<i64> TotalMemoryLimit_ = 0;
    std::atomic<i64> AnonymousMemoryLimit_ = 0;

    i64 SafeGetAnonymousMemoryLimit(const TString& cgroupPath, i64 totalMemoryLimit);
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TResourceTracker)

class TResourceTracker
    : public NProfiling::ISensorProducer
{
public:
    explicit TResourceTracker();

    double GetUserCpu();
    double GetSystemCpu();
    double GetCpuWait();

    i64 GetTotalMemoryLimit();
    i64 GetAnonymousMemoryLimit();

    void CollectSensors(ISensorWriter* writer) override;

    void SetVCpuFactor(double factor);

private:
    std::atomic<double> VCpuFactor_{0.0};

    i64 TicksPerSecond_;
    TInstant LastUpdateTime_;

    // Value below are in percents.
    std::atomic<double> LastUserCpu_{0.0};
    std::atomic<double> LastSystemCpu_{0.0};
    std::atomic<double> LastCpuWait_{0.0};

    std::atomic<double> MaxThreadPoolUtilization_ = {0.0};

    struct TTimings
    {
        i64 UserJiffies = 0;
        i64 SystemJiffies = 0;
        i64 CpuWaitNsec = 0;

        TTimings operator-(const TTimings& other) const;
        TTimings& operator+=(const TTimings& other);
    };

    struct TThreadInfo
    {
        TString ThreadName;
        TTimings Timings;
        bool IsYtThread = true;
        //! This key is IsYtThread ? ThreadName : ThreadName + "@".
        //! It allows to distinguish YT threads from non-YT threads that
        //! inherited parent YT thread name.
        TString ProfilingKey;
    };

    // thread id -> stats
    using TThreadMap = THashMap<TString, TThreadInfo>;

    TThreadMap TidToInfo_;

    TCpuCgroupTrackerPtr CpuCgroupTracker_;
    TMemoryCgroupTrackerPtr MemoryCgroupTracker_;

    void EnqueueUsage();

    void EnqueueThreadStats();

    bool ProcessThread(TString tid, TThreadInfo* result);
    TThreadMap ProcessThreads();

    void CollectSensorsAggregatedTimings(
        ISensorWriter* writer,
        const TThreadMap& oldTidToInfo,
        const TThreadMap& newTidToInfo,
        i64 timeDeltaUsec);
};

TResourceTrackerPtr GetResourceTracker();

void EnableResourceTracker();

//! If this vCpuFactor is set, additional metrics will be reported:
//!   user, system, total cpu multiplied by given factor.
//! E. g. system_vcpu = system_cpu * vcpu_factor.
void SetVCpuFactor(double vCpuFactor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
