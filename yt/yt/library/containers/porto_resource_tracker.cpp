#include "porto_resource_tracker.h"
#include "private.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/library/containers/cgroup.h>
#include <yt/yt/library/containers/config.h>
#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/porto_executor.h>
#include <yt/yt/library/containers/public.h>

namespace NYT::NContainers {

using namespace NProfiling;

static const auto& Logger = ContainersLogger;

#ifdef _linux_

////////////////////////////////////////////////////////////////////////////////

struct TPortoProfilers
    : public TRefCounted
{
    TPortoResourceProfilerPtr DaemonProfiler;
    TPortoResourceProfilerPtr ContainerProfiler;

    TPortoProfilers(
        TPortoResourceProfilerPtr daemonProfiler,
        TPortoResourceProfilerPtr containerProfiler)
        : DaemonProfiler(std::move(daemonProfiler))
        , ContainerProfiler(std::move(containerProfiler))
    { }
};

DEFINE_REFCOUNTED_TYPE(TPortoProfilers)

////////////////////////////////////////////////////////////////////////////////

static TErrorOr<ui64> GetFieldOrError(
    const TResourceUsage& usage,
    EStatField field)
{
    auto it = usage.find(field);
    if (it == usage.end()) {
        return TError("Resource usage is missing %Qlv field", field);
    }
    const auto& errorOrValue = it->second;
    if (errorOrValue.FindMatching(EPortoErrorCode::NotSupported)) {
        return TError("Property %Qlv not supported in Porto response", field);
    }
    return errorOrValue;
}

////////////////////////////////////////////////////////////////////////////////

TPortoResourceTracker::TPortoResourceTracker(
    IInstancePtr instance,
    TDuration updatePeriod,
    bool isDeltaTracker,
    bool isForceUpdate)
    : Instance_(std::move(instance))
    , UpdatePeriod_(updatePeriod)
    , IsDeltaTracker_(isDeltaTracker)
    , IsForceUpdate_(isForceUpdate)
{
    ResourceUsage_ = {
        {EStatField::IOReadByte, 0},
        {EStatField::IOWriteByte, 0},
        {EStatField::IOBytesLimit, 0},
        {EStatField::IOReadOps, 0},
        {EStatField::IOWriteOps, 0},
        {EStatField::IOOps, 0},
        {EStatField::IOOpsLimit, 0},
        {EStatField::IOTotalTime, 0},
        {EStatField::IOWaitTime, 0}
    };
    ResourceUsageDelta_ = ResourceUsage_;
}

static TErrorOr<TDuration> ExtractDuration(TErrorOr<ui64> timeNs)
{
    if (timeNs.IsOK()) {
        return TErrorOr<TDuration>(TDuration::MicroSeconds(timeNs.Value() / 1000));
    } else {
        return TError(timeNs);
    }
}

TCpuStatistics TPortoResourceTracker::ExtractCpuStatistics(const TResourceUsage& resourceUsage) const
{
    // NB: Job proxy uses last sample of CPU statistics but we are interested in
    // peak thread count value.
    auto currentThreadCountPeak = GetFieldOrError(resourceUsage, EStatField::ThreadCount);

    PeakThreadCount_ = currentThreadCountPeak.IsOK() && PeakThreadCount_.IsOK()
        ? std::max<ui64>(
            PeakThreadCount_.Value(),
            currentThreadCountPeak.Value())
        : currentThreadCountPeak.IsOK() ? currentThreadCountPeak : PeakThreadCount_;

    auto totalTimeNs = GetFieldOrError(resourceUsage, EStatField::CpuUsage);
    auto systemTimeNs = GetFieldOrError(resourceUsage, EStatField::CpuSystemUsage);
    auto userTimeNs = GetFieldOrError(resourceUsage, EStatField::CpuUserUsage);
    auto waitTimeNs = GetFieldOrError(resourceUsage, EStatField::CpuWait);
    auto throttledNs = GetFieldOrError(resourceUsage, EStatField::CpuThrottled);
    auto limitTimeNs = GetFieldOrError(resourceUsage, EStatField::CpuLimit);
    auto guaranteeTimeNs = GetFieldOrError(resourceUsage, EStatField::CpuGuarantee);

    return TCpuStatistics{
        .TotalUsageTime = ExtractDuration(totalTimeNs),
        .UserUsageTime = ExtractDuration(userTimeNs),
        .SystemUsageTime = ExtractDuration(systemTimeNs),
        .WaitTime = ExtractDuration(waitTimeNs),
        .ThrottledTime = ExtractDuration(throttledNs),
        .ThreadCount = GetFieldOrError(resourceUsage, EStatField::ThreadCount),
        .ContextSwitches = GetFieldOrError(resourceUsage, EStatField::ContextSwitches),
        .ContextSwitchesDelta = GetFieldOrError(resourceUsage, EStatField::ContextSwitchesDelta),
        .PeakThreadCount = PeakThreadCount_,
        .LimitTime = ExtractDuration(limitTimeNs),
        .GuaranteeTime = ExtractDuration(guaranteeTimeNs),
    };
}

TMemoryStatistics TPortoResourceTracker::ExtractMemoryStatistics(const TResourceUsage& resourceUsage) const
{
    return TMemoryStatistics{
        .Rss = GetFieldOrError(resourceUsage, EStatField::Rss),
        .MappedFile = GetFieldOrError(resourceUsage, EStatField::MappedFile),
        .MinorPageFaults = GetFieldOrError(resourceUsage, EStatField::MinorPageFaults),
        .MajorPageFaults = GetFieldOrError(resourceUsage, EStatField::MajorPageFaults),
        .FileCacheUsage = GetFieldOrError(resourceUsage, EStatField::FileCacheUsage),
        .AnonUsage = GetFieldOrError(resourceUsage, EStatField::AnonMemoryUsage),
        .AnonLimit = GetFieldOrError(resourceUsage, EStatField::AnonMemoryLimit),
        .MemoryUsage = GetFieldOrError(resourceUsage, EStatField::MemoryUsage),
        .MemoryGuarantee = GetFieldOrError(resourceUsage, EStatField::MemoryGuarantee),
        .MemoryLimit = GetFieldOrError(resourceUsage, EStatField::MemoryLimit),
        .MaxMemoryUsage = GetFieldOrError(resourceUsage, EStatField::MaxMemoryUsage),
        .OomKills = GetFieldOrError(resourceUsage, EStatField::OomKills),
        .OomKillsTotal = GetFieldOrError(resourceUsage, EStatField::OomKillsTotal)
    };
}

TBlockIOStatistics TPortoResourceTracker::ExtractBlockIOStatistics(const TResourceUsage& resourceUsage) const
{
    auto totalTimeNs = GetFieldOrError(resourceUsage, EStatField::IOTotalTime);
    auto waitTimeNs = GetFieldOrError(resourceUsage, EStatField::IOWaitTime);

    return TBlockIOStatistics{
        .IOReadByte = GetFieldOrError(resourceUsage, EStatField::IOReadByte),
        .IOWriteByte = GetFieldOrError(resourceUsage, EStatField::IOWriteByte),
        .IOBytesLimit = GetFieldOrError(resourceUsage, EStatField::IOBytesLimit),
        .IOReadOps = GetFieldOrError(resourceUsage, EStatField::IOReadOps),
        .IOWriteOps = GetFieldOrError(resourceUsage, EStatField::IOWriteOps),
        .IOOps = GetFieldOrError(resourceUsage, EStatField::IOOps),
        .IOOpsLimit = GetFieldOrError(resourceUsage, EStatField::IOOpsLimit),
        .IOTotalTime = ExtractDuration(totalTimeNs),
        .IOWaitTime = ExtractDuration(waitTimeNs)
    };
}

TNetworkStatistics TPortoResourceTracker::ExtractNetworkStatistics(const TResourceUsage& resourceUsage) const
{
    return TNetworkStatistics{
        .TxBytes = GetFieldOrError(resourceUsage, EStatField::NetTxBytes),
        .TxPackets = GetFieldOrError(resourceUsage, EStatField::NetTxPackets),
        .TxDrops = GetFieldOrError(resourceUsage, EStatField::NetTxDrops),
        .TxLimit = GetFieldOrError(resourceUsage, EStatField::NetTxLimit),

        .RxBytes = GetFieldOrError(resourceUsage, EStatField::NetRxBytes),
        .RxPackets = GetFieldOrError(resourceUsage, EStatField::NetRxPackets),
        .RxDrops = GetFieldOrError(resourceUsage, EStatField::NetRxDrops),
        .RxLimit = GetFieldOrError(resourceUsage, EStatField::NetRxLimit),
    };
}

TTotalStatistics TPortoResourceTracker::ExtractTotalStatistics(const TResourceUsage& resourceUsage) const
{
    return TTotalStatistics{
        .CpuStatistics = ExtractCpuStatistics(resourceUsage),
        .MemoryStatistics = ExtractMemoryStatistics(resourceUsage),
        .BlockIOStatistics = ExtractBlockIOStatistics(resourceUsage),
        .NetworkStatistics = ExtractNetworkStatistics(resourceUsage),
    };
}

TCpuStatistics TPortoResourceTracker::GetCpuStatistics() const
{
    return GetStatistics(
        CachedCpuStatistics_,
        "CPU",
        [&] (TResourceUsage& resourceUsage) {
            return ExtractCpuStatistics(resourceUsage);
        });
}

TMemoryStatistics TPortoResourceTracker::GetMemoryStatistics() const
{
    return GetStatistics(
        CachedMemoryStatistics_,
        "memory",
        [&] (TResourceUsage& resourceUsage) {
            return ExtractMemoryStatistics(resourceUsage);
        });
}

TBlockIOStatistics TPortoResourceTracker::GetBlockIOStatistics() const
{
    return GetStatistics(
        CachedBlockIOStatistics_,
        "block IO",
        [&] (TResourceUsage& resourceUsage) {
            return ExtractBlockIOStatistics(resourceUsage);
        });
}

TNetworkStatistics TPortoResourceTracker::GetNetworkStatistics() const
{
    return GetStatistics(
        CachedNetworkStatistics_,
        "network",
        [&] (TResourceUsage& resourceUsage) {
            return ExtractNetworkStatistics(resourceUsage);
        });
}

TTotalStatistics TPortoResourceTracker::GetTotalStatistics() const
{
    return GetStatistics(
        CachedTotalStatistics_,
        "total",
        [&] (TResourceUsage& resourceUsage) {
            return ExtractTotalStatistics(resourceUsage);
        });
}

template <class T, class F>
T TPortoResourceTracker::GetStatistics(
    std::optional<T>& cachedStatistics,
    const TString& statisticsKind,
    F extractor) const
{
    UpdateResourceUsageStatisticsIfExpired();

    auto guard = Guard(SpinLock_);
    try {
        auto newStatistics = extractor(IsDeltaTracker_ ? ResourceUsageDelta_ : ResourceUsage_);
        cachedStatistics = newStatistics;
        return newStatistics;
    } catch (const std::exception& ex) {
        if (!cachedStatistics) {
            THROW_ERROR_EXCEPTION("Unable to get %v statistics", statisticsKind)
                << ex;
        }
        YT_LOG_WARNING(ex, "Unable to get %v statistics; using the last one", statisticsKind);
        return *cachedStatistics;
    }
}

bool TPortoResourceTracker::AreResourceUsageStatisticsExpired() const
{
    return TInstant::Now() - LastUpdateTime_.load() > UpdatePeriod_;
}

TInstant TPortoResourceTracker::GetLastUpdateTime() const
{
    return LastUpdateTime_.load();
}

void TPortoResourceTracker::UpdateResourceUsageStatisticsIfExpired() const
{
    if (IsForceUpdate_ || AreResourceUsageStatisticsExpired()) {
        DoUpdateResourceUsage();
    }
}

TErrorOr<ui64> TPortoResourceTracker::CalculateCounterDelta(
    const TErrorOr<ui64>& oldValue,
    const TErrorOr<ui64>& newValue) const
{
    if (oldValue.IsOK() && newValue.IsOK()) {
        return newValue.Value() - oldValue.Value();
    } else if (newValue.IsOK()) {
        // It is better to return an error than an incorrect value.
        return oldValue;
    } else {
        return newValue;
    }
}

static bool IsCumulativeStatistics(EStatField statistic)
{
    return
        statistic == EStatField::CpuUsage ||
        statistic == EStatField::CpuUserUsage ||
        statistic == EStatField::CpuSystemUsage ||
        statistic == EStatField::CpuWait ||
        statistic == EStatField::CpuThrottled ||

        statistic == EStatField::ContextSwitches ||

        statistic == EStatField::MinorPageFaults ||
        statistic == EStatField::MajorPageFaults ||
        statistic == EStatField::OomKills ||
        statistic == EStatField::OomKillsTotal ||

        statistic == EStatField::IOReadByte ||
        statistic == EStatField::IOWriteByte ||
        statistic == EStatField::IOReadOps ||
        statistic == EStatField::IOWriteOps ||
        statistic == EStatField::IOOps ||
        statistic == EStatField::IOTotalTime ||
        statistic == EStatField::IOWaitTime ||

        statistic == EStatField::NetTxBytes ||
        statistic == EStatField::NetTxPackets ||
        statistic == EStatField::NetTxDrops ||
        statistic == EStatField::NetRxBytes ||
        statistic == EStatField::NetRxPackets ||
        statistic == EStatField::NetRxDrops;
}

void TPortoResourceTracker::ReCalculateResourceUsage(const TResourceUsage& newResourceUsage) const
{
    auto guard = Guard(SpinLock_);

    TResourceUsage resourceUsage;
    TResourceUsage resourceUsageDelta;

    for (const auto& stat : InstanceStatFields) {
        TErrorOr<ui64> oldValue;
        TErrorOr<ui64> newValue;

        if (auto newValueIt = newResourceUsage.find(stat); newValueIt.IsEnd()) {
            newValue = TError("Missing property %Qlv in Porto response", stat)
                << TErrorAttribute("container", Instance_->GetName());
        } else {
            newValue = newValueIt->second;
        }

        if (auto oldValueIt = ResourceUsage_.find(stat); oldValueIt.IsEnd()) {
            oldValue = newValue;
        } else {
            oldValue = oldValueIt->second;
        }

        if (newValue.IsOK()) {
            resourceUsage[stat] = newValue;
        } else {
            resourceUsage[stat] = oldValue;
        }

        if (IsCumulativeStatistics(stat)) {
            resourceUsageDelta[stat] = CalculateCounterDelta(oldValue, newValue);
        } else {
            if (newValue.IsOK()) {
                resourceUsageDelta[stat] = newValue;
            } else {
                resourceUsageDelta[stat] = oldValue;
            }
        }
    }

    ResourceUsage_ = resourceUsage;
    ResourceUsageDelta_ = resourceUsageDelta;
    LastUpdateTime_.store(TInstant::Now());
}

void TPortoResourceTracker::DoUpdateResourceUsage() const
{
    try {
        ReCalculateResourceUsage(Instance_->GetResourceUsage());
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(
            ex,
            "Couldn't get metrics from porto");
    }
}

////////////////////////////////////////////////////////////////////////////////

TPortoResourceProfiler::TPortoResourceProfiler(
    TPortoResourceTrackerPtr tracker,
    TPodSpecConfigPtr podSpec,
    const TProfiler& profiler)
    : ResourceTracker_(std::move(tracker))
    , PodSpec_(std::move(podSpec))
{
    profiler.AddProducer("", MakeStrong(this));
}

static void WriteGaugeIfOk(
    ISensorWriter* writer,
    const TString& path,
    TErrorOr<ui64> valueOrError)
{
    if (valueOrError.IsOK()) {
        i64 value = static_cast<i64>(valueOrError.Value());

        if (value >= 0) {
            writer->AddGauge(path, value);
        }
    }
}

static void WriteCumulativeGaugeIfOk(
    ISensorWriter* writer,
    const TString& path,
    TErrorOr<ui64> valueOrError,
    i64 timeDeltaUsec)
{
    if (valueOrError.IsOK()) {
        i64 value = static_cast<i64>(valueOrError.Value());

        if (value >= 0) {
            writer->AddGauge(path,
                1.0 * value * ResourceUsageUpdatePeriod.MicroSeconds() / timeDeltaUsec);
        }
    }
}

void TPortoResourceProfiler::WriteCpuMetrics(
    ISensorWriter* writer,
    TTotalStatistics& totalStatistics,
    i64 timeDeltaUsec)
{
    {
        if (totalStatistics.CpuStatistics.UserUsageTime.IsOK()) {
            i64 userUsageTimeUs = totalStatistics.CpuStatistics.UserUsageTime.Value().MicroSeconds();
            double userUsagePercent = std::max<double>(0.0, 100. * userUsageTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/user", userUsagePercent);
        }

        if (totalStatistics.CpuStatistics.SystemUsageTime.IsOK()) {
            i64 systemUsageTimeUs = totalStatistics.CpuStatistics.SystemUsageTime.Value().MicroSeconds();
            double systemUsagePercent = std::max<double>(0.0, 100. * systemUsageTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/system", systemUsagePercent);
        }

        if (totalStatistics.CpuStatistics.WaitTime.IsOK()) {
            i64 waitTimeUs = totalStatistics.CpuStatistics.WaitTime.Value().MicroSeconds();
            double waitPercent = std::max<double>(0.0, 100. * waitTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/wait", waitPercent);
        }

        if (totalStatistics.CpuStatistics.ThrottledTime.IsOK()) {
            i64 throttledTimeUs = totalStatistics.CpuStatistics.ThrottledTime.Value().MicroSeconds();
            double throttledPercent = std::max<double>(0.0, 100. * throttledTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/throttled", throttledPercent);
        }

        if (totalStatistics.CpuStatistics.TotalUsageTime.IsOK()) {
            i64 totalUsageTimeUs = totalStatistics.CpuStatistics.TotalUsageTime.Value().MicroSeconds();
            double totalUsagePercent = std::max<double>(0.0, 100. * totalUsageTimeUs / timeDeltaUsec);
            writer->AddGauge("/cpu/total", totalUsagePercent);
        }

        if (totalStatistics.CpuStatistics.GuaranteeTime.IsOK()) {
            i64 guaranteeTimeUs = totalStatistics.CpuStatistics.GuaranteeTime.Value().MicroSeconds();
            double guaranteePercent = std::max<double>(0.0, (100. * guaranteeTimeUs) / (1'000'000L));
            writer->AddGauge("/cpu/guarantee", guaranteePercent);
        }

        if (totalStatistics.CpuStatistics.LimitTime.IsOK()) {
            i64 limitTimeUs = totalStatistics.CpuStatistics.LimitTime.Value().MicroSeconds();
            double limitPercent = std::max<double>(0.0, (100. * limitTimeUs) / (1'000'000L));
            writer->AddGauge("/cpu/limit", limitPercent);
        }
    }

    if (PodSpec_->CpuToVCpuFactor) {
        auto factor = *PodSpec_->CpuToVCpuFactor;

        writer->AddGauge("/cpu_to_vcpu_factor", factor);

        if (totalStatistics.CpuStatistics.UserUsageTime.IsOK()) {
            i64 userUsageTimeUs = totalStatistics.CpuStatistics.UserUsageTime.Value().MicroSeconds();
            double userUsagePercent = std::max<double>(0.0, 100. * userUsageTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/user", userUsagePercent);
        }

        if (totalStatistics.CpuStatistics.SystemUsageTime.IsOK()) {
            i64 systemUsageTimeUs = totalStatistics.CpuStatistics.SystemUsageTime.Value().MicroSeconds();
            double systemUsagePercent = std::max<double>(0.0, 100. * systemUsageTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/system", systemUsagePercent);
        }

        if (totalStatistics.CpuStatistics.WaitTime.IsOK()) {
            i64 waitTimeUs = totalStatistics.CpuStatistics.WaitTime.Value().MicroSeconds();
            double waitPercent = std::max<double>(0.0, 100. * waitTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/wait", waitPercent);
        }

        if (totalStatistics.CpuStatistics.ThrottledTime.IsOK()) {
            i64 throttledTimeUs = totalStatistics.CpuStatistics.ThrottledTime.Value().MicroSeconds();
            double throttledPercent = std::max<double>(0.0, 100. * throttledTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/throttled", throttledPercent);
        }

        if (totalStatistics.CpuStatistics.TotalUsageTime.IsOK()) {
            i64 totalUsageTimeUs = totalStatistics.CpuStatistics.TotalUsageTime.Value().MicroSeconds();
            double totalUsagePercent = std::max<double>(0.0, 100. * totalUsageTimeUs * factor / timeDeltaUsec);
            writer->AddGauge("/vcpu/total", totalUsagePercent);
        }

        if (totalStatistics.CpuStatistics.GuaranteeTime.IsOK()) {
            i64 guaranteeTimeUs = totalStatistics.CpuStatistics.GuaranteeTime.Value().MicroSeconds();
            double guaranteePercent = std::max<double>(0.0, 100. * guaranteeTimeUs * factor / 1'000'000L);
            writer->AddGauge("/vcpu/guarantee", guaranteePercent);
        }

        if (totalStatistics.CpuStatistics.LimitTime.IsOK()) {
            i64 limitTimeUs = totalStatistics.CpuStatistics.LimitTime.Value().MicroSeconds();
            double limitPercent = std::max<double>(0.0, 100. * limitTimeUs * factor / 1'000'000L);
            writer->AddGauge("/vcpu/limit", limitPercent);
        }
    }

    WriteGaugeIfOk(writer, "/cpu/thread_count", totalStatistics.CpuStatistics.ThreadCount);
    WriteGaugeIfOk(writer, "/cpu/context_switches", totalStatistics.CpuStatistics.ContextSwitches);
}

void TPortoResourceProfiler::WriteMemoryMetrics(
    ISensorWriter* writer,
    TTotalStatistics& totalStatistics,
    i64 timeDeltaUsec)
{
    WriteCumulativeGaugeIfOk(writer,
        "/memory/minor_page_faults",
        totalStatistics.MemoryStatistics.MinorPageFaults,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(writer,
        "/memory/major_page_faults",
        totalStatistics.MemoryStatistics.MajorPageFaults,
        timeDeltaUsec);

    WriteGaugeIfOk(writer, "/memory/oom_kills", totalStatistics.MemoryStatistics.OomKills);
    WriteGaugeIfOk(writer, "/memory/oom_kills_total", totalStatistics.MemoryStatistics.OomKillsTotal);

    WriteGaugeIfOk(writer, "/memory/file_cache_usage", totalStatistics.MemoryStatistics.FileCacheUsage);
    WriteGaugeIfOk(writer, "/memory/anon_usage", totalStatistics.MemoryStatistics.AnonUsage);
    WriteGaugeIfOk(writer, "/memory/anon_limit", totalStatistics.MemoryStatistics.AnonLimit);
    WriteGaugeIfOk(writer, "/memory/memory_usage", totalStatistics.MemoryStatistics.MemoryUsage);
    WriteGaugeIfOk(writer, "/memory/memory_guarantee", totalStatistics.MemoryStatistics.MemoryGuarantee);
    WriteGaugeIfOk(writer, "/memory/memory_limit", totalStatistics.MemoryStatistics.MemoryLimit);
}

void TPortoResourceProfiler::WriteBlockingIOMetrics(
    ISensorWriter* writer,
    TTotalStatistics& totalStatistics,
    i64 timeDeltaUsec)
{
    WriteCumulativeGaugeIfOk(writer,
        "/io/read_bytes",
        totalStatistics.BlockIOStatistics.IOReadByte,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(writer,
        "/io/write_bytes",
        totalStatistics.BlockIOStatistics.IOWriteByte,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(writer,
        "/io/read_ops",
        totalStatistics.BlockIOStatistics.IOReadOps,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(writer,
        "/io/write_ops",
        totalStatistics.BlockIOStatistics.IOWriteOps,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(writer,
        "/io/ops",
        totalStatistics.BlockIOStatistics.IOOps,
        timeDeltaUsec);

    WriteGaugeIfOk(writer,
        "/io/bytes_limit",
        totalStatistics.BlockIOStatistics.IOBytesLimit);
    WriteGaugeIfOk(writer,
        "/io/ops_limit",
        totalStatistics.BlockIOStatistics.IOOpsLimit);

    if (totalStatistics.BlockIOStatistics.IOTotalTime.IsOK()) {
        i64 totalTimeUs = totalStatistics.BlockIOStatistics.IOTotalTime.Value().MicroSeconds();
        double totalPercent = std::max<double>(0.0, 100. * totalTimeUs / timeDeltaUsec);
        writer->AddGauge("/io/total", totalPercent);
    }

    if (totalStatistics.BlockIOStatistics.IOWaitTime.IsOK()) {
        i64 waitTimeUs = totalStatistics.BlockIOStatistics.IOWaitTime.Value().MicroSeconds();
        double waitPercent = std::max<double>(0.0, 100. * waitTimeUs / timeDeltaUsec);
        writer->AddGauge("/io/wait", waitPercent);
    }
}

void TPortoResourceProfiler::WriteNetworkMetrics(
    ISensorWriter* writer,
    TTotalStatistics& totalStatistics,
    i64 timeDeltaUsec)
{
    WriteCumulativeGaugeIfOk(
        writer,
        "/network/rx_bytes",
        totalStatistics.NetworkStatistics.RxBytes,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(
        writer,
        "/network/rx_drops",
        totalStatistics.NetworkStatistics.RxDrops,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(
        writer,
        "/network/rx_packets",
        totalStatistics.NetworkStatistics.RxPackets,
        timeDeltaUsec);
    WriteGaugeIfOk(
        writer,
        "/network/rx_limit",
        totalStatistics.NetworkStatistics.RxLimit);

    WriteCumulativeGaugeIfOk(
        writer,
        "/network/tx_bytes",
        totalStatistics.NetworkStatistics.TxBytes,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(
        writer,
        "/network/tx_drops",
        totalStatistics.NetworkStatistics.TxDrops,
        timeDeltaUsec);
    WriteCumulativeGaugeIfOk(
        writer,
        "/network/tx_packets",
        totalStatistics.NetworkStatistics.TxPackets,
        timeDeltaUsec);
    WriteGaugeIfOk(
        writer,
        "/network/tx_limit",
        totalStatistics.NetworkStatistics.TxLimit);
}

void TPortoResourceProfiler::CollectSensors(ISensorWriter* writer)
{
    i64 lastUpdate = ResourceTracker_->GetLastUpdateTime().MicroSeconds();

    auto totalStatistics = ResourceTracker_->GetTotalStatistics();
    i64 timeDeltaUsec = TInstant::Now().MicroSeconds() - lastUpdate;

    WriteCpuMetrics(writer, totalStatistics, timeDeltaUsec);
    WriteMemoryMetrics(writer, totalStatistics, timeDeltaUsec);
    WriteBlockingIOMetrics(writer, totalStatistics, timeDeltaUsec);
    WriteNetworkMetrics(writer, totalStatistics, timeDeltaUsec);
}

////////////////////////////////////////////////////////////////////////////////

TPortoResourceProfilerPtr CreatePortoProfilerWithTags(
    const IInstancePtr& instance,
    const TString containerCategory,
    const TPodSpecConfigPtr& podSpec)
{
    auto portoResourceTracker = New<TPortoResourceTracker>(
        instance,
        ResourceUsageUpdatePeriod,
        true,
        true);

    return New<TPortoResourceProfiler>(
        portoResourceTracker,
        podSpec,
        TProfiler("/porto")
            .WithTag("container_category", containerCategory));
}

////////////////////////////////////////////////////////////////////////////////

#endif

#ifdef __linux__
void EnablePortoResourceTracker(const TPodSpecConfigPtr& podSpec)
{
    try {
        auto executor = CreatePortoExecutor(New<TPortoExecutorDynamicConfig>(), "porto-tracker");

        executor->SubscribeFailed(BIND([=] (const TError& error) {
            YT_LOG_ERROR(error, "Fatal error during Porto polling");
        }));

        LeakyRefCountedSingleton<TPortoProfilers>(
            CreatePortoProfilerWithTags(GetSelfPortoInstance(executor), "daemon", podSpec),
            CreatePortoProfilerWithTags(GetRootPortoInstance(executor), "pod", podSpec));
    } catch(const std::exception& exception) {
        YT_LOG_ERROR(exception, "Failed to enable porto profiler");
    }
}
#else
void EnablePortoResourceTracker(const TPodSpecConfigPtr& /*podSpec*/)
{
    YT_LOG_WARNING("Porto resource tracker not supported");
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
