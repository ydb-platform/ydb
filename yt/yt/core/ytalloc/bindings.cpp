#include "bindings.h"
#include "config.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/string_builder.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <library/cpp/yt/yson_string/string.h>

#include <util/system/env.h>

#include <cstdio>

namespace NYT::NYTAlloc {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

const NLogging::TLogger& GetLogger()
{
    struct TSingleton
    {
        NLogging::TLogger Logger{"YTAlloc"};
    };

    return LeakySingleton<TSingleton>()->Logger;
}

NLogging::ELogLevel SeverityToLevel(NYTAlloc::ELogEventSeverity severity)
{
    switch (severity) {

        case ELogEventSeverity::Debug:   return NLogging::ELogLevel::Debug;
        case ELogEventSeverity::Info:    return NLogging::ELogLevel::Info;
        case ELogEventSeverity::Warning: return NLogging::ELogLevel::Warning;
        case ELogEventSeverity::Error:   return NLogging::ELogLevel::Error;
        default:                         Y_UNREACHABLE();
    }
}

void LogHandler(const NYTAlloc::TLogEvent& event)
{
    YT_LOG_EVENT(GetLogger(), SeverityToLevel(event.Severity), event.Message);
}

} // namespace

void EnableYTLogging()
{
    EnableLogging(LogHandler);
}

////////////////////////////////////////////////////////////////////////////////

class TProfilingStatisticsProducer
    : public NProfiling::ISensorProducer
{
public:
    TProfilingStatisticsProducer(const TYTProfilingConfigPtr& config)
        : Config(config ? config : NYT::New<TYTProfilingConfig>())
    {
        NProfiling::TProfiler profiler{""};
        profiler.AddProducer("/yt_alloc", MakeStrong(this));
    }

    void CollectSensors(NProfiling::ISensorWriter* writer) override
    {
        PushSystemAllocationStatistics(writer);
        PushTotalAllocationStatistics(writer);

        if (Config->EnableDetailedAllocationStatistics.value_or(true)) {
            PushSmallAllocationStatistics(writer);
            PushLargeAllocationStatistics(writer);
            PushHugeAllocationStatistics(writer);
            PushUndumpableAllocationStatistics(writer);
        }

        PushTimingStatistics(writer);
    }

private:
    template <class TCounters>
    static void PushAllocationCounterStatistics(
        NProfiling::ISensorWriter* writer,
        const TString& prefix,
        const TCounters& counters)
    {
        for (auto counter : TEnumTraits<typename TCounters::TIndex>::GetDomainValues()) {
            // TODO(prime@): Fix type.
            writer->AddGauge(prefix + "/" + FormatEnum(counter), counters[counter]);
        }
    }

    void PushSystemAllocationStatistics(NProfiling::ISensorWriter* writer)
    {
        auto counters = GetSystemAllocationCounters();
        PushAllocationCounterStatistics(writer, "/system", counters);
    }

    void PushTotalAllocationStatistics(NProfiling::ISensorWriter* writer)
    {
        auto counters = GetTotalAllocationCounters();
        PushAllocationCounterStatistics(writer, "/total", counters);
    }

    void PushHugeAllocationStatistics(NProfiling::ISensorWriter* writer)
    {
        auto counters = GetHugeAllocationCounters();
        PushAllocationCounterStatistics(writer, "/huge", counters);
    }

    void PushUndumpableAllocationStatistics(NProfiling::ISensorWriter* writer)
    {
        auto counters = GetUndumpableAllocationCounters();
        PushAllocationCounterStatistics(writer, "/undumpable", counters);
    }

    void PushSmallArenaStatistics(
        NProfiling::ISensorWriter* writer,
        size_t rank,
        const TEnumIndexedArray<ESmallArenaCounter, ssize_t>& counters)
    {
        NProfiling::TWithTagGuard withTagGuard(writer, "rank", ToString(rank));
        PushAllocationCounterStatistics(writer, "/small_arena", counters);
    }

    void PushSmallAllocationStatistics(NProfiling::ISensorWriter* writer)
    {
        auto counters = GetSmallAllocationCounters();
        PushAllocationCounterStatistics(writer, "/small", counters);

        auto arenaCounters = GetSmallArenaAllocationCounters();
        for (size_t rank = 1; rank < SmallRankCount; ++rank) {
            PushSmallArenaStatistics(writer, rank, arenaCounters[rank]);
        }
    }

    void PushLargeArenaStatistics(
        NProfiling::ISensorWriter* writer,
        size_t rank,
        const TEnumIndexedArray<ELargeArenaCounter, ssize_t>& counters)
    {
        NProfiling::TWithTagGuard withTagGuard(writer, "rank", ToString(rank));

        PushAllocationCounterStatistics(writer, "/large_arena", counters);

        ssize_t bytesFreed = counters[ELargeArenaCounter::BytesFreed];
        ssize_t bytesReleased = counters[ELargeArenaCounter::PagesReleased] * PageSize;
        int poolHitRatio;
        if (bytesFreed == 0) {
            poolHitRatio = 100;
        } else if (bytesReleased > bytesFreed) {
            poolHitRatio = 0;
        } else {
            poolHitRatio = 100 - bytesReleased * 100 / bytesFreed;
        }

        writer->AddGauge("/large_arena/pool_hit_ratio", poolHitRatio);
    }

    void PushLargeAllocationStatistics(NProfiling::ISensorWriter* writer)
    {
        auto counters = GetLargeAllocationCounters();
        PushAllocationCounterStatistics(writer, "/large", counters);

        auto arenaCounters = GetLargeArenaAllocationCounters();
        for (size_t rank = MinLargeRank; rank < LargeRankCount; ++rank) {
            PushLargeArenaStatistics(writer, rank, arenaCounters[rank]);
        }
    }

    void PushTimingStatistics(NProfiling::ISensorWriter* writer)
    {
        auto timingEventCounters = GetTimingEventCounters();
        for (auto type : TEnumTraits<ETimingEventType>::GetDomainValues()) {
            const auto& counters = timingEventCounters[type];

            NProfiling::TWithTagGuard withTagGuard(writer, "type", ToString(type));
            writer->AddGauge("/timing_events/count", counters.Count);
            writer->AddGauge("/timing_events/size", counters.Size);
        }
    }

private:
    const TYTProfilingConfigPtr Config;
};

void EnableYTProfiling(const TYTProfilingConfigPtr& config)
{
    LeakyRefCountedSingleton<TProfilingStatisticsProducer>(config);
}

////////////////////////////////////////////////////////////////////////////////


namespace {

std::atomic<bool> ConfiguredFromEnv_;

void DoConfigure(const TYTAllocConfigPtr& config, bool fromEnv)
{
    if (config->SmallArenasToProfile) {
        for (size_t rank = 1; rank < SmallRankCount; ++rank) {
            SetSmallArenaAllocationProfilingEnabled(rank, false);
        }
        for (auto rank : *config->SmallArenasToProfile) {
            if (rank < 1 || rank >= SmallRankCount) {
                THROW_ERROR_EXCEPTION("Unable to enable allocation profiling for small arena %v since its rank is out of range",
                    rank);
            }
            SetSmallArenaAllocationProfilingEnabled(rank, true);
        }
    }

    if (config->LargeArenasToProfile) {
        for (size_t rank = 1; rank < LargeRankCount; ++rank) {
            SetLargeArenaAllocationProfilingEnabled(rank, false);
        }
        for (auto rank : *config->LargeArenasToProfile) {
            if (rank < 1 || rank >= LargeRankCount) {
                THROW_ERROR_EXCEPTION("Unable to enable allocation profiling for large arena %v since its rank is out of range",
                    rank);
            }
            SetLargeArenaAllocationProfilingEnabled(rank, true);
        }
    }

    if (config->EnableAllocationProfiling) {
        SetAllocationProfilingEnabled(*config->EnableAllocationProfiling);
    }

    if (config->AllocationProfilingSamplingRate) {
        SetAllocationProfilingSamplingRate(*config->AllocationProfilingSamplingRate);
    }

    if (config->ProfilingBacktraceDepth) {
        SetProfilingBacktraceDepth(*config->ProfilingBacktraceDepth);
    }

    if (config->MinProfilingBytesUsedToReport) {
        SetMinProfilingBytesUsedToReport(*config->MinProfilingBytesUsedToReport);
    }

    if (config->StockpileInterval) {
        SetStockpileInterval(*config->StockpileInterval);
    }

    if (config->StockpileThreadCount) {
        SetStockpileThreadCount(*config->StockpileThreadCount);
    }

    if (config->StockpileSize) {
        SetStockpileSize(*config->StockpileSize);
    }

    if (config->EnableEagerMemoryRelease) {
        SetEnableEagerMemoryRelease(*config->EnableEagerMemoryRelease);
    }

    if (config->EnableMadvisePopulate) {
        SetEnableMadvisePopulate(*config->EnableMadvisePopulate);
    }

    if (config->LargeUnreclaimableCoeff) {
        SetLargeUnreclaimableCoeff(*config->LargeUnreclaimableCoeff);
    }

    if (config->MinLargeUnreclaimableBytes) {
        SetMinLargeUnreclaimableBytes(*config->MinLargeUnreclaimableBytes);
    }

    if (config->MaxLargeUnreclaimableBytes) {
        SetMaxLargeUnreclaimableBytes(*config->MaxLargeUnreclaimableBytes);
    }

    ConfiguredFromEnv_.store(fromEnv);
}

} // namespace

void Configure(const TYTAllocConfigPtr& config)
{
    DoConfigure(config, false);
}

bool ConfigureFromEnv()
{
    const auto& Logger = GetLogger();

    static const TString ConfigEnvVarName = "YT_ALLOC_CONFIG";
    auto configVarValue = GetEnv(ConfigEnvVarName);
    if (!configVarValue) {
        YT_LOG_DEBUG("No %v environment variable is found",
            ConfigEnvVarName);
        return false;
    }

    TYTAllocConfigPtr config;
    try {
        config = ConvertTo<TYTAllocConfigPtr>(NYson::TYsonString(configVarValue));
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error parsing environment variable %v",
            ConfigEnvVarName);
        return false;
    }

    YT_LOG_DEBUG("%v environment variable parsed successfully",
        ConfigEnvVarName);

    try {
        DoConfigure(config, true);
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error applying configuration parsed from environment variable");
        return false;
    }

    return true;
}

bool IsConfiguredFromEnv()
{
    return ConfiguredFromEnv_.load();
}

////////////////////////////////////////////////////////////////////////////////

void InitializeLibunwindInterop()
{
    // COMPAT(babenko): intentionally empty, this code will die anyway.
}

TString FormatAllocationCounters()
{
    TStringBuilder builder;

    auto formatCounters = [&] (const auto& counters) {
        using T = typename std::decay_t<decltype(counters)>::TIndex;
        builder.AppendString("{");
        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);
        for (auto counter : TEnumTraits<T>::GetDomainValues()) {
            delimitedBuilder->AppendFormat("%v: %v", counter, counters[counter]);
        }
        builder.AppendString("}");
    };

    builder.AppendString("Total = {");
    formatCounters(GetTotalAllocationCounters());

    builder.AppendString("}, System = {");
    formatCounters(GetSystemAllocationCounters());

    builder.AppendString("}, Small = {");
    formatCounters(GetSmallAllocationCounters());

    builder.AppendString("}, Large = {");
    formatCounters(GetLargeAllocationCounters());

    builder.AppendString("}, Huge = {");
    formatCounters(GetHugeAllocationCounters());

    builder.AppendString("}");
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
