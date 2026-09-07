#pragma once

#include "public.h"

#include "config.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

using NYdb::NBS::UpdateCountersInterval;
using NYdb::NBS::UpdateStatsInterval;

////////////////////////////////////////////////////////////////////////////////

class TVolumePerformanceCalculator
{
    static constexpr const ui32 SampleCount =
        UpdateCountersInterval.Seconds() / UpdateStatsInterval.Seconds();

private:
    const NCloud::NProto::EStorageMediaKind MediaKind;
    const TVolumePerfSettings ConfigSettings;
    const ui32 ExpectedIoParallelism;

    TAtomic ExpectedScore = 0;
    TAtomic CurrentScore = 0;

    ui64 UpdateCounter = 0;
    TIntrusivePtr<NMonitoring::TCounterForPtr> Counter;
    TIntrusivePtr<NMonitoring::TCounterForPtr> SmoothCounter;
    TIntrusivePtr<NMonitoring::TCounterForPtr> CriticalCounter;

    bool IsEnabled = false;
    THotSwap<TVolumePerfSettings> PerfSettings;

    struct TSample
    {
        bool Suffered = false;
        long ExpectedScore = 0;
        long ActualScore = 0;
    };

    std::array<TSample, SampleCount> Samples = {};
    TAtomic SufferCount = 0;
    TAtomic SmoothSufferCount = 0;
    TAtomic CriticalSufferCount = 0;

private:
    TVolumePerfSettings GetConfigSettings(
        TDiagnosticsConfigPtr diagnosticsConfig) const;
    bool
    DidSuffer(ui64 expectedScore, ui64 actualScore, TDuration window) const;

public:
    TVolumePerformanceCalculator(
        const NProto::TVolume& volume,
        TDiagnosticsConfigPtr diagnosticsConfig);

    void Register(
        NMonitoring::TDynamicCounters& counters,
        const NProto::TVolume& volume);
    void Register(const NProto::TVolume& volume);

    void OnRequestCompleted(
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        ui64 requestCompleted,
        ui64 waitTime,
        ui32 requestBytes);

    bool UpdateStats();

    ui32 GetSufferCount() const
    {
        return AtomicGet(SufferCount);
    }

    bool IsSuffering() const
    {
        return GetSufferCount() != 0;
    }

    ui32 GetSmoothSufferCount() const
    {
        return AtomicGet(SmoothSufferCount);
    }

    bool IsSufferingSmooth() const
    {
        return GetSmoothSufferCount() != 0;
    }

    ui32 GetCriticalSufferCount() const
    {
        return AtomicGet(CriticalSufferCount);
    }

    bool IsSufferingCritically() const
    {
        return GetCriticalSufferCount() != 0;
    }

    // for testing purpose
    TDuration GetExpectedReadCost(ui32 requestBytes) const;
    TDuration GetExpectedWriteCost(ui32 requestBytes) const;

    TDuration GetExpectedCost() const;
    TDuration GetCurrentCost() const;
};

////////////////////////////////////////////////////////////////////////////////

class TSufferCounters
{
private:
    using TDynamicCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

    template <typename T>
    using TSufferArray =
        std::array<T, NCloud::NProto::EStorageMediaKind_ARRAYSIZE>;

    const TString DisksSufferCounterName;
    NMonitoring::TDynamicCountersPtr Counters;
    TDynamicCounterPtr Total;

    TSufferArray<ui64> RunCounters = {};

    TDynamicCounterPtr Hdd;
    TDynamicCounterPtr Ssd;
    TDynamicCounterPtr SsdNonrepl;
    TDynamicCounterPtr HddNonrepl;
    TDynamicCounterPtr SsdMirror2;
    TDynamicCounterPtr SsdMirror3;
    TDynamicCounterPtr SsdLocal;
    TDynamicCounterPtr HddLocal;

public:
    explicit TSufferCounters(
        const TString& disksSufferCounterName,
        NMonitoring::TDynamicCountersPtr counters)
        : DisksSufferCounterName(disksSufferCounterName)
        , Counters(std::move(counters))
    {}

    ui64 UpdateCounter(
        TDynamicCounterPtr& counter,
        const TString& diskType,
        ui64 value);

    void PublishCounters();

    void OnDiskSuffer(NCloud::NProto::EStorageMediaKind kind)
    {
        ++RunCounters[kind];
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
