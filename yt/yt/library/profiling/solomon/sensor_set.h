#pragma once

#include "cube.h"
#include "tag_registry.h"
#include "sensor.h"

#include <yt/yt/library/profiling/tag.h>
#include <yt/yt/library/profiling/solomon/sensor_dump.pb.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCounterState)

struct TCounterState final
{
    TCounterState(
        TWeakPtr<TRefCounted> owner,
        std::function<i64()> reader,
        TTagIdSet tagSet)
        : Owner(std::move(owner))
        , Reader(std::move(reader))
        , TagSet(std::move(tagSet))
    { }

    const TWeakPtr<TRefCounted> Owner;
    const std::function<i64()> Reader;
    TTagIdSet TagSet;

    i64 LastValue = 0;
};

DEFINE_REFCOUNTED_TYPE(TCounterState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTimeCounterState)

struct TTimeCounterState final
{
    TTimeCounterState(
        TWeakPtr<ITimeCounter> owner,
        TTagIdSet tagSet)
        : Owner(std::move(owner))
        , TagSet(std::move(tagSet))
    { }

    const TWeakPtr<ITimeCounter> Owner;
    TTagIdSet TagSet;

    TDuration LastValue = TDuration::Zero();
};

DEFINE_REFCOUNTED_TYPE(TTimeCounterState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TGaugeState)

struct TGaugeState final
{
    TGaugeState(
        TWeakPtr<TRefCounted> owner,
        std::function<double()> reader,
        TTagIdSet tagSet)
        : Owner(std::move(owner))
        , Reader(std::move(reader))
        , TagSet(std::move(tagSet))
    { }

    const TWeakPtr<TRefCounted> Owner;
    const std::function<double()> Reader;
    TTagIdSet TagSet;
};

DEFINE_REFCOUNTED_TYPE(TGaugeState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSummaryState)

struct TSummaryState final
{
    TSummaryState(
        TWeakPtr<ISummary> owner,
        TTagIdSet tagSet)
        : Owner(std::move(owner))
        , TagSet(std::move(tagSet))
    { }

    const TWeakPtr<ISummary> Owner;
    TTagIdSet TagSet;
};

DEFINE_REFCOUNTED_TYPE(TSummaryState)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTimerSummaryState)

struct TTimerSummaryState final
{
    TTimerSummaryState(
        TWeakPtr<ITimer> owner,
        TTagIdSet tagSet)
        : Owner(owner)
        , TagSet(std::move(tagSet))
    { }

    const TWeakPtr<ITimer> Owner;
    TTagIdSet TagSet;
};

DEFINE_REFCOUNTED_TYPE(TTimerSummaryState)

////////////////////////////////////////////////////////////////////////////////


DECLARE_REFCOUNTED_STRUCT(THistogramState)

struct THistogramState final
{
    THistogramState(
        TWeakPtr<THistogram> owner,
        TTagIdSet tagSet)
        : Owner(owner)
        , TagSet(std::move(tagSet))
    { }

    const TWeakPtr<THistogram> Owner;
    TTagIdSet TagSet;
};

DEFINE_REFCOUNTED_TYPE(THistogramState)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESensorType,
    ((Counter)        (1))
    ((TimeCounter)    (2))
    ((Gauge)          (3))
    ((Summary)        (4))
    ((Timer)          (5))
    ((TimeHistogram)  (6))
    ((GaugeHistogram) (7))
    ((RateHistogram)  (8))
);

////////////////////////////////////////////////////////////////////////////////

class TSensorSet
{
public:
    TSensorSet(
        TSensorOptions options,
        i64 iteration,
        int windowSize,
        int gridFactor);

    bool IsEmpty() const;

    void Profile(const TWeakProfiler& profiler);
    void ValidateOptions(const TSensorOptions& options);

    void AddCounter(TCounterStatePtr counter);
    void AddGauge(TGaugeStatePtr gauge);
    void AddSummary(TSummaryStatePtr summary);
    void AddTimerSummary(TTimerSummaryStatePtr timer);
    void AddTimeCounter(TTimeCounterStatePtr counter);
    void AddTimeHistogram(THistogramStatePtr histogram);
    void AddGaugeHistogram(THistogramStatePtr histogram);
    void AddRateHistogram(THistogramStatePtr histogram);

    void RenameDynamicTag(const TDynamicTagPtr& dynamicTag, TTagId newTag);

    int Collect();

    void ReadSensors(
        const std::string& name,
        TReadOptions readOptions,
        TTagWriter* tagWriter,
        ::NMonitoring::IMetricConsumer* consumer) const;

    int ReadSensorValues(
        const TTagIdList& tagIds,
        int index,
        TReadOptions readOptions,
        const TTagRegistry& tagRegistry,
        NYTree::TFluentAny fluent) const;

    void DumpCube(NProto::TCube* cube, const std::vector<TTagIdList>& extraProjections) const;

    int GetGridFactor() const;
    int GetObjectCount() const;
    int GetCubeSize() const;
    const TError& GetError() const;
    std::optional<ESensorType> GetType() const;

private:
    friend class TRemoteRegistry;

    const TSensorOptions Options_;
    const int GridFactor_;

    TError Error_;

    THashSet<TCounterStatePtr> Counters_;
    TCounterCube CountersCube_;

    THashSet<TTimeCounterStatePtr> TimeCounters_;
    TTimeCounterCube TimeCountersCube_;

    THashSet<TGaugeStatePtr> Gauges_;
    TGaugeCube GaugesCube_;

    THashSet<TSummaryStatePtr> Summaries_;
    TSummaryCube SummariesCube_;

    THashSet<TTimerSummaryStatePtr> Timers_;
    TTimerCube TimersCube_;

    THashSet<THistogramStatePtr> TimeHistograms_;
    TTimeHistogramCube TimeHistogramsCube_;

    THashSet<THistogramStatePtr> GaugeHistograms_;
    TGaugeHistogramCube GaugeHistogramsCube_;

    THashSet<THistogramStatePtr> RateHistograms_;
    TRateHistogramCube RateHistogramsCube_;

    std::optional<ESensorType> Type_;
    TGauge CubeSize_;
    TGauge SensorsEmitted_;

    void OnError(TError error);

    void InitializeType(ESensorType type);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
