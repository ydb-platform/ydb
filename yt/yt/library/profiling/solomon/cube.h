#pragma once

#include "private.h"
#include "tag_registry.h"

#include <limits>
#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/summary.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/monlib/metrics/metric_consumer.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

using TReadWindow = std::vector<std::pair<std::vector<int>, TInstant>>;

struct TReadOptions
{
    TReadWindow Times;

    std::function<bool(const TString&)> SensorFilter;

    bool ConvertCountersToRateGauge = false;
    bool RenameConvertedCounters = true;
    double RateDenominator = 1.0;
    bool EnableHistogramCompat = false;

    bool EnableSolomonAggregationWorkaround = false;

    // Direct summary export is not supported by solomon, yet.
    ESummaryPolicy SummaryPolicy = ESummaryPolicy::Default;

    bool MarkAggregates = false;

    std::optional<TString> Host;

    std::vector<TTag> InstanceTags;

    bool Sparse = false;
    bool Global = false;
    bool DisableSensorsRename = false;
    bool DisableDefault = false;

    int LingerWindowSize = 0;

    // Used only in ReadRecentSensorValue.
    bool ReadAllProjections = false;

    // Only makes sense with ExportSummaryAsMax and ReadAllProjections.
    bool SummaryAsMaxForAllTime = false;

    // Drop all prefix before last '/'.
    bool StripSensorsNamePrefix = false;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool IsZeroValue(const T& value)
{
    T zeroValue{};
    return value == zeroValue;
}

template <class T>
class TCube
{
public:
    TCube(int windowSize, i64 nextIteration);

    void Add(TTagIdList tagIds);
    void AddAll(const TTagIdList& tagIds, const TProjectionSet& projections);
    void Remove(TTagIdList tagIds);
    void RemoveAll(const TTagIdList& tagIds, const TProjectionSet& projections);

    void Update(TTagIdList tagIds, T value);
    void StartIteration();
    void FinishIteration();

    struct TProjection
    {
        T Rollup{};
        std::vector<T> Values;
        std::vector<bool> HasValue;

        bool IsZero(int index) const;
        bool IsLingering(i64 iteration) const;

        i64 LastNonZeroIteration = std::numeric_limits<i64>::min();
        int UsageCount = 0;
    };

    const THashMap<TTagIdList, TCube::TProjection>& GetProjections() const;
    int GetSize() const;

    int GetIndex(i64 iteration) const;
    i64 GetIteration(int index) const;
    T Rollup(const TProjection& window, int index) const;

    int ReadSensors(
        const TString& name,
        const TReadOptions& options,
        TTagWriter* tagWriter,
        ::NMonitoring::IMetricConsumer* consumer) const;

    int ReadSensorValues(
        const TTagIdList& tagIds,
        int index,
        const TReadOptions& options,
        const TTagRegistry& tagRegistry,
        NYTree::TFluentAny fluent) const;

    void DumpCube(NProto::TCube* cube, const std::vector<TTagId>& extraTags) const;

private:
    const int WindowSize_;

    i64 NextIteration_;
    i64 BaseIteration_;
    int Index_ = 0;

    THashMap<TTagIdList, TProjection> Projections_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
