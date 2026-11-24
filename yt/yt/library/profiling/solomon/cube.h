#pragma once

#include "private.h"
#include "tag_registry.h"

#include <limits>
#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/summary.h>

#include <yt/yt/core/misc/bitmap.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/monlib/metrics/metric_consumer.h>

#include <library/cpp/containers/absl/flat_hash_set.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

using TReadWindow = std::vector<std::pair<std::vector<int>, TInstant>>;

struct TReadOptions
{
    TReadWindow Times;

    std::function<bool(TStringBuf)> SensorFilter;

    bool ConvertCountersToRateGauge = false;
    bool ConvertCountersToDeltaGauge = false;
    bool RenameConvertedCounters = true;
    double RateDenominator = 1.0;
    bool EnableHistogramCompat = false;
    bool SplitRateHistogramIntoGauges = false;
    bool ReportTimestampsForRateMetrics = true;

    bool EnableSolomonAggregationWorkaround = false;

    // Direct summary export is not supported by solomon, yet.
    ESummaryPolicy SummaryPolicy = ESummaryPolicy::Default;

    bool MarkAggregates = false;
    bool EnableSolomonAggregates = false;
    bool ExportGlobalsAsMemOnly = false;

    std::optional<std::string> Host;

    std::vector<TTag> InstanceTags;

    bool Sparse = false;
    bool Global = false;
    bool DisableSensorsRename = false;
    bool DisableDefault = false;
    bool MemOnly = false;

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
bool IsZeroValue(const TSummarySnapshot<T>& value)
{
    T zeroValue{};
    return value.Min() == zeroValue && value.Max() == zeroValue;
}

template <class T>
class TCube
    : private TNonCopyable
{
public:
    TCube(int windowSize, i64 nextIteration);

    //! Must be created via #NewWithExtraSpace with #GetExtraSpaceSize(windowSize) bytes of extra space.
    class alignas(T) TProjection
        : public TRefCounted
        , public TWithExtraSpace<TProjection>
    {
    public:
        TProjection(TTagIdList tagIds, int windowSize);
        ~TProjection();

        static size_t GetExtraSpaceSize(int windowSize);

        const TTagIdList TagIds;
        const int WindowSize;

        T Rollup{};
        TMutableRange<T> Values();
        TRange<T> Values() const;
        TMutableBitmap HasValue();
        TBitmap HasValue() const;

        bool IsZero(int index) const;
        bool IsLingering(i64 iteration) const;

        i64 LastNonZeroIteration = std::numeric_limits<i64>::min();
        int UsageCount = 0;
    };

    using TProjectionPtr = TIntrusivePtr<TProjection>;

    //! Opaque handle returned by #AddAll(), used by #UpdateAll() and consumed by #RemoveAll().
    class TProjectionCookie
        : private TMoveOnly
    {
    private:
        friend class TCube;

        ui64 OwnerCubeId_ = 0;
        std::vector<TProjectionPtr> Projections_;
    };

    void Add(TTagIdList tagIds);
    TProjectionCookie AddAll(const TTagIdSet& tagSet);
    void Remove(TTagIdList tagIds);
    void RemoveAll(TProjectionCookie cookie);

    void Update(TTagIdList tagIds, T value);
    void UpdateAll(const TProjectionCookie& cookie, const T& value);
    void StartIteration();
    void FinishIteration();

    struct TProjectionTagIdsHash
    {
        using is_transparent = void;

        template <class TValue>
        size_t operator()(const TValue& value) const;
    };

    struct TProjectionTagIdsEqual
    {
        using is_transparent = void;

        template <class TLhs, class TRhs>
        bool operator()(const TLhs& lhs, const TRhs& rhs) const;
    };

    using TProjectionHashSet = absl::flat_hash_set<TProjectionPtr, TProjectionTagIdsHash, TProjectionTagIdsEqual>;

    const TProjectionHashSet& GetProjections() const;
    int GetSize() const;

    int GetIndex(i64 iteration) const;
    i64 GetIteration(int index) const;
    T Rollup(const TProjection& window, int index) const;

    int ReadSensors(
        TStringBuf name,
        const TReadOptions& options,
        TTagWriter* tagWriter,
        ::NMonitoring::IMetricConsumer* consumer) const;

    int ReadSensorValues(
        const TTagIdList& tagIds,
        int index,
        const TReadOptions& options,
        const TTagRegistry& tagRegistry,
        NYTree::TFluentAny fluent) const;

    // Each projection from `extraProjections` added to each inner projection of this cube.
    void DumpCube(NProto::TCube* cube, const std::vector<TTagIdList>& extraProjections) const;
    void DumpCube(NProto::TCube* cube, const TTagIdList& extraTagIds = {}) const;

private:
    const int WindowSize_;
    const ui64 CubeId_;

    i64 NextIteration_;
    i64 BaseIteration_;
    int Index_ = 0;

    TProjectionHashSet Projections_;

    static const TTagIdList& GetTagIds(const TTagIdList& tagIds);
    static const TTagIdList& GetTagIds(const TProjectionPtr& projection);

    TProjectionPtr DoAdd(TTagIdList tagIds);
    void DoUpdate(TProjection* projection, const T& value);
};

////////////////////////////////////////////////////////////////////////////////

using TGaugeCube = TCube<double>;
using TCounterCube = TCube<i64>;
using TTimeCounterCube = TCube<TDuration>;
using TSummaryCube = TCube<TSummarySnapshot<double>>;
using TTimerCube = TCube<TSummarySnapshot<TDuration>>;
using TTimeHistogramCube = TCube<TTimeHistogramSnapshot>;
using TGaugeHistogramCube = TCube<TGaugeHistogramSnapshot>;
using TRateHistogramCube = TCube<TRateHistogramSnapshot>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

#define CUBE_INL_H_
#include "cube-inl.h"
#undef CUBE_INL_H_
