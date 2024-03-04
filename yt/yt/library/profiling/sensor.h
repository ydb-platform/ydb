#pragma once

#include "public.h"
#include "tag.h"
#include "histogram_snapshot.h"

#include <library/cpp/yt/misc/preprocessor.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>
#include <library/cpp/yt/memory/weak_ptr.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/misc/enum.h>

#include <vector>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TCounter
{
public:
    //! Inc increments counter.
    /*!
     *  @delta MUST be >= 0.
     */
    void Increment(i64 delta = 1) const;

    explicit operator bool() const;

private:
    friend class TProfiler;
    friend struct TTesting;

    ICounterImplPtr Counter_;
};

////////////////////////////////////////////////////////////////////////////////

class TTimeCounter
{
public:
    void Add(TDuration delta) const;

    explicit operator bool() const;

private:
    friend class TProfiler;
    friend struct TTesting;

    ITimeCounterImplPtr Counter_;
};

////////////////////////////////////////////////////////////////////////////////

class TGauge
{
public:
    void Update(double value) const;

    explicit operator bool() const;

private:
    friend class TProfiler;
    friend struct TTesting;

    IGaugeImplPtr Gauge_;
};

////////////////////////////////////////////////////////////////////////////////

class TTimeGauge
{
public:
    void Update(TDuration value) const;

    explicit operator bool() const;

private:
    friend class TProfiler;
    friend struct TTesting;

    ITimeGaugeImplPtr Gauge_;
};

////////////////////////////////////////////////////////////////////////////////

class TSummary
{
public:
    void Record(double value) const;

    explicit operator bool() const;

private:
    friend class TProfiler;

    ISummaryImplPtr Summary_;
};

////////////////////////////////////////////////////////////////////////////////

class TEventTimer
{
public:
    void Record(TDuration value) const;

    explicit operator bool() const;

private:
    friend class TProfiler;

    ITimerImplPtr Timer_;
};

////////////////////////////////////////////////////////////////////////////////

class TEventTimerGuard
{
public:
    explicit TEventTimerGuard(TEventTimer timer);
    explicit TEventTimerGuard(TTimeGauge gauge);
    TEventTimerGuard(TEventTimerGuard&& other) = default;
    ~TEventTimerGuard();

    TDuration GetElapsedTime() const;

private:
    TEventTimer Timer_;
    TTimeGauge TimeGauge_;
    TCpuInstant StartTime_;
};

////////////////////////////////////////////////////////////////////////////////

class TGaugeHistogram
{
public:
    void Add(double value, int count = 1) const noexcept;
    void Remove(double value, int count = 1) const noexcept;
    void Reset() const noexcept;

    THistogramSnapshot GetSnapshot() const;
    void LoadSnapshot(THistogramSnapshot snapshot) const;

    explicit operator bool() const;

private:
    friend class TProfiler;

    IHistogramImplPtr Histogram_;
};

////////////////////////////////////////////////////////////////////////////////

class TRateHistogram
{
public:
    void Add(double value, int count = 1) const noexcept;

    explicit operator bool() const;

private:
    friend class TProfiler;
    friend struct TTesting;

    IHistogramImplPtr Histogram_;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(ESummaryPolicy, ui8,
    ((Default)             (0x0000))
    // Aggregation policy.
    ((All)                 (0x0001))
    ((Sum)                 (0x0002))
    ((Min)                 (0x0004))
    ((Max)                 (0x0008))
    ((Avg)                 (0x0010))
    // Export policy.
    ((OmitNameLabelSuffix) (0x0020))
);

struct TSummaryPolicyConflicts
{
    bool AllPolicyWithSpecifiedAggregates;
    bool OmitNameLabelSuffixWithSeveralAggregates;
};

TSummaryPolicyConflicts GetSummaryPolicyConflicts(ESummaryPolicy policy);

bool CheckSummaryPolicy(ESummaryPolicy policy);

////////////////////////////////////////////////////////////////////////////////

struct TSensorOptions
{
    bool Global = false;
    bool Sparse = false;
    bool Hot = false;
    bool DisableSensorsRename = false;
    bool DisableDefault = false;
    bool DisableProjections = false;
    bool ProducerRemoveSupport = false;

    TDuration HistogramMin;
    TDuration HistogramMax;

    std::vector<TDuration> TimeHistogramBounds;

    std::vector<double> HistogramBounds;

    ESummaryPolicy SummaryPolicy = ESummaryPolicy::Default;

    bool IsCompatibleWith(const TSensorOptions& other) const;
};

TString ToString(const TSensorOptions& options);

////////////////////////////////////////////////////////////////////////////////

//! TProfiler stores common settings of profiling counters.
class TProfiler
{
public:
    //! Default constructor creates null registry. Every method of null registry is no-op.
    /*!
     *  Default constructor is useful for implementing optional profiling. E.g:
     *
     *      TCache CreateCache(const TProfiler& profiler = {});
     *
     *      void Example()
     *      {
     *          auto cache = CreateCache(); // Create cache without profiling
     *          auto profiledCache = CreateCache(TProfiler{"/my_cache"}); // Enable profiling
     *      }
     */
    TProfiler() = default;

    static constexpr auto DefaultNamespace = "yt";

    TProfiler(
        const IRegistryImplPtr& impl,
        const TString& prefix,
        const TString& _namespace = DefaultNamespace);

    explicit TProfiler(
        const TString& prefix,
        const TString& _namespace = DefaultNamespace,
        const TTagSet& tags = {},
        const IRegistryImplPtr& impl = nullptr,
        TSensorOptions options = {});

    TProfiler WithPrefix(const TString& prefix) const;

    //! Tag settings control local aggregates.
    /*!
     *  See README.md for more details.
     *  #parent is negative number representing parent tag index.
     *  #alternativeTo is negative number representing alternative tag index.
     */
    TProfiler WithTag(const TString& name, const TString& value, int parent = NoParent) const;
    TProfiler WithRequiredTag(const TString& name, const TString& value, int parent = NoParent) const;
    TProfiler WithExcludedTag(const TString& name, const TString& value, int parent = NoParent) const;
    TProfiler WithAlternativeTag(const TString& name, const TString& value, int alternativeTo, int parent = NoParent) const;
    TProfiler WithExtensionTag(const TString& name, const TString& value, int parent = NoParent) const;
    TProfiler WithTags(const TTagSet& tags) const;

    //! Rename tag in all previously registered sensors.
    /*!
     *  NOTE: this is O(n) operation.
     */
    void RenameDynamicTag(const TDynamicTagPtr& tag, const TString& name, const TString& value) const;

    //! WithSparse sets sparse flags on all sensors created using returned registry.
    /*!
     *  Sparse sensors with zero value are omitted from profiling results.
     */
    TProfiler WithSparse() const;

    //! WithDense clears sparse flags on all sensors created using returned registry.
    TProfiler WithDense() const;

    //! WithGlobal marks all sensors as global.
    /*!
     *  Global sensors are exported without host= tag and instance tags.
     */
    TProfiler WithGlobal() const;

    //! WithDefaultDisabled disables export of default values.
    /*!
     *  By default, gauges report zero value after creation. With this setting enabled,
     *  gauges are not exported before first call to Update().
     */
    TProfiler WithDefaultDisabled() const;

    //! WithProjectionsDisabled disables local aggregation.
    TProfiler WithProjectionsDisabled() const;

    //! WithRenameDisabled disables sensors name normalization.
    TProfiler WithRenameDisabled() const;

    //! WithProducerRemoveSupport removes sensors that were absent on producer iteration.
    /*!
     *  By default, if sensor is absent on producer iteration, profiler keeps repeating
     *  previous sensor value.
     */
    TProfiler WithProducerRemoveSupport() const;

    //! WithHot sets hot flag on all sensors created using returned registry.
    /*!
     *  Hot sensors are implemented using per-cpu sharding, that increases
     *  performance under contention, but also increases memory consumption.
     *
     *  Default implementation:
     *    24 bytes - Counter, TimeCounter and Gauge
     *    64 bytes - Timer and Summary
     *
     *  Per-CPU implementation:
     *    4160 bytes - Counter, TimeCounter, Gauge, Timer, Summary
     */
    TProfiler WithHot(bool value = true) const;

    //! Counter is used to measure rate of events.
    TCounter Counter(const TString& name) const;

    //! Counter is used to measure CPU time consumption.
    TTimeCounter TimeCounter(const TString& name) const;

    //! Gauge is used to measure instant value.
    TGauge Gauge(const TString& name) const;

    //! TimeGauge is used to measure instant duration.
    TTimeGauge TimeGauge(const TString& name) const;

    //! Summary is used to measure distribution of values.
    TSummary Summary(const TString& name, ESummaryPolicy summaryPolicy = ESummaryPolicy::Default) const;

    //! GaugeSummary is used to aggregate multiple values locally.
    /*!
     *  Each TGauge tracks single value. Values are aggregated using Summary rules.
     */
    TGauge GaugeSummary(const TString& name, ESummaryPolicy summaryPolicy = ESummaryPolicy::Default) const;

    //! TimeGaugeSummary is used to aggregate multiple values locally.
    /*!
     *  Each TGauge tracks single value. Values are aggregated using Summary rules.
     */
    TTimeGauge TimeGaugeSummary(const TString& name, ESummaryPolicy summaryPolicy = ESummaryPolicy::Default) const;

    //! Timer is used to measure distribution of event durations.
    /*!
     *  Currently, max value during 5 second interval is exported to solomon.
     *  Use it, when you need a cheap way to monitor lag spikes.
     */
    TEventTimer Timer(const TString& name) const;

    //! TimeHistogram is used to measure distribution of event durations.
    /*!
     *  Bins are distributed _almost_ exponentially with step of 2; the only difference is that 64
     *  is followed by 125, 64'000 is followed by 125'000 and so on for the sake of better human-readability
     *  of upper limit.
     *
     *  The first several bin marks are:
     *  1, 2, 4, 8, 16, 32, 64, 125, 250, 500, 1000, 2000, 4000, 8000, 16'000, 32'000, 64'000, 125'000, ...
     *
     *  In terms of time this can be read as:
     *  1us, 2us, 4us, 8us, ..., 500us, 1ms, 2ms, ..., 500ms, 1s, ...
     */
    TEventTimer TimeHistogram(const TString& name, TDuration min, TDuration max) const;

    //! TimeHistogram is used to measure distribution of event durations.
    /*!
     *  Allows to use custom bounds, bounds should be sorted (maximum 51 elements are allowed).
     */
    TEventTimer TimeHistogram(const TString& name, std::vector<TDuration> bounds) const;

    //! GaugeHistogram is used to measure distribution of set of samples.
    TGaugeHistogram GaugeHistogram(const TString& name, std::vector<double> buckets) const;

    //! RateHistogram is used to measure distribution of set of samples.
    /*!
     *  Bucket values at the next point will be calculated as a derivative.
     */
    TRateHistogram RateHistogram(const TString& name, std::vector<double> buckets) const;

    void AddFuncCounter(
        const TString& name,
        const TRefCountedPtr& owner,
        std::function<i64()> reader) const;

    void AddFuncGauge(
        const TString& name,
        const TRefCountedPtr& owner,
        std::function<double()> reader) const;

    void AddProducer(
        const TString& prefix,
        const ISensorProducerPtr& producer) const;

    const IRegistryImplPtr& GetRegistry() const;

private:
    friend struct TTesting;

    bool Enabled_ = false;
    TString Prefix_;
    TString Namespace_;
    TTagSet Tags_;
    TSensorOptions Options_;
    IRegistryImplPtr Impl_;
};

using TRegistry = TProfiler;

////////////////////////////////////////////////////////////////////////////////

//! Measures execution time of the statement that immediately follows this macro.
#define YT_PROFILE_TIMING(name) \
    static auto PP_CONCAT(TimingProfiler__, __LINE__) = ::NYT::NProfiling::TProfiler(name).WithHot().Timer(""); \
    if (auto PP_CONCAT(timingProfilerGuard__, __LINE__) = ::NYT::NProfiling::TEventTimerGuard(PP_CONCAT(TimingProfiler__, __LINE__)); false) \
    { Y_UNREACHABLE(); } \
    else

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
