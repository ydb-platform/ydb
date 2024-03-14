#include "sensor.h"
#include "impl.h"

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/string/format.h>

#include <util/system/compiler.h>

#include <atomic>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

TSummaryPolicyConflicts GetSummaryPolicyConflicts(ESummaryPolicy policy)
{
    bool isAllPolicy = Any(policy & ESummaryPolicy::All);
    int specifiedAggregateCount = static_cast<int>(Any(policy & ESummaryPolicy::Sum)) +
        static_cast<int>(Any(policy & ESummaryPolicy::Min)) +
        static_cast<int>(Any(policy & ESummaryPolicy::Max)) +
        static_cast<int>(Any(policy & ESummaryPolicy::Avg));

    return {
        .AllPolicyWithSpecifiedAggregates = isAllPolicy && specifiedAggregateCount > 0,
        .OmitNameLabelSuffixWithSeveralAggregates = (isAllPolicy || specifiedAggregateCount != 1) &&
            Any(policy & ESummaryPolicy::OmitNameLabelSuffix),
    };
}

bool CheckSummaryPolicy(ESummaryPolicy policy)
{
    auto conflicts = GetSummaryPolicyConflicts(policy);
    return !conflicts.AllPolicyWithSpecifiedAggregates && !conflicts.OmitNameLabelSuffixWithSeveralAggregates;
}

////////////////////////////////////////////////////////////////////////////////

void TCounter::Increment(i64 delta) const
{
    if (!Counter_) {
        return;
    }

    Counter_->Increment(delta);
}

TCounter::operator bool() const
{
    return Counter_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

void TTimeCounter::Add(TDuration delta) const
{
    if (!Counter_) {
        return;
    }

    Counter_->Add(delta);
}

TTimeCounter::operator bool() const
{
    return Counter_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

void TGauge::Update(double value) const
{
    if (!Gauge_) {
        return;
    }

    Gauge_->Update(value);
}

TGauge::operator bool() const
{
    return Gauge_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

void TTimeGauge::Update(TDuration value) const
{
    if (!Gauge_) {
        return;
    }

    Gauge_->Update(value);
}

TTimeGauge::operator bool() const
{
    return Gauge_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

void TSummary::Record(double value) const
{
    if (!Summary_) {
        return;
    }

    Summary_->Record(value);
}

TSummary::operator bool() const
{
    return Summary_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

void TEventTimer::Record(TDuration value) const
{
    if (!Timer_) {
        return;
    }

    Timer_->Record(value);
}

TEventTimer::operator bool() const
{
    return Timer_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

TEventTimerGuard::TEventTimerGuard(TEventTimer timer)
    : Timer_(std::move(timer))
    , StartTime_(GetCpuInstant())
{ }

TEventTimerGuard::TEventTimerGuard(TTimeGauge timeGauge)
    : TimeGauge_(std::move(timeGauge))
    , StartTime_(GetCpuInstant())
{ }

TEventTimerGuard::~TEventTimerGuard()
{
    if (!Timer_ && !TimeGauge_) {
        return;
    }

    auto duration = GetElapsedTime();
    if (Timer_) {
        Timer_.Record(duration);
    }
    if (TimeGauge_) {
        TimeGauge_.Update(duration);
    }
}

TDuration TEventTimerGuard::GetElapsedTime() const
{
    return CpuDurationToDuration(GetCpuInstant() - StartTime_);
}

////////////////////////////////////////////////////////////////////////////////

void TGaugeHistogram::Add(double value, int count) const noexcept
{
    if (!Histogram_) {
        return;
    }

    Histogram_->Add(value, count);
}

void TGaugeHistogram::Remove(double value, int count) const noexcept
{
    if (!Histogram_) {
        return;
    }

    Histogram_->Remove(value, count);
}

void TGaugeHistogram::Reset() const noexcept
{
    if (!Histogram_) {
        return;
    }

    Histogram_->Reset();
}

THistogramSnapshot TGaugeHistogram::GetSnapshot() const
{
    if (!Histogram_) {
        return {};
    }

    return Histogram_->GetSnapshot(false);
}

void TGaugeHistogram::LoadSnapshot(THistogramSnapshot snapshot) const
{
    if (!Histogram_) {
        return;
    }

    Histogram_->LoadSnapshot(snapshot);
}

TGaugeHistogram::operator bool() const
{
    return Histogram_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

void TRateHistogram::Add(double value, int count) const noexcept
{
    if (!Histogram_) {
        return;
    }

    Histogram_->Add(value, count);
}

TRateHistogram::operator bool() const
{
    return Histogram_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TSensorOptions& options)
{
    return Format(
        "{sparse=%v;global=%v;hot=%v;histogram_min=%v;histogram_max=%v;time_histogram_bounds=%v;histogram_bounds=%v;summary_policy=%v}",
        options.Sparse,
        options.Global,
        options.Hot,
        options.HistogramMin,
        options.HistogramMax,
        options.TimeHistogramBounds,
        options.HistogramBounds,
        options.SummaryPolicy);
}

bool TSensorOptions::IsCompatibleWith(const TSensorOptions& other) const
{
    return Sparse == other.Sparse &&
        Global == other.Global &&
        DisableSensorsRename == other.DisableSensorsRename &&
        DisableDefault == other.DisableDefault &&
        DisableProjections == other.DisableProjections &&
        SummaryPolicy == other.SummaryPolicy;
}

////////////////////////////////////////////////////////////////////////////////

TProfiler::TProfiler(
    const IRegistryImplPtr& impl,
    const TString& prefix,
    const TString& _namespace)
    : Enabled_(true)
    , Prefix_(prefix)
    , Namespace_(_namespace)
    , Impl_(impl)
{ }

TProfiler::TProfiler(
    const TString& prefix,
    const TString& _namespace,
    const TTagSet& tags,
    const IRegistryImplPtr& impl,
    TSensorOptions options)
    : Enabled_(true)
    , Prefix_(prefix)
    , Namespace_(_namespace)
    , Tags_(tags)
    , Options_(options)
    , Impl_(impl ? impl : GetGlobalRegistry())
{ }

TProfiler TProfiler::WithPrefix(const TString& prefix) const
{
    if (!Enabled_) {
        return {};
    }

    return TProfiler(Prefix_ + prefix, Namespace_, Tags_, Impl_, Options_);
}

TProfiler TProfiler::WithTag(const TString& name, const TString& value, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.AddTag(std::pair(name, value), parent);
    return TProfiler(Prefix_, Namespace_, allTags, Impl_, Options_);
}

void TProfiler::RenameDynamicTag(const TDynamicTagPtr& tag, const TString& name, const TString& value) const
{
    if (!Impl_) {
        return;
    }

    Impl_->RenameDynamicTag(tag, name, value);
}

TProfiler TProfiler::WithRequiredTag(const TString& name, const TString& value, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.AddRequiredTag(std::pair(name, value), parent);
    return TProfiler(Prefix_, Namespace_, allTags, Impl_, Options_);
}

TProfiler TProfiler::WithExcludedTag(const TString& name, const TString& value, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.AddExcludedTag(std::pair(name, value), parent);
    return TProfiler(Prefix_, Namespace_, allTags, Impl_, Options_);
}

TProfiler TProfiler::WithAlternativeTag(const TString& name, const TString& value, int alternativeTo, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;

    allTags.AddAlternativeTag(std::pair(name, value), alternativeTo, parent);
    return TProfiler(Prefix_, Namespace_, allTags, Impl_, Options_);
}

TProfiler TProfiler::WithExtensionTag(const TString& name, const TString& value, int extensionOf) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;

    allTags.AddExtensionTag(std::pair(name, value), extensionOf);
    return TProfiler(Prefix_, Namespace_, allTags, Impl_, Options_);
}

TProfiler TProfiler::WithTags(const TTagSet& tags) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.Append(tags);
    return TProfiler(Prefix_, Namespace_, allTags, Impl_, Options_);
}

TProfiler TProfiler::WithSparse() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.Sparse = true;
    return TProfiler(Prefix_, Namespace_, Tags_, Impl_, opts);
}

TProfiler TProfiler::WithDense() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.Sparse = false;
    return TProfiler(Prefix_, Namespace_, Tags_, Impl_, opts);
}

TProfiler TProfiler::WithGlobal() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.Global = true;
    return TProfiler(Prefix_, Namespace_, Tags_, Impl_, opts);
}

TProfiler TProfiler::WithDefaultDisabled() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.DisableDefault = true;
    return TProfiler(Prefix_, Namespace_, Tags_, Impl_, opts);
}

TProfiler TProfiler::WithProjectionsDisabled() const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.SetEnabled(false);

    auto opts = Options_;
    opts.DisableProjections = true;

    return TProfiler(Prefix_, Namespace_, allTags, Impl_, opts);
}

TProfiler TProfiler::WithRenameDisabled() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.DisableSensorsRename = true;
    return TProfiler(Prefix_, Namespace_, Tags_, Impl_, opts);
}

TProfiler TProfiler::WithProducerRemoveSupport() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.ProducerRemoveSupport = true;
    return TProfiler(Prefix_, Namespace_, Tags_, Impl_, opts);
}

TProfiler TProfiler::WithHot(bool value) const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.Hot = value;
    return TProfiler(Prefix_, Namespace_, Tags_, Impl_, opts);
}

TCounter TProfiler::Counter(const TString& name) const
{
    if (!Impl_) {
        return {};
    }

    TCounter counter;
    counter.Counter_ = Impl_->RegisterCounter(Namespace_ + Prefix_ + name, Tags_, Options_);
    return counter;
}

TTimeCounter TProfiler::TimeCounter(const TString& name) const
{
    if (!Impl_) {
        return {};
    }

    TTimeCounter counter;
    counter.Counter_ = Impl_->RegisterTimeCounter(Namespace_ + Prefix_ + name, Tags_, Options_);
    return counter;
}

TGauge TProfiler::Gauge(const TString& name) const
{
    if (!Impl_) {
        return TGauge();
    }

    TGauge gauge;
    gauge.Gauge_ = Impl_->RegisterGauge(Namespace_ + Prefix_ + name, Tags_, Options_);
    return gauge;
}

TTimeGauge TProfiler::TimeGauge(const TString& name) const
{
    if (!Impl_) {
        return TTimeGauge();
    }

    TTimeGauge gauge;
    gauge.Gauge_ = Impl_->RegisterTimeGauge(Namespace_ + Prefix_ + name, Tags_, Options_);
    return gauge;
}

TSummary TProfiler::Summary(const TString& name, ESummaryPolicy summaryPolicy) const
{
    if (!Impl_) {
        return {};
    }

    auto options = Options_;
    options.SummaryPolicy = summaryPolicy;

    TSummary summary;
    summary.Summary_ = Impl_->RegisterSummary(Namespace_ + Prefix_ + name, Tags_, std::move(options));
    return summary;
}

TGauge TProfiler::GaugeSummary(const TString& name, ESummaryPolicy summaryPolicy) const
{
    if (!Impl_) {
        return {};
    }

    auto options = Options_;
    options.SummaryPolicy = summaryPolicy;

    TGauge gauge;
    gauge.Gauge_ = Impl_->RegisterGaugeSummary(Namespace_ + Prefix_ + name, Tags_, std::move(options));
    return gauge;
}

TTimeGauge TProfiler::TimeGaugeSummary(const TString& name, ESummaryPolicy summaryPolicy) const
{
    if (!Impl_) {
        return {};
    }

    auto options = Options_;
    options.SummaryPolicy = summaryPolicy;

    TTimeGauge gauge;
    gauge.Gauge_ = Impl_->RegisterTimeGaugeSummary(Namespace_ + Prefix_ + name, Tags_, std::move(options));
    return gauge;
}

TEventTimer TProfiler::Timer(const TString& name) const
{
    if (!Impl_) {
        return {};
    }

    TEventTimer timer;
    timer.Timer_ = Impl_->RegisterTimerSummary(Namespace_ + Prefix_ + name, Tags_, Options_);
    return timer;
}

TEventTimer TProfiler::TimeHistogram(const TString& name, TDuration min, TDuration max) const
{
    if (!Impl_) {
        return {};
    }

    auto options = Options_;
    options.HistogramMin = min;
    options.HistogramMax = max;

    TEventTimer timer;
    timer.Timer_ = Impl_->RegisterTimeHistogram(Namespace_ + Prefix_ + name, Tags_, options);
    return timer;
}

TEventTimer TProfiler::TimeHistogram(const TString& name, std::vector<TDuration> bounds) const
{
    if (!Impl_) {
        return {};
    }

    TEventTimer timer;
    auto options = Options_;
    options.TimeHistogramBounds = std::move(bounds);
    timer.Timer_ = Impl_->RegisterTimeHistogram(Namespace_ + Prefix_ + name, Tags_, options);
    return timer;
}

TGaugeHistogram TProfiler::GaugeHistogram(const TString& name, std::vector<double> buckets) const
{
    if (!Impl_) {
        return {};
    }

    TGaugeHistogram histogram;
    auto options = Options_;
    options.HistogramBounds = std::move(buckets);
    histogram.Histogram_ = Impl_->RegisterGaugeHistogram(Namespace_ + Prefix_ + name, Tags_, options);
    return histogram;
}

TRateHistogram TProfiler::RateHistogram(const TString& name, std::vector<double> buckets) const
{
    if (!Impl_) {
        return {};
    }

    TRateHistogram histogram;
    auto options = Options_;
    options.HistogramBounds = std::move(buckets);
    histogram.Histogram_ = Impl_->RegisterRateHistogram(Namespace_ + Prefix_ + name, Tags_, options);
    return histogram;
}

void TProfiler::AddFuncCounter(
    const TString& name,
    const TRefCountedPtr& owner,
    std::function<i64()> reader) const
{
    if (!Impl_) {
        return;
    }

    Impl_->RegisterFuncCounter(Namespace_ + Prefix_ + name, Tags_, Options_, owner, reader);
}

void TProfiler::AddFuncGauge(
    const TString& name,
    const TRefCountedPtr& owner,
    std::function<double()> reader) const
{
    if (!Impl_) {
        return;
    }

    Impl_->RegisterFuncGauge(Namespace_ + Prefix_ + name, Tags_, Options_, owner, reader);
}

void TProfiler::AddProducer(
    const TString& prefix,
    const ISensorProducerPtr& producer) const
{
    if (!Impl_) {
        return;
    }

    Impl_->RegisterProducer(Namespace_ + Prefix_ + prefix, Tags_, Options_, producer);
}

const IRegistryImplPtr& TProfiler::GetRegistry() const
{
    return Impl_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
