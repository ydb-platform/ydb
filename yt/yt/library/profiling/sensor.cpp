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

void FormatValue(TStringBuilderBase* builder, const TSensorOptions& options, TStringBuf /*spec*/)
{
    Format(
        builder,
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

namespace NDetail {

template <bool UseWeakPtr>
TRegistryHolderBase<UseWeakPtr>::TRegistryHolderBase(const IRegistryPtr& impl)
    : Impl_(impl)
{ }

template <bool UseWeakPtr>
const IRegistryPtr& TRegistryHolderBase<UseWeakPtr>::GetRegistry() const
{
    return Impl_;
}

////////////////////////////////////////////////////////////////////////////////

TRegistryHolderBase<true>::TRegistryHolderBase(const IRegistryPtr& impl)
    : Impl_(impl)
{ }

IRegistryPtr TRegistryHolderBase<true>::GetRegistry() const
{
    return Impl_.Lock();
}

// NB(arkady-e1ppa): Explicit template instantiation somehow does not require
// base classes to be instantiated therefore we must do that by hand separately.
// template class TRegistryHolderBase<true>;
// ^However, full specialization is
// treated differently and is pointless and not required == banned by clang :).
template class TRegistryHolderBase<false>;

////////////////////////////////////////////////////////////////////////////////

template <bool UseWeakPtr>
TProfiler<UseWeakPtr>::TProfiler(
    const IRegistryPtr& impl,
    const std::string& prefix,
    const std::string& _namespace)
    : TBase(impl)
    , Enabled_(true)
    , Prefix_(prefix)
    , Namespace_(_namespace)
{ }

template <bool UseWeakPtr>
TProfiler<UseWeakPtr>::TProfiler(
    const std::string& prefix,
    const std::string& _namespace,
    const TTagSet& tags,
    const IRegistryPtr& impl,
    TSensorOptions options)
    : TBase(impl ? impl : GetGlobalRegistry())
    , Enabled_(true)
    , Prefix_(prefix)
    , Namespace_(_namespace)
    , Tags_(tags)
    , Options_(options)
{ }

template <bool UseWeakPtr>
bool TProfiler<UseWeakPtr>::IsEnabled() const
{
    return Enabled_;
}

template <bool UseWeakPtr>
const TTagSet& TProfiler<UseWeakPtr>::GetTags() const
{
    return Tags_;
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithPrefix(const std::string& prefix) const
{
    if (!Enabled_) {
        return {};
    }

    return TProfiler<UseWeakPtr>(Prefix_ + prefix, Namespace_, Tags_, GetRegistry(), Options_);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithTag(const std::string& name, const std::string& value, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.AddTag(std::pair(name, value), parent);
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, allTags, GetRegistry(), Options_);
}

template <bool UseWeakPtr>
void TProfiler<UseWeakPtr>::RenameDynamicTag(const TDynamicTagPtr& dynamicTag, const std::string& name, const std::string& value) const
{
    dynamicTag->Tag.Exchange(std::pair{name, value});

    if (const auto& impl = GetRegistry()) {
        impl->RenameDynamicTag(dynamicTag, name, value);
    }
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithRequiredTag(const std::string& name, const std::string& value, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.AddRequiredTag(std::pair(name, value), parent);
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, allTags, GetRegistry(), Options_);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithExcludedTag(const std::string& name, const std::string& value, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.AddExcludedTag(std::pair(name, value), parent);
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, allTags, GetRegistry(), Options_);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithAlternativeTag(const std::string& name, const std::string& value, int alternativeTo, int parent) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;

    allTags.AddAlternativeTag(std::pair(name, value), alternativeTo, parent);
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, allTags, GetRegistry(), Options_);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithExtensionTag(const std::string& name, const std::string& value, int extensionOf) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;

    allTags.AddExtensionTag(std::pair(name, value), extensionOf);
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, allTags, GetRegistry(), Options_);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithTags(const TTagSet& tags) const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.Append(tags);
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, allTags, GetRegistry(), Options_);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithSparse() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.Sparse = true;
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, Tags_, GetRegistry(), opts);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithDense() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.Sparse = false;
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, Tags_, GetRegistry(), opts);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithGlobal() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.Global = true;
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, Tags_, GetRegistry(), opts);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithDefaultDisabled() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.DisableDefault = true;
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, Tags_, GetRegistry(), opts);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithProjectionsDisabled() const
{
    if (!Enabled_) {
        return {};
    }

    auto allTags = Tags_;
    allTags.SetEnabled(false);

    auto opts = Options_;
    opts.DisableProjections = true;

    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, allTags, GetRegistry(), opts);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithRenameDisabled() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.DisableSensorsRename = true;
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, Tags_, GetRegistry(), opts);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithProducerRemoveSupport() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.ProducerRemoveSupport = true;
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, Tags_, GetRegistry(), opts);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithHot(bool value) const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.Hot = value;
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, Tags_, GetRegistry(), opts);
}

template <bool UseWeakPtr>
TProfiler<UseWeakPtr> TProfiler<UseWeakPtr>::WithMemOnly() const
{
    if (!Enabled_) {
        return {};
    }

    auto opts = Options_;
    opts.MemOnly = true;
    return TProfiler<UseWeakPtr>(Prefix_, Namespace_, Tags_, GetRegistry(), opts);
}

template <bool UseWeakPtr>
TCounter TProfiler<UseWeakPtr>::Counter(const std::string& name) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    TCounter counter;
    counter.Counter_ = impl->RegisterCounter(Namespace_ + Prefix_ + name, Tags_, Options_);
    return counter;
}

template <bool UseWeakPtr>
TTimeCounter TProfiler<UseWeakPtr>::TimeCounter(const std::string& name) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    TTimeCounter counter;
    counter.Counter_ = impl->RegisterTimeCounter(Namespace_ + Prefix_ + name, Tags_, Options_);
    return counter;
}

template <bool UseWeakPtr>
TGauge TProfiler<UseWeakPtr>::Gauge(const std::string& name) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    TGauge gauge;
    gauge.Gauge_ = impl->RegisterGauge(Namespace_ + Prefix_ + name, Tags_, Options_);
    return gauge;
}

template <bool UseWeakPtr>
TTimeGauge TProfiler<UseWeakPtr>::TimeGauge(const std::string& name) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    TTimeGauge gauge;
    gauge.Gauge_ = impl->RegisterTimeGauge(Namespace_ + Prefix_ + name, Tags_, Options_);
    return gauge;
}

template <bool UseWeakPtr>
TSummary TProfiler<UseWeakPtr>::Summary(const std::string& name, ESummaryPolicy summaryPolicy) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    auto options = Options_;
    options.SummaryPolicy = summaryPolicy;

    TSummary summary;
    summary.Summary_ = impl->RegisterSummary(Namespace_ + Prefix_ + name, Tags_, std::move(options));
    return summary;
}

template <bool UseWeakPtr>
TGauge TProfiler<UseWeakPtr>::GaugeSummary(const std::string& name, ESummaryPolicy summaryPolicy) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    auto options = Options_;
    options.SummaryPolicy = summaryPolicy;

    TGauge gauge;
    gauge.Gauge_ = impl->RegisterGaugeSummary(Namespace_ + Prefix_ + name, Tags_, std::move(options));
    return gauge;
}

template <bool UseWeakPtr>
TTimeGauge TProfiler<UseWeakPtr>::TimeGaugeSummary(const std::string& name, ESummaryPolicy summaryPolicy) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    auto options = Options_;
    options.SummaryPolicy = summaryPolicy;

    TTimeGauge gauge;
    gauge.Gauge_ = impl->RegisterTimeGaugeSummary(Namespace_ + Prefix_ + name, Tags_, std::move(options));
    return gauge;
}

template <bool UseWeakPtr>
TEventTimer TProfiler<UseWeakPtr>::Timer(const std::string& name) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    TEventTimer timer;
    timer.Timer_ = impl->RegisterTimerSummary(Namespace_ + Prefix_ + name, Tags_, Options_);
    return timer;
}

template <bool UseWeakPtr>
TEventTimer TProfiler<UseWeakPtr>::TimeHistogram(const std::string& name, TDuration min, TDuration max) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    auto options = Options_;
    options.HistogramMin = min;
    options.HistogramMax = max;

    TEventTimer timer;
    timer.Timer_ = impl->RegisterTimeHistogram(Namespace_ + Prefix_ + name, Tags_, options);
    return timer;
}

template <bool UseWeakPtr>
TEventTimer TProfiler<UseWeakPtr>::TimeHistogram(const std::string& name, std::vector<TDuration> bounds) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    TEventTimer timer;
    auto options = Options_;
    options.TimeHistogramBounds = std::move(bounds);
    timer.Timer_ = impl->RegisterTimeHistogram(Namespace_ + Prefix_ + name, Tags_, options);
    return timer;
}

template <bool UseWeakPtr>
TGaugeHistogram TProfiler<UseWeakPtr>::GaugeHistogram(const std::string& name, std::vector<double> buckets) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    TGaugeHistogram histogram;
    auto options = Options_;
    options.HistogramBounds = std::move(buckets);
    histogram.Histogram_ = impl->RegisterGaugeHistogram(Namespace_ + Prefix_ + name, Tags_, options);
    return histogram;
}

template <bool UseWeakPtr>
TRateHistogram TProfiler<UseWeakPtr>::RateHistogram(const std::string& name, std::vector<double> buckets) const
{
    const auto& impl = GetRegistry();
    if (!impl) {
        return {};
    }

    TRateHistogram histogram;
    auto options = Options_;
    options.HistogramBounds = std::move(buckets);
    histogram.Histogram_ = impl->RegisterRateHistogram(Namespace_ + Prefix_ + name, Tags_, options);
    return histogram;
}

template <bool UseWeakPtr>
void TProfiler<UseWeakPtr>::AddFuncCounter(
    const std::string& name,
    const TRefCountedPtr& owner,
    std::function<i64()> reader) const
{
    if (const auto& impl = GetRegistry()) {
        impl->RegisterFuncCounter(Namespace_ + Prefix_ + name, Tags_, Options_, owner, reader);
    }
}

template <bool UseWeakPtr>
void TProfiler<UseWeakPtr>::AddFuncGauge(
    const std::string& name,
    const TRefCountedPtr& owner,
    std::function<double()> reader) const
{
    if (const auto& impl = GetRegistry()) {
        impl->RegisterFuncGauge(Namespace_ + Prefix_ + name, Tags_, Options_, owner, reader);
    }
}

template <bool UseWeakPtr>
void TProfiler<UseWeakPtr>::AddProducer(
    const std::string& prefix,
    const ISensorProducerPtr& producer) const
{
    if (const auto& impl = GetRegistry()) {
        impl->RegisterProducer(Namespace_ + Prefix_ + prefix, Tags_, Options_, producer);
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template class NDetail::TProfiler<true>;
template class NDetail::TProfiler<false>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
