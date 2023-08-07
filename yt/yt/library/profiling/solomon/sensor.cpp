#include "sensor.h"

#include <library/cpp/yt/assert/assert.h>

#include <atomic>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TSimpleCounter)
DEFINE_REFCOUNTED_TYPE(TSimpleGauge)

////////////////////////////////////////////////////////////////////////////////

void TSimpleGauge::Update(double value)
{
    Value_.store(value, std::memory_order::relaxed);
}

double TSimpleGauge::GetValue()
{
    return Value_.load(std::memory_order::relaxed);
}

void TSimpleGauge::Record(double /*value*/)
{
    YT_UNIMPLEMENTED();
}

TSummarySnapshot<double> TSimpleGauge::GetSummary()
{
    TSummarySnapshot<double> summary;
    summary.Record(GetValue());
    return summary;
}

TSummarySnapshot<double> TSimpleGauge::GetSummaryAndReset()
{
    return GetSummary();
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleTimeGauge::Update(TDuration value)
{
    Value_.store(value.GetValue(), std::memory_order::relaxed);
}

TDuration TSimpleTimeGauge::GetValue()
{
    return TDuration::FromValue(Value_.load(std::memory_order::relaxed));
}

void TSimpleTimeGauge::Record(TDuration /*value*/)
{
    YT_UNIMPLEMENTED();
}

TSummarySnapshot<TDuration> TSimpleTimeGauge::GetSummary()
{
    TSummarySnapshot<TDuration> summary;
    summary.Record(GetValue());
    return summary;
}

TSummarySnapshot<TDuration> TSimpleTimeGauge::GetSummaryAndReset()
{
    return GetSummary();
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleCounter::Increment(i64 delta)
{
    YT_VERIFY(delta >= 0);
    Value_.fetch_add(delta, std::memory_order::relaxed);
}

i64 TSimpleCounter::GetValue()
{
    return Value_.load(std::memory_order::relaxed);
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleTimeCounter::Add(TDuration delta)
{
    Value_.fetch_add(delta.GetValue(), std::memory_order::relaxed);
}

TDuration TSimpleTimeCounter::GetValue()
{
    return TDuration::FromValue(Value_.load(std::memory_order::relaxed));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TSimpleSummary<T>::Record(T value)
{
    auto guard = Guard(Lock_);
    Value_.Record(value);
}

template <class T>
TSummarySnapshot<T> TSimpleSummary<T>::GetSummary()
{
    auto guard = Guard(Lock_);
    return Value_;
}

template <class T>
TSummarySnapshot<T> TSimpleSummary<T>::GetSummaryAndReset()
{
    auto guard = Guard(Lock_);

    auto value = Value_;
    Value_ = {};
    return value;
}

template class TSimpleSummary<double>;
template class TSimpleSummary<TDuration>;

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxBinCount = 65;

static auto GenericBucketBounds()
{
    std::array<ui64, MaxBinCount> result;

    for (int index = 0; index <= 6; ++index) {
        result[index] = 1ull << index;
    }

    for (int index = 7; index < 10; ++index) {
        result[index] = 1000ull >> (10 - index);
    }

    for (int index = 10; index < MaxBinCount; ++index) {
        result[index] = 1000 * result[index - 10];
    }

    return result;
}

std::vector<double> GenerateGenericBucketBounds()
{
    // BEWARE: Changing this variable will lead to master snapshots becoming invalid.
    constexpr int MaxHistogramBinCount = 38;
    std::vector<double> result;

    auto genericBounds = GenericBucketBounds();
    result.reserve(MaxHistogramBinCount);

    for (int i = 0; i < std::ssize(genericBounds) && i < MaxHistogramBinCount; ++i) {
        result.push_back(genericBounds[i]);
    }

    return result;
}

static std::vector<double> BucketBounds(const TSensorOptions& options)
{
    if (!options.HistogramBounds.empty()) {
        return options.HistogramBounds;
    }

    std::vector<double> bounds;
    if (!options.TimeHistogramBounds.empty()) {
        for (auto b : options.TimeHistogramBounds) {
            bounds.push_back(b.SecondsFloat());
        }
        return bounds;
    }

    if (options.HistogramMin.Zero() && options.HistogramMax.Zero()) {
        return {};
    }

    for (auto bound : GenericBucketBounds()) {
        auto duration = TDuration::FromValue(bound);
        if (options.HistogramMin <= duration && duration <= options.HistogramMax) {
            bounds.push_back(duration.SecondsFloat());
        }
    }

    return bounds;
}

THistogram::THistogram(const TSensorOptions& options)
    : Bounds_(BucketBounds(options))
    , Buckets_(Bounds_.size() + 1)
{
    YT_VERIFY(!Bounds_.empty());
    YT_VERIFY(Bounds_.size() <= MaxBinCount);
}

void THistogram::Record(TDuration value)
{
    auto it = std::lower_bound(Bounds_.begin(), Bounds_.end(), value.SecondsFloat());
    Buckets_[it - Bounds_.begin()].fetch_add(1, std::memory_order::relaxed);
}

void THistogram::Add(double value, int count) noexcept
{
    auto it = std::lower_bound(Bounds_.begin(), Bounds_.end(), value);
    Buckets_[it - Bounds_.begin()].fetch_add(count, std::memory_order::relaxed);
}

void THistogram::Remove(double value, int count) noexcept
{
    auto it = std::lower_bound(Bounds_.begin(), Bounds_.end(), value);
    Buckets_[it - Bounds_.begin()].fetch_sub(count, std::memory_order::relaxed);
}

void THistogram::Reset() noexcept
{
    for (int i = 0; i < std::ssize(Buckets_); ++i) {
        Buckets_[i] = 0;
    }
}

THistogramSnapshot THistogram::GetSnapshot(bool reset)
{
    THistogramSnapshot snapshot;
    snapshot.Bounds = Bounds_;
    snapshot.Values.resize(Buckets_.size());

    for (int i = 0; i < std::ssize(Buckets_); ++i) {
        if (!reset) {
            snapshot.Values[i] = Buckets_[i].load(std::memory_order::relaxed);
        } else {
            snapshot.Values[i] = Buckets_[i].exchange(0, std::memory_order::relaxed);
        }
    }

    return snapshot;
}

void THistogram::LoadSnapshot(THistogramSnapshot snapshot)
{
    for (int i = 0; i < std::ssize(snapshot.Bounds); ++i) {
        YT_VERIFY(Bounds_[i] == snapshot.Bounds[i]);
    }

    YT_VERIFY(std::ssize(Buckets_) == std::ssize(Bounds_) + 1);

    for (int i = 0; i < std::ssize(snapshot.Values); ++i) {
        Buckets_[i].store(snapshot.Values[i], std::memory_order::relaxed);
    }
}

TSummarySnapshot<TDuration> THistogram::GetSummary()
{
    YT_UNIMPLEMENTED();
}

TSummarySnapshot<TDuration> THistogram::GetSummaryAndReset()
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
