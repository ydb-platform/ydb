#pragma once

#include <yt/yt/library/profiling/impl.h>
#include <yt/yt/library/profiling/summary.h>
#include <yt/yt/library/profiling/histogram_snapshot.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TSimpleGauge
    : public IGaugeImpl
    , public ISummaryImpl
{
public:
    void Update(double value) override;

    double GetValue() override;

    void Record(double value) override;

    TSummarySnapshot<double> GetSummary() override;
    TSummarySnapshot<double> GetSummaryAndReset() override;

private:
    std::atomic<double> Value_ = 0.0;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleTimeGauge
    : public ITimeGaugeImpl
    , public ITimerImpl
{
public:
    void Update(TDuration value) override;

    TDuration GetValue() override;

    void Record(TDuration value) override;

    TSummarySnapshot<TDuration> GetSummary() override;
    TSummarySnapshot<TDuration> GetSummaryAndReset() override;

private:
    std::atomic<TDuration::TValue> Value_ = 0.0;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleCounter
    : public ICounterImpl
{
public:
    void Increment(i64 delta) override;

    i64 GetValue() override;

private:
    std::atomic<i64> Value_ = 0;
};

static_assert(sizeof(TSimpleCounter) == 24);

////////////////////////////////////////////////////////////////////////////////

class TSimpleTimeCounter
    : public ITimeCounterImpl
{
public:
    void Add(TDuration delta) override;

    TDuration GetValue() override;

private:
    std::atomic<TDuration::TValue> Value_{0};
};

static_assert(sizeof(TSimpleTimeCounter) == 24);

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TSimpleSummary
    : public ISummaryImplBase<T>
{
public:
    void Record(T value) override;

    TSummarySnapshot<T> GetSummary() override;
    TSummarySnapshot<T> GetSummaryAndReset() override;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TSummarySnapshot<T> Value_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THistogram)

std::vector<double> GenerateGenericBucketBounds();

class THistogram
    : public ISummaryImplBase<TDuration>
    , public IHistogramImpl
{
public:
    THistogram(const TSensorOptions& options);

    void Record(TDuration value) override;

    void Add(double value, int count) noexcept override;
    void Remove(double value, int count) noexcept override;
    void Reset() noexcept override;

    THistogramSnapshot GetSnapshot(bool reset) override;
    void LoadSnapshot(THistogramSnapshot snapshot) override;

private:
    std::vector<double> Bounds_;
    std::vector<std::atomic<int>> Buckets_;

    // These two methods are not used.
    TSummarySnapshot<TDuration> GetSummary() override;
    TSummarySnapshot<TDuration> GetSummaryAndReset() override;
};

DEFINE_REFCOUNTED_TYPE(THistogram)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
