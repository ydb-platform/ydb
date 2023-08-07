#pragma once

#include <yt/yt/library/profiling/impl.h>
#include <yt/yt/library/profiling/summary.h>

#include <yt/yt/core/profiling/tscp.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TPerCpuCounter
    : public ICounterImpl
{
public:
    void Increment(i64 delta) override;

    i64 GetValue() override;

private:
    struct alignas(CacheLineSize) TShard
    {
        std::atomic<i64> Value = 0;
    };

    std::array<TShard, TTscp::MaxProcessorId> Shards_;
};

static_assert(sizeof(TPerCpuCounter) == 64 + 64 * 64);

////////////////////////////////////////////////////////////////////////////////

class TPerCpuTimeCounter
    : public ITimeCounterImpl
{
public:
    void Add(TDuration delta) override;

    TDuration GetValue() override;

private:
    struct alignas(CacheLineSize) TShard
    {
        std::atomic<TDuration::TValue> Value = 0;
    };

    std::array<TShard, TTscp::MaxProcessorId> Shards_;
};

static_assert(sizeof(TPerCpuCounter) == 64 + 64 * 64);

////////////////////////////////////////////////////////////////////////////////

class TPerCpuGauge
    : public IGaugeImpl
{
public:
    void Update(double value) override;

    double GetValue() override;

private:
    struct TWrite
    {
        double Value;
        TCpuInstant Timestamp;

        __int128 Pack();
        static TWrite Unpack(__int128 i);
    };

    struct alignas(CacheLineSize) TShard
    {
#ifdef __clang__
        std::atomic<__int128> Value = {};
#else
        TSpinLock Lock;
        TWrite Value;
#endif
    };

#ifdef __clang__
    static_assert(std::atomic<TWrite>::is_always_lock_free);
#endif

    std::array<TShard, TTscp::MaxProcessorId> Shards_;
};

static_assert(sizeof(TPerCpuCounter) == 64 + 64 * 64);

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TPerCpuSummary
    : public ISummaryImplBase<T>
{
public:
    void Record(T value) override;

    TSummarySnapshot<T> GetSummary() override;
    TSummarySnapshot<T> GetSummaryAndReset() override;

private:
    struct alignas(CacheLineSize) TShard
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        TSummarySnapshot<T> Value;
    };

    std::array<TShard, TTscp::MaxProcessorId> Shards_;
};

DEFINE_REFCOUNTED_TYPE(TPerCpuSummary<double>)
DEFINE_REFCOUNTED_TYPE(TPerCpuSummary<TDuration>)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
