#pragma once

#include "impl.h"
#include "summary.h"

#include <library/cpp/yt/system/tscp.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

// The sharded sensors below index a fixed per-processor array by TTscp::Get() and update
// the shard with a plain atomic. They are the default hot implementation and the fallback
// everywhere the rseq fast path is not enabled; the rseq-backed counterparts live in
// yt/yt/library/profiling/rseq and are selected at construction time (see registry.cpp).

class TPerCpuCounter
    : public ICounter
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
    : public ITimeCounter
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

static_assert(sizeof(TPerCpuTimeCounter) == 64 + 64 * 64);

////////////////////////////////////////////////////////////////////////////////

class TPerCpuGauge
    : public IGauge
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
        std::atomic<__int128> Value = 0;
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

// One cache line larger than the counters: IGauge inherits TRefCounted virtually.
static_assert(sizeof(TPerCpuGauge) == 2 * 64 + 64 * 64);

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TPerCpuSummary
    : public ISummaryBase<T>
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
