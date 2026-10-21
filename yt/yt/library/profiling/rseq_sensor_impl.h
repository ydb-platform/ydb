#pragma once

#include "impl.h"

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/memory/public.h>
#include <library/cpp/yt/memory/range.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

// rseq-backed hot sensors. Each update commits to the calling CPU's shard lock-free via an
// rseq critical section (see library/cpp/yt/rseq) -- no atomic and no lock on the fast
// path. The per-CPU shard array (one slot per NRseq::GetCpuCount()) is allocated inline
// with the object via NewWithExtraSpace, so a sensor costs a single allocation; the object
// is cache-line aligned so the trailing shards are too. rseq is Linux-only, so this header
// is included (and rseq_sensor_impl.cpp is built) only on Linux; the sensors are
// constructed (via Create) only when the rseq fast path is enabled (see
// TSolomonRegistry::IsRseqEnabled), with the atomic sharded counterparts in
// per_cpu_sensor_impl.h used otherwise.

////////////////////////////////////////////////////////////////////////////////

struct alignas(CacheLineSize) TRseqCounterShard
{
    i64 Value = 0;
};

static_assert(sizeof(TRseqCounterShard) == CacheLineSize);

struct alignas(CacheLineSize) TRseqTimeCounterShard
{
    TDuration::TValue Value = 0;
};

static_assert(sizeof(TRseqTimeCounterShard) == CacheLineSize);

struct TRseqGaugeWrite
{
    double Value = 0;
    TCpuInstant Timestamp = 0;
};

static_assert(sizeof(TRseqGaugeWrite) == 16);

struct alignas(CacheLineSize) TRseqGaugeShard
{
    TRseqGaugeWrite Value;
};

static_assert(sizeof(TRseqGaugeShard) == CacheLineSize);

////////////////////////////////////////////////////////////////////////////////

// Owns the inline per-CPU shard array shared by the rseq sensors below. The shards live in
// the extra space allocated alongside the object (NewWithExtraSpace); the base inherits
// TWithExtraSpace<TDerived>, locates the shards itself and exposes them through GetShards().
// CRTP on the derived sensor so the inherited Create() allocates the leaf type.
template <class TDerived, class TShard>
class TRseqCounterBase
    : public TWithExtraSpace<TDerived>
{
public:
    static TIntrusivePtr<TDerived> Create();

protected:
    const int ShardCount_;

    explicit TRseqCounterBase(int shardCount);
    ~TRseqCounterBase();

    TMutableRange<TShard> GetShards();

private:
    TShard* const Shards_;
};

////////////////////////////////////////////////////////////////////////////////

class alignas(CacheLineSize) TRseqCounter
    : public ICounter
    , public TRseqCounterBase<TRseqCounter, TRseqCounterShard>
{
public:
    void Increment(i64 delta) override;

    i64 GetValue() override;

private:
    explicit TRseqCounter(int shardCount);

    DECLARE_NEW_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

class alignas(CacheLineSize) TRseqTimeCounter
    : public ITimeCounter
    , public TRseqCounterBase<TRseqTimeCounter, TRseqTimeCounterShard>
{
public:
    void Add(TDuration delta) override;

    TDuration GetValue() override;

private:
    explicit TRseqTimeCounter(int shardCount);

    DECLARE_NEW_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

class alignas(CacheLineSize) TRseqGauge
    : public IGauge
    , public TRseqCounterBase<TRseqGauge, TRseqGaugeShard>
{
public:
    void Update(double value) override;

    double GetValue() override;

private:
    explicit TRseqGauge(int shardCount);

    DECLARE_NEW_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

#define RSEQ_SENSOR_IMPL_INL_H_
#include "rseq_sensor_impl-inl.h"
#undef RSEQ_SENSOR_IMPL_INL_H_
