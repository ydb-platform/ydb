#include "rseq_sensor_impl.h"

#include <library/cpp/yt/rseq/per_cpu.h>

#include <util/system/types.h>

#include <array>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

TRseqCounter::TRseqCounter(int shardCount)
    : TRseqCounterBase(shardCount)
{ }

void TRseqCounter::Increment(i64 delta)
{
    NRseq::AddPerCpu(GetShards().Begin(), &TRseqCounterShard::Value, delta);
}

i64 TRseqCounter::GetValue()
{
    i64 total = 0;
    for (const auto& shard : GetShards()) {
        total += __atomic_load_n(&shard.Value, __ATOMIC_RELAXED);
    }
    return total;
}

////////////////////////////////////////////////////////////////////////////////

TRseqTimeCounter::TRseqTimeCounter(int shardCount)
    : TRseqCounterBase(shardCount)
{ }

void TRseqTimeCounter::Add(TDuration delta)
{
    NRseq::AddPerCpu(GetShards().Begin(), &TRseqTimeCounterShard::Value, delta.GetValue());
}

TDuration TRseqTimeCounter::GetValue()
{
    auto total = TDuration::Zero();
    for (const auto& shard : GetShards()) {
        total += TDuration::FromValue(__atomic_load_n(&shard.Value, __ATOMIC_RELAXED));
    }
    return total;
}

////////////////////////////////////////////////////////////////////////////////

TRseqGauge::TRseqGauge(int shardCount)
    : TRseqCounterBase(shardCount)
{ }

void TRseqGauge::Update(double value)
{
    NRseq::StorePerCpu(GetShards().Begin(), &TRseqGaugeShard::Value, TRseqGaugeWrite{value, GetApproximateCpuInstant()});
}

double TRseqGauge::GetValue()
{
    double lastValue = 0.0;
    TCpuInstant maxTimestamp = 0;

    for (const auto& shard : GetShards()) {
        // Read the two 8-byte halves with relaxed atomic loads, matching how the fallback
        // path stores them (StorePerCpu uses paired relaxed stores there). The rseq fast
        // path commits all 16 bytes at once, so a reader may still observe a torn
        // (value, timestamp) -- the documented last-writer-wins tradeoff a gauge tolerates.
        const auto* halves = reinterpret_cast<const ui64*>(&shard.Value);
        auto write = __builtin_bit_cast(TRseqGaugeWrite, std::array<ui64, 2>{
            __atomic_load_n(&halves[0], __ATOMIC_RELAXED),
            __atomic_load_n(&halves[1], __ATOMIC_RELAXED),
        });
        if (write.Timestamp > maxTimestamp) {
            maxTimestamp = write.Timestamp;
            lastValue = write.Value;
        }
    }

    return lastValue;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
