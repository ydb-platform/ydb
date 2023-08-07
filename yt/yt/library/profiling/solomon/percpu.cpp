#include "percpu.h"
#include "yt/yt/library/profiling/summary.h"

#include <yt/yt/core/profiling/tscp.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void TPerCpuCounter::Increment(i64 delta)
{
    auto tscp = TTscp::Get();
    Shards_[tscp.ProcessorId].Value.fetch_add(delta, std::memory_order::relaxed);
}

i64 TPerCpuCounter::GetValue()
{
    i64 total = 0;
    for (const auto& shard : Shards_) {
        total += shard.Value.load();
    }
    return total;
}

////////////////////////////////////////////////////////////////////////////////

void TPerCpuTimeCounter::Add(TDuration delta)
{
    auto tscp = TTscp::Get();
    Shards_[tscp.ProcessorId].Value.fetch_add(delta.GetValue(), std::memory_order::relaxed);
}

TDuration TPerCpuTimeCounter::GetValue()
{
    TDuration total = TDuration::Zero();
    for (const auto& shard : Shards_) {
        total += TDuration::FromValue(shard.Value.load());
    }
    return total;
}

////////////////////////////////////////////////////////////////////////////////

__int128 TPerCpuGauge::TWrite::Pack()
{
    static_assert(sizeof(TWrite) == 16);

    __int128 i;
    memcpy(&i, this, 16);
    return i;
}

TPerCpuGauge::TWrite TPerCpuGauge::TWrite::Unpack(__int128 i)
{
    TWrite w;
    memcpy(&w, &i, 16);
    return w;
}

void TPerCpuGauge::Update(double value)
{
    auto tscp = TTscp::Get();

    TWrite write{value, tscp.Instant};
#ifdef __clang__
    Shards_[tscp.ProcessorId].Value.store(write.Pack(), std::memory_order::relaxed);
#else
    auto guard = Guard(Shards_[tscp.ProcessorId].Lock);
    Shards_[tscp.ProcessorId].Value = write;
#endif
}

double TPerCpuGauge::GetValue()
{
    double lastValue = 0.0;
    TCpuInstant maxTimestamp = 0;

    for (const auto& shard : Shards_) {
#ifdef __clang__
        auto write = TWrite::Unpack(shard.Value.load());
#else
        auto guard = Guard(shard.Lock);
        auto write = shard.Value;
#endif

        if (write.Timestamp > maxTimestamp) {
            maxTimestamp = write.Timestamp;
            lastValue = write.Value;
        }
    }

    return lastValue;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TPerCpuSummary<T>::Record(T value)
{
    auto tscp = TTscp::Get();
    auto guard = Guard(Shards_[tscp.ProcessorId].Lock);
    Shards_[tscp.ProcessorId].Value.Record(value);
}

template <class T>
TSummarySnapshot<T> TPerCpuSummary<T>::GetSummary()
{
    TSummarySnapshot<T> value;
    for (const auto& shard : Shards_) {
        auto guard = Guard(shard.Lock);
        value += shard.Value;
    }
    return value;
}

template <class T>
TSummarySnapshot<T> TPerCpuSummary<T>::GetSummaryAndReset()
{
    TSummarySnapshot<T> value;
    for (auto& shard : Shards_) {
        auto guard = Guard(shard.Lock);
        value += shard.Value;
        shard.Value = {};
    }
    return value;
}

template class TPerCpuSummary<double>;
template class TPerCpuSummary<TDuration>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
