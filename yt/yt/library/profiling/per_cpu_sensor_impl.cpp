#include "per_cpu_sensor_impl.h"

#include "summary.h"

#include <library/cpp/yt/system/tscp.h>

#ifdef __linux__
#include <sched.h>
#endif

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentCpuShardIndex()
{
#ifdef __linux__
    // Avoid rseq-backed CPU lookup: profiling may live in a late-loaded DSO whose
    // legacy rseq TLS offset is not process-wide.
    int cpuId = ::sched_getcpu();
    if (cpuId < 0) {
        return 0;
    }
#else
    int cpuId = TTscp::Get().ProcessorId;
#endif

    return cpuId & (TTscp::MaxProcessorId - 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TPerCpuCounter::Increment(i64 delta)
{
    int processorId = GetCurrentCpuShardIndex();
    Shards_[processorId].Value.fetch_add(delta, std::memory_order::relaxed);
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
    int processorId = GetCurrentCpuShardIndex();
    Shards_[processorId].Value.fetch_add(delta.GetValue(), std::memory_order::relaxed);
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
#ifdef __linux__
    int processorId = GetCurrentCpuShardIndex();
    TCpuInstant timestamp = GetApproximateCpuInstant();
#else
    auto tscp = TTscp::Get();
    int processorId = tscp.ProcessorId;
    TCpuInstant timestamp = tscp.Instant;
#endif

    TWrite write{value, timestamp};
#ifdef __clang__
    Shards_[processorId].Value.store(write.Pack(), std::memory_order::relaxed);
#else
    auto guard = Guard(Shards_[processorId].Lock);
    Shards_[processorId].Value = write;
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
    auto& shard = Shards_[GetCurrentCpuShardIndex()];
    auto guard = Guard(shard.Lock);
    shard.Value.Record(value);
    shard.Empty.store(false, std::memory_order::release);
}

template <class T>
TSummarySnapshot<T> TPerCpuSummary<T>::GetSummary()
{
    TSummarySnapshot<T> value;
    for (const auto& shard : Shards_) {
        if (shard.Empty.load(std::memory_order::acquire)) {
            continue;
        }
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
        if (shard.Empty.load(std::memory_order::acquire)) {
            continue;
        }
        auto guard = Guard(shard.Lock);
        value += shard.Value;
        shard.Value = {};
        shard.Empty.store(true, std::memory_order::release);
    }
    return value;
}

template class TPerCpuSummary<double>;
template class TPerCpuSummary<TDuration>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
