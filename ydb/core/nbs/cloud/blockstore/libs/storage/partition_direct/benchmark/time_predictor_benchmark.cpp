#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/time_predictor.h>

#include <util/generic/vector.h>
#include <util/random/fast.h>

#include <benchmark/benchmark.h>

using namespace NYdb::NBS::NBlockStore;
using namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect;

namespace {

constexpr size_t HistorySize = 100;
constexpr size_t NthFromEnd = 1;

// Power of two so the loop can wrap with a mask instead of a division.
constexpr size_t SampleCount = 1024;

struct TSample
{
    THostIndex Host = 0;
    TDuration Time;
};

// Request completion time: a bulk of fast requests plus a heavy tail. The
// spread is what matters here - it decides where in the multiset the inserted
// and the evicted values land.
TDuration MakeLatency(TReallyFastRng32& rng)
{
    const ui32 micros = rng.Uniform(100, 1500);
    const bool tail = rng.Uniform(100) < 2;
    return TDuration::MicroSeconds(tail ? micros * 30 : micros);
}

TVector<TSample> MakeSamples(TReallyFastRng32& rng)
{
    TVector<TSample> samples(Reserve(SampleCount));
    for (size_t i = 0; i < SampleCount; ++i) {
        samples.push_back(TSample{
            .Host = static_cast<THostIndex>(
                rng.Uniform(ui32(DirectBlockGroupHostCount))),
            .Time = MakeLatency(rng)});
    }
    return samples;
}

}   // namespace

// Add() is called on the datapath for every request completion: it inserts
// into the multiset and erases the value evicted from the ring buffer. Sample
// generation and history warmup happen before the timed loop, so only Add() is
// measured.
static void BM_TimePredictorAdd(benchmark::State& state)
{
    TReallyFastRng32 rng(42);
    const auto samples = MakeSamples(rng);

    TTimePredictor predictor(HistorySize, NthFromEnd);
    for (size_t host = 0; host < DirectBlockGroupHostCount; ++host) {
        for (size_t i = 0; i < HistorySize; ++i) {
            predictor.Add(static_cast<THostIndex>(host), MakeLatency(rng));
        }
    }

    size_t index = 0;
    auto* predictorPtr = &predictor;
    for (auto _: state) {
        const auto& sample = samples[index++ & (SampleCount - 1)];
        predictor.Add(sample.Host, sample.Time);
        benchmark::DoNotOptimize(predictorPtr);
    }
}

BENCHMARK(BM_TimePredictorAdd)->Unit(benchmark::kNanosecond);
