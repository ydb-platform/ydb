#include <yql/essentials/core/histogram/test_support/eq_height_histogram_test_api.h>
#include <yql/essentials/core/histogram/test_support/eq_height_histogram_test_helpers.h>

#include <benchmark/benchmark.h>

#include <util/generic/yexception.h>
#include <util/random/fast.h>
#include <util/random/shuffle.h>

namespace {

void UnsortedAddFlushCount(benchmark::State& state) {
    constexpr ui32 numBuckets = 10;
    constexpr ui32 numKeys = 20000;
    constexpr ui32 copiesPerKey = 1;
    constexpr ui64 shuffleSeed = 7;
    const auto params = NKikimr::NHistogramTest::MakeParams(numBuckets);
    TFastRng64 rng(shuffleSeed);
    auto keys = NKikimr::NHistogramTest::MakeSortedStream(numKeys, copiesPerKey);
    Shuffle(keys.begin(), keys.end(), rng);

    for (auto _ : state) {
        auto builder = NKikimr::NHistogramTest::BuildFromStream(keys, params);
        Y_ENSURE(builder.GetTotalCount() == numKeys, "Unexpected total count: " << builder.GetTotalCount());
        const ui64 flushCount = NKikimr::TEqHeightHistogramBuilderTestApi(builder).GetFlushCount();
        // Frequent flushes of a growing summary would make unsorted Add quadratic.
        Y_ENSURE(flushCount * params.EmissionRate < numKeys / 2,
                 "Flush called " << flushCount << " times; naive every-EmissionRate flushing is ~"
                                 << (numKeys / params.EmissionRate));
        benchmark::DoNotOptimize(builder);
    }
}

BENCHMARK(UnsortedAddFlushCount);

} // namespace
