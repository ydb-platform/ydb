// Cost of the two checksums on the interconnect hot path: CRC32c, which v1 runs over every packet, and
// XXH3, which v2 runs over every event when TInterconnectSettings::V2::ChecksumEvents is on. Both are
// measured in one-shot form and in the incremental form the sessions actually use, since the serializers
// feed them chunk by chunk rather than over one contiguous block.

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/string.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

namespace {

    // v2 splits an event into chunks bounded by the per-channel quota, so incremental checksumming happens
    // in pieces of roughly this size rather than over the whole event at once.
    constexpr size_t ChunkSize = 4096;

    TString MakeData(size_t size) {
        return TString(size, 'x');
    }

    void Crc32cOneShot(benchmark::State& state) {
        const TString data = MakeData(state.range(0));
        for (auto _ : state) {
            benchmark::DoNotOptimize(Crc32c(data.data(), data.size()));
        }
        state.SetBytesProcessed(state.iterations() * data.size());
    }

    void Crc32cIncremental(benchmark::State& state) {
        const TString data = MakeData(state.range(0));
        for (auto _ : state) {
            ui32 checksum = 0;
            for (size_t offset = 0; offset < data.size(); offset += ChunkSize) {
                checksum = Crc32cExtend(checksum, data.data() + offset, Min(ChunkSize, data.size() - offset));
            }
            benchmark::DoNotOptimize(checksum);
        }
        state.SetBytesProcessed(state.iterations() * data.size());
    }

    void Xxh3OneShot(benchmark::State& state) {
        const TString data = MakeData(state.range(0));
        for (auto _ : state) {
            benchmark::DoNotOptimize(XXH3_64bits(data.data(), data.size()));
        }
        state.SetBytesProcessed(state.iterations() * data.size());
    }

    void Xxh3Incremental(benchmark::State& state) {
        const TString data = MakeData(state.range(0));
        for (auto _ : state) {
            XXH3_state_t xxh;
            XXH3_64bits_reset(&xxh);
            for (size_t offset = 0; offset < data.size(); offset += ChunkSize) {
                XXH3_64bits_update(&xxh, data.data() + offset, Min(ChunkSize, data.size() - offset));
            }
            benchmark::DoNotOptimize(XXH3_64bits_digest(&xxh));
        }
        state.SetBytesProcessed(state.iterations() * data.size());
    }

    void ApplyArgs(benchmark::Benchmark* bench) {
        bench->ArgName("bytes")
            ->RangeMultiplier(16)
            ->Range(64, 1 << 20)
            ->Unit(benchmark::kNanosecond);
    }

    BENCHMARK(Crc32cOneShot)->Apply(ApplyArgs);
    BENCHMARK(Crc32cIncremental)->Apply(ApplyArgs);
    BENCHMARK(Xxh3OneShot)->Apply(ApplyArgs);
    BENCHMARK(Xxh3Incremental)->Apply(ApplyArgs);

} // namespace
