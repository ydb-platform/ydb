#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/block_range_field.h>

#include <util/generic/vector.h>
#include <util/random/fast.h>
#include <util/string/cast.h>

#include <benchmark/benchmark.h>

#include <array>
#include <span>

using namespace NYdb::NBS::NBlockStore;

namespace {
constexpr size_t BlockCount = 32768;
constexpr size_t BitMaskSizeBytes = BlockCount / 8;
constexpr size_t RemoveRangeLength = 256;
constexpr size_t ExpandCount = 16;
constexpr size_t SetNodeSize = 8;

struct TBackendCase
{
    IBlockRangeFieldImpl::EBackend Backend;
    size_t MaxSupportedRangeCount;
};

constexpr TBackendCase BackendCases[] = {
    {.Backend = IBlockRangeFieldImpl::EBackend::StdSet,
     .MaxSupportedRangeCount = BlockCount / 2},
    {.Backend = IBlockRangeFieldImpl::EBackend::Set,
     .MaxSupportedRangeCount = BitMaskSizeBytes / SetNodeSize},
    {.Backend = IBlockRangeFieldImpl::EBackend::Bitmask,
     .MaxSupportedRangeCount = BlockCount / 2},
};

// Describes what percentage of generated ranges has the specified length.
struct TRangeSizeShare
{
    ui16 RangeLength;
    ui32 Percent;
};

constexpr std::array RangeSizeDistribution = {
    TRangeSizeShare{.RangeLength = 1, .Percent = 25},
    TRangeSizeShare{.RangeLength = 2, .Percent = 25},
    TRangeSizeShare{.RangeLength = 4, .Percent = 20},
    TRangeSizeShare{.RangeLength = 8, .Percent = 15},
    TRangeSizeShare{.RangeLength = 16, .Percent = 15},
};

void ValidateDistribution(std::span<const TRangeSizeShare> distribution)
{
    ui32 totalPercent = 0;
    for (const auto& item: distribution) {
        Y_ABORT_UNLESS(item.RangeLength > 0 && item.RangeLength <= BlockCount);
        totalPercent += item.Percent;
    }
    Y_ABORT_UNLESS(totalPercent == 100);
}

ui16 MakeRangeLength(
    TReallyFastRng32& rng,
    std::span<const TRangeSizeShare> distribution)
{
    const ui32 percentile = rng.Uniform(100);
    ui32 cumulativePercent = 0;
    for (const auto& item: distribution) {
        cumulativePercent += item.Percent;
        if (percentile < cumulativePercent) {
            return item.RangeLength;
        }
    }
    Y_ABORT("Invalid range size distribution");
}

TVector<TBlockRange16> MakeRandomRanges(
    size_t rangeCount,
    std::span<const TRangeSizeShare> distribution)
{
    ValidateDistribution(distribution);

    TReallyFastRng32 rng(42);
    TVector<TBlockRange16> ranges(Reserve(rangeCount));
    for (size_t i = 0; i < rangeCount; ++i) {
        const ui16 length = MakeRangeLength(rng, distribution);
        const ui32 maxRangeStart = BlockCount - length;
        const ui16 start = static_cast<ui16>(rng.Uniform(maxRangeStart + 1));
        ranges.push_back(TBlockRange16::WithLength(start, length));
    }
    return ranges;
}

}   // namespace

// Measures inserting a batch of reproducible random ranges. Range generation,
// field construction and destruction are excluded from the timed section.
static void BM_AddRandom(
    benchmark::State& state,
    TBlockRangeField::EBackend backend)
{
    const size_t rangeCount = state.range(0);
    const auto ranges = MakeRandomRanges(rangeCount, RangeSizeDistribution);

    size_t itemProcessed = 0;
    for (auto _: state) {
        state.PauseTiming();
        auto field = std::make_unique<TBlockRangeField>(
            CreateArenaAllocator(),
            BlockCount,
            backend);
        state.ResumeTiming();

        for (const auto& range: ranges) {
            field->Add(range);
            ++itemProcessed;
        }

        state.PauseTiming();
        field.reset();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(itemProcessed);
}

// Measures removing the random block spaces.
static void BM_RemoveRandom(
    benchmark::State& state,
    TBlockRangeField::EBackend backend)
{
    const size_t rangeCount = state.range(0);
    const auto ranges = MakeRandomRanges(rangeCount, RangeSizeDistribution);

    size_t itemProcessed = 0;
    for (auto _: state) {
        state.PauseTiming();
        auto field = std::make_unique<TBlockRangeField>(
            CreateArenaAllocator(),
            BlockCount,
            backend);
        field->Add(TBlockRange16::WithLength(0, BlockCount));
        state.ResumeTiming();

        for (const auto& range: ranges) {
            field->Remove(range);
            ++itemProcessed;
        }

        state.PauseTiming();
        field.reset();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(itemProcessed);
}

// Measures removing the whole block space in consecutive 256-block ranges.
// Random field population and destruction are excluded from the timed section.
static void BM_RemoveSeq(
    benchmark::State& state,
    TBlockRangeField::EBackend backend)
{
    const size_t rangeCount = state.range(0);
    const auto ranges = MakeRandomRanges(rangeCount, RangeSizeDistribution);

    size_t itemProcessed = 0;
    for (auto _: state) {
        state.PauseTiming();
        auto field = std::make_unique<TBlockRangeField>(
            CreateArenaAllocator(),
            BlockCount,
            backend);
        for (const auto& range: ranges) {
            field->Add(range);
            ++itemProcessed;
        }
        state.ResumeTiming();

        for (size_t i = 0; i < BlockCount; i += RemoveRangeLength) {
            field->Remove(TBlockRange16::WithLength(i, RemoveRangeLength));
            ++itemProcessed;
        }

        state.PauseTiming();
        field.reset();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(itemProcessed);
}

// Measures inserting and expanding a batch of reproducible random ranges.
//  Range generation, field construction and destruction are excluded from the
//  timed section.
static void BM_ExpandRandom(
    benchmark::State& state,
    TBlockRangeField::EBackend backend)
{
    const size_t rangeCount = state.range(0);
    const auto ranges = MakeRandomRanges(rangeCount, RangeSizeDistribution);

    size_t itemProcessed = 0;
    for (auto _: state) {
        state.PauseTiming();
        auto field = std::make_unique<TBlockRangeField>(
            CreateArenaAllocator(),
            BlockCount,
            backend);
        state.ResumeTiming();

        for (const auto& range: ranges) {
            field->Add(range);
            ++itemProcessed;
            for (size_t i = 0; i < ExpandCount; ++i) {
                auto start = range.End + i + 1;
                if (start > BlockCount - 1) {
                    continue;
                }
                field->Add(TBlockRange16::WithLength(start, 1));
                ++itemProcessed;
            }
        }

        state.PauseTiming();
        field.reset();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(itemProcessed);
}

// Measures adding odd blocks one at a time to right.
static void BM_ExpandRight(
    benchmark::State& state,
    TBlockRangeField::EBackend backend)
{
    const size_t rangeCount = state.range(0);

    size_t itemProcessed = 0;
    for (auto _: state) {
        state.PauseTiming();
        auto field = std::make_unique<TBlockRangeField>(
            CreateArenaAllocator(),
            BlockCount,
            backend);
        state.ResumeTiming();

        for (ui16 r = 0; r < rangeCount; ++r) {
            field->Add(TBlockRange16::MakeOneBlock(r * 2));
            ++itemProcessed;
        }

        state.PauseTiming();
        field.reset();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(itemProcessed);
}

// Measures adding odd blocks one at a time to left.
static void BM_ExpandLeft(
    benchmark::State& state,
    TBlockRangeField::EBackend backend)
{
    const size_t rangeCount = state.range(0);

    size_t itemProcessed = 0;
    for (auto _: state) {
        state.PauseTiming();
        auto field = std::make_unique<TBlockRangeField>(
            CreateArenaAllocator(),
            BlockCount,
            backend);
        state.ResumeTiming();

        for (ui16 r = 0; r < rangeCount; ++r) {
            field->Add(TBlockRange16::MakeOneBlock(BlockCount - r * 2 - 1));
            ++itemProcessed;
        }

        state.PauseTiming();
        field.reset();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(itemProcessed);
}

namespace {

// Adds a logarithmic sequence of Arg() values: min, min*16, min*256, ...,
// always finishing exactly at max.
benchmark::Benchmark*
AddArgRange(benchmark::Benchmark* benchmark, size_t max, size_t min = 64)
{
    Y_ABORT_UNLESS(min <= max);
    for (size_t arg = min; arg < max; arg *= 8) {
        benchmark->Arg(arg);
    }
    benchmark->Arg(max);
    return benchmark;
}

// Registers every benchmark once per backend from BackendCases. Runs
// during static initialization, before main(), just like the BENCHMARK()
// macro does.
const auto RegisteredBenchmarks = []()
{
    for (const auto& backendCase: BackendCases) {
        const TString suffix = "/" + ToString(backendCase.Backend);
        const size_t max = backendCase.MaxSupportedRangeCount;

        AddArgRange(
            benchmark::RegisterBenchmark(
                ("BM_AddRandom" + suffix).c_str(),
                BM_AddRandom,
                backendCase.Backend),
            max)
            ->Unit(benchmark::kMicrosecond);

        AddArgRange(
            benchmark::RegisterBenchmark(
                ("BM_RemoveRandom" + suffix).c_str(),
                BM_RemoveRandom,
                backendCase.Backend),
            max)
            ->Unit(benchmark::kMicrosecond);

        AddArgRange(
            benchmark::RegisterBenchmark(
                ("BM_RemoveSeq" + suffix).c_str(),
                BM_RemoveSeq,
                backendCase.Backend),
            max)
            ->Unit(benchmark::kMicrosecond);

        AddArgRange(
            benchmark::RegisterBenchmark(
                ("BM_ExpandRandom" + suffix).c_str(),
                BM_ExpandRandom,
                backendCase.Backend),
            max)
            ->Unit(benchmark::kMicrosecond);

        AddArgRange(
            benchmark::RegisterBenchmark(
                ("BM_ExpandRight" + suffix).c_str(),
                BM_ExpandRight,
                backendCase.Backend),
            max)
            ->Unit(benchmark::kMicrosecond);

        AddArgRange(
            benchmark::RegisterBenchmark(
                ("BM_ExpandLeft" + suffix).c_str(),
                BM_ExpandLeft,
                backendCase.Backend),
            max)
            ->Unit(benchmark::kMicrosecond);
    }
    return 0;
}();

}   // namespace
