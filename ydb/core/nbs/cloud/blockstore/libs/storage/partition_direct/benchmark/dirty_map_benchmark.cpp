#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <benchmark/benchmark.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

constexpr size_t InflightCount = 100'000 * 10;
constexpr ui32 BlockSize = 4096;
constexpr ui16 BlockCount = 32768;

template <typename TRange>
void BM_BlockRangeMapMemory(benchmark::State& state)
{
    for (auto _: state) {
        Y_UNUSED(_);

        auto allocator = CreateArenaAllocator();
        TArenaAllocatorPool arenaAllocatorPool{allocator};

        TBlockRangeMap<ui64, TString, TRange, true> rangeMap(
            &arenaAllocatorPool);
        const size_t baseUsedSize = arenaAllocatorPool.GetUsedSize();
        const size_t baseAllocatedSize = arenaAllocatorPool.GetAllocatedSize();

        for (size_t i = 0; i < InflightCount; ++i) {
            rangeMap.AddRange(i + 1, TRange::MakeOneBlock(i % BlockCount));
        }

        const size_t usedSize = arenaAllocatorPool.GetUsedSize() - baseUsedSize;
        const size_t allocatedSize =
            arenaAllocatorPool.GetAllocatedSize() - baseAllocatedSize;
        benchmark::DoNotOptimize(rangeMap.Size());

        state.counters["usedSize"] = static_cast<double>(usedSize);
        state.counters["usedPerInflight"] =
            static_cast<double>(usedSize) / InflightCount;
        state.counters["allocatedSize"] = static_cast<double>(allocatedSize);
        state.counters["allocatedPerInflight"] =
            static_cast<double>(allocatedSize) / InflightCount;
    }
}

void BM_DirtyMapInflightMemory(benchmark::State& state)
{
    for (auto _: state) {
        Y_UNUSED(_);

        auto allocator = CreateArenaAllocator();
        const auto config = TVChunkConfig::MakeDefault(
            /*vChunkIndex=*/0,
            /*hostCount=*/5,
            /*primaryCount=*/3);

        TBlocksDirtyMap dirtyMap(allocator, config, BlockSize, BlockCount);
        const size_t baseUsedSize = dirtyMap.GetUsedSize();
        const size_t baseAllocatedSize = dirtyMap.GetAllocatedSize();

        for (size_t i = 0; i < InflightCount; ++i) {
            dirtyMap.RegisterInflightWrite(
                TPBufferKey{.Generation = 1, .Lsn = i + 1},
                TBlockRange16::MakeOneBlock(i % BlockCount));
        }

        const size_t usedSize = dirtyMap.GetUsedSize() - baseUsedSize;
        const size_t allocatedSize =
            dirtyMap.GetAllocatedSize() - baseAllocatedSize;
        benchmark::DoNotOptimize(dirtyMap.GetInflightCount());

        state.counters["usedSize"] = static_cast<double>(usedSize);
        state.counters["usedPerInflight"] =
            static_cast<double>(usedSize) / InflightCount;
        state.counters["allocatedSize"] = static_cast<double>(allocatedSize);
        state.counters["allocatedPerInflight"] =
            static_cast<double>(allocatedSize) / InflightCount;
    }
}

}   // namespace

BENCHMARK(BM_BlockRangeMapMemory<TBlockRange16>)->Iterations(1);
BENCHMARK(BM_BlockRangeMapMemory<TBlockRange32>)->Iterations(1);
BENCHMARK(BM_BlockRangeMapMemory<TBlockRange64>)->Iterations(1);
BENCHMARK(BM_DirtyMapInflightMemory)->Iterations(1);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
