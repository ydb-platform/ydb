#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <benchmark/benchmark.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

constexpr size_t InflightCount = 100'000 * 10;
constexpr ui32 BlockSize = 4096;
constexpr ui16 BlockCount = 32768;

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
        const size_t baseSize = allocator->AllocatedSize();

        for (size_t i = 0; i < InflightCount; ++i) {
            dirtyMap.RegisterInflightWrite(
                TPBufferKey{.Generation = 1, .Lsn = i + 1},
                TBlockRange64::MakeOneBlock(i % BlockCount));
        }

        const size_t inflightSize = allocator->AllocatedSize() - baseSize;
        benchmark::DoNotOptimize(dirtyMap.GetInflightCount());
        state.counters["InflightBytes"] = static_cast<double>(inflightSize);
        state.counters["BytesPerInflight"] =
            static_cast<double>(inflightSize) / InflightCount;
    }
}

}   // namespace

BENCHMARK(BM_DirtyMapInflightMemory)->Iterations(1);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
