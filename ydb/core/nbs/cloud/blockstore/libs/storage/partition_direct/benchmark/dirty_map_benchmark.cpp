#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <util/random/fast.h>

#include <benchmark/benchmark.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

constexpr ui32 BlockSize = 4096;
constexpr ui16 BlockCount = 32768;

template <typename TKey>
TKey Make(ui64 k);

template <>
ui64 Make(ui64 k)
{
    return k;
}

template <>
TPBufferKey Make(ui64 k)
{
    return TPBufferKey{.Generation = 0, .Lsn = k};
}

void PrintAllocatorStats(const IArenaAllocatorPtr& allocator)
{
    Cout << "allocator:" << Endl;
    size_t reservedSize = 0;
    size_t usedSize = 0;
    for (const auto& stats: allocator->GetStats()) {
        Cout << "  slotSize=" << stats.SlotSize
             << ", arenaSize=" << stats.ArenaSize
             << ", reservedSize=" << FormatByteSize(stats.ReservedSize)
             << ", usedSize=" << FormatByteSize(stats.UsedSize)
             << ", maxUsedSize=" << FormatByteSize(stats.MaxUsedSize)
             << ", count=" << stats.Count << Endl;
        reservedSize += stats.ReservedSize;
        usedSize += stats.UsedSize;
    }
    const double usage = reservedSize ? 100.0 * usedSize / reservedSize : 0.0;
    Cout << "  total: reservedSize=" << FormatByteSize(reservedSize)
         << ", usedSize=" << FormatByteSize(usedSize) << ", usage=" << usage
         << '%' << Endl;
}

void FlushAndErase(TBlocksDirtyMap& dirtyMap)
{
    for (;;) {
        auto flushHints = dirtyMap.MakeFlushHint(1);
        for (const auto& [route, hint]: flushHints.GetAllHints()) {
            dirtyMap.FlushFinished(route, MakePBufferKeys(hint.Segments), {});
        }

        auto eraseHints = dirtyMap.MakeEraseHint(1);

        for (const auto& [host, hint]: eraseHints.GetAllHints()) {
            dirtyMap.EraseFinished(host, MakePBufferKeys(hint.Segments), {});
        }
        if (flushHints.Empty() && eraseHints.Empty()) {
            break;
        }
    }
}

template <typename TKey, size_t LoadSize>
void BM_BlockRangeMapMemory(benchmark::State& state)
{
    struct TLoadSimulation
    {
        std::array<ui64, (LoadSize + sizeof(ui64) - 1) / sizeof(ui64)> Data;
    };

    const size_t rangeCount = state.range(0);

    for (auto _: state) {
        Y_UNUSED(_);

        auto allocator = CreateArenaAllocator();
        TArenaAllocatorPool arenaAllocatorPool{allocator};

        TBlockRangeMap<TKey, TLoadSimulation, TBlockRange16, true> rangeMap(
            &arenaAllocatorPool);

        for (size_t i = 0; i < rangeCount; ++i) {
            rangeMap.AddRange(
                Make<TKey>(i + 1),
                TBlockRange16::MakeOneBlock(i % BlockCount));
        }

        const size_t usedSize = arenaAllocatorPool.GetUsedSize();
        benchmark::DoNotOptimize(rangeMap.Size());

        state.counters["keySize"] = static_cast<double>(sizeof(TKey));
        state.counters["loadSize"] = static_cast<double>(LoadSize);
        state.counters["usedSize"] = static_cast<double>(usedSize);
        state.counters["usedPerInflight"] =
            static_cast<double>(usedSize) / rangeCount;
    }
}

void BM_DirtyMapInflightMemory(benchmark::State& state)
{
    const size_t dirtyMapCount = state.range(0);
    const size_t inflightCount = state.range(1);

    for (auto _: state) {
        Y_UNUSED(_);

        auto arenaAllocatorPool = CreateArenaAllocatorPool();
        auto allocator = arenaAllocatorPool->GetAllocator();
        const auto config = TVChunkConfig::MakeDefault(
            /*vChunkIndex=*/0,
            /*hostCount=*/5,
            /*primaryCount=*/3);

        TVector<TBlocksDirtyMapPtr> dirtyMaps;
        dirtyMaps.reserve(dirtyMapCount);
        for (size_t i = 0; i < dirtyMapCount; ++i) {
            dirtyMaps.push_back(std::make_shared<TBlocksDirtyMap>(
                arenaAllocatorPool,
                config,
                BlockSize,
                BlockCount));
        }

        THostMask hosts = THostMask::MakeAll(3);
        TReallyFastRng32 rng(42);
        for (size_t i = 0; i < inflightCount; ++i) {
            const auto key = TPBufferKey{.Generation = 1, .Lsn = i + 1};
            const auto range = TBlockRange16::MakeOneBlock(i % BlockCount);
            auto& dirtyMap = *dirtyMaps[rng.Uniform(dirtyMaps.size())];
            dirtyMap.RegisterInflightWrite(key, range);
            dirtyMap.WriteFinished(key, range, hosts, hosts);
        }

        state.counters["usedPerInflight"] =
            static_cast<double>(allocator->AllocatedSize()) / inflightCount;

        Cout << "after WriteFinished:" << Endl;
        PrintAllocatorStats(allocator);

        for (size_t i = 0; i < dirtyMaps.size(); ++i) {
            FlushAndErase(*dirtyMaps[i]);
        }
        Cout << "after FlushAndEraseFinished:" << Endl;
        PrintAllocatorStats(allocator);

        Cout << "after Trim:" << Endl;
        for (size_t i = 0; i < dirtyMaps.size(); ++i) {
            dirtyMaps[i]->Trim();
        }
        PrintAllocatorStats(allocator);
    }
}

}   // namespace

struct TCompactInflightInfo
{
    IReadyQueue* ReadyQueue = nullptr;
    TInstant StartAt;
    ui32 PersistGeneration = 0;
    ui32 PBuffersLockCount : 24 = 0;
    ui8 State : 8 = 0;
    ui64 Mask = 0;
};

BENCHMARK(BM_BlockRangeMapMemory<TPBufferKey, sizeof(TInflightInfo)>)
    ->Args({100'000 * 15})
    ->Iterations(1);
BENCHMARK(BM_BlockRangeMapMemory<ui64, sizeof(TInflightInfo)>)
    ->Args({100'000 * 15})
    ->Iterations(1);
BENCHMARK(BM_BlockRangeMapMemory<ui64, sizeof(TCompactInflightInfo)>)
    ->Args({100'000 * 15})
    ->Iterations(1);
BENCHMARK(BM_DirtyMapInflightMemory)->Args({10, 100'000 * 15})->Iterations(1);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
